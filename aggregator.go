package tatami

// A single Cluster broker serves the shards one process can route over and keep a
// working set of open. The whole crawl is far larger than that: a hundred thousand
// shards is hundreds of leaves' worth, and no one process holds the routing for
// all of them or opens their files. An Aggregator is the next tier up, the tree of
// brokers the M8 note described: it routes a query to the leaf Clusters that can
// contribute, fans out to those, has each leaf route and prune over its own shards,
// and merges the leaves' partial results into one fleet-wide top-k (scale/11 lever
// four).
//
// The whole tier reuses the machinery a Cluster already is, because a leaf
// summarizes as a shard. A search.RoutingIndex satisfies search.RoutingSource
// (LiveDocs and EachTerm), so folding each leaf's own routing index into one more
// RoutingBuilder yields a routing index whose shards are leaves: its per-(term,
// leaf) posting carries the leaf's fleet-frequency contribution and the ceiling
// frequency any document in the leaf can reach. That one structure, the box-level
// summary, is two things at once. It is what the aggregator routes on, one entry per
// (term, leaf) rather than per (term, shard), so a fleet of leaves is a small
// summary. And it is the fleet-wide GlobalStats every leaf is scored with, because
// its NumDocs is the fleet total and its DocFreq sums a term's frequency across
// every leaf, so a term's IDF is identical on every leaf and the partial top-k lists
// are on one scale.
//
// Two things make the merge exact rather than a best-effort blend. First, every leaf
// scores against that one fleet-wide GlobalStats, so the partial lists are
// comparable. Second, a leaf the summary does not route to holds no query term, so
// it can contribute no document to the top-k, and skipping it changes nothing; a
// leaf the summary does route to is scored in full against fleet statistics, so its
// own early stop stays a true upper bound at fleet scale. The merged top-k is
// therefore byte-identical to a single broker over every shard, which a test checks
// directly.
//
// The fan-out is concurrent: each leaf Cluster serializes its own queries behind its
// own mutex, but different leaves are independent objects, so a fleet of L leaves
// answers in the time of the slowest routed leaf plus a small merge, not the sum.
// That is the property that keeps the latency budget flat as the shard count grows:
// more shards means more leaves, and the routed leaves run in parallel. Routing on
// the summary is what keeps the fan-out to the leaves that can answer rather than
// every leaf that exists, which is the cross-box analogue of a Cluster routing to
// its shards.

import (
	"sort"
	"sync"

	"github.com/tamnd/tatami/search"
)

// Aggregator routes a query to the leaf Clusters that can contribute, fans out to
// them, and merges their results into a fleet-wide top-k. It holds the box-level
// routing summary, which doubles as the fleet statistics every leaf is scored
// against. It is safe for concurrent queries to the extent its leaves are: each leaf
// serializes internally, and a query touches each routed leaf once.
type Aggregator struct {
	leaves  []*Cluster
	summary *search.RoutingIndex // routing index whose shards are leaves; also the fleet GlobalStats
}

// OpenAggregator returns an aggregator over the given leaf clusters, building the
// box-level summary by folding each leaf's routing index in as one shard. The leaf's
// id in the summary is its index in the slice, so a routed leaf maps straight back to
// its Cluster. The leaves keep their own routing and open-segment caches; the
// aggregator adds only the summary, the routed fan-out, and the merge.
func OpenAggregator(leaves []*Cluster) *Aggregator {
	srcs := make([]search.RoutingSource, len(leaves))
	for i, l := range leaves {
		srcs[i] = l.Routing()
	}
	return &Aggregator{leaves: leaves, summary: search.BuildRouting(srcs)}
}

// Stats exposes the fleet statistics, for callers that want to score or route with
// the same corpus-wide IDF the aggregator uses. It is the box-level summary, which
// satisfies search.GlobalStats.
func (a *Aggregator) Stats() search.GlobalStats { return a.summary }

// Summary exposes the box-level routing summary, for stats and for persisting it as
// a sidecar the way a Cluster persists its own routing.
func (a *Aggregator) Summary() *search.RoutingIndex { return a.summary }

// NumLeaves is how many leaf clusters the aggregator fans out to.
func (a *Aggregator) NumLeaves() int { return len(a.leaves) }

// NumShards is the total shard count across every leaf.
func (a *Aggregator) NumShards() int {
	n := 0
	for _, l := range a.leaves {
		n += l.NumShards()
	}
	return n
}

// NumDocs is the fleet-wide live document count.
func (a *Aggregator) NumDocs() int { return a.summary.NumDocs() }

// Close closes every leaf cluster.
func (a *Aggregator) Close() error {
	var first error
	for _, l := range a.leaves {
		if err := l.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// AggStats reports how a routed fan-out query was answered: how many leaves the
// fleet holds, how many the summary routed to, and the totals of the per-leaf
// routing and pruning across the routed leaves. LeavesVisited well below Leaves is
// the box-level routing that skips leaves holding no query term; ShardsVisited well
// below Candidates is the per-leaf pruning; Candidates well below the fleet shard
// count is the per-leaf routing. Every layer keeps the answer exact.
type AggStats struct {
	Leaves        int // leaves in the fleet
	LeavesVisited int // leaves the summary routed to and fanned out to
	Candidates    int // candidate shards across the routed leaves
	ShardsVisited int // shards actually opened and scored across the routed leaves
}

// Search routes the query on the box-level summary to the leaves that can hold it,
// fans out to those leaves concurrently, each leaf routing, pruning, and scoring its
// own shards against the fleet statistics, then merges the leaves' partial top-k
// lists into one fleet-wide top-k. It dedups a page surfaced by more than one leaf
// after a recrawl by its stable doc_id and keeps the highest-scoring copy, the same
// discipline the leaf broker uses within itself. The total order is score descending
// then doc_id ascending, identical to a single broker over every shard, and a leaf
// the summary skips holds no query term so it could contribute nothing, so the
// result is byte-identical to that broker.
func (a *Aggregator) Search(query string, k int) ([]SearchResult, AggStats, error) {
	if k <= 0 {
		return nil, AggStats{}, nil
	}
	terms := tokenize(query)
	route := a.summary.RouteWith(terms, a.summary)
	agg := AggStats{Leaves: len(a.leaves), LeavesVisited: len(route)}
	if len(route) == 0 {
		return nil, agg, nil
	}

	type leafOut struct {
		res   []SearchResult
		stats QueryStats
		err   error
	}
	outs := make([]leafOut, len(route))
	var wg sync.WaitGroup
	for i := range route {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			leaf := a.leaves[route[i].Shard]
			res, st, err := leaf.SearchWith(query, k, a.summary)
			outs[i] = leafOut{res: res, stats: st, err: err}
		}(i)
	}
	wg.Wait()

	lists := make([][]SearchResult, 0, len(outs))
	for _, o := range outs {
		if o.err != nil {
			return nil, agg, o.err
		}
		agg.Candidates += o.stats.Candidates
		agg.ShardsVisited += o.stats.Visited
		lists = append(lists, o.res)
	}
	return mergeLeafResults(lists, k), agg, nil
}

// mergeLeafResults folds the leaves' partial top-k lists into one fleet-wide top-k:
// dedup a page surfaced by more than one leaf after a recrawl by its stable doc_id,
// keep the highest-scoring copy, and order by score descending then doc_id
// ascending. This is the only fleet-size-dependent step on the query path, so the
// scale projection times it directly to show the root merge stays cheap as the leaf
// count grows (13-search-only-and-scale.md).
func mergeLeafResults(lists [][]SearchResult, k int) []SearchResult {
	best := make(map[string]SearchResult)
	for _, list := range lists {
		for _, r := range list {
			if cur, ok := best[r.DocID]; !ok || betterResult(r, cur) {
				best[r.DocID] = r
			}
		}
	}
	ranked := make([]SearchResult, 0, len(best))
	for _, r := range best {
		ranked = append(ranked, r)
	}
	sort.Slice(ranked, func(i, j int) bool { return betterResult(ranked[i], ranked[j]) })
	if len(ranked) > k {
		ranked = ranked[:k]
	}
	return ranked
}

// betterResult is the total order the fleet merge imposes: higher score first,
// then lower doc_id, the same order the leaf broker's clusterCand.better uses so
// the aggregator and a single broker agree exactly.
func betterResult(a, b SearchResult) bool {
	if a.Score != b.Score {
		return a.Score > b.Score
	}
	return a.DocID < b.DocID
}
