package tatami

// A single Cluster broker serves the shards one process can route over and keep a
// working set of open. The whole crawl is far larger than that: a hundred thousand
// shards is hundreds of leaves' worth, and no one process holds the routing for
// all of them or opens their files. An Aggregator is the next tier up, the tree of
// brokers the M8 note described and did not build: it fans a query out to many
// leaf Clusters at once, has each leaf route and prune over its own shards, and
// merges the leaves' partial results into one fleet-wide top-k
// (13-search-only-and-scale.md).
//
// Two things make the merge exact rather than a best-effort blend. First, every
// leaf scores against the same fleet-wide statistics: an Aggregator sums the
// per-leaf document counts and per-term document frequencies into one GlobalStats
// and passes it down, so a term's IDF is identical on every leaf and the partial
// top-k lists are on one scale. Second, each leaf's routing bound is computed from
// those same fleet-wide statistics, so a leaf's early stop stays a true upper
// bound at fleet scale and prunes without dropping a result. The merged top-k is
// therefore byte-identical to a single broker over every shard, which a test
// checks directly.
//
// The fan-out is concurrent: each leaf Cluster serializes its own queries behind
// its own mutex, but different leaves are independent objects, so a fleet of L
// leaves answers in the time of the slowest single leaf plus a small merge, not
// the sum. That is the property that keeps the latency budget flat as the shard
// count grows: more shards means more leaves, and leaves run in parallel.

import (
	"sort"
	"sync"

	"github.com/tamnd/tatami/search"
)

// Aggregator fans a query out to a set of leaf Clusters and merges their results
// into a fleet-wide top-k. It holds the fleet statistics every leaf is scored
// against. It is safe for concurrent queries to the extent its leaves are: each
// leaf serializes internally, and a query touches each leaf once.
type Aggregator struct {
	leaves []*Cluster
	stats  *fleetStats
}

// fleetStats sums the corpus statistics across every leaf's routing index, so a
// query scores against the whole fleet's document count and per-term document
// frequency rather than any one leaf's. It satisfies search.GlobalStats.
type fleetStats struct {
	leaves []*search.RoutingIndex
	n      int
}

func newFleetStats(leaves []*Cluster) *fleetStats {
	fs := &fleetStats{}
	for _, l := range leaves {
		ri := l.Routing()
		fs.leaves = append(fs.leaves, ri)
		fs.n += ri.NumDocs()
	}
	return fs
}

// NumDocs is the fleet-wide live document count, summed across leaves.
func (f *fleetStats) NumDocs() int { return f.n }

// DocFreq is a term's document frequency summed across every leaf.
func (f *fleetStats) DocFreq(term string) int {
	df := 0
	for _, ri := range f.leaves {
		df += ri.DocFreq(term)
	}
	return df
}

// OpenAggregator returns an aggregator over the given leaf clusters, precomputing
// the fleet statistics. The leaves keep their own routing and open-segment caches;
// the aggregator adds only the fan-out and the merge.
func OpenAggregator(leaves []*Cluster) *Aggregator {
	return &Aggregator{leaves: leaves, stats: newFleetStats(leaves)}
}

// Stats exposes the fleet statistics, for callers that want to score or route with
// the same corpus-wide IDF the aggregator uses.
func (a *Aggregator) Stats() search.GlobalStats { return a.stats }

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
func (a *Aggregator) NumDocs() int { return a.stats.NumDocs() }

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

// AggStats reports how a fan-out query was answered: how many leaves it touched,
// and the totals of the per-leaf routing and pruning. ShardsVisited well below
// Candidates is the pruning that keeps the budget; Candidates well below the fleet
// shard count is the routing that keeps it.
type AggStats struct {
	Leaves        int // leaves queried
	Candidates    int // candidate shards across all leaves
	ShardsVisited int // shards actually opened and scored across all leaves
}

// Search fans the query out to every leaf concurrently, each leaf routing, pruning,
// and scoring its own shards against the fleet statistics, then merges the leaves'
// partial top-k lists into one fleet-wide top-k. It dedups a page surfaced by more
// than one leaf after a recrawl by its stable doc_id and keeps the highest-scoring
// copy, the same discipline the leaf broker uses within itself. The total order is
// score descending then doc_id ascending, identical to a single broker over every
// shard, so the result is byte-identical to that broker.
func (a *Aggregator) Search(query string, k int) ([]SearchResult, AggStats, error) {
	if k <= 0 {
		return nil, AggStats{}, nil
	}
	type leafOut struct {
		res   []SearchResult
		stats QueryStats
		err   error
	}
	outs := make([]leafOut, len(a.leaves))
	var wg sync.WaitGroup
	for i := range a.leaves {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, st, err := a.leaves[i].SearchWith(query, k, a.stats)
			outs[i] = leafOut{res: res, stats: st, err: err}
		}(i)
	}
	wg.Wait()

	agg := AggStats{Leaves: len(a.leaves)}
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
