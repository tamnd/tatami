package tatami

// A leaf node serves a handful of segments by opening all of them, and an Index
// does exactly that. A broker over a hundred thousand shards cannot: it cannot
// hold that many files open, and it cannot afford to run the WAND loop on every
// shard for every query. A Cluster is the broker that makes both costs scale with
// the shards that can actually contribute to the answer rather than the shards
// that exist (12-distributed-serving.md).
//
// It rests on three things built elsewhere in this milestone. A routing index
// (search.RoutingIndex) maps each term to the shards that hold it with a per-shard
// impact bound, so a query visits shards in descending bound order and stops the
// moment the next shard's bound cannot beat the current k-th best score. The same
// routing index is the global-statistics source every shard is scored with, so
// the partial top-k lists the broker merges are on one scale and the merged top-k
// is exact. A lazy LRU cache keeps only the working set of shards open, so the
// open-file cost is the cache capacity, not the shard count.
//
// The early stop is safe because a shard's bound is a true upper bound on any
// score it can produce: BM25 here disables length normalization, so a term's
// contribution is monotonic in frequency and the shard maximum frequency gives a
// real ceiling. A shard is skipped only when that ceiling is strictly below the
// current k-th best score, so no document it holds could have entered the top-k
// under any tie-break. The pruned result is therefore byte-identical to visiting
// every shard.

import (
	"sort"

	"github.com/tamnd/tatami/search"
)

// Cluster serves many search-segment files as one logical index, routing each
// query to the shards that can contribute and keeping only a bounded working set
// of segments open. It is safe for concurrent queries: the open-segment working
// set lives in a concurrent reference-counted cache (segCache) whose lock guards
// only the residency bookkeeping, never the WAND loop or a column read, and the
// segments it serves are reentrant for read. So a server can drive one Cluster
// from thousands of goroutines and they route, prune, and score in parallel
// rather than queuing behind one lock (14-serving.md).
type Cluster struct {
	paths   []string
	routing *search.RoutingIndex
	bigram  *search.BigramRouting
	cache   *segCache
	// noSeed disables cross-shard threshold sharing (scale/07, M5). It exists only
	// so a benchmark can measure the same routed query with and without the seed; in
	// production the seed is always on, since it cannot change the result, only the
	// per-shard work.
	noSeed bool
}

// ClusterOptions tunes a Cluster. CacheSize caps how many segments stay open at
// once; a query that routes to more shards than this still answers correctly,
// evicting the least recently used segment to stay within the cap.
type ClusterOptions struct {
	CacheSize int
}

// DefaultCacheSize is the open-segment cap when ClusterOptions leaves it zero. It
// is the working-set size a broker keeps resident, deliberately tiny next to the
// shard count.
const DefaultCacheSize = 64

// OpenCluster builds a routing index over every shard in paths and returns a
// broker that serves them. Building the routing index opens each file once to read
// its dictionary, then closes it, so the steady-state open-file count is the cache
// cap rather than the shard count. The shard ids the routing index assigns are the
// indices into paths, so a routed shard maps straight back to its file.
func OpenCluster(paths []string, opts ClusterOptions) (*Cluster, error) {
	b := search.NewRoutingBuilder()
	for _, p := range paths {
		seg, err := OpenSearch(p)
		if err != nil {
			return nil, err
		}
		b.AddShard(seg.Inverted())
		if err := seg.Close(); err != nil {
			return nil, err
		}
	}
	return OpenClusterWithRouting(paths, b.Build(), opts), nil
}

// OpenClusterWithRouting returns a broker over paths using an already-built
// routing index, for callers that loaded the routing sidecar instead of
// re-scanning every shard. The routing index must have been built over the same
// paths in the same order, since shard ids are path indices.
func OpenClusterWithRouting(paths []string, routing *search.RoutingIndex, opts ClusterOptions) *Cluster {
	c := &Cluster{paths: paths, routing: routing}
	c.cache = newSegCache(opts.CacheSize, func(shard int) (*SearchSegment, error) {
		return OpenSearch(paths[shard])
	})
	return c
}

// Routing exposes the routing index, for stats and for persisting the sidecar.
func (c *Cluster) Routing() *search.RoutingIndex { return c.routing }

// WithBigramRouting attaches a phrase routing sidecar and returns the cluster for
// chaining. It is the only way to enable the phrase path: with it, QueryPhrase
// narrows a phrase to the shards holding its adjacencies; without it, QueryPhrase
// falls back to the bag route. The sidecar must have been built over the same
// shards in the same order as the routing index, since both name shards by the
// same ids (07-routing-latency.md, section 4.1).
func (c *Cluster) WithBigramRouting(br *search.BigramRouting) *Cluster {
	c.bigram = br
	return c
}

// NumShards is how many shards the cluster serves.
func (c *Cluster) NumShards() int { return len(c.paths) }

// NumDocs is the live document count across every shard, from the routing index,
// without opening a segment.
func (c *Cluster) NumDocs() int { return c.routing.NumDocs() }

// Close closes every segment still open in the cache.
func (c *Cluster) Close() error { return c.cache.closeAll() }

// CacheLen reports how many segments are currently resident, for tests and
// metrics.
func (c *Cluster) CacheLen() int { return c.cache.len() }

// QueryStats reports how a routed query was answered: how many shards held a query
// term, how many the broker actually visited before the bound pruned the rest, and
// the score threshold the prune fired against. A test asserts Visited is far below
// Candidates while the results stay exact.
type QueryStats struct {
	Candidates int     // shards holding at least one query term
	Visited    int     // shards actually opened and scored
	Threshold  float32 // k-th best score when the walk stopped, zero if fewer than k hits
}

// Query is the retrieval-only path: the global top-k of (shard, dense id, score)
// across the routed shards, scored with global statistics and pruned by the bound
// walk, without fetching stored fields. It is what the scale latency benchmark
// times. The second return value reports the routing and pruning that produced it.
func (c *Cluster) Query(query string, k int) ([]ClusterHit, QueryStats, error) {
	return c.QueryWith(query, k, c.routing)
}

// QueryWith is Query with the corpus statistics supplied from outside. An
// aggregator passes fleet-wide stats so this leaf scores and prunes against the
// same IDF every other leaf uses, which is what makes the merged cross-leaf top-k
// exact (13-search-only-and-scale.md). The shard bounds are computed from the
// same stats, so the early stop stays safe.
func (c *Cluster) QueryWith(query string, k int, stats search.GlobalStats) ([]ClusterHit, QueryStats, error) {
	if k <= 0 {
		return nil, QueryStats{}, nil
	}
	terms := tokenize(query)
	bounds := c.routing.RouteWith(terms, stats)
	return c.runQuery(terms, k, stats, bounds)
}

// QueryPhrase is Query for a phrase: it narrows the candidate shards to those that
// hold one of the phrase's adjacencies before scoring, instead of the union of the
// shards holding any of its words. A common-word phrase unions into nearly every
// shard as a bag of words, but its adjacencies are rare, so the phrase route is a
// fraction of the bag route (07-routing-latency.md, section 4.1). The narrowing is
// exact at the shard level: a shard absent from the phrase route provably holds no
// document with the adjacency, so it could not hold a phrase match. When no bigram
// sidecar is attached, or an adjacency is untracked, or the query is a single word,
// it falls back to the exact bag route so the answer is never wrong, only wider.
//
// Scoring within a routed shard is still the bag-of-words WAND, because the
// inverted index carries frequencies, not positions, so a routed shard's hits are
// its best word-scored documents, biased to the shards where the phrase occurs.
// Document-level phrase filtering waits on a positional index; this lever is the
// routing half, which is where the fan-out cost lives.
func (c *Cluster) QueryPhrase(query string, k int) ([]ClusterHit, QueryStats, error) {
	return c.QueryPhraseWith(query, k, c.routing)
}

// QueryPhraseWith is QueryPhrase with corpus statistics supplied from outside, the
// aggregator path, mirroring QueryWith.
func (c *Cluster) QueryPhraseWith(query string, k int, stats search.GlobalStats) ([]ClusterHit, QueryStats, error) {
	if k <= 0 {
		return nil, QueryStats{}, nil
	}
	terms := tokenize(query)
	if bounds, ok := c.phraseBounds(terms, stats); ok {
		return c.runQuery(terms, k, stats, bounds)
	}
	return c.QueryWith(query, k, stats)
}

// phraseBounds returns the phrase route for terms and whether the phrase path
// applies. It applies only when a bigram sidecar is attached and every adjacency in
// the phrase is tracked, so the route is exact; otherwise the caller falls back to
// the bag route. A tracked phrase that occurs in no shard returns an empty route
// with ok true, which is the correct exact answer of no candidates, not a reason to
// fall back.
func (c *Cluster) phraseBounds(terms []string, stats search.GlobalStats) ([]search.ShardBound, bool) {
	if c.bigram == nil {
		return nil, false
	}
	bounds, covered := c.bigram.RoutePhrase(terms, stats)
	if !covered {
		return nil, false
	}
	return bounds, true
}

// runQuery scores a fixed candidate set: it walks the shards in descending bound
// order, seeds each shard's WAND with the running global k-th once the heap is full
// (the M5 threshold sharing), stops when the next bound cannot beat the threshold,
// and merges the per-shard hits into the global top-k. Both QueryWith and
// QueryPhrase reach it, differing only in how they routed the candidate set.
func (c *Cluster) runQuery(terms []string, k int, stats search.GlobalStats, bounds []search.ShardBound) ([]ClusterHit, QueryStats, error) {
	qstats := QueryStats{Candidates: len(bounds)}

	var cands []ClusterHit
	th := newMinHeap(k)
	for _, sb := range bounds {
		if th.full() && sb.Bound < th.min() {
			break
		}
		// Cross-shard threshold sharing (scale/07, M5): once the broker has filled
		// the global top-k, its running k-th best is a true lower bound on the final
		// global k-th, so it seeds this shard's WAND. The shard then prunes documents
		// that cannot beat the global threshold before it scores them, doing less work
		// for the same merged result. Zero until the heap is full, which is the
		// unseeded path for the first shards.
		var seed search.Score
		if th.full() && !c.noSeed {
			seed = th.min()
		}
		h, err := c.cache.acquire(int(sb.Shard))
		if err != nil {
			return nil, qstats, err
		}
		qstats.Visited++
		for _, hit := range h.seg.SearchTermsSeeded(terms, k, stats, seed) {
			cands = append(cands, ClusterHit{Shard: int(sb.Shard), Doc: uint32(hit.Doc), Score: float32(hit.Score)})
			th.push(hit.Score)
		}
		c.cache.release(h)
	}
	if th.full() {
		qstats.Threshold = float32(th.min())
	}

	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Score != cands[j].Score {
			return cands[i].Score > cands[j].Score
		}
		if cands[i].Shard != cands[j].Shard {
			return cands[i].Shard < cands[j].Shard
		}
		return cands[i].Doc < cands[j].Doc
	})
	if len(cands) > k {
		cands = cands[:k]
	}
	return cands, qstats, nil
}

// Search runs the routed, pruned retrieval and then fetches the url and title of
// each surviving hit, deduplicating a page that more than one shard carries after
// a recrawl by its stable doc_id and keeping the highest-scoring copy. Like the
// leaf Index it over-fetches per shard so duplicates collapsing cannot leave fewer
// than k distinct results, and it prunes against the k-th best distinct score.
func (c *Cluster) Search(query string, k int) ([]SearchResult, QueryStats, error) {
	return c.SearchWith(query, k, c.routing)
}

// SearchWith is Search with the corpus statistics supplied from outside, the path
// an aggregator drives so every leaf scores against fleet-wide IDF and the merged
// result is the exact fleet top-k. The dedup, over-fetch, and pruning are
// identical to Search; only the statistics differ.
func (c *Cluster) SearchWith(query string, k int, stats search.GlobalStats) ([]SearchResult, QueryStats, error) {
	if k <= 0 {
		return nil, QueryStats{}, nil
	}
	terms := tokenize(query)
	bounds := c.routing.RouteWith(terms, stats)
	qstats := QueryStats{Candidates: len(bounds)}
	// Over-fetch per shard so duplicates collapsing cannot leave fewer than k
	// distinct results, and so a tie at the k-th score keeps the doc_id-smaller
	// copy. This is unconditional rather than gated on the shard count: when this
	// cluster is a leaf under an aggregator, a leaf holding a single candidate
	// shard must still surface the tie candidates the fleet merge ranks against,
	// exactly as a single broker over every shard would (13-search-only-and-scale.md).
	perShard := k * 2

	best := make(map[string]clusterCand)
	for _, sb := range bounds {
		var seed search.Score
		if th, ok := kthDistinct(best, k); ok {
			if sb.Bound < th {
				break
			}
			// Seed this shard with the running k-th best distinct score (scale/07, M5).
			// The over-fetch and dedup are unchanged: the seed only floors the per-shard
			// WAND at a score the global answer already clears, so a dropped duplicate
			// cannot uncover a needed result that scored below it.
			if !c.noSeed {
				seed = search.Score(th)
			}
		}
		h, err := c.cache.acquire(int(sb.Shard))
		if err != nil {
			return nil, qstats, err
		}
		qstats.Visited++
		for _, hit := range h.seg.SearchTermsSeeded(terms, perShard, stats, seed) {
			id, err := h.seg.globalDocID(uint32(hit.Doc))
			if err != nil {
				c.cache.release(h)
				return nil, qstats, err
			}
			cand := clusterCand{score: float32(hit.Score), shard: int(sb.Shard), dense: uint32(hit.Doc), id: id}
			if cur, ok := best[id]; !ok || cand.better(cur) {
				best[id] = cand
			}
		}
		c.cache.release(h)
	}
	if th, ok := kthDistinct(best, k); ok {
		qstats.Threshold = float32(th)
	}

	ranked := make([]clusterCand, 0, len(best))
	for _, c := range best {
		ranked = append(ranked, c)
	}
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].better(ranked[j]) })
	if len(ranked) > k {
		ranked = ranked[:k]
	}

	out := make([]SearchResult, 0, len(ranked))
	for _, rc := range ranked {
		h, err := c.cache.acquire(rc.shard)
		if err != nil {
			return nil, qstats, err
		}
		f, err := h.seg.storedFields(rc.dense)
		c.cache.release(h)
		if err != nil {
			return nil, qstats, err
		}
		out = append(out, SearchResult{Doc: rc.dense, DocID: rc.id, URL: f.url, Title: f.title, Snippet: f.snippet, Score: rc.score})
	}
	return out, qstats, nil
}

// ClusterHit is a scored hit tagged with the shard that produced it.
type ClusterHit struct {
	Shard int
	Doc   uint32
	Score float32
}

// clusterCand is one candidate on its way through the dedup merge.
type clusterCand struct {
	score float32
	shard int
	dense uint32
	id    string
}

// better orders candidates by score descending, then by stable doc_id ascending,
// the same total order the leaf Index merge uses so the two agree exactly.
func (a clusterCand) better(b clusterCand) bool {
	if a.score != b.score {
		return a.score > b.score
	}
	return a.id < b.id
}

// kthDistinct returns the k-th best distinct score among the deduped candidates so
// far, the threshold the bound walk prunes against, and whether at least k distinct
// candidates exist yet.
func kthDistinct(best map[string]clusterCand, k int) (search.Score, bool) {
	if len(best) < k {
		return 0, false
	}
	scores := make([]float32, 0, len(best))
	for _, c := range best {
		scores = append(scores, c.score)
	}
	sort.Slice(scores, func(i, j int) bool { return scores[i] > scores[j] })
	return search.Score(scores[k-1]), true
}

// minHeap is a bounded min-heap of scores that tracks the k-th best score seen, so
// the retrieval path knows its pruning threshold without re-sorting all candidates.
// It keeps the k largest scores; once full, its minimum is the k-th best.
type minHeap struct {
	data []search.Score
	cap  int
}

func newMinHeap(k int) *minHeap { return &minHeap{cap: k} }

func (h *minHeap) full() bool { return len(h.data) >= h.cap }

func (h *minHeap) min() search.Score { return h.data[0] }

func (h *minHeap) push(v search.Score) {
	if h.cap <= 0 {
		return
	}
	if len(h.data) < h.cap {
		h.data = append(h.data, v)
		h.up(len(h.data) - 1)
		return
	}
	if v <= h.data[0] {
		return
	}
	h.data[0] = v
	h.down(0)
}

func (h *minHeap) up(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if h.data[p] <= h.data[i] {
			break
		}
		h.data[p], h.data[i] = h.data[i], h.data[p]
		i = p
	}
}

func (h *minHeap) down(i int) {
	n := len(h.data)
	for {
		l, r, small := 2*i+1, 2*i+2, i
		if l < n && h.data[l] < h.data[small] {
			small = l
		}
		if r < n && h.data[r] < h.data[small] {
			small = r
		}
		if small == i {
			break
		}
		h.data[small], h.data[i] = h.data[i], h.data[small]
		i = small
	}
}
