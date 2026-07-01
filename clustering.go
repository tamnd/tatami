package tatami

// Content clustering is lever two of the partitioning redesign (Spec 2066,
// scale/12). Coarsening (lever one) bounds the shard count so the fan-out stays
// parallel, but the documents in each coarse shard are an arbitrary bag from
// whatever crawl files fell in the bucket, so every shard has a middling impact
// bound for almost every term and almost every shard clears the routing prune.
// Clustering gives each shard a topic: a topically coherent shard has a tight
// bound for terms outside its topic, so the descending-bound walk prunes it before
// scoring it, and a topical query's fan-out drops from nearly every shard to the
// handful whose topic it belongs to.
//
// The result does not change. The broker skips a shard only when its bound, a true
// upper bound on any score the shard can produce, is below the current k-th best,
// so a clustered corpus returns the same top-k as a crawl-order one, hit for hit
// and score for score. Clustering only changes how many shards clear that test.
// This is the line between tatami routing and approximate nearest-neighbor search:
// an IVF index skips on distance and loses documents in skipped clusters, while
// tatami skips on a proven bound and loses nothing.

import (
	"math"
	"runtime"
	"sort"
	"sync"
)

// ClusterPlanOptions tunes a content clustering. Shards is the number of clusters,
// which is the coarsening target (scale/11 lever one), a small multiple of the
// serving box's cores. Dims is the feature-hash width: a document's term set hashes
// into this many buckets, a topic sketch that fits in cache and collides rarely
// enough that disjoint topics land far apart. Iters is the k-means pass count over
// the sample. Slack is the per-shard capacity headroom over the mean, so a shard's
// cap is ceil(N/Shards) times (1+Slack); a document whose nearest centroid is full
// spills to its next-nearest with room, which keeps every shard under the cap.
type ClusterPlanOptions struct {
	Shards int
	Dims   int
	Iters  int
	Seed   int
	Slack  float64
}

func (o ClusterPlanOptions) withDefaults() ClusterPlanOptions {
	if o.Shards < 1 {
		o.Shards = 1
	}
	if o.Dims < 1 {
		o.Dims = 512
	}
	if o.Iters < 1 {
		o.Iters = 10
	}
	if o.Slack <= 0 {
		o.Slack = 0.15
	}
	return o
}

// ContentClusterer assigns a document to a shard by its content. It is built once
// from a sample of the corpus (FitClusterer), told the corpus size (SetCapacity),
// then called per document (Assign) during the ingest pass that writes the shards.
// It is not safe for concurrent Assign: the capacity fill counts are mutated in
// place, so the assigning pass is single-threaded, which is fine because it is
// cheap next to the per-shard tokenization it feeds.
type ContentClusterer struct {
	centroids [][]float32 // Shards x Dims, each L2-normalized
	dims      int
	fill      []int // per-shard document count during assignment
	cap       int   // per-shard capacity, zero until SetCapacity
}

// featureVector reduces a document's token list to a fixed-width L2-normalized
// vector by feature hashing: each distinct token hashes to one of dims buckets and
// adds its frequency there, then the vector is scaled to unit length so long and
// short documents compare on direction, not magnitude. It is deterministic, which
// is what makes a rebuild of the same corpus yield the same clustering.
func featureVector(tokens []string, dims int) []float32 {
	v := make([]float32, dims)
	d := uint32(dims)
	for _, t := range tokens {
		// FNV-1a inlined over the token's bytes: the same hash hash/fnv.New32a computes,
		// without allocating a hasher (and a []byte copy of the string) per token, which
		// on a corpus of long web documents is tens of millions of allocations the fit
		// otherwise pays before it does any arithmetic.
		h := uint32(2166136261)
		for i := 0; i < len(t); i++ {
			h ^= uint32(t[i])
			h *= 16777619
		}
		v[h%d]++
	}
	normalize(v)
	return v
}

// normalize scales v to unit L2 length in place, leaving an all-zero vector
// unchanged so an empty document does not divide by zero.
func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

// dot is the cosine similarity of two L2-normalized vectors, so the nearest
// centroid by Euclidean distance is the one with the largest dot.
func dot(a, b []float32) float32 {
	var s float32
	for i := range a {
		s += a[i] * b[i]
	}
	return s
}

// parallelChunks splits [0,n) into up to GOMAXPROCS contiguous ranges and runs fn on
// each concurrently, waiting for all to finish. Every index is covered by exactly one
// range, so a callback that writes per-index state needs no synchronization. It is the
// fit's parallelism: the centroid scans over the sample are the fit's cost and each
// vector is independent.
func parallelChunks(n int, fn func(lo, hi int)) {
	if n == 0 {
		return
	}
	workers := min(runtime.GOMAXPROCS(0), n)
	if workers <= 1 {
		fn(0, n)
		return
	}
	chunk := (n + workers - 1) / workers
	var wg sync.WaitGroup
	for w := range workers {
		lo := w * chunk
		if lo >= n {
			break
		}
		hi := min(lo+chunk, n)
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			fn(lo, hi)
		}(lo, hi)
	}
	wg.Wait()
}

// FitClusterer learns the cluster centroids from a sample of documents by spherical
// k-means over their feature vectors. The sample bounds the fit cost: a few tens of
// thousands of documents pin the topic structure and more only refines it. The
// centroids are initialized by a deterministic strided pick over the sample and an
// empty cluster is reseeded to the sample point farthest from its own centroid, so
// the fit takes no randomness and is reproducible for the same input.
func FitClusterer(sample [][]string, opts ClusterPlanOptions) *ContentClusterer {
	opts = opts.withDefaults()
	k, dims := opts.Shards, opts.Dims

	vecs := make([][]float32, len(sample))
	parallelChunks(len(sample), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			vecs[i] = featureVector(sample[i], dims)
		}
	})

	c := &ContentClusterer{dims: dims}
	if len(vecs) == 0 {
		// No sample to learn from: every centroid is the zero vector, so Assign is a
		// pure round-robin fill, still balanced and still exact, just not topical.
		c.centroids = make([][]float32, k)
		for j := range c.centroids {
			c.centroids[j] = make([]float32, dims)
		}
		return c
	}

	// Deterministic farthest-first init: seed the first centroid from a fixed sample
	// point, then repeatedly add the sample point least similar to every centroid
	// chosen so far. This spreads the seeds across the topics whatever order the
	// sample is in, where a strided pick can land on duplicate topics when the
	// stride shares a factor with a round-robin corpus (scale/12, section 3).
	start := 0
	if opts.Seed > 0 {
		start = opts.Seed % len(vecs)
	}
	c.centroids = make([][]float32, 0, k)
	c.centroids = append(c.centroids, append([]float32(nil), vecs[start]...))
	// Track each sample's similarity to its nearest chosen centroid and update it as
	// each new centroid is added, so the farthest-first pick is an argmin over this
	// slice rather than a rescan against every centroid chosen so far. Maintaining the
	// running nearest makes the init O(K*n*d) instead of O(K^2*n*d): nearest[i] is
	// exactly the max dot a from-scratch rescan would recompute.
	nearest := make([]float32, len(vecs))
	first := c.centroids[0]
	parallelChunks(len(vecs), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			nearest[i] = dot(vecs[i], first)
		}
	})
	for len(c.centroids) < k {
		worst, worstDot := 0, float32(2)
		for i, s := range nearest {
			if s < worstDot {
				worst, worstDot = i, s
			}
		}
		nc := append([]float32(nil), vecs[worst]...)
		c.centroids = append(c.centroids, nc)
		parallelChunks(len(vecs), func(lo, hi int) {
			for i := lo; i < hi; i++ {
				if d := dot(vecs[i], nc); d > nearest[i] {
					nearest[i] = d
				}
			}
		})
	}

	assign := make([]int, len(vecs))
	bestDots := make([]float32, len(vecs))
	for it := 0; it < opts.Iters; it++ {
		// The per-vector nearest-centroid scan is the fit's cost and every vector is
		// independent, so it parallelizes over disjoint ranges: each worker writes only
		// its own assign entries, and the change flag folds in under a lock. Each vector's
		// similarity to its chosen centroid is kept in bestDots so an empty cluster can be
		// reseeded from the worst-fitting vectors without a fresh O(n*k*d) rescan.
		changed := false
		var chMu sync.Mutex
		parallelChunks(len(vecs), func(lo, hi int) {
			local := false
			for i := lo; i < hi; i++ {
				v := vecs[i]
				best, bestDot := 0, float32(-2)
				for j := range c.centroids {
					if d := dot(v, c.centroids[j]); d > bestDot {
						best, bestDot = j, d
					}
				}
				bestDots[i] = bestDot
				if assign[i] != best {
					assign[i] = best
					local = true
				}
			}
			if local {
				chMu.Lock()
				changed = true
				chMu.Unlock()
			}
		})
		// Recompute each centroid as the normalized mean of its members.
		sums := make([][]float32, k)
		counts := make([]int, k)
		for j := range sums {
			sums[j] = make([]float32, dims)
		}
		for i, v := range vecs {
			j := assign[i]
			counts[j]++
			for d := range v {
				sums[j][d] += v[d]
			}
		}
		// Reseed empty clusters from the worst-fitting vectors so a dead cluster does
		// not stay dead. bestDots already ranks every vector by how well it fits its
		// nearest centroid, so the worst fits are an order over that slice, taken once
		// and distinct, which is O(n log n) only in the rare iteration that empties a
		// cluster rather than O(n*k*d) per empty cluster.
		var worstOrder []int
		nextWorst := 0
		for j := range c.centroids {
			if counts[j] == 0 {
				if worstOrder == nil {
					worstOrder = make([]int, len(vecs))
					for i := range worstOrder {
						worstOrder[i] = i
					}
					sort.Slice(worstOrder, func(a, b int) bool {
						return bestDots[worstOrder[a]] < bestDots[worstOrder[b]]
					})
				}
				c.centroids[j] = append([]float32(nil), vecs[worstOrder[nextWorst]]...)
				nextWorst++
				continue
			}
			normalize(sums[j])
			c.centroids[j] = sums[j]
		}
		if !changed && it > 0 {
			break
		}
	}
	return c
}

// SetCapacity fixes the per-shard capacity from the corpus size and resets the fill
// counts, so the assigning pass keeps every shard within ceil(N/Shards)*(1+Slack)
// documents. It must be called after FitClusterer and before Assign.
func (c *ContentClusterer) SetCapacity(totalDocs int, slack float64) {
	k := max(len(c.centroids), 1)
	mean := (totalDocs + k - 1) / k
	c.cap = max(int(math.Ceil(float64(mean)*(1+slack))), 1)
	c.fill = make([]int, len(c.centroids))
}

// Shards is the number of clusters the plan assigns to.
func (c *ContentClusterer) Shards() int { return len(c.centroids) }

// Vector returns the feature-hash topic sketch for a token list, the vector Assign
// scores against the centroids. Exposing it lets a parallel ingest compute the vectors
// off the assignment path (the hashing is the per-document cost) and feed them to
// AssignVec, which keeps only the fill-count mutation single-threaded.
func (c *ContentClusterer) Vector(tokens []string) []float32 {
	return featureVector(tokens, c.dims)
}

// Assign places a document in a shard by its nearest centroid, spilling to the
// next-nearest with room when the nearest is at capacity, so the shard sizes stay
// balanced. The spill trades a little topical purity for a hard size bound, the
// right trade because an over-full shard costs more on every query than a spilled
// document's looser bound costs on the few queries that touch its topic. Assign is
// exact whatever it returns: the shard only decides which routing bound a document
// contributes to, never whether it can be retrieved.
func (c *ContentClusterer) Assign(tokens []string) int {
	return c.AssignVec(featureVector(tokens, c.dims))
}

// AssignVec is Assign for a pre-computed feature vector (Vector), the split that lets
// the vector hashing parallelize while the fill mutation stays single-threaded.
func (c *ContentClusterer) AssignVec(v []float32) int {
	if len(c.centroids) == 1 {
		if c.fill != nil {
			c.fill[0]++
		}
		return 0
	}

	// Order shards by descending similarity, take the first with capacity. When no
	// capacity is set every shard is open, so this is a plain nearest-centroid pick.
	best, bestDot := -1, float32(-2)
	for j := range c.centroids {
		if c.cap > 0 && c.fill[j] >= c.cap {
			continue
		}
		if d := dot(v, c.centroids[j]); d > bestDot {
			best, bestDot = j, d
		}
	}
	if best < 0 {
		// Every shard is full (cap*shards < corpus, or a rounding edge): fall back to
		// the globally nearest so the document still lands somewhere sensible.
		for j := range c.centroids {
			if d := dot(v, c.centroids[j]); d > bestDot {
				best, bestDot = j, d
			}
		}
	}
	if c.fill != nil {
		c.fill[best]++
	}
	return best
}
