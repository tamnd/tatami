package tatami

// Sharded real-data benchmark for the scale redesign (Spec 2066, scale/12 and
// scale/13). The monolithic-segment tier benchmark (scale_tiers_test.go) is a
// stress baseline: it makes one giant segment per tier, so a common term's posting
// walk grows with the whole corpus and a weak box blows the budget well before
// 10M. That is not the config the 100M-docs/machine goal assumes. The goal assumes
// sharding, and this is the sharded path: one shard per WET Parquet file (about
// twenty thousand documents each), a routing index over all of them, and a broker
// that routes each query to the shards that can contribute, prunes the rest by
// their impact bound, and shares the cross-shard threshold so a later shard never
// does work a better-scoring shard already ruled out.
//
// The serving path matters. A single common term, or a selective query, routes and
// prunes to a handful of shards and the bag route is enough. A common multi-word
// phrase is different: each of its words lands in most shards, so the bag route
// fans out to nearly every shard and the per-shard scoring, summed over the fan,
// breaks the budget as the shard count grows. The design's answer is the phrase
// route (scale/07, section 4.1): route on the rarer adjacency, which is in a
// fraction of the shards, not on the common words. So this benchmark serves every
// query through the phrase path, which falls back to the bag route for single
// words and untracked adjacencies, the way a real broker would.
//
// It reads the same WET Parquet shards as the tier benchmark, so CI skips it
// cleanly. Point it at the 10M corpus and run it:
//
//	TATAMI_WET_DIR=$HOME/data/ccrawl/wet-parquet10m go test -run TestClusterScale10M -v -timeout 3h
//
// TATAMI_MAX_SHARDS caps the shard count for a quick smoke run; zero or unset uses
// every Parquet file. TATAMI_BUILD_WORKERS caps the build concurrency to bound
// peak memory; it defaults to a few workers so the per-shard builders do not pile
// up on a small box.

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/tatami/search"
)

// maxShards reads the optional shard cap. Zero means no cap.
func maxShards() int { return envInt("TATAMI_MAX_SHARDS", 0) }

// buildWorkers is the build concurrency. Each in-flight worker holds a shard's
// builder resident, and a shard builder with bigram capture on is the memory peak
// of the whole run, so this stays small by default to fit a box with tens of GB
// rather than hundreds.
func buildWorkers() int {
	n := envInt("TATAMI_BUILD_WORKERS", 4)
	if g := runtime.GOMAXPROCS(0); n > g {
		n = g
	}
	if n < 1 {
		n = 1
	}
	return n
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// targetPairs is the set of adjacencies the benchmark queries route on. The
// bigram sidecar tracks only these pairs, not every adjacency in ten million
// documents, which would be an unbounded dictionary. This is faithful to what the
// queries measure: RoutePhrase looks up only a query's own pairs, so the route, the
// fan-out, and the latency for these queries are identical whether the sidecar
// holds these few pairs or the whole corpus. It just bounds build memory.
func targetPairs() map[search.BigramKey]bool {
	keep := map[search.BigramKey]bool{}
	for _, q := range append(append([]string{}, benchQueries...), phraseQueries...) {
		for _, p := range search.PhraseAdjacencies(tokenize(q)) {
			keep[p] = true
		}
	}
	return keep
}

// keptBigrams wraps a shard's bigram source and forwards only the target pairs to
// the routing builder, so the accumulated sidecar stays bounded to the query set.
type keptBigrams struct {
	src  search.BigramSource
	keep map[search.BigramKey]bool
}

func (k keptBigrams) EachBigram(fn func(a, b string, df int, maxFreq uint32)) {
	k.src.EachBigram(func(a, b string, df int, maxFreq uint32) {
		if k.keep[search.BigramKey{A: a, B: b}] {
			fn(a, b, df, maxFreq)
		}
	})
}

// The sharded corpus is built once and shared by every test in this file: the
// build is the expensive part, and the tests all serve the same shards.
var (
	shardedOnce sync.Once
	shardedC    *Cluster
	shardedDocs int
	shardedErr  error
	shardedSkip string
)

// buildShardedCorpus turns each Parquet file in the WET directory into one tatami
// search shard, building the phrase routing sidecar over the query pairs alongside,
// and returns the open broker plus the document count. The shards are built in
// parallel across a bounded worker pool, since each file is independent, and each
// shard's builder is discarded as soon as it is written and folded, so peak memory
// is the worker count of builders, not the whole corpus. The shards are search-only
// (the body is tokenized into the inverted index, then dropped to a short snippet),
// the 100M-docs/machine config scale/13 describes, so the build holds no gigabytes
// of WET text and query latency touches only the index.
func buildShardedCorpus(tb testing.TB) (*Cluster, int) {
	shardedOnce.Do(func() {
		dir := wetDir()
		matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
		if err != nil || len(matches) == 0 {
			shardedSkip = fmt.Sprintf("WET corpus unavailable (%v); set TATAMI_WET_DIR to run", err)
			return
		}
		sort.Strings(matches)
		if cap := maxShards(); cap > 0 && cap < len(matches) {
			matches = matches[:cap]
		}

		tmp, err := os.MkdirTemp("", "tatami-cluster10m-")
		if err != nil {
			shardedErr = err
			return
		}

		t0 := nowMono()
		paths := make([]string, len(matches))
		counts := make([]int, len(matches))
		bb := search.NewBigramRoutingBuilder()
		keep := targetPairs()
		var mu sync.Mutex // guards bb folding, the done counter, and firstErr
		var done int
		var firstErr error

		jobs := make(chan int)
		var wg sync.WaitGroup
		for w := 0; w < buildWorkers(); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true, Bigrams: true})
					n, err := eachWETFile(matches[i], math.MaxInt32, func(d SearchDoc) { b.Add(d) })
					if err == nil {
						err = b.Write(filepath.Join(tmp, fmt.Sprintf("seg-%05d.tatami", i)), WriterOptions{})
					}
					mu.Lock()
					if err != nil {
						if firstErr == nil {
							firstErr = fmt.Errorf("shard %d (%s): %w", i, matches[i], err)
						}
						mu.Unlock()
						continue
					}
					paths[i] = filepath.Join(tmp, fmt.Sprintf("seg-%05d.tatami", i))
					counts[i] = n
					// The shard id the routing index assigns is the path index
					// (OpenCluster keys shards by their position in paths), so fold the
					// bigram sidecar under the same id to keep the phrase route and the
					// bag route naming one shard set.
					bb.AddShard(uint32(i), keptBigrams{src: b, keep: keep})
					done++
					if done%50 == 0 {
						tb.Logf("built %d/%d shards, %v elapsed", done, len(matches), nowMono().Sub(t0).Round(time.Second))
					}
					mu.Unlock()
				}
			}()
		}
		for i := range matches {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		if firstErr != nil {
			_ = os.RemoveAll(tmp)
			shardedErr = firstErr
			return
		}
		for _, n := range counts {
			shardedDocs += n
		}

		// Cache every shard: the 10ms budget is a warm-serving claim, so after warmup
		// the broker finds each routed shard resident and runs the posting walk from
		// memory. The cold first-touch decode is the separate cost the open-segment
		// cache and shard repackaging exist to bound, measured by the cache-bound test.
		c, err := OpenCluster(paths, ClusterOptions{CacheSize: len(paths)})
		if err != nil {
			_ = os.RemoveAll(tmp)
			shardedErr = err
			return
		}
		tb.Logf("built %d shards, %d docs, %d terms in routing, %v",
			c.NumShards(), c.NumDocs(), c.Routing().NumTerms(), nowMono().Sub(t0).Round(time.Second))
		shardedC = c.WithBigramRouting(bb.Build())
	})
	if shardedSkip != "" {
		tb.Skip(shardedSkip)
	}
	if shardedErr != nil {
		tb.Fatal(shardedErr)
	}
	return shardedC, shardedDocs
}

// TestClusterScale10M builds the sharded corpus and enforces the headline claim:
// retrieval p99 under 10ms, warm, at scale, serving each query the way a real
// broker would. It routes through the phrase path, which narrows a common multi-word
// phrase to the shards holding its adjacency and falls back to the bag route for a
// single word, then logs the per-query p50 and p99 and how the phrase route cut the
// fan-out: candidates is shards routed to, visited is shards actually opened after
// the bound walk and threshold sharing pruned the rest.
func TestClusterScale10M(t *testing.T) {
	c, docs := buildShardedCorpus(t)
	t.Logf("cluster: %d shards, %d docs", c.NumShards(), docs)

	// Warm the routed shards into the open-segment cache so the measured pass
	// reflects warm serving.
	for _, q := range benchQueries {
		if _, _, err := c.QueryPhrase(q, 10); err != nil {
			t.Fatal(err)
		}
	}

	const reps = 200
	var all []time.Duration
	for _, q := range benchQueries {
		_, bagStats, err := c.QueryWith(q, 10, c.Routing())
		if err != nil {
			t.Fatal(err)
		}
		samples := make([]time.Duration, 0, reps)
		var last QueryStats
		for range reps {
			start := nowMono()
			_, st, err := c.QueryPhrase(q, 10)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, nowMono().Sub(start))
			last = st
		}
		slices.Sort(samples)
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v candidates=%-4d (bag %-4d) visited=%d",
			q, p50, p99, last.Candidates, bagStats.Candidates, last.Visited)
		all = append(all, samples...)
	}
	slices.Sort(all)
	overall := all[(len(all)*99)/100]
	t.Logf("overall p50=%v p99=%v", all[len(all)/2], overall)
	if overall > 10*time.Millisecond {
		t.Fatalf("sharded retrieval p99 %v exceeds the 10ms target", overall)
	}
}
