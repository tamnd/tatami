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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/tamnd/tatami/search"
)

// maxShards reads the optional shard cap. Zero means no cap.
func maxShards() int { return envInt("TATAMI_MAX_SHARDS", 0) }

// filesPerShard groups this many Parquet files into one shard. One file per shard
// (the default) is the finest split, but the routing index replicates every common
// term across every shard, so its resident footprint grows with the shard count:
// at one file per shard the unigram routing for a 500-file corpus runs to several
// gigabytes, more than a box with tens of GB can hold next to the segment cache.
// Grouping files into coarser shards is the scale/12 repackaging lever: it collapses
// that replication (a term common to every file now has one posting per group, not
// per file) at the cost of a longer per-shard posting walk, which the bound walk and
// threshold sharing keep bounded. The 10M run sets this so the routing index fits.
func filesPerShard() int { return envInt("TATAMI_FILES_PER_SHARD", 1) }

// targetShards is the coarsening lever (scale/11 lever one): the number of shards
// to aim for regardless of how many Parquet files the corpus has. Zero (the
// default) leaves the split to filesPerShard. When set, the pipeline groups the
// files so the shard count lands near this target, so the same target yields ~128
// shards whether the corpus is 500 files or 5,000. The right target is a small
// multiple of the serving box's cores: past that the fan-out stops parallelizing
// and every extra shard is fixed per-shard cost with nothing to hide it under,
// while below it each shard's posting walk grows until a common term breaks budget.
func targetShards() int { return envInt("TATAMI_TARGET_SHARDS", 0) }

// clusterShards is the content-clustering lever (scale/12 lever two): the number of
// topical shards to partition the corpus into by content instead of by crawl order.
// Zero (the default) keeps the crawl-order file grouping the other levers use. When
// set, the build fits a clustering model over a sample, then repartitions every
// document into the shard whose topic it is nearest, size-balanced by a capacity
// cap. The routing bound stays a true upper bound, so the routed result is identical
// to the crawl-order one; what changes is that a topical query prunes to the handful
// of shards that hold its topic instead of fanning out to nearly all of them. It is
// the coarsening target: a small multiple of the serving box's cores.
func clusterShards() int { return envInt("TATAMI_CLUSTER_SHARDS", 0) }

// clusterSample caps how many documents the k-means fit sees. A sample pins the
// topic structure of the corpus; more documents only refine the centroids, so this
// bounds the fit cost independent of the corpus size.
func clusterSample() int { return envInt("TATAMI_CLUSTER_SAMPLE", 50000) }

// clusterDims is the feature-hash width, the number of buckets a document's term
// set hashes into to form its topic sketch. A few hundred to a few thousand fits in
// cache and collides rarely enough that disjoint topics land far apart.
func clusterDims() int { return envInt("TATAMI_CLUSTER_DIMS", 1024) }

// clusterIters is the k-means pass count over the sample.
func clusterIters() int { return envInt("TATAMI_CLUSTER_ITERS", 15) }

// chunkFiles groups paths into runs of up to per files, preserving order, so each
// run becomes one shard.
func chunkFiles(paths []string, per int) [][]string {
	if per < 1 {
		per = 1
	}
	var groups [][]string
	for i := 0; i < len(paths); i += per {
		hi := i + per
		if hi > len(paths) {
			hi = len(paths)
		}
		groups = append(groups, paths[i:hi])
	}
	return groups
}

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

// buildShardedCorpus turns each group of Parquet files in the WET directory into one
// tatami search shard, captures the query set's adjacencies for the phrase routing
// sidecar alongside, and returns the open broker plus the document count. The shards
// are built in parallel across a bounded worker pool, since each group is
// independent, and each shard's builder is discarded as soon as it is written, so
// peak memory is the worker count of builders, not the whole corpus. The shards are
// search-only (the body is tokenized into the inverted index, then dropped to a
// short snippet), the 100M-docs/machine config scale/13 describes, so the build holds
// no gigabytes of WET text and query latency touches only the index.
//
// The build is resumable, which matters because a 10M build runs for hours at low
// priority on a shared box and can be interrupted. With TATAMI_SEG_DIR set, each
// shard writes its segment and a tiny per-shard bigram sidecar (seg-NNNNN.bgr); a
// shard whose segment and sidecar both exist is skipped on the next run. Nothing
// else is persisted: OpenCluster rebuilds the routing index by reading the segment
// dictionaries (no Parquet, no re-tokenize), so the only thing the sidecars carry is
// the adjacency counts a segment cannot reconstruct (the index stores frequencies,
// not positions). A killed build therefore loses at most the shards in flight, and a
// re-run finishes the rest, then a quiet-window measurement re-runs in the time it
// takes to read the dictionaries. With no segment directory set the build goes to a
// temp dir and is not reused.
func buildShardedCorpus(tb testing.TB) (*Cluster, int) {
	shardedOnce.Do(func() {
		segDir := os.Getenv("TATAMI_SEG_DIR")
		measureOnly := os.Getenv("TATAMI_MEASURE_ONLY") != ""

		dir := wetDir()
		matches, gerr := filepath.Glob(filepath.Join(dir, "*.parquet"))
		noCorpus := gerr != nil || len(matches) == 0

		// Measure-only fast path: reopen an already-built segment directory without
		// touching the WET corpus. Forced with TATAMI_MEASURE_ONLY, or taken
		// automatically when the corpus is gone but a full set of segments is on disk,
		// which is the case after the segments are copied to a quiet, idle box to take
		// the warm latency reading off the busy build box. It rebuilds the routing
		// index from the segment dictionaries and folds the per-shard sidecars, so it
		// yields the same cluster a fresh build does, on a box that never saw the 41GB
		// corpus.
		if segDir != "" && (measureOnly || noCorpus) {
			c, docs, rerr := reopenSegmentCluster(segDir)
			if rerr == nil {
				tb.Logf("measure-only: reopened %d shards, %d docs from %s", c.NumShards(), docs, segDir)
				shardedC, shardedDocs = c, docs
				return
			}
			if measureOnly {
				shardedErr = fmt.Errorf("measure-only reopen of %s failed: %w", segDir, rerr)
				return
			}
			// Fall through: no corpus and no usable segments, so skip below.
		}
		if noCorpus {
			shardedSkip = fmt.Sprintf("WET corpus unavailable (%v); set TATAMI_WET_DIR to run", gerr)
			return
		}

		sort.Strings(matches)
		per := filesPerShard()
		// The coarsening lever overrides the fixed files-per-shard: given a target
		// shard count, group the files so the count lands near it, so the shard count
		// tracks the box, not the corpus size (scale/11 lever one).
		if ts := targetShards(); ts > 0 && ts < len(matches) {
			per = (len(matches) + ts - 1) / ts
		}
		// A shard cap bounds the work for a smoke run. It caps the number of shards,
		// so it consumes at most cap*per input files.
		if cap := maxShards(); cap > 0 && cap*per < len(matches) {
			matches = matches[:cap*per]
		}
		groups := chunkFiles(matches, per)

		tmp := segDir
		cleanup := false
		if tmp == "" {
			t, err := os.MkdirTemp("", "tatami-cluster10m-")
			if err != nil {
				shardedErr = err
				return
			}
			tmp = t
			cleanup = true
		} else if err := os.MkdirAll(tmp, 0o755); err != nil {
			shardedErr = err
			return
		}

		segPath := func(i int) string { return filepath.Join(tmp, fmt.Sprintf("seg-%05d.tatami", i)) }
		bgrPath := func(i int) string { return filepath.Join(tmp, fmt.Sprintf("seg-%05d.bgr", i)) }

		t0 := nowMono()
		keep := targetPairs()

		// Content-clustering path (scale/12 lever two). When a cluster target is set,
		// the corpus is repartitioned by content instead of by crawl-file grouping, so
		// the group-per-shard worker pool below does not apply. The clustered build
		// spills documents to per-shard files so only one builder is resident at a
		// time, then folds the routing and phrase sidecars exactly as the crawl-order
		// tail does.
		if clusterShards() > 0 {
			c, docs, cerr := buildClusteredCorpus(tb, matches, tmp, keep)
			if cleanup && cerr != nil {
				_ = os.RemoveAll(tmp)
			}
			shardedC, shardedDocs, shardedErr = c, docs, cerr
			return
		}

		var mu sync.Mutex // guards the counters and firstErr
		var done, built, resumed int
		var firstErr error

		jobs := make(chan int)
		var wg sync.WaitGroup
		for w := 0; w < buildWorkers(); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					// Resume: a shard whose segment and bigram sidecar both exist is
					// already built. The sidecar is written only after the segment is
					// renamed into place, so its presence means the segment is complete.
					if fileExists(segPath(i)) && fileExists(bgrPath(i)) {
						mu.Lock()
						done++
						resumed++
						mu.Unlock()
						continue
					}
					b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true, Bigrams: true})
					var err error
					for _, f := range groups[i] {
						if _, ferr := eachWETFile(f, math.MaxInt32, func(d SearchDoc) { b.Add(d) }); ferr != nil {
							err = ferr
							break
						}
					}
					if err == nil {
						err = writeSegmentAtomic(b, segPath(i))
					}
					if err == nil {
						err = writeBigramSidecar(bgrPath(i), keptBigrams{src: b, keep: keep})
					}
					mu.Lock()
					if err != nil {
						if firstErr == nil {
							firstErr = fmt.Errorf("shard %d (%v): %w", i, groups[i], err)
						}
						mu.Unlock()
						continue
					}
					done++
					built++
					if done%50 == 0 {
						tb.Logf("%d/%d shards ready (%d built, %d resumed), %v",
							done, len(groups), built, resumed, nowMono().Sub(t0).Round(time.Second))
					}
					mu.Unlock()
				}
			}()
		}
		for i := range groups {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		if firstErr != nil {
			if cleanup {
				_ = os.RemoveAll(tmp)
			}
			shardedErr = firstErr
			return
		}

		// Every shard now has a segment on disk. OpenCluster rebuilds the routing index
		// from the segment dictionaries, and the per-shard sidecars fold back into the
		// phrase routing under the same shard ids (the path index), so the phrase route
		// and the bag route name one shard set.
		//
		// Cache the working set: the 10ms budget is a warm-serving claim, so the cache
		// should hold the shards the query set opens, which the phrase route and the
		// bound walk keep to a fraction of the shard count. Caching every shard is the
		// simplest way to guarantee that but costs one resident segment per shard, too
		// much on a box already running other work, so the cap is tunable and defaults
		// to every shard only when that fits.
		paths := make([]string, len(groups))
		for i := range groups {
			paths[i] = segPath(i)
		}
		cache := envInt("TATAMI_CACHE_SHARDS", len(paths))
		c, err := OpenCluster(paths, ClusterOptions{CacheSize: cache})
		if err != nil {
			shardedErr = err
			return
		}
		bb := search.NewBigramRoutingBuilder()
		for i := range paths {
			src, err := readBigramSidecar(bgrPath(i))
			if err != nil {
				shardedErr = fmt.Errorf("read bigram sidecar %d: %w", i, err)
				return
			}
			bb.AddShard(uint32(i), src)
		}
		tb.Logf("cluster ready: %d shards, %d docs, %d terms in routing, %v (%d built, %d resumed)",
			c.NumShards(), c.NumDocs(), c.Routing().NumTerms(),
			nowMono().Sub(t0).Round(time.Second), built, resumed)
		shardedC = c.WithBigramRouting(bb.Build())
		shardedDocs = c.NumDocs()
	})
	if shardedSkip != "" {
		tb.Skip(shardedSkip)
	}
	if shardedErr != nil {
		tb.Fatal(shardedErr)
	}
	return shardedC, shardedDocs
}

// parseFilesParallel runs fn over every file index across a bounded worker pool and
// returns the first error. Pass one's files are independent (count and sample), so the
// pool needs no ordering: the caller merges the per-file results in index order after.
func parseFilesParallel(matches []string, workers int, fn func(fi int, f string) error) error {
	jobs := make(chan int)
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range jobs {
				if err := fn(fi, matches[fi]); err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for fi := range matches {
		jobs <- fi
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

// buildClusteredCorpus partitions the corpus into clusterShards() topical shards by
// content and returns the open broker plus the document count (scale/12 lever two).
// It runs three passes over the crawl so peak memory stays bounded whatever the
// corpus size: pass one counts every document and samples the first clusterSample()
// to fit the k-means model; pass two assigns each document to its nearest topical
// shard under a capacity cap and spills it to that shard's file, holding only the
// clusterer and one open spill writer per shard; pass three builds each shard from
// its spill file one at a time, so only one SearchBuilder is resident. The routing
// index and phrase sidecars fold exactly as the crawl-order build's tail does, and
// the segments are written with the same seg-NNNNN names so a later measure-only
// reopen cannot tell a clustered corpus from a crawl-order one.
func buildClusteredCorpus(tb testing.TB, matches []string, tmp string, keep map[search.BigramKey]bool) (*Cluster, int, error) {
	k := clusterShards()
	t0 := nowMono()

	// Pass one: count every document and collect a bounded sample to fit the model.
	// Files parse in parallel across the build workers, each contributing its own count
	// and a bounded per-file slice of the sample, merged in file order afterward so the
	// total and the sample are the same whatever order the workers finish in (the
	// per-file quota also spreads the sample across the corpus instead of drawing it all
	// from the first file). One parse yields both, so the fit costs no extra pass.
	workers := buildWorkers()
	sampleCap := clusterSample()
	perFileSample := (sampleCap + len(matches) - 1) / max(len(matches), 1)
	counts := make([]int, len(matches))
	samples := make([][][]string, len(matches))
	if err := parseFilesParallel(matches, workers, func(fi int, f string) error {
		n, err := eachWETFile(f, math.MaxInt32, func(d SearchDoc) {
			if len(samples[fi]) < perFileSample {
				samples[fi] = append(samples[fi], tokenize(d.Body))
			}
		})
		if err != nil {
			return fmt.Errorf("cluster sample pass %s: %w", filepath.Base(f), err)
		}
		counts[fi] = n
		return nil
	}); err != nil {
		return nil, 0, err
	}
	total := 0
	var sample [][]string
	for fi := range matches {
		total += counts[fi]
		if room := sampleCap - len(sample); room > 0 {
			s := samples[fi]
			if len(s) > room {
				s = s[:room]
			}
			sample = append(sample, s...)
		}
	}
	cl := FitClusterer(sample, ClusterPlanOptions{
		Shards: k, Dims: clusterDims(), Iters: clusterIters(), Seed: 1, Slack: 0.15,
	})
	cl.SetCapacity(total, 0.15)
	tb.Logf("clustering: fit %d centroids over %d of %d docs, %v",
		cl.Shards(), len(sample), total, nowMono().Sub(t0).Round(time.Second))

	// Pass two: assign and spill. One writer per shard stays open, so a document from
	// any crawl file streams straight into its shard's file with no buffering of the
	// whole corpus. The spill carries the whole SearchDoc, since the shard build
	// re-tokenizes it into the index and keeps a snippet, so an uncompressed spill would
	// put a second full copy of the corpus body on disk (about 10GB at 1M docs, and it is
	// what filled the disk and stalled the clustered build at scale). The bodies are text
	// and compress several-fold, so each spill is a zstd stream at the fastest level,
	// with one goroutine per shard so the k open encoders do not oversubscribe the box.
	// That drops the scratch high-water mark to a fraction of the raw body size, which is
	// what lets a 10M or 100M clustered build run where a full-size spill would not fit.
	spillPath := func(i int) string { return filepath.Join(tmp, fmt.Sprintf("spill-%05d.gob.zst", i)) }
	files := make([]*os.File, k)
	zws := make([]*zstd.Encoder, k)
	encs := make([]*gob.Encoder, k)
	for i := range files {
		f, err := os.Create(spillPath(i))
		if err != nil {
			return nil, 0, fmt.Errorf("create spill %d: %w", i, err)
		}
		zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderConcurrency(1))
		if err != nil {
			f.Close()
			return nil, 0, fmt.Errorf("spill compressor %d: %w", i, err)
		}
		files[i], zws[i], encs[i] = f, zw, gob.NewEncoder(zw)
	}
	sizes := make([]int, k)
	var spillErr error
	// Parse a window of files in parallel and compute each document's topic vector on
	// the worker (the vector hashing is the per-document cost), then assign and spill in
	// crawl order on this goroutine. The assignment mutates the capacity fill counts, so
	// it stays single-threaded, but it is only a dot over k centroids per document, cheap
	// next to the parse it now overlaps. Ordering the windowed results by file keeps the
	// capacity-spill assignment deterministic: the same corpus yields the same shards.
	type docVec struct {
		doc SearchDoc
		vec []float32
	}
	for base := 0; base < len(matches) && spillErr == nil; base += workers {
		hi := min(base+workers, len(matches))
		window := matches[base:hi]
		parsed := make([][]docVec, len(window))
		perr := make([]error, len(window))
		var wg sync.WaitGroup
		for j := range window {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				var out []docVec
				if _, err := eachWETFile(window[j], math.MaxInt32, func(d SearchDoc) {
					out = append(out, docVec{doc: d, vec: cl.Vector(tokenize(d.Body))})
				}); err != nil {
					perr[j] = err
					return
				}
				parsed[j] = out
			}(j)
		}
		wg.Wait()
		for j := range window {
			if perr[j] != nil {
				spillErr = fmt.Errorf("cluster assign pass %s: %w", filepath.Base(window[j]), perr[j])
				break
			}
			for _, dv := range parsed[j] {
				s := cl.AssignVec(dv.vec)
				d := dv.doc
				if err := encs[s].Encode(&d); err != nil {
					spillErr = fmt.Errorf("spill encode shard %d: %w", s, err)
					break
				}
				sizes[s]++
			}
			if spillErr != nil {
				break
			}
		}
	}
	// Flush each zstd stream before closing its file: the compressor buffers, so the
	// spill's bytes are not all on disk until the encoder is closed.
	for i := range files {
		if cerr := zws[i].Close(); cerr != nil && spillErr == nil {
			spillErr = cerr
		}
		if cerr := files[i].Close(); cerr != nil && spillErr == nil {
			spillErr = cerr
		}
	}
	if spillErr != nil {
		return nil, 0, spillErr
	}
	tb.Logf("clustering: spilled %d docs into %d shards, sizes min %d max %d, %v",
		total, k, minInt(sizes), maxInt(sizes), nowMono().Sub(t0).Round(time.Second))

	// Pass three: build each non-empty shard from its spill, writing the segment and
	// its bigram sidecar under seg-NNNNN names, then removing the spill so the disk
	// high-water mark is the corpus once, not twice. The shards are independent, so the
	// build fans out across the workers exactly as the crawl-order build does: peak
	// memory is the worker count of resident builders, not the whole corpus.
	segPath := func(i int) string { return filepath.Join(tmp, fmt.Sprintf("seg-%05d.tatami", i)) }
	bgrPath := func(i int) string { return filepath.Join(tmp, fmt.Sprintf("seg-%05d.bgr", i)) }
	var todo []int
	for i := range sizes {
		if sizes[i] == 0 {
			_ = os.Remove(spillPath(i))
			continue
		}
		todo = append(todo, i)
	}
	buildOne := func(i int) error {
		b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true, Bigrams: true})
		f, err := os.Open(spillPath(i))
		if err != nil {
			return fmt.Errorf("open spill %d: %w", i, err)
		}
		zr, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
		if err != nil {
			f.Close()
			return fmt.Errorf("open spill decompressor %d: %w", i, err)
		}
		dec := gob.NewDecoder(zr)
		for {
			var d SearchDoc
			if derr := dec.Decode(&d); derr != nil {
				if derr == io.EOF {
					break
				}
				zr.Close()
				f.Close()
				return fmt.Errorf("spill decode shard %d: %w", i, derr)
			}
			b.Add(d)
		}
		zr.Close()
		f.Close()
		if err := writeSegmentAtomic(b, segPath(i)); err != nil {
			return fmt.Errorf("write clustered segment %d: %w", i, err)
		}
		if err := writeBigramSidecar(bgrPath(i), keptBigrams{src: b, keep: keep}); err != nil {
			return fmt.Errorf("write clustered sidecar %d: %w", i, err)
		}
		_ = os.Remove(spillPath(i))
		return nil
	}
	jobs := make(chan int)
	var bmu sync.Mutex
	var buildErr error
	var bwg sync.WaitGroup
	for range workers {
		bwg.Add(1)
		go func() {
			defer bwg.Done()
			for i := range jobs {
				if err := buildOne(i); err != nil {
					bmu.Lock()
					if buildErr == nil {
						buildErr = err
					}
					bmu.Unlock()
				}
			}
		}()
	}
	for _, i := range todo {
		jobs <- i
	}
	close(jobs)
	bwg.Wait()
	if buildErr != nil {
		return nil, 0, buildErr
	}
	// Assemble the segment paths in shard-index order, the order a measure-only reopen
	// sees them (it globs and sorts seg-*.tatami), so the two agree on shard ids.
	paths := make([]string, 0, len(todo))
	for _, i := range todo {
		paths = append(paths, segPath(i))
	}

	// Shared tail: build the routing index over the clustered shards and fold their
	// phrase sidecars, the same broker the crawl-order build returns.
	cache := envInt("TATAMI_CACHE_SHARDS", len(paths))
	c, err := OpenCluster(paths, ClusterOptions{CacheSize: cache})
	if err != nil {
		return nil, 0, err
	}
	bb := search.NewBigramRoutingBuilder()
	for i, p := range paths {
		src, err := readBigramSidecar(strings.TrimSuffix(p, ".tatami") + ".bgr")
		if err != nil {
			return nil, 0, fmt.Errorf("read clustered sidecar %d: %w", i, err)
		}
		bb.AddShard(uint32(i), src)
	}
	tb.Logf("clustered cluster ready: %d shards, %d docs, %d terms in routing, %v",
		c.NumShards(), c.NumDocs(), c.Routing().NumTerms(), nowMono().Sub(t0).Round(time.Second))
	return c.WithBigramRouting(bb.Build()), c.NumDocs(), nil
}

// minInt and maxInt report the extremes of a slice, for logging the clustered shard
// balance.
func minInt(s []int) int {
	m := s[0]
	for _, v := range s {
		m = min(m, v)
	}
	return m
}

func maxInt(s []int) int {
	m := s[0]
	for _, v := range s {
		m = max(m, v)
	}
	return m
}

// reopenSegmentCluster builds a broker from the segments already in segDir, with no
// WET corpus in hand. It rebuilds the routing index from the segment dictionaries
// (OpenCluster) and folds each shard's persisted adjacency sidecar, the same cluster
// a fresh build yields. It is how the warm latency reading moves off the busy build
// box: copy the seg-*.tatami and seg-*.bgr files to an idle box and reopen them.
func reopenSegmentCluster(segDir string) (*Cluster, int, error) {
	paths, err := filepath.Glob(filepath.Join(segDir, "seg-*.tatami"))
	if err != nil {
		return nil, 0, err
	}
	if len(paths) == 0 {
		return nil, 0, fmt.Errorf("no segments in %s", segDir)
	}
	sort.Strings(paths)
	for _, p := range paths {
		if !fileExists(strings.TrimSuffix(p, ".tatami") + ".bgr") {
			return nil, 0, fmt.Errorf("segment %s has no bigram sidecar", filepath.Base(p))
		}
	}
	cache := envInt("TATAMI_CACHE_SHARDS", len(paths))
	// Prefer the off-heap routing (scale/11 lever three): if a routing.bin sits next
	// to the segments, mmap it and skip the rebuild, so the reopen box pays the file,
	// not the tens-of-gigabytes routing rebuild. The first reopen has no file, so it
	// rebuilds once and writes routing.bin for every reopen after.
	rpath := filepath.Join(segDir, "routing.bin")
	var c *Cluster
	if fileExists(rpath) {
		c, err = OpenClusterWithRoutingFile(paths, rpath, ClusterOptions{CacheSize: cache})
	} else {
		c, err = OpenCluster(paths, ClusterOptions{CacheSize: cache})
		if err == nil {
			if werr := c.PersistRouting(rpath); werr != nil {
				return nil, 0, fmt.Errorf("persist routing.bin: %w", werr)
			}
		}
	}
	if err != nil {
		return nil, 0, err
	}
	bb := search.NewBigramRoutingBuilder()
	for i, p := range paths {
		src, err := readBigramSidecar(strings.TrimSuffix(p, ".tatami") + ".bgr")
		if err != nil {
			return nil, 0, err
		}
		bb.AddShard(uint32(i), src)
	}
	return c.WithBigramRouting(bb.Build()), c.NumDocs(), nil
}

// fileExists reports whether path is a present, non-empty regular file.
func fileExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular() && fi.Size() > 0
}

// writeSegmentAtomic writes the builder's segment to a temp file and renames it into
// place, so a reader (or a resuming build) never sees a half-written segment.
func writeSegmentAtomic(b *SearchBuilder, path string) error {
	tmp := path + ".tmp"
	if err := b.Write(tmp, WriterOptions{}); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// writeBigramSidecar persists one shard's adjacency counts: the pairs the query set
// routes on, each with the shard's document frequency and maximum per-document count.
// This is the only thing a segment cannot reconstruct, because the inverted index
// stores frequencies, not positions. The format is a varint count followed by each
// pair's two tokens and its df and maxFreq. It is written atomically so its presence
// is the build's done marker for the shard.
func writeBigramSidecar(path string, src search.BigramSource) error {
	var buf bytes.Buffer
	var scratch [binary.MaxVarintLen64]byte
	put := func(v uint64) {
		n := binary.PutUvarint(scratch[:], v)
		buf.Write(scratch[:n])
	}
	var pairs int
	var body bytes.Buffer
	src.EachBigram(func(a, b string, df int, maxFreq uint32) {
		pairs++
		n := binary.PutUvarint(scratch[:], uint64(len(a)))
		body.Write(scratch[:n])
		body.WriteString(a)
		n = binary.PutUvarint(scratch[:], uint64(len(b)))
		body.Write(scratch[:n])
		body.WriteString(b)
		n = binary.PutUvarint(scratch[:], uint64(df))
		body.Write(scratch[:n])
		n = binary.PutUvarint(scratch[:], uint64(maxFreq))
		body.Write(scratch[:n])
	})
	put(uint64(pairs))
	buf.Write(body.Bytes())
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// fileBigrams is a shard's adjacency counts read back from a sidecar, exposed as a
// BigramSource so the phrase routing builder folds it exactly as it folded the live
// builder at index time.
type fileBigrams struct {
	pairs []bgrPair
}

type bgrPair struct {
	a, b    string
	df      int
	maxFreq uint32
}

func (f fileBigrams) EachBigram(fn func(a, b string, df int, maxFreq uint32)) {
	for _, p := range f.pairs {
		fn(p.a, p.b, p.df, p.maxFreq)
	}
}

func readBigramSidecar(path string) (search.BigramSource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	r := bytes.NewReader(data)
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	readStr := func() (string, error) {
		l, err := binary.ReadUvarint(r)
		if err != nil {
			return "", err
		}
		bs := make([]byte, l)
		if _, err := io.ReadFull(r, bs); err != nil {
			return "", err
		}
		return string(bs), nil
	}
	f := fileBigrams{}
	for i := uint64(0); i < n; i++ {
		a, err := readStr()
		if err != nil {
			return nil, err
		}
		b, err := readStr()
		if err != nil {
			return nil, err
		}
		df, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		mf, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, err
		}
		f.pairs = append(f.pairs, bgrPair{a: a, b: b, df: int(df), maxFreq: uint32(mf)})
	}
	return f, nil
}

// TestClusterScale10M builds the sharded corpus and enforces the headline claim:
// retrieval p99 under 10ms, warm, at scale, serving each query the way a real
// broker would. It routes through the phrase path, which narrows a common multi-word
// phrase to the shards holding its adjacency and falls back to the bag route for a
// single word, then logs the per-query p50 and p99 and how the phrase route cut the
// fan-out: candidates is shards routed to, visited is shards actually opened after
// the bound walk and threshold sharing pruned the rest.
func TestClusterScale10M(t *testing.T) {
	// This is the 10M sharded benchmark, and it only means something pointed at the
	// 10M corpus or a segment set built from it, which takes minutes to build and
	// hours to run through the whole 41GB. It must be opted into explicitly. Without
	// TATAMI_SEG_DIR (reopen pre-built segments) or TATAMI_WET_DIR (build from the
	// corpus) it skips, so a plain `go test ./...` on a dev box, whose default WET
	// directory holds the smaller tier corpus, does not silently start a multi-minute
	// build and trip the package timeout.
	if os.Getenv("TATAMI_SEG_DIR") == "" && os.Getenv("TATAMI_WET_DIR") == "" {
		t.Skip("set TATAMI_SEG_DIR (reopen segments) or TATAMI_WET_DIR (build corpus) to run the 10M benchmark")
	}

	// Give the Windows measurement box a 1ms scheduler tick so the tail reflects the
	// engine rather than the default 15.6ms timer quantum (no-op off Windows).
	raiseTimerResolution()

	c, docs := buildShardedCorpus(t)
	t.Logf("cluster: %d shards, %d docs", c.NumShards(), docs)

	// Sweep the build's transient garbage once before the measured pass. The routing
	// index is flat and pointer-free (a sorted term blob plus column arrays, not a
	// map of pointers), so the collector traces a handful of slice headers and none
	// of the tens of millions of terms inside them. That is what lets the GC run
	// normally through the measured pass: a cycle costs almost nothing to scan, so it
	// reclaims each query's top-k slices without the second-long pauses a pointer-rich
	// routing map used to impose, and without needing to freeze the GC (which, frozen,
	// let the measured pass accumulate query garbage until the box ran out of memory).
	runtime.GC()

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
