package tatami

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

// This file proves the M9 headline on real data: a search-only segment that drops
// the document body saves most of the file, and a tree of broker leaves answers a
// fleet-wide query under 10ms at a shard count no single process could hold open.
// It reuses the ccrawl markdown shard the other real-data tests run against and is
// gated on that local file, so CI skips it cleanly (13-search-only-and-scale.md).

// fleetShards is how many shards the real corpus is split into for the scale tests.
// It matches the granularity the M8 cluster tests use, so the per-shard size sits
// near the tiny end of the distribution repackaging consolidates.
const fleetShards = 256

// shardsPerLeaf is how many shards one leaf broker serves. A leaf holds a working
// set of segments and routes over them; the fleet is many such leaves. This sets
// the fan-out: fleetShards / shardsPerLeaf leaves over the real corpus.
const shardsPerLeaf = 32

var (
	scaleFleetOnce  sync.Once
	scaleFleetPaths []string
	scaleFleetDir   string
	scaleFleetDocs  int
	scaleFleetErr   error
)

// loadFleetShards reads the real shard once and writes it out as fleetShards
// search-only (snippet) segments in a temp dir, the shape a fleet actually serves.
// It caches the paths for every test in this file.
func loadFleetShards(tb testing.TB) []string {
	scaleFleetOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			scaleFleetErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			scaleFleetErr = err
			return
		}
		scaleFleetDocs = len(docs)
		dir, err := os.MkdirTemp("", "tatami-fleet-")
		if err != nil {
			scaleFleetErr = err
			return
		}
		scaleFleetDir = dir
		per := (len(docs) + fleetShards - 1) / fleetShards
		for s := 0; s < fleetShards; s++ {
			lo := s * per
			if lo >= len(docs) {
				break
			}
			hi := lo + per
			if hi > len(docs) {
				hi = len(docs)
			}
			b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true})
			for _, d := range docs[lo:hi] {
				b.Add(d)
			}
			p := filepath.Join(dir, fmt.Sprintf("seg-%05d.tatami", s))
			if err := b.Write(p, WriterOptions{}); err != nil {
				scaleFleetErr = err
				return
			}
			scaleFleetPaths = append(scaleFleetPaths, p)
		}
	})
	if scaleFleetErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", scaleFleetErr)
	}
	return scaleFleetPaths
}

// openFleet partitions the cached fleet shards into leaves of shardsPerLeaf and
// returns an aggregator over them, the tree-of-brokers tier under test.
func openFleet(tb testing.TB) *Aggregator {
	paths := loadFleetShards(tb)
	var leaves []*Cluster
	for lo := 0; lo < len(paths); lo += shardsPerLeaf {
		hi := lo + shardsPerLeaf
		if hi > len(paths) {
			hi = len(paths)
		}
		c, err := OpenCluster(paths[lo:hi], ClusterOptions{})
		if err != nil {
			tb.Fatal(err)
		}
		leaves = append(leaves, c)
	}
	return OpenAggregator(leaves)
}

// TestSnippetSpaceSaving builds the same real corpus twice, once as full-document
// segments that keep the markdown body and once as search-only segments that keep
// only a short snippet, and reports the bytes each shape costs. Search needs only
// the index plus the display fields, so the body is dead weight once the postings
// are built; the table makes the saving concrete on real crawl text.
func TestSnippetSpaceSaving(t *testing.T) {
	src := shardPath()
	if _, err := os.Stat(src); err != nil {
		t.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", err)
	}
	docs, err := readMarkdownShard(src)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()

	fullPath := filepath.Join(dir, "full.tatami")
	fb := NewSearchBuilder()
	for _, d := range docs {
		fb.Add(d)
	}
	if err := fb.Write(fullPath, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	snipPath := filepath.Join(dir, "snip.tatami")
	sb := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true})
	for _, d := range docs {
		sb.Add(d)
	}
	if err := sb.Write(snipPath, WriterOptions{}); err != nil {
		t.Fatal(err)
	}

	var rawBody int64
	for _, d := range docs {
		rawBody += int64(len(d.Body))
	}
	full := fileSize(t, fullPath)
	snip := fileSize(t, snipPath)
	saved := full - snip

	t.Logf("%d documents, %s of raw markdown body", len(docs), humanByteCount(int(rawBody)))
	t.Logf("%-22s %s", "full-document segment", humanByteCount(int(full)))
	t.Logf("%-22s %s", "search-only segment", humanByteCount(int(snip)))
	t.Logf("%-22s %s (%.1f%% smaller)", "saved", humanByteCount(int(saved)), 100*float64(saved)/float64(full))

	if snip >= full {
		t.Fatalf("search-only segment (%d B) is not smaller than full-document (%d B)", snip, full)
	}
}

// TestAggregatorScaleLatency serves the real corpus as a fleet of leaf brokers
// behind an aggregator and asserts the fleet-wide keyword search p99 stays under
// 10ms. The fan-out runs the leaves concurrently, so the wall clock is the slowest
// leaf plus the root merge, not the sum across leaves.
func TestAggregatorScaleLatency(t *testing.T) {
	agg := openFleet(t)
	defer agg.Close()
	t.Logf("fleet: %d leaves, %d shards, %d live docs", agg.NumLeaves(), agg.NumShards(), agg.NumDocs())

	// Warm every leaf's segment and column caches before timing, the warm-cache
	// premise the M8 cluster latency test also runs under. A cold first touch
	// decodes a shard's inverted region and is not the steady-state serving cost.
	for w := 0; w < 3; w++ {
		for _, q := range benchQueries {
			if _, _, err := agg.Search(q, 10); err != nil {
				t.Fatal(err)
			}
		}
	}

	const reps = 200
	var all []time.Duration
	for _, q := range benchQueries {
		var samples []time.Duration
		var last AggStats
		for i := 0; i < reps; i++ {
			start := time.Now()
			_, st, err := agg.Search(q, 10)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
			last = st
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v shards visited=%d/%d", q, p50, p99, last.ShardsVisited, last.Candidates)
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overallP99 := all[(len(all)*99)/100]
	t.Logf("overall p50=%v p99=%v over %d leaves", all[len(all)/2], overallP99, agg.NumLeaves())
	if overallP99 > 10*time.Millisecond {
		t.Fatalf("fleet search p99 %v exceeds the 10ms target", overallP99)
	}
}

// TestAggregatorScaleExact checks that the fleet's top-k equals a single broker's
// top-k over every shard, on the real corpus, so fanning out and merging changes
// nothing about the answer.
func TestAggregatorScaleExact(t *testing.T) {
	paths := loadFleetShards(t)
	single, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	agg := openFleet(t)
	defer agg.Close()

	for _, q := range benchQueries {
		for _, k := range []int{1, 10, 50} {
			want, _, err := single.Search(q, k)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := agg.Search(q, k)
			if err != nil {
				t.Fatal(err)
			}
			if !sameResults(got, want) {
				t.Fatalf("q=%q k=%d: fleet result differs from single broker\n got %d\n want %d", q, k, len(got), len(want))
			}
		}
	}
}

// fleetFanout is how many children one aggregator merges. A fleet of leaves is too
// many for one root to merge in budget, so aggregators stack into a tree: each tier
// merges fanout partial top-k lists in parallel and hands its own top-k up. The
// merge stays exact at every tier because a global top-k member ranks no lower than
// k within any tier it passes through, so each tier's top-k carries it upward.
const fleetFanout = 64

// TestFleetScaleProjection proves the 100,000-shard claim without standing up
// 100,000 shards. A fan-out runs its children in parallel, so a tier adds only its
// own merge to the latency, and the fleet latency is one leaf's search plus the
// merge at each tier of the broker tree from leaf to root. It measures both terms
// on real data: the per-leaf search p99 over the real corpus, and the real merge
// cost at the fan-out each tier runs. It reports the single-root cost too, the wall
// the tree exists to clear, then projects the tree total and asserts the
// 100,000-shard fleet still answers under 10ms.
func TestFleetScaleProjection(t *testing.T) {
	agg := openFleet(t)
	defer agg.Close()
	stats := agg.Stats()

	// Per-leaf search p99: the dominant term, and flat in the fleet size because
	// leaves run in parallel. Measure every leaf over every query.
	const reps = 100
	var leafLat []time.Duration
	for _, leaf := range agg.leaves {
		for _, q := range benchQueries {
			for i := 0; i < reps; i++ {
				start := time.Now()
				if _, _, err := leaf.SearchWith(q, 10, stats); err != nil {
					t.Fatal(err)
				}
				leafLat = append(leafLat, time.Since(start))
			}
		}
	}
	sort.Slice(leafLat, func(i, j int) bool { return leafLat[i] < leafLat[j] })
	leafP99 := leafLat[(len(leafLat)*99)/100]
	t.Logf("per-leaf search p99 = %v (%d shards/leaf, flat in fleet size)", leafP99, shardsPerLeaf)
	t.Logf("aggregator fan-out = %d children per tier", fleetFanout)

	// Merge cost at each fleet size: the single-root cost (one merge over every
	// leaf, the naive design) next to the tree cost (one merge per tier at the
	// fan-out, the design that scales). Both are measured directly on real merge
	// code, not extrapolated.
	t.Logf("%-12s %-10s %-12s %-14s %-7s %-12s %s", "fleet", "leaves", "leaf p99", "single-root", "tiers", "tree merge", "projected p99")
	const k = 10
	var projected100k time.Duration
	for _, fleet := range []int{1000, 10000, 100000} {
		leaves := (fleet + shardsPerLeaf - 1) / shardsPerLeaf
		singleRoot := medianMerge(syntheticLeafLists(leaves, k), k, 50)
		tiers, treeMerge := treeMergeCost(leaves, fleetFanout, k)
		total := leafP99 + treeMerge
		t.Logf("%-12d %-10d %-12v %-14v %-7d %-12v %v", fleet, leaves, leafP99, singleRoot, tiers, treeMerge, total)
		if fleet == 100000 {
			projected100k = total
		}
	}
	if projected100k > 10*time.Millisecond {
		t.Fatalf("projected 100,000-shard fleet p99 %v exceeds the 10ms target", projected100k)
	}
}

// treeMergeCost models the merge latency of a balanced aggregator tree over
// totalLeaves leaves with the given fan-out. Each tier merges in parallel, so the
// tree adds one merge per tier, sized to the fan-out, and the tiers run in
// sequence from leaf to root. It returns the tier count and the summed merge cost,
// measuring each tier's merge on real merge code at its real fan-out.
func treeMergeCost(totalLeaves, fanout, k int) (tiers int, total time.Duration) {
	n := totalLeaves
	for n > 1 {
		groups := (n + fanout - 1) / fanout
		total += medianMerge(syntheticLeafLists(min(fanout, n), k), k, 50)
		tiers++
		n = groups
	}
	return tiers, total
}

// syntheticLeafLists builds the partial top-k lists a root merge sees from `leaves`
// leaves, k results each, with stable doc_ids spread so the dedup map does real
// work and a handful collide across leaves the way a recrawled page would.
func syntheticLeafLists(leaves, k int) [][]SearchResult {
	lists := make([][]SearchResult, leaves)
	for l := 0; l < leaves; l++ {
		list := make([]SearchResult, k)
		for i := 0; i < k; i++ {
			// One id per (leaf, rank), plus a shared id at rank 0 every few leaves
			// so dedup collapses some duplicates, as a recrawl would.
			id := fmt.Sprintf("doc-%06d-%03d", l, i)
			if i == 0 && l%8 == 0 {
				id = "doc-shared-000"
			}
			list[i] = SearchResult{
				DocID: id,
				URL:   "https://example.com/" + id,
				Title: id,
				Score: float32(k-i) + float32(l%3)*0.01,
			}
		}
		lists[l] = list
	}
	return lists
}

// medianMerge times mergeLeafResults over the given lists and returns the median of
// reps runs, the real root-merge cost at this leaf count.
func medianMerge(lists [][]SearchResult, k, reps int) time.Duration {
	samples := make([]time.Duration, reps)
	for i := 0; i < reps; i++ {
		start := time.Now()
		_ = mergeLeafResults(lists, k)
		samples[i] = time.Since(start)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples[len(samples)/2]
}

// fileSize returns the size of a file in bytes.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

// BenchmarkAggregatorScaleSearch times the full fleet search path: concurrent leaf
// fan-out, per-leaf routing and pruning against fleet statistics, and the root
// merge, over the real corpus split into a fleet of leaves.
func BenchmarkAggregatorScaleSearch(b *testing.B) {
	agg := openFleet(b)
	defer agg.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := agg.Search(benchQueries[i%len(benchQueries)], 10); err != nil {
			b.Fatal(err)
		}
	}
}
