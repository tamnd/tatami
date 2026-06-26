package tatami

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/tatami/search"
)

// This file proves the M8 headline on real data: a broker that routes a query to
// the shards that can contribute, prunes the rest by their impact bound, and keeps
// only a small working set of shards open answers in well under 10ms even when the
// corpus is split into many shards, the shape a hundred-thousand-shard fleet takes
// at the leaf. It splits a real ccrawl shard into a large fan of small shards, the
// tiny-shard tail repackaging is meant to consolidate, and serves them through a
// Cluster. Like the other real-data tests it is gated on a local file, so CI skips
// it cleanly (12-distributed-serving.md).

// numClusterShards splits the real shard into this many contiguous shards. A few
// hundred shards over a twenty-thousand-document shard puts each shard near the
// tiny end of the size distribution, which is exactly the case routing and the
// open-segment cache exist to handle: most shards never get opened.
const numClusterShards = 256

// clusterCacheCap is the open-segment cap for the scale broker. It sits below the
// shard count but above the working set of any single query, so steady-state
// serving runs warm while the cache still bounds the open-file count well under
// the shard total. Cold first-touch of a broad query that opens many shards costs
// the per-shard decode on top; that is the cost repackaging shrinks by collapsing
// the tiny-shard tail, and the synthetic eviction test exercises a cap below the
// working set on purpose.
const clusterCacheCap = 128

var (
	scaleClusterOnce  sync.Once
	scaleClusterC     *Cluster
	scaleClusterPaths []string
	scaleClusterErr   error
)

func loadScaleCluster(tb testing.TB) (*Cluster, []string) {
	scaleClusterOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			scaleClusterErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			scaleClusterErr = err
			return
		}
		dir, err := os.MkdirTemp("", "tatami-cluster-")
		if err != nil {
			scaleClusterErr = err
			return
		}
		per := (len(docs) + numClusterShards - 1) / numClusterShards
		var paths []string
		for s := 0; s < numClusterShards; s++ {
			lo := s * per
			if lo >= len(docs) {
				break
			}
			hi := lo + per
			if hi > len(docs) {
				hi = len(docs)
			}
			b := NewSearchBuilder()
			for _, d := range docs[lo:hi] {
				b.Add(d)
			}
			p := filepath.Join(dir, fmt.Sprintf("seg-%05d.tatami", s))
			if err := b.Write(p, WriterOptions{}); err != nil {
				scaleClusterErr = err
				return
			}
			paths = append(paths, p)
		}
		scaleClusterPaths = paths
		scaleClusterC, scaleClusterErr = OpenCluster(paths, ClusterOptions{CacheSize: clusterCacheCap})
	})
	if scaleClusterErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", scaleClusterErr)
	}
	return scaleClusterC, scaleClusterPaths
}

// TestClusterScaleLatency splits a real shard into many small shards, serves them
// through the routed, pruned, lazily cached broker, and asserts the keyword
// retrieval p99 stays under 10ms. It logs, per query, how many shards held a term
// versus how many the broker actually opened, the pruning that makes the budget
// hold as the shard count grows.
func TestClusterScaleLatency(t *testing.T) {
	c, _ := loadScaleCluster(t)
	t.Logf("cluster: %d shards, %d live docs, cache cap %d", c.NumShards(), c.NumDocs(), clusterCacheCap)

	const reps = 200
	var all []time.Duration
	for _, q := range benchQueries {
		var samples []time.Duration
		var lastStats QueryStats
		for i := 0; i < reps; i++ {
			start := time.Now()
			_, st, err := c.Query(q, 10)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
			lastStats = st
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v shards visited=%d/%d", q, p50, p99, lastStats.Visited, lastStats.Candidates)
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overallP99 := all[(len(all)*99)/100]
	t.Logf("overall p50=%v p99=%v over %d shards", all[len(all)/2], overallP99, c.NumShards())
	if overallP99 > 10*time.Millisecond {
		t.Fatalf("routed retrieval p99 %v exceeds the 10ms target", overallP99)
	}
}

// TestClusterScaleExact checks that the pruned broker returns exactly what a full
// fan-out over every shard returns, on the real corpus, so the latency win does
// not cost correctness.
func TestClusterScaleExact(t *testing.T) {
	c, paths := loadScaleCluster(t)
	for _, q := range benchQueries {
		for _, k := range []int{1, 10, 50} {
			got, _, err := c.Query(q, k)
			if err != nil {
				t.Fatal(err)
			}
			want := bruteForceQuery(t, paths, c.Routing(), q, k)
			if !sameHits(got, want) {
				t.Fatalf("query %q k=%d: pruned result differs from full fan-out\n got  %d hits\n want %d hits", q, k, len(got), len(want))
			}
		}
	}
}

// TestClusterScaleCacheBound checks the open-file count never exceeds the cache cap
// no matter how many shards a query touches, the property that lets one process
// serve a shard count it could never hold open at once.
func TestClusterScaleCacheBound(t *testing.T) {
	c, _ := loadScaleCluster(t)
	for _, q := range benchQueries {
		if _, _, err := c.Query(q, 10); err != nil {
			t.Fatal(err)
		}
		if c.CacheLen() > clusterCacheCap {
			t.Fatalf("after %q cache holds %d segments, over cap %d", q, c.CacheLen(), clusterCacheCap)
		}
	}
}

// TestClusterRoutingFootprint reports how the routing sidecar grows with shard
// granularity over the same corpus, the number behind both the routing-index
// footprint and the case for repackaging. The dominant cost of the routing index
// is one posting per (term, shard) pair, so splitting a fixed corpus into more
// shards replicates a term's posting across every shard it lands in and inflates
// the index, while merging shards collapses those postings back. The table makes
// that effect concrete: the same documents routed at a few granularities, with the
// sidecar size and the (term, shard) posting count at each (12-distributed-serving.md).
func TestClusterRoutingFootprint(t *testing.T) {
	src := shardPath()
	if _, err := os.Stat(src); err != nil {
		t.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", err)
	}
	docs, err := readMarkdownShard(src)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()

	t.Logf("%d documents, routed at several shard granularities", len(docs))
	t.Logf("%-8s %-10s %-14s %-14s %s", "shards", "terms", "postings", "sidecar", "bytes/posting")
	for _, n := range []int{1, 8, 32, 128, 254} {
		ri := buildRoutingAt(t, dir, docs, n)
		enc := search.EncodeRouting(ri)
		postings := countPostings(ri)
		t.Logf("%-8d %-10d %-14d %-14s %.2f",
			ri.NumShards(), ri.NumTerms(), postings, humanByteCount(len(enc)),
			float64(len(enc))/float64(postings))
	}
}

// buildRoutingAt splits docs into n contiguous shards, writes them, and returns
// the routing index over them, the same path OpenCluster builds.
func buildRoutingAt(t *testing.T, dir string, docs []SearchDoc, n int) *search.RoutingIndex {
	t.Helper()
	per := (len(docs) + n - 1) / n
	b := search.NewRoutingBuilder()
	for s := 0; s < n; s++ {
		lo := s * per
		if lo >= len(docs) {
			break
		}
		hi := lo + per
		if hi > len(docs) {
			hi = len(docs)
		}
		sb := NewSearchBuilder()
		for _, d := range docs[lo:hi] {
			sb.Add(d)
		}
		p := filepath.Join(dir, fmt.Sprintf("rf-%d-%05d.tatami", n, s))
		if err := sb.Write(p, WriterOptions{}); err != nil {
			t.Fatal(err)
		}
		seg, err := OpenSearch(p)
		if err != nil {
			t.Fatal(err)
		}
		b.AddShard(seg.Inverted())
		_ = seg.Close()
		_ = os.Remove(p)
	}
	return b.Build()
}

// countPostings sums the (term, shard) postings in a routing index, the quantity
// its size is proportional to.
func countPostings(ri *search.RoutingIndex) int64 {
	var n int64
	ri.EachPosting(func(term string, shard, df, maxFreq uint32) { n++ })
	return n
}

// humanByteCount formats a byte count with a binary unit suffix.
func humanByteCount(n int) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// BenchmarkClusterScaleQuery times the routed, pruned, lazily cached retrieval path
// over the many-shard corpus, the number the at-scale <10ms claim rests on.
func BenchmarkClusterScaleQuery(b *testing.B) {
	c, _ := loadScaleCluster(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := c.Query(benchQueries[i%len(benchQueries)], 10); err != nil {
			b.Fatal(err)
		}
	}
}
