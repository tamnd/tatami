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

// This file proves the M7 headline: keyword retrieval stays under 10ms when the
// same real crawl is served not as one segment but as many, the way a leaf node
// holds a manifest of segments at web scale. It splits a ccrawl shard into a
// fleet of segments, serves them through an Index, and times the fan-out plus
// global-merge path. Like the single-segment test it is gated on a local data
// file, so CI skips it cleanly.

// numScaleSegments is how many segments the shard is split into. Twenty puts the
// per-segment doc count near the spec's floor for a young tier and forces the
// fan-out and global top-k merge to do real work on every query.
const numScaleSegments = 20

var (
	scaleIndexOnce sync.Once
	scaleIndex     *Index
	scaleIndexErr  error
)

// loadScaleIndex reads the shard once, splits it into numScaleSegments contiguous
// segments written to a temp dir, opens them as one Index, and caches the handle.
func loadScaleIndex(tb testing.TB) *Index {
	scaleIndexOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			scaleIndexErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			scaleIndexErr = err
			return
		}
		dir, err := os.MkdirTemp("", "tatami-scale-")
		if err != nil {
			scaleIndexErr = err
			return
		}
		per := (len(docs) + numScaleSegments - 1) / numScaleSegments
		var paths []string
		for s := 0; s < numScaleSegments; s++ {
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
			p := filepath.Join(dir, fmt.Sprintf("seg-%03d.tatami", s))
			if err := b.Write(p, WriterOptions{}); err != nil {
				scaleIndexErr = err
				return
			}
			paths = append(paths, p)
		}
		scaleIndex, scaleIndexErr = OpenIndex(paths)
	})
	if scaleIndexErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", scaleIndexErr)
	}
	return scaleIndex
}

// TestScaleIndexLatency splits a real ccrawl shard into many segments, serves
// them as one Index, and asserts the fan-out keyword retrieval p99 stays under
// 10ms, the M7 service goal at segment scale. A leaf holding a manifest of
// segments must serve each query against all of them and merge a global top-k;
// this proves that path stays well inside the budget.
func TestScaleIndexLatency(t *testing.T) {
	ix := loadScaleIndex(t)
	t.Logf("index: %d segments, %d live docs", len(ix.Segments()), ix.NumDocs())

	const reps = 200
	var all []time.Duration
	for _, q := range benchQueries {
		var samples []time.Duration
		for i := 0; i < reps; i++ {
			start := time.Now()
			_ = ix.Query(q, 10)
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v hits=%d", q, p50, p99, len(ix.Query(q, 10)))
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overallP99 := all[(len(all)*99)/100]
	t.Logf("overall p50=%v p99=%v", all[len(all)/2], overallP99)
	if overallP99 > 10*time.Millisecond {
		t.Fatalf("fan-out retrieval p99 %v exceeds the 10ms target", overallP99)
	}
}

// BenchmarkScaleIndexQuery times the multi-segment fan-out retrieval path across
// the mixed query set, the number the at-scale <10ms claim rests on.
func BenchmarkScaleIndexQuery(b *testing.B) {
	ix := loadScaleIndex(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ix.Query(benchQueries[i%len(benchQueries)], 10)
	}
}

// BenchmarkScaleIndexSearch times the full fan-out query-to-results path,
// including dedup by stable doc_id and the columnar fetch of each hit's fields.
func BenchmarkScaleIndexSearch(b *testing.B) {
	ix := loadScaleIndex(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ix.Search(benchQueries[i%len(benchQueries)], 10); err != nil {
			b.Fatal(err)
		}
	}
}
