package tatami

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// This file proves the M6 headline: keyword retrieval under 10ms on a real
// crawl. It reads a ccrawl-cli markdown shard (the same zstd Parquet ami and
// ccrawl write), builds a tatami search segment from it, and times queries
// against the block-max WAND loop. It is gated on a local data file, so CI,
// which does not carry crawl output, skips it cleanly.

// shardPath is the ccrawl markdown shard the latency tests run against. Override
// with TATAMI_BENCH_SHARD to point at a different Parquet file.
func shardPath() string {
	if p := os.Getenv("TATAMI_BENCH_SHARD"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "data", "ccrawl", "markdown", "CC-MAIN-2026-25", "000000.parquet")
}

// benchQueries are keywords with a range of selectivities: common web words that
// hit long posting lists and rarer ones that hit short lists, plus multi-term
// queries that exercise the WAND pivot logic.
var benchQueries = []string{
	"the",
	"data",
	"contact us",
	"privacy policy",
	"open source software",
	"machine learning model",
	"python",
	"download free",
}

var (
	realSegOnce sync.Once
	realSeg     *SearchSegment
	realSegErr  error
	realSegPath string
)

// loadRealSegment reads the shard once, builds a search segment in a temp dir,
// and caches the open handle for every test and benchmark in this file.
func loadRealSegment(tb testing.TB) *SearchSegment {
	realSegOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			realSegErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			realSegErr = err
			return
		}
		b := NewSearchBuilder()
		for _, d := range docs {
			b.Add(d)
		}
		dir, err := os.MkdirTemp("", "tatami-realseg-")
		if err != nil {
			realSegErr = err
			return
		}
		realSegPath = filepath.Join(dir, "shard.tatami")
		if err := b.Write(realSegPath, WriterOptions{}); err != nil {
			realSegErr = err
			return
		}
		realSeg, realSegErr = OpenSearch(realSegPath)
	})
	if realSegErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", realSegErr)
	}
	return realSeg
}

// readMarkdownShard reads the doc_id, url, and markdown columns of a ccrawl
// Parquet shard into SearchDocs. The markdown is the body; the url doubles as
// the title since the crawl schema carries no separate title column.
func readMarkdownShard(path string) ([]SearchDoc, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(in, info.Size())
	if err != nil {
		return nil, err
	}
	// Map column names to leaf positions; the schema is flat.
	idx := map[string]int{}
	for i, f := range pf.Schema().Fields() {
		idx[f.Name()] = i
	}
	docCol, urlCol, mdCol := idx["doc_id"], idx["url"], idx["markdown"]

	reader := parquet.NewGenericReader[any](pf)
	defer func() { _ = reader.Close() }()
	rows := make([]parquet.Row, 4096)
	var docs []SearchDoc
	for {
		n, rerr := reader.ReadRows(rows)
		for i := 0; i < n; i++ {
			row := rows[i]
			docs = append(docs, SearchDoc{
				DocID: row[docCol].String(),
				URL:   row[urlCol].String(),
				Title: row[urlCol].String(),
				Body:  row[mdCol].String(),
			})
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	return docs, nil
}

// TestRealShardLatency builds a search segment from a real ccrawl shard and
// asserts that the keyword retrieval p99 stays under 10ms, the M6 service goal.
// It reports the full latency spread so a regression is visible, not just a
// pass/fail.
func TestRealShardLatency(t *testing.T) {
	seg := loadRealSegment(t)
	t.Logf("segment: %d docs, %d terms", seg.NumDocs(), seg.NumTerms())

	const reps = 200
	var all []time.Duration
	for _, q := range benchQueries {
		var samples []time.Duration
		for i := 0; i < reps; i++ {
			start := time.Now()
			_ = seg.Query(q, 10)
			samples = append(samples, time.Since(start))
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v hits=%d", q, p50, p99, len(seg.Query(q, 10)))
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overallP99 := all[(len(all)*99)/100]
	t.Logf("overall p50=%v p99=%v", all[len(all)/2], overallP99)
	if overallP99 > 10*time.Millisecond {
		t.Fatalf("retrieval p99 %v exceeds the 10ms target", overallP99)
	}
}

// BenchmarkRealShardQuery times the retrieval-only hot path (no stored-field
// fetch) across the mixed query set, the number the <10ms claim rests on.
func BenchmarkRealShardQuery(b *testing.B) {
	seg := loadRealSegment(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = seg.Query(benchQueries[i%len(benchQueries)], 10)
	}
}

// BenchmarkRealShardSearch times the full query-to-results path including the
// columnar fetch of each hit's url and title.
func BenchmarkRealShardSearch(b *testing.B) {
	seg := loadRealSegment(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := seg.Search(benchQueries[i%len(benchQueries)], 10); err != nil {
			b.Fatal(err)
		}
	}
}
