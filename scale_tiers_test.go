package tatami

// Multi-tier real-data benchmark for the scale redesign (Spec 2066, scale/09).
// It builds tatami search segments from real Common Crawl WET text at 10k, 100k,
// and 1M documents and enforces the headline claim at each tier: keyword
// retrieval p99 under 10 ms. The numbers it logs (build time, on-disk size, open
// time, per-query p50 and p99) are the measured evidence the implementation notes
// quote.
//
// It reads the WET Parquet shards ami and ccrawl-cli write (record_id, url, text)
// from a local directory, so CI skips it cleanly. Point it elsewhere with
// TATAMI_WET_DIR. Run one tier:
//
//	TATAMI_WET_DIR=$HOME/data/ccrawl/wet-parquet go test -run TestScaleTier/1M -v

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// wetDir is the directory of WET Parquet shards the tier benchmark reads.
func wetDir() string {
	if p := os.Getenv("TATAMI_WET_DIR"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "data", "ccrawl", "wet-parquet")
}

// eachWET streams up to limit documents across the directory's Parquet shards, in
// filename order, mapping the WET columns onto SearchDoc and handing each to fn.
// It never accumulates the documents, so the 10M tier (whose raw text is tens of
// gigabytes, more than this box's RAM) flows straight into the streaming builder
// instead of materializing a giant slice. It stops as soon as it has fed limit
// docs so a tier costs only the files it needs.
func eachWET(dir string, limit int, fn func(SearchDoc)) (int, error) {
	matches, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		return 0, err
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return 0, fmt.Errorf("no parquet shards in %s", dir)
	}
	seen := 0
	for _, path := range matches {
		if seen >= limit {
			break
		}
		got, err := eachWETFile(path, limit-seen, fn)
		if err != nil {
			return seen, err
		}
		seen += got
	}
	if seen < limit {
		return seen, fmt.Errorf("only %d docs available in %s, need %d", seen, dir, limit)
	}
	return seen, nil
}

func eachWETFile(path string, limit int, fn func(SearchDoc)) (int, error) {
	in, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(in, info.Size())
	if err != nil {
		return 0, err
	}
	idx := map[string]int{}
	for i, f := range pf.Schema().Fields() {
		idx[f.Name()] = i
	}
	idCol, urlCol, textCol := idx["record_id"], idx["url"], idx["text"]

	reader := parquet.NewGenericReader[any](pf)
	defer func() { _ = reader.Close() }()
	rows := make([]parquet.Row, 4096)
	fed := 0
	for fed < limit {
		n, rerr := reader.ReadRows(rows)
		for i := 0; i < n && fed < limit; i++ {
			row := rows[i]
			fn(SearchDoc{
				DocID: row[idCol].String(),
				URL:   row[urlCol].String(),
				Title: row[urlCol].String(),
				Body:  row[textCol].String(),
			})
			fed++
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return fed, rerr
		}
	}
	return fed, nil
}

// buildTierSegment reads limit real docs, builds a segment, and returns the open
// handle plus the build time and on-disk size. The caller closes the handle.
func buildTierSegment(tb testing.TB, limit int) (seg *SearchSegment, build time.Duration, onDisk int64, open time.Duration) {
	dir := wetDir()

	tmp, err := os.MkdirTemp("", "tatami-tier-")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = os.RemoveAll(tmp) })
	path := filepath.Join(tmp, "tier.tatami")

	// Search-only segments are the realistic 100M-docs/machine config (scale/13):
	// the body is tokenized into the inverted index and then dropped, so the
	// builder does not hold gigabytes of WET text resident. Query latency, the
	// thing the <10ms gate measures, touches only the inverted index, so this is
	// the faithful configuration for the scale claim.
	//
	// The segment is built through the streaming external-merge writer (scale/06,
	// M3), the production path at scale: it spills sorted runs at a byte budget and
	// k-way merges them, so build memory stays bounded by the batch budget instead
	// of holding the whole posting map resident. The in-memory builder cannot reach
	// the 1M and 10M tiers on this box; the streaming builder can.
	t0 := nowMono()
	sb, err := NewStreamingSearchBuilder(path, tmp, StreamingOptions{Snippet: true})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := eachWET(dir, limit, func(d SearchDoc) { sb.Add(d) }); err != nil {
		tb.Skipf("WET corpus unavailable (%v); set TATAMI_WET_DIR to run", err)
	}
	if err := sb.Close(); err != nil {
		tb.Fatal(err)
	}
	build = nowMono().Sub(t0)

	fi, err := os.Stat(path)
	if err != nil {
		tb.Fatal(err)
	}
	onDisk = fi.Size()

	t1 := nowMono()
	seg, err = OpenSearch(path)
	if err != nil {
		tb.Fatal(err)
	}
	open = nowMono().Sub(t1)
	tb.Cleanup(func() { _ = seg.Close() })
	return seg, build, onDisk, open
}

// nowMono is a small monotonic-clock helper so the tier timings do not depend on
// wall-clock adjustments.
func nowMono() time.Time { return time.Now() }

// TestScaleTier builds a segment at each tier and enforces the p99 < 10 ms gate
// over the mixed query set, logging the build, size, open, and latency numbers.
func TestScaleTier(t *testing.T) {
	for _, tier := range []struct {
		name string
		docs int
	}{
		{"10k", 10_000},
		{"100k", 100_000},
		{"1M", 1_000_000},
		{"10M", 10_000_000},
	} {
		t.Run(tier.name, func(t *testing.T) {
			seg, build, onDisk, open := buildTierSegment(t, tier.docs)
			t.Logf("docs=%d terms=%d build=%v on-disk=%s open=%v",
				seg.NumDocs(), seg.NumTerms(), build.Round(time.Millisecond), humanByteCount(int(onDisk)), open.Round(time.Millisecond))

			const iters = 200
			var all []time.Duration
			for _, q := range benchQueries {
				samples := make([]time.Duration, 0, iters)
				for range iters {
					t0 := nowMono()
					_ = seg.Query(q, 10)
					samples = append(samples, nowMono().Sub(t0))
				}
				slices.Sort(samples)
				p50 := samples[len(samples)/2]
				p99 := samples[(len(samples)*99)/100]
				t.Logf("  %-26q p50=%-10v p99=%-10v hits=%d", q, p50, p99, len(seg.Query(q, 10)))
				all = append(all, samples...)
			}
			slices.Sort(all)
			p50 := all[len(all)/2]
			p99 := all[(len(all)*99)/100]
			t.Logf("  overall p50=%v p99=%v", p50, p99)
			if p99 > 10*time.Millisecond {
				t.Fatalf("tier %s p99 %v exceeds the 10ms target", tier.name, p99)
			}
		})
	}
}
