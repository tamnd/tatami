package search

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// posting is a flattened (term, shard, df, maxFreq) row, the unit EachPosting
// emits, used to compare two indexes for exact equality.
type posting struct {
	term          string
	shard, df, mx uint32
}

func collectPostings(ri *RoutingIndex) []posting {
	var out []posting
	ri.EachPosting(func(term string, shard, df, mx uint32) {
		out = append(out, posting{term, shard, df, mx})
	})
	return out
}

// sameRouting asserts two indexes carry identical facts, comparing them through
// the public surface (totals, per-shard counts, and every posting) rather than
// the struct fields, since the mmap-backed index aliases the file while the
// in-heap one owns slices, so a struct compare would differ on backing even when
// the facts match.
func sameRouting(t *testing.T, want, got *RoutingIndex) {
	t.Helper()
	if want.NumDocs() != got.NumDocs() {
		t.Fatalf("NumDocs: want %d, got %d", want.NumDocs(), got.NumDocs())
	}
	if want.NumShards() != got.NumShards() {
		t.Fatalf("NumShards: want %d, got %d", want.NumShards(), got.NumShards())
	}
	if want.NumTerms() != got.NumTerms() {
		t.Fatalf("NumTerms: want %d, got %d", want.NumTerms(), got.NumTerms())
	}
	for s := range want.NumShards() {
		if want.ShardDocs(s) != got.ShardDocs(s) {
			t.Fatalf("ShardDocs(%d): want %d, got %d", s, want.ShardDocs(s), got.ShardDocs(s))
		}
	}
	wp, gp := collectPostings(want), collectPostings(got)
	if len(wp) != len(gp) {
		t.Fatalf("posting count: want %d, got %d", len(wp), len(gp))
	}
	for i := range wp {
		if wp[i] != gp[i] {
			t.Fatalf("posting %d: want %+v, got %+v", i, wp[i], gp[i])
		}
	}
}

// bigRouting builds an index with enough shards and terms to exercise the section
// alignment and offset tables, deterministically so the test is reproducible.
func bigRouting() *RoutingIndex {
	var shards []RoutingSource
	for s := range 17 {
		terms := map[string]fakeTerm{}
		for w := range 40 {
			// Skip some terms per shard so the postings columns are ragged, which is the
			// case the per-term postOff table has to get right.
			if (w+s)%3 == 0 {
				continue
			}
			terms[fmt.Sprintf("term%03d", w)] = fakeTerm{df: w + s + 1, maxFreq: uint32(1 + (w*s)%11)}
		}
		shards = append(shards, fakeShard{live: 100 + s, terms: terms})
	}
	return BuildRouting(shards)
}

func TestRoutingFileRoundtrip(t *testing.T) {
	ri := bigRouting()
	path := filepath.Join(t.TempDir(), "routing.bin")
	if err := WriteRoutingFile(path, ri); err != nil {
		t.Fatalf("WriteRoutingFile: %v", err)
	}

	got, err := OpenRoutingFile(path)
	if err != nil {
		t.Fatalf("OpenRoutingFile: %v", err)
	}
	defer got.Close()

	sameRouting(t, ri, got)

	// Route must agree on the mmap-backed index, since it is the query-time surface
	// and the whole point is the file serves identically to the rebuild.
	for _, q := range [][]string{
		{"term000"}, {"term001", "term002"}, {"term039"},
		{"term005", "term005"}, {"absent"}, {"term010", "absent", "term020"},
	} {
		want := ri.Route(q)
		have := got.Route(q)
		if len(want) != len(have) {
			t.Fatalf("Route(%v): want %d shards, got %d", q, len(want), len(have))
		}
		for i := range want {
			if want[i] != have[i] {
				t.Fatalf("Route(%v)[%d]: want %+v, got %+v", q, i, want[i], have[i])
			}
		}
	}
}

func TestRoutingFileEmpty(t *testing.T) {
	ri := BuildRouting(nil)
	path := filepath.Join(t.TempDir(), "empty.bin")
	if err := WriteRoutingFile(path, ri); err != nil {
		t.Fatalf("WriteRoutingFile: %v", err)
	}
	got, err := OpenRoutingFile(path)
	if err != nil {
		t.Fatalf("OpenRoutingFile: %v", err)
	}
	defer got.Close()
	sameRouting(t, ri, got)
}

// TestRoutingFileCloseIdempotent checks Close can be called more than once and on
// an in-heap index, so a caller can defer it unconditionally.
func TestRoutingFileCloseIdempotent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routing.bin")
	if err := WriteRoutingFile(path, bigRouting()); err != nil {
		t.Fatal(err)
	}
	got, err := OpenRoutingFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := got.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// An index that never mapped a file closes to nil.
	if err := bigRouting().Close(); err != nil {
		t.Fatalf("in-heap Close: %v", err)
	}
}

func TestRoutingFileBadHeader(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "good.bin")
	if err := WriteRoutingFile(good, bigRouting()); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(good)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"too small", func(b []byte) []byte { return b[:8] }},
		{"bad magic", func(b []byte) []byte { c := clone(b); c[0] = 'X'; return c }},
		{"bad version", func(b []byte) []byte { c := clone(b); c[4] = 9; return c }},
		{"truncated body", func(b []byte) []byte { return b[:len(b)-16] }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(dir, "bad.bin")
			if err := os.WriteFile(p, tc.mutate(raw), 0o644); err != nil {
				t.Fatal(err)
			}
			if ri, err := OpenRoutingFile(p); err == nil {
				ri.Close()
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
