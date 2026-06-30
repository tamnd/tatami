package tatami

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/tamnd/tatami/search"
)

// streamDocs builds a deterministic synthetic corpus. A small shared vocabulary
// drives multi-posting lists (so the merge has real work) while a per-doc unique
// term exercises the singleton path, the same mix the in-memory round-trip test
// uses.
func streamDocs(n int, seed int64) []SearchDoc {
	rng := rand.New(rand.NewSource(seed))
	vocab := []string{"tatami", "search", "columnar", "index", "crawl", "segment", "posting", "block", "scale", "merge"}
	docs := make([]SearchDoc, n)
	for d := range docs {
		var body bytes.Buffer
		for _, w := range vocab {
			for r := rng.Intn(4); r > 0; r-- {
				body.WriteString(w)
				body.WriteByte(' ')
			}
		}
		fmt.Fprintf(&body, "u%08x", d) // unique singleton term
		docs[d] = SearchDoc{
			DocID: fmt.Sprintf("doc-%d", d),
			URL:   fmt.Sprintf("https://example.com/%d", d),
			Title: fmt.Sprintf("Title %d %s", d, vocab[d%len(vocab)]),
			Body:  body.String(),
		}
	}
	return docs
}

// buildInMemory seals docs with the in-memory SearchBuilder and returns the path.
func buildInMemory(t *testing.T, dir string, docs []SearchDoc, opts SearchBuilderOptions) string {
	t.Helper()
	b := NewSearchBuilderWith(opts)
	for _, d := range docs {
		b.Add(d)
	}
	path := filepath.Join(dir, "mem.tatami")
	if err := b.Write(path, WriterOptions{}); err != nil {
		t.Fatalf("in-memory write: %v", err)
	}
	return path
}

// buildStreaming seals docs with the streaming builder at the given batch budget
// and returns the path. A small budget forces many spills.
func buildStreaming(t *testing.T, dir string, docs []SearchDoc, snippet bool, budget int) string {
	t.Helper()
	path := filepath.Join(dir, fmt.Sprintf("stream-%d.tatami", budget))
	sb, err := NewStreamingSearchBuilder(path, dir, StreamingOptions{
		Snippet:          snippet,
		BatchBudgetBytes: budget,
	})
	if err != nil {
		t.Fatalf("new streaming builder: %v", err)
	}
	for _, d := range docs {
		sb.Add(d)
	}
	if err := sb.Close(); err != nil {
		t.Fatalf("streaming close: %v", err)
	}
	return path
}

// TestM3StreamEncoderByteIdentity is the core byte-identity gate: feeding the same
// term stream to StreamEncoder must produce exactly the bytes EncodeInverted
// produces from a fully built Inverted. This is checked at the inverted-run level
// so it is independent of forward-column row-group framing.
func TestM3StreamEncoderByteIdentity(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	vocab := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	ib := search.NewInvertedBuilder()
	termPostings := map[string][]search.Posting{}
	const n = 4000
	for d := 0; d < n; d++ {
		tf := map[string]uint32{}
		for _, w := range vocab {
			if rng.Float64() < 0.25 {
				tf[w] = uint32(1 + rng.Intn(6))
			}
		}
		tf[fmt.Sprintf("u%08x", d)] = 1 // singleton
		ib.AddDocument(tf)
		for term, fr := range tf {
			// Docs are added in ascending order, so per-term postings stay sorted.
			termPostings[term] = append(termPostings[term], search.Posting{Doc: search.DocID(d), Frequency: fr})
		}
	}
	inv, err := ib.Build()
	if err != nil {
		t.Fatal(err)
	}
	td0, pp0, sk0 := search.EncodeInverted(inv)

	// Drive StreamEncoder from the same postings in ascending term order, the
	// stream the external merge yields.
	terms := make([]string, 0, len(termPostings))
	for term := range termPostings {
		terms = append(terms, term)
	}
	sort.Strings(terms)
	enc := search.NewStreamEncoder()
	for _, term := range terms {
		if err := enc.Add(term, termPostings[term]); err != nil {
			t.Fatal(err)
		}
	}
	td1, pp1, sk1, err := enc.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(td0, td1) {
		t.Fatalf("term dict differs: %d vs %d bytes", len(td0), len(td1))
	}
	if !bytes.Equal(pp0, pp1) {
		t.Fatalf("posting payloads differ: %d vs %d bytes", len(pp0), len(pp1))
	}
	if !bytes.Equal(sk0, sk1) {
		t.Fatalf("skip tables differ: %d vs %d bytes", len(sk0), len(sk1))
	}
}

// TestM3StreamingByteIdentitySingleBatch checks that a streaming build that never
// spills (budget larger than the corpus) is byte-for-byte identical to the
// in-memory build at the whole-file level: same single forward Append, same
// attached inverted runs, so the same framing.
func TestM3StreamingByteIdentitySingleBatch(t *testing.T) {
	dir := t.TempDir()
	docs := streamDocs(2000, 7)
	mem := buildInMemory(t, dir, docs, SearchBuilderOptions{})
	stream := buildStreaming(t, dir, docs, false, 1<<30) // 1 GiB: no spill

	a, err := os.ReadFile(mem)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(stream)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("single-batch streamed file differs from in-memory: %d vs %d bytes", len(a), len(b))
	}
}

// queryAll runs every probe query against a segment and returns the flat hit list,
// the observable retrieval behavior used for semantic identity.
func queryAll(t *testing.T, path string) []search.Hit {
	t.Helper()
	s, err := OpenSearch(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var out []search.Hit
	for _, q := range []string{"tatami", "search index", "columnar segment posting", "scale merge block", "u00000001"} {
		out = append(out, s.Query(q, 20)...)
	}
	return out
}

func sameStreamHits(a, b []search.Hit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Doc != b[i].Doc || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}

// TestM3SpillCountInvariance is the semantic-identity gate across spill counts:
// the streaming build must return identical retrieval whether it spills once or
// many times, and identical to the in-memory build.
func TestM3SpillCountInvariance(t *testing.T) {
	dir := t.TempDir()
	docs := streamDocs(6000, 11)
	want := queryAll(t, buildInMemory(t, dir, docs, SearchBuilderOptions{}))

	// Budgets chosen to force 1, a handful, and many spills.
	for _, budget := range []int{1 << 30, 1 << 20, 64 << 10, 8 << 10} {
		sdir := filepath.Join(dir, fmt.Sprintf("b%d", budget))
		if err := os.MkdirAll(sdir, 0o755); err != nil {
			t.Fatal(err)
		}
		got := queryAll(t, buildStreaming(t, sdir, docs, false, budget))
		if !sameStreamHits(want, got) {
			t.Fatalf("budget %d: retrieval differs from in-memory", budget)
		}
	}
}

// TestM3SnippetMode checks the search-only forward store streams correctly: the
// streamed snippet segment matches the in-memory snippet segment byte-for-byte in
// the single-batch case.
func TestM3SnippetMode(t *testing.T) {
	dir := t.TempDir()
	docs := streamDocs(1500, 13)
	mem := buildInMemory(t, dir, docs, SearchBuilderOptions{Snippet: true})
	stream := buildStreaming(t, dir, docs, true, 1<<30)
	a, _ := os.ReadFile(mem)
	b, _ := os.ReadFile(stream)
	if !bytes.Equal(a, b) {
		t.Fatalf("snippet single-batch streamed file differs: %d vs %d bytes", len(a), len(b))
	}
}

// TestM3EdgeCorpora runs the differential gate over corpora that stress the merge
// boundaries: a singleton corpus, a term present in every doc (full DF), a corpus
// sized to an exact block multiple, and one whose spill boundary straddles a term.
func TestM3EdgeCorpora(t *testing.T) {
	cases := []struct {
		name  string
		build func() []SearchDoc
	}{
		{"singleton", func() []SearchDoc { return streamDocs(1, 1) }},
		{"exact-block-multiple", func() []SearchDoc { return streamDocs(search.BlockSize*4, 2) }},
		{"full-df", func() []SearchDoc {
			docs := streamDocs(500, 3)
			for i := range docs {
				docs[i].Body = "everywhere " + docs[i].Body
			}
			return docs
		}},
		{"spill-straddle", func() []SearchDoc { return streamDocs(777, 4) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			docs := tc.build()
			want := queryAll(t, buildInMemory(t, dir, docs, SearchBuilderOptions{}))
			sdir := filepath.Join(dir, "s")
			if err := os.MkdirAll(sdir, 0o755); err != nil {
				t.Fatal(err)
			}
			// A tiny budget so even small corpora spill across term boundaries.
			got := queryAll(t, buildStreaming(t, sdir, docs, false, 4<<10))
			if !sameStreamHits(want, got) {
				t.Fatalf("%s: retrieval differs from in-memory", tc.name)
			}
		})
	}
}

// TestM3Fuzz drives randomly sized corpora at random small budgets through the
// differential gate, so spill boundaries fall at varied term and docid offsets.
func TestM3Fuzz(t *testing.T) {
	if testing.Short() {
		t.Skip("fuzz pass skipped in -short")
	}
	rng := rand.New(rand.NewSource(202066))
	for trial := 0; trial < 12; trial++ {
		dir := t.TempDir()
		n := 50 + rng.Intn(3000)
		docs := streamDocs(n, int64(trial*7+1))
		want := queryAll(t, buildInMemory(t, dir, docs, SearchBuilderOptions{}))
		sdir := filepath.Join(dir, "s")
		if err := os.MkdirAll(sdir, 0o755); err != nil {
			t.Fatal(err)
		}
		budget := 1 << (10 + rng.Intn(8)) // 1 KiB .. 128 KiB
		got := queryAll(t, buildStreaming(t, sdir, docs, false, budget))
		if !sameStreamHits(want, got) {
			t.Fatalf("trial %d (n=%d budget=%d): retrieval differs", trial, n, budget)
		}
	}
}
