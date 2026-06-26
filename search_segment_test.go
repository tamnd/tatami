package tatami

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestSearchSegmentEndToEnd builds a search segment from a small document set,
// seals it to a tatami file, reopens it, and checks that a query returns the
// right documents in rank order with their stored url and title fetched back
// from the forward columns. This is the M6 contract: a tatami file that is both
// a forward store and a search index.
func TestSearchSegmentEndToEnd(t *testing.T) {
	docs := []SearchDoc{
		{DocID: "d0", URL: "https://a.example/one", Title: "Format notes", Body: "tatami is a columnar file format"},
		{DocID: "d1", URL: "https://b.example/two", Title: "Retrieval notes", Body: "wand skips blocks during retrieval over postings"},
		{DocID: "d2", URL: "https://c.example/both", Title: "Engine overview", Body: "tatami search tatami search engine over a columnar store"},
		{DocID: "d3", URL: "https://d.example/cooking", Title: "Rice and miso", Body: "a recipe with no relevance to the query at all"},
	}
	b := NewSearchBuilder()
	for _, d := range docs {
		b.Add(d)
	}
	if b.NumDocs() != len(docs) {
		t.Fatalf("NumDocs %d want %d", b.NumDocs(), len(docs))
	}

	path := filepath.Join(t.TempDir(), "seg.tatami")
	if err := b.Write(path, WriterOptions{}); err != nil {
		t.Fatal(err)
	}

	seg, err := OpenSearch(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seg.Close() }()

	if seg.NumDocs() != len(docs) {
		t.Fatalf("segment NumDocs %d want %d", seg.NumDocs(), len(docs))
	}

	// "tatami search" matches d2 (both terms) and d0/d1 (one term each); d3 never.
	res, err := seg.Search("tatami search", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("expected hits for 'tatami search'")
	}
	if res[0].URL != "https://c.example/both" {
		t.Fatalf("top hit url = %q, want the doc carrying both terms", res[0].URL)
	}
	for _, r := range res {
		if r.URL == "https://d.example/cooking" {
			t.Fatal("irrelevant doc d3 should not match")
		}
		if r.Title == "" {
			t.Fatalf("stored title not fetched for %s", r.URL)
		}
	}

	// Reject a non-search file: write a plain document store and try to open it.
	plain := filepath.Join(t.TempDir(), "plain.tatami")
	writePlainFile(t, plain)
	if _, err := OpenSearch(plain); err == nil {
		t.Fatal("OpenSearch on a non-search file should error")
	}
}

// writePlainFile writes a minimal document-store tatami file with no search role.
func writePlainFile(t *testing.T, path string) {
	t.Helper()
	schema := &Schema{Fields: []Field{{Name: "x", Type: TypeString}}}
	w, f, err := Create(path, schema, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Batch{Columns: []Column{{Data: []string{"hello", "world"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSearchSegmentManyGroups indexes enough documents to span several row
// groups so the docID-to-group mapping and the per-group stored-field cache are
// exercised on a hit set that straddles group boundaries.
func TestSearchSegmentManyGroups(t *testing.T) {
	const n = 5000
	b := NewSearchBuilder()
	for i := 0; i < n; i++ {
		body := "common term"
		if i%500 == 0 {
			body = "common term rareword"
		}
		b.Add(SearchDoc{
			DocID: fmt.Sprintf("d%d", i),
			URL:   fmt.Sprintf("https://example/%d", i),
			Title: fmt.Sprintf("Document %d", i),
			Body:  body,
		})
	}
	path := filepath.Join(t.TempDir(), "many.tatami")
	if err := b.Write(path, WriterOptions{RowGroupMaxRows: 1024}); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSearch(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seg.Close() }()

	res, err := seg.Search("rareword", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 10 {
		t.Fatalf("rareword hits = %d, want 10", len(res))
	}
	// Every returned doc must carry the rare word, i.e. an index divisible by 500.
	for _, r := range res {
		var id int
		if _, err := fmt.Sscanf(r.URL, "https://example/%d", &id); err != nil {
			t.Fatal(err)
		}
		if id%500 != 0 {
			t.Fatalf("doc %d returned for rareword but does not carry it", id)
		}
		if r.Title != fmt.Sprintf("Document %d", id) {
			t.Fatalf("stored title mismatch for doc %d: %q", id, r.Title)
		}
	}
}
