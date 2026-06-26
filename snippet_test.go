package tatami

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// These tests cover the search-only segment shape: a forward store that drops the
// document body and keeps a short snippet for the result row. The point of the
// shape is that it indexes the body exactly as a full-document segment does, so
// retrieval is identical, while storing a fraction of the bytes
// (13-search-only-and-scale.md). They run on synthetic data so CI needs no crawl
// shard.

// sampleDocs builds a handful of documents with bodies long enough to truncate.
func snippetDocs(n int) []SearchDoc {
	docs := make([]SearchDoc, n)
	for i := range docs {
		body := fmt.Sprintf("document %d alpha beta gamma", i)
		for j := 0; j < 60; j++ {
			body += fmt.Sprintf(" word%d", j)
		}
		docs[i] = SearchDoc{
			DocID: fmt.Sprintf("doc-%03d", i),
			URL:   fmt.Sprintf("https://example.com/page/%d", i),
			Title: fmt.Sprintf("Page %d", i),
			Body:  body,
		}
	}
	return docs
}

func writeSegment(t *testing.T, dir, name string, docs []SearchDoc, opts SearchBuilderOptions) string {
	t.Helper()
	b := NewSearchBuilderWith(opts)
	for _, d := range docs {
		b.Add(d)
	}
	p := filepath.Join(dir, name)
	if err := b.Write(p, WriterOptions{}); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

// TestSnippetSchemaAndStore checks that a search-only segment carries a snippet
// string in the variable column, reports itself as snippet-only, and returns a
// trimmed lead excerpt as the result snippet, while a full-document segment over
// the same docs reports no snippet.
func TestSnippetSchemaAndStore(t *testing.T) {
	dir := t.TempDir()
	docs := snippetDocs(5)

	snipPath := writeSegment(t, dir, "snip.tatami", docs, SearchBuilderOptions{Snippet: true, SnippetRunes: 40})
	fullPath := writeSegment(t, dir, "full.tatami", docs, SearchBuilderOptions{})

	// The variable column is a snippet string in the snippet segment and a body
	// blob in the full one.
	r, f, err := OpenFile(snipPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Schema().Fields[colVariable]; got.Name != colSnippet || got.Type != TypeString {
		t.Fatalf("snippet segment variable column = %+v, want snippet string", got)
	}
	_ = f.Close()
	r, f, err = OpenFile(fullPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Schema().Fields[colVariable]; got.Name != colBody || got.Type != TypeBlobRef {
		t.Fatalf("full segment variable column = %+v, want body blobref", got)
	}
	_ = f.Close()

	snip, err := OpenSearch(snipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snip.Close()
	if !snip.SnippetOnly() {
		t.Fatal("snippet segment did not report SnippetOnly")
	}
	res, err := snip.Search("document alpha", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("no results")
	}
	for _, r := range res {
		want := makeSnippet(docs[r.Doc].Body, 40)
		if r.Snippet != want {
			t.Fatalf("doc %d snippet = %q, want %q", r.Doc, r.Snippet, want)
		}
		if r.URL == "" || r.Title == "" {
			t.Fatalf("doc %d missing url/title: %+v", r.Doc, r)
		}
	}

	full, err := OpenSearch(fullPath)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if full.SnippetOnly() {
		t.Fatal("full segment reported SnippetOnly")
	}
	fres, err := full.Search("document alpha", 5)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range fres {
		if r.Snippet != "" {
			t.Fatalf("full segment returned a snippet: %q", r.Snippet)
		}
	}
}

// TestSnippetRetrievalIdentical checks that indexing is unchanged by the snippet
// shape: a snippet segment and a full-document segment over the same documents
// return byte-identical top-k for every query, because both build the same
// inverted index from the same body text.
func TestSnippetRetrievalIdentical(t *testing.T) {
	dir := t.TempDir()
	docs := snippetDocs(50)
	snipPath := writeSegment(t, dir, "snip.tatami", docs, SearchBuilderOptions{Snippet: true})
	fullPath := writeSegment(t, dir, "full.tatami", docs, SearchBuilderOptions{})

	snip, err := OpenSearch(snipPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snip.Close()
	full, err := OpenSearch(fullPath)
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()

	for _, q := range []string{"document", "alpha", "word0", "word17 beta", "missing"} {
		for _, k := range []int{1, 5, 20} {
			a := snip.Query(q, k)
			b := full.Query(q, k)
			if len(a) != len(b) {
				t.Fatalf("query %q k=%d: snippet %d hits, full %d hits", q, k, len(a), len(b))
			}
			for i := range a {
				if a[i].Doc != b[i].Doc || a[i].Score != b[i].Score {
					t.Fatalf("query %q k=%d hit %d: snippet %+v, full %+v", q, k, i, a[i], b[i])
				}
			}
		}
	}
}

// TestMakeSnippet pins the excerpt builder: whitespace collapses to single spaces,
// the ends trim, a short body passes through whole, and a long one cuts at a word
// boundary with a plain marker.
func TestMakeSnippet(t *testing.T) {
	cases := []struct {
		in   string
		max  int
		want string
	}{
		{"  Hello   world  foo  ", 100, "Hello world foo"},
		{"alpha beta gamma delta", 12, "alpha beta..."},
		{"short", 100, "short"},
		{"a\n\nb\tc", 100, "a b c"},
		{"", 50, ""},
		{"oneword", 0, "oneword"},
	}
	for _, c := range cases {
		if got := makeSnippet(c.in, c.max); got != c.want {
			t.Errorf("makeSnippet(%q, %d) = %q, want %q", c.in, c.max, got, c.want)
		}
	}
	// A truncated snippet ends with the marker and never exceeds the rune budget by
	// much.
	long := strings.Repeat("alpha ", 200)
	s := makeSnippet(long, 30)
	if !strings.HasSuffix(s, "...") {
		t.Errorf("expected truncation marker, got %q", s)
	}
}

// TestSnippetMergePreserves checks that merging two snippet segments yields a
// snippet segment whose result rows still carry the excerpt, so repackaging the
// tiny-shard tail does not lose the display field.
func TestSnippetMergePreserves(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "a.tatami", snippetDocs(10), SearchBuilderOptions{Snippet: true, SnippetRunes: 50})
	b := writeSegment(t, dir, "b.tatami", snippetDocs(10), SearchBuilderOptions{Snippet: true, SnippetRunes: 50})

	sa, err := OpenSearch(a)
	if err != nil {
		t.Fatal(err)
	}
	sb, err := OpenSearch(b)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "merged.tatami")
	if err := MergeSegments([]*SearchSegment{sa, sb}, out, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	_ = sa.Close()
	_ = sb.Close()

	m, err := OpenSearch(out)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if !m.SnippetOnly() {
		t.Fatal("merged segment lost the snippet shape")
	}
	res, err := m.Search("document alpha", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("no results from merged segment")
	}
	for _, r := range res {
		if r.Snippet == "" {
			t.Fatalf("merged result %+v lost its snippet", r)
		}
	}
}

// TestMergeRejectsMixedShapes checks the merge refuses to combine a snippet segment
// with a full-document segment, which would silently drop one side's stored field.
func TestMergeRejectsMixedShapes(t *testing.T) {
	dir := t.TempDir()
	a := writeSegment(t, dir, "snip.tatami", snippetDocs(5), SearchBuilderOptions{Snippet: true})
	b := writeSegment(t, dir, "full.tatami", snippetDocs(5), SearchBuilderOptions{})
	sa, err := OpenSearch(a)
	if err != nil {
		t.Fatal(err)
	}
	defer sa.Close()
	sb, err := OpenSearch(b)
	if err != nil {
		t.Fatal(err)
	}
	defer sb.Close()
	if err := MergeSegments([]*SearchSegment{sa, sb}, filepath.Join(dir, "x.tatami"), WriterOptions{}); err == nil {
		t.Fatal("expected MergeSegments to reject mixed snippet and full-document segments")
	}
}
