package tatami

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/tamnd/tatami/search"
)

// buildSegmentFile builds a search segment from docs and returns its path.
func buildSegmentFile(t *testing.T, dir, name string, docs []SearchDoc) string {
	t.Helper()
	b := NewSearchBuilder()
	for _, d := range docs {
		b.Add(d)
	}
	path := filepath.Join(dir, name)
	if err := b.Write(path, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	return path
}

// sampleDocs builds a deterministic document set with a mix of common and rare
// terms so queries return non-trivial rankings.
func sampleDocs(n int) []SearchDoc {
	vocab := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	docs := make([]SearchDoc, n)
	for i := 0; i < n; i++ {
		// A reproducible pseudo-random body from the vocab.
		var body string
		x := uint32(i)*2654435761 + 12345
		for j := 0; j < 6; j++ {
			x = x*1103515245 + 12345
			w := vocab[(x>>16)%uint32(len(vocab))]
			body += w + " "
		}
		docs[i] = SearchDoc{
			DocID: fmt.Sprintf("doc-%06d", i),
			URL:   fmt.Sprintf("https://example/%d", i),
			Title: fmt.Sprintf("Title %d", i),
			Body:  body,
		}
	}
	return docs
}

// TestMergeMatchesMonolith is the merge-correctness gate: merging the live
// documents of several segments produces the same retrieval as building one
// segment over all those documents in the same order. The merge re-derives the
// posting lists from scratch, so this proves the re-derivation is faithful.
func TestMergeMatchesMonolith(t *testing.T) {
	dir := t.TempDir()
	docs := sampleDocs(3000)

	// Split into three contiguous segments so the concatenation order matches a
	// monolith over the whole set.
	s0 := buildSegmentFile(t, dir, "s0.tatami", docs[:1000])
	s1 := buildSegmentFile(t, dir, "s1.tatami", docs[1000:2000])
	s2 := buildSegmentFile(t, dir, "s2.tatami", docs[2000:])
	mono := buildSegmentFile(t, dir, "mono.tatami", docs)

	var segs []*SearchSegment
	for _, p := range []string{s0, s1, s2} {
		seg, err := OpenSearch(p)
		if err != nil {
			t.Fatal(err)
		}
		defer func(s *SearchSegment) { _ = s.Close() }(seg)
		segs = append(segs, seg)
	}
	merged := filepath.Join(dir, "merged.tatami")
	if err := MergeSegments(segs, merged, WriterOptions{}); err != nil {
		t.Fatal(err)
	}

	mseg, err := OpenSearch(merged)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mseg.Close() }()
	monoSeg, err := OpenSearch(mono)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = monoSeg.Close() }()

	if mseg.NumDocs() != monoSeg.NumDocs() {
		t.Fatalf("merged NumDocs %d, monolith %d", mseg.NumDocs(), monoSeg.NumDocs())
	}
	if mseg.NumTerms() != monoSeg.NumTerms() {
		t.Fatalf("merged NumTerms %d, monolith %d", mseg.NumTerms(), monoSeg.NumTerms())
	}

	for _, q := range []string{"alpha", "beta gamma", "delta epsilon zeta", "theta"} {
		mr, err := mseg.Search(q, 25)
		if err != nil {
			t.Fatal(err)
		}
		nr, err := monoSeg.Search(q, 25)
		if err != nil {
			t.Fatal(err)
		}
		if len(mr) != len(nr) {
			t.Fatalf("query %q: merged %d hits, monolith %d", q, len(mr), len(nr))
		}
		for i := range mr {
			if mr[i].URL != nr[i].URL || mr[i].Score != nr[i].Score {
				t.Fatalf("query %q rank %d: merged %+v, monolith %+v", q, i, mr[i], nr[i])
			}
		}
	}
}

// TestDeleteExcludesFromResults checks that a deleted document never appears in a
// result and that the survivor ranking is otherwise preserved.
func TestDeleteExcludesFromResults(t *testing.T) {
	dir := t.TempDir()
	path := buildSegmentFile(t, dir, "seg.tatami", sampleDocs(2000))
	seg, err := OpenSearch(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seg.Close() }()

	before, err := seg.Search("alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) == 0 {
		t.Fatal("expected hits for alpha")
	}
	// Delete the current top hit by its global doc_id.
	topURL := before[0].URL
	topID, err := seg.globalDocID(before[0].Doc)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := seg.Delete(topID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("delete should have found the doc")
	}
	if seg.NumDeleted() != 1 {
		t.Fatalf("NumDeleted=%d want 1", seg.NumDeleted())
	}

	after, err := seg.Search("alpha", 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range after {
		if r.URL == topURL {
			t.Fatal("deleted document still appears in results")
		}
	}
	// The survivors keep their relative order: after[0] should be before[1].
	if len(before) > 1 && after[0].URL != before[1].URL {
		t.Fatalf("survivor order changed: after[0]=%s want %s", after[0].URL, before[1].URL)
	}
}

// TestMergeDropsDeleted checks that a merge reclaims deleted documents: the
// merged segment holds exactly the live documents and none of the deleted urls.
func TestMergeDropsDeleted(t *testing.T) {
	dir := t.TempDir()
	docs := sampleDocs(2000)
	s0 := buildSegmentFile(t, dir, "s0.tatami", docs[:1000])
	s1 := buildSegmentFile(t, dir, "s1.tatami", docs[1000:])

	seg0, err := OpenSearch(s0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seg0.Close() }()
	seg1, err := OpenSearch(s1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = seg1.Close() }()

	// Delete a known set of doc_ids, split across both segments.
	deleted := map[string]bool{}
	for _, i := range []int{5, 100, 999, 1000, 1500, 1999} {
		id := fmt.Sprintf("doc-%06d", i)
		var ok bool
		if i < 1000 {
			ok, err = seg0.Delete(id)
		} else {
			ok, err = seg1.Delete(id)
		}
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("delete of %s failed", id)
		}
		deleted[id] = true
	}

	merged := filepath.Join(dir, "merged.tatami")
	if err := MergeSegments([]*SearchSegment{seg0, seg1}, merged, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	mseg, err := OpenSearch(merged)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mseg.Close() }()

	if mseg.NumDocs() != len(docs)-len(deleted) {
		t.Fatalf("merged NumDocs %d, want %d", mseg.NumDocs(), len(docs)-len(deleted))
	}
	if mseg.NumDeleted() != 0 {
		t.Fatalf("merged segment should have no deletions, got %d", mseg.NumDeleted())
	}
	// None of the deleted doc_ids should resolve in the merged segment.
	if err := mseg.buildDocIndex(); err != nil {
		t.Fatal(err)
	}
	for id := range deleted {
		if _, ok := mseg.docIndex[id]; ok {
			t.Fatalf("deleted doc_id %s survived the merge", id)
		}
	}
}

// TestIndexFanOutAndDedup checks the multi-segment serving path: a query fans out
// to every segment, the global top-k is merged, and a page present in two segments
// (a recrawl) is returned once.
func TestIndexFanOutAndDedup(t *testing.T) {
	dir := t.TempDir()
	docs := sampleDocs(1500)
	s0 := buildSegmentFile(t, dir, "s0.tatami", docs[:500])
	s1 := buildSegmentFile(t, dir, "s1.tatami", docs[500:1000])
	// s2 re-crawls the first 100 docs: same doc_id and url, so they are duplicates.
	s2 := buildSegmentFile(t, dir, "s2.tatami", docs[:100])

	ix, err := OpenIndex([]string{s0, s1, s2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()

	if ix.NumDocs() != 1100 {
		t.Fatalf("index NumDocs %d, want 1100 (live docs across segments)", ix.NumDocs())
	}

	res, err := ix.Search("alpha beta", 30)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("expected hits")
	}
	seen := map[string]bool{}
	for _, r := range res {
		if seen[r.URL] {
			t.Fatalf("duplicate url %s in deduped results", r.URL)
		}
		seen[r.URL] = true
	}
	// Scores must be in non-increasing order.
	for i := 1; i < len(res); i++ {
		if res[i].Score > res[i-1].Score {
			t.Fatalf("results not sorted by score at rank %d: %v > %v", i, res[i].Score, res[i-1].Score)
		}
	}
}

// TestIndexSelectMerge wires the policy to a real index of many small segments
// and checks it selects a batch to merge, then performs that merge.
func TestIndexSelectMerge(t *testing.T) {
	dir := t.TempDir()
	docs := sampleDocs(1100)
	var paths []string
	for i := 0; i < 11; i++ {
		lo := i * 100
		hi := lo + 100
		paths = append(paths, buildSegmentFile(t, dir, fmt.Sprintf("s%d.tatami", i), docs[lo:hi]))
	}
	ix, err := OpenIndex(paths)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ix.Close() }()

	batch := ix.SelectMerge(search.DefaultMergePolicy())
	if len(batch) < 2 {
		t.Fatalf("expected the policy to select a merge batch, got %v", batch)
	}
	var toMerge []*SearchSegment
	for _, idx := range batch {
		toMerge = append(toMerge, ix.Segments()[idx])
	}
	out := filepath.Join(dir, "merged.tatami")
	if err := MergeSegments(toMerge, out, WriterOptions{}); err != nil {
		t.Fatal(err)
	}
	mseg, err := OpenSearch(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mseg.Close() }()

	var want int
	for _, idx := range batch {
		want += ix.Segments()[idx].LiveDocs()
	}
	if mseg.NumDocs() != want {
		t.Fatalf("merged NumDocs %d, want %d (sum of merged inputs)", mseg.NumDocs(), want)
	}
}
