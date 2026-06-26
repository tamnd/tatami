package tatami

import (
	"fmt"
	"path/filepath"
	"testing"
)

// buildCollection writes nFiles sorted, disjoint shard files into dir and
// catalogs them. Each file holds rowsPer rows of (id, status, host): id is the
// global ascending sort key, so file k owns a contiguous, non-overlapping key
// range, which is what lets the manifest prune to one file on a lookup. status
// is clustered per file (all 200 in even files, all 404 in odd) so a status
// predicate prunes whole files.
func buildCollection(t *testing.T, dir string, nFiles, rowsPer int) *Collection {
	t.Helper()
	schema, err := NewSchema(
		Field{Name: "id", Type: TypeString, SortKey: true},
		Field{Name: "status", Type: TypeInt32},
		Field{Name: "host", Type: TypeString},
	)
	if err != nil {
		t.Fatal(err)
	}
	c, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}
	seq := 0
	for k := 0; k < nFiles; k++ {
		ids := make([]string, rowsPer)
		status := make([]int32, rowsPer)
		hosts := make([]string, rowsPer)
		st := int32(200)
		if k%2 == 1 {
			st = 404
		}
		for i := 0; i < rowsPer; i++ {
			ids[i] = fmt.Sprintf("doc%08d", seq)
			status[i] = st
			hosts[i] = fmt.Sprintf("h%03d.example", k)
			seq++
		}
		rel := fmt.Sprintf("%06d.tatami", k)
		w, f, err := Create(filepath.Join(dir, rel), schema, WriterOptions{RowGroupMaxRows: 128, PageMaxValues: 64})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Batch{Columns: []Column{{Data: ids}, {Data: status}, {Data: hosts}}}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if err := c.AddFile(rel); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func TestCollectionLookupOpensOneFile(t *testing.T) {
	dir := t.TempDir()
	const nFiles, rowsPer = 64, 256
	buildCollection(t, dir, nFiles, rowsPer)

	// Re-open from disk to prove the manifest alone drives the query.
	c, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Members()) != nFiles {
		t.Fatalf("collection has %d members, want %d", len(c.Members()), nFiles)
	}

	for _, seq := range []int{0, 1000, 8191, 16383} {
		key := fmt.Sprintf("doc%08d", seq)
		hit, found, opened, err := c.Lookup(key)
		if err != nil {
			t.Fatalf("lookup %q: %v", key, err)
		}
		if !found {
			t.Fatalf("lookup %q: not found", key)
		}
		if opened != 1 {
			t.Fatalf("lookup %q opened %d files, want 1 (manifest should narrow to one)", key, opened)
		}
		// Confirm the hit lands on the right row.
		r, f, err := OpenFile(filepath.Join(dir, hit.Member))
		if err != nil {
			t.Fatal(err)
		}
		idCol, err := r.ReadColumn(hit.Group, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := idCol.Data.([]string)[hit.Row]; got != key {
			t.Fatalf("lookup %q landed on %q", key, got)
		}
		_ = f.Close()
	}

	// An absent key opens at most one file and reports missing.
	if _, found, opened, err := c.Lookup("doc99999999"); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("absent key reported found")
	} else if opened > 1 {
		t.Fatalf("absent key opened %d files, want at most 1", opened)
	}
}

func TestCollectionScanPrunesFiles(t *testing.T) {
	dir := t.TempDir()
	const nFiles, rowsPer = 64, 256
	buildCollection(t, dir, nFiles, rowsPer)
	c, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}

	// status == 404 lives only in the odd files; the zone rollup prunes the even
	// half before any file is opened.
	res, err := c.Scan(Eq("status", int32(404)), "id", "status")
	if err != nil {
		t.Fatal(err)
	}
	if want := nFiles / 2 * rowsPer; len(res.Rows) != want {
		t.Fatalf("got %d rows, want %d", len(res.Rows), want)
	}
	if res.FilesScanned != nFiles/2 {
		t.Fatalf("scanned %d/%d files, want %d (half pruned)", res.FilesScanned, res.FilesTotal, nFiles/2)
	}
	for _, row := range res.Rows {
		if row[1].(int32) != 404 {
			t.Fatalf("scan returned a non-404 row: %v", row)
		}
	}
}

func TestCollectionKeyRangePrunes(t *testing.T) {
	dir := t.TempDir()
	const nFiles, rowsPer = 32, 256
	buildCollection(t, dir, nFiles, rowsPer)
	c, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}

	// A key range that falls inside one file's span should prune to that file.
	res, err := c.Scan(Between("id", "doc00000300", "doc00000400"), "id")
	if err != nil {
		t.Fatal(err)
	}
	if res.FilesScanned >= res.FilesTotal {
		t.Fatalf("key-range scan did not prune: scanned %d/%d", res.FilesScanned, res.FilesTotal)
	}
	for _, row := range res.Rows {
		id := row[0].(string)
		if id < "doc00000300" || id > "doc00000400" {
			t.Fatalf("scan returned out-of-range id %q", id)
		}
	}
}

func TestCollectionMergeSwapsManifest(t *testing.T) {
	dir := t.TempDir()
	const nFiles, rowsPer = 8, 128
	buildCollection(t, dir, nFiles, rowsPer)
	c, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}

	before := len(c.Members())
	opts := WriterOptions{RowGroupMaxRows: 256, PageMaxValues: 64}
	if err := c.Merge([]string{"000000.tatami", "000001.tatami"}, "merged-0.tatami", opts, 1719360000000); err != nil {
		t.Fatal(err)
	}
	if got := len(c.Members()); got != before-1 {
		t.Fatalf("after merging 2 into 1, have %d members, want %d", got, before-1)
	}

	// The merged file is sorted and queryable: a lookup for a key from each input
	// resolves through the manifest.
	c2, err := OpenCollection(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, seq := range []int{5, rowsPer + 5} { // one row from each merged input
		key := fmt.Sprintf("doc%08d", seq)
		hit, found, _, err := c2.Lookup(key)
		if err != nil {
			t.Fatalf("lookup %q after merge: %v", key, err)
		}
		if !found {
			t.Fatalf("lookup %q after merge: not found", key)
		}
		if hit.Member != "merged-0.tatami" {
			t.Fatalf("lookup %q resolved to %q, want the merged file", key, hit.Member)
		}
	}
}
