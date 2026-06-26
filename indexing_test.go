package tatami

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// indexCorpus builds a sorted crawl-shaped file: id is the ascending sort key,
// status is clustered (the first half is 200, the second 404) so its zone maps
// prune hard, and url carries a membership filter. Small group and page limits
// force many groups and many pages so the coarse and fine index levels both get
// exercised.
func indexCorpus(t *testing.T, path string) (*Schema, int) {
	t.Helper()
	schema, err := NewSchema(
		Field{Name: "id", Type: TypeString, SortKey: true},
		Field{Name: "status", Type: TypeInt32},
		Field{Name: "url", Type: TypeString, BloomFilter: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	const n = 2000
	ids := make([]string, n)
	status := make([]int32, n)
	urls := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("doc%05d", i)
		if i < n/2 {
			status[i] = 200
		} else {
			status[i] = 404
		}
		urls[i] = fmt.Sprintf("https://h%02d.example/p%d", i%50, i)
	}
	w, f, err := Create(path, schema, WriterOptions{RowGroupMaxRows: 256, PageMaxValues: 64})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Batch{Columns: []Column{
		{Data: ids}, {Data: status}, {Data: urls},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return schema, n
}

func TestLookupBoundedSeeks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.tatami")
	_, n := indexCorpus(t, path)
	r, f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	if r.Header().Flags&FlagSorted == 0 {
		t.Fatal("sorted flag not set on a sort-key file")
	}
	if r.Header().Flags&FlagHasIndexRegion == 0 {
		t.Fatal("index region flag not set on a bloom-filtered file")
	}

	// Every key is present and lands on the row whose id matches.
	for _, i := range []int{0, 1, 255, 256, 999, 1000, 1234, n - 1} {
		key := fmt.Sprintf("doc%05d", i)
		ref, found, err := r.Lookup(key)
		if err != nil {
			t.Fatalf("lookup %q: %v", key, err)
		}
		if !found {
			t.Fatalf("lookup %q: not found", key)
		}
		idCol, err := r.ReadColumn(ref.Group, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := idCol.Data.([]string)[ref.Row]; got != key {
			t.Fatalf("lookup %q landed on %q (group %d row %d)", key, got, ref.Group, ref.Row)
		}
	}

	// Absent keys, inside and outside the key range, are reported missing.
	for _, key := range []string{"aaaaaaaa", "doc00000x", "doc99999", "zzz"} {
		if _, found, err := r.Lookup(key); err != nil {
			t.Fatalf("lookup %q: %v", key, err)
		} else if found {
			t.Fatalf("lookup %q reported found for an absent key", key)
		}
	}
}

func TestPredicateScanPrunesGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.tatami")
	_, n := indexCorpus(t, path)
	r, f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// status == 404 lives only in the second half; the clustered zone maps prune
	// every group whose status range is [200,200].
	res, err := r.Scan(Eq("status", int32(404)), "id", "status")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != n/2 {
		t.Fatalf("got %d rows, want %d", len(res.Rows), n/2)
	}
	for _, row := range res.Rows {
		if row[1].(int32) != 404 {
			t.Fatalf("scan returned a non-404 row: %v", row)
		}
	}
	if res.GroupsScanned >= res.GroupsTotal {
		t.Fatalf("predicate scanned %d/%d groups, expected pruning", res.GroupsScanned, res.GroupsTotal)
	}
	if res.GroupsScanned > res.GroupsTotal/2+1 {
		t.Fatalf("zone maps pruned too little: scanned %d/%d groups", res.GroupsScanned, res.GroupsTotal)
	}
}

func TestBloomScanPrunesGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.tatami")
	_, _ = indexCorpus(t, path)
	r, f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// A url equality probe cannot lean on zone maps (url is unsorted), so the
	// membership filter is what prunes. Only the one group holding the url, plus
	// the odd false positive, should survive.
	target := "https://h34.example/p1234"
	res, err := r.Scan(Eq("url", target), "id", "url")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if res.Rows[0][1].(string) != target {
		t.Fatalf("scan returned wrong url %q", res.Rows[0][1])
	}
	if res.Rows[0][0].(string) != "doc01234" {
		t.Fatalf("scan returned wrong id %q", res.Rows[0][0])
	}
	// 2000 rows / 256 per group = 8 groups; bloom should leave only a couple.
	if res.GroupsScanned > 3 {
		t.Fatalf("bloom pruned too little: scanned %d/%d groups", res.GroupsScanned, res.GroupsTotal)
	}
}

func TestProjectionScanNoPredicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.tatami")
	_, n := indexCorpus(t, path)
	r, f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	res, err := r.Scan(nil, "id")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != n {
		t.Fatalf("projection scan got %d rows, want %d", len(res.Rows), n)
	}
	if res.GroupsScanned != res.GroupsTotal {
		t.Fatalf("a no-predicate scan should touch every group, scanned %d/%d", res.GroupsScanned, res.GroupsTotal)
	}
	if len(res.Columns) != 1 || res.Columns[0] != "id" {
		t.Fatalf("unexpected projection columns %v", res.Columns)
	}
}

func TestRangeScanZonePrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "idx.tatami")
	_, _ = indexCorpus(t, path)
	r, f, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// id is the sort key, so a range on it prunes by the row-group zone maps.
	res, err := r.Scan(Between("id", "doc00100", "doc00199"), "id")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 100 {
		t.Fatalf("range scan got %d rows, want 100", len(res.Rows))
	}
	if res.GroupsScanned >= res.GroupsTotal {
		t.Fatalf("range over the sort key did not prune: scanned %d/%d", res.GroupsScanned, res.GroupsTotal)
	}
	for _, row := range res.Rows {
		id := row[0].(string)
		if id < "doc00100" || id > "doc00199" {
			t.Fatalf("range scan returned out-of-range id %q", id)
		}
	}
}

func TestM3ByteStable(t *testing.T) {
	dir := t.TempDir()
	a := filepath.Join(dir, "a.tatami")
	b := filepath.Join(dir, "b.tatami")
	indexCorpus(t, a)
	indexCorpus(t, b)
	ab, err := os.ReadFile(a)
	if err != nil {
		t.Fatal(err)
	}
	bb, err := os.ReadFile(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ab, bb) {
		t.Fatalf("two writes of the same corpus differ: %d vs %d bytes", len(ab), len(bb))
	}
}
