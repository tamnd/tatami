package tatami

import (
	"bytes"
	"fmt"
	"testing"
)

// docBody builds a markdown-shaped document with heavy shared boilerplate so the
// trained dictionary has something to bite on, plus a per-row unique core so the
// values are not all identical.
func docBody(i int) []byte {
	const header = "# Article\n\n> Cookie notice: we use cookies to improve your experience. " +
		"Read our privacy policy and terms of service. Subscribe to our newsletter.\n\n"
	const footer = "\n\n---\nShare this post. Follow us on social media. " +
		"Copyright notice all rights reserved. Back to top. Read more articles below.\n"
	body := fmt.Sprintf("%sThis is the body of article number %d. ", header, i)
	for s := 0; s < 8; s++ {
		body += fmt.Sprintf("Section %d covers topic %d in detail with examples and notes. ", s, (i+s)%37)
	}
	return []byte(body + footer)
}

func blobSchema(t *testing.T, bodyType LogicalType) *Schema {
	t.Helper()
	s, err := NewSchema(
		Field{Name: "id", Type: TypeInt64, SortKey: true},
		Field{Name: "url", Type: TypeString},
		Field{Name: "body", Type: bodyType, Nullable: true},
	)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	return s
}

func blobBatch(n int) (Batch, [][]byte, []bool) {
	id := make([]int64, n)
	url := make([]string, n)
	body := make([][]byte, n)
	bodyValid := make([]bool, n)
	for i := 0; i < n; i++ {
		id[i] = int64(i)
		url[i] = fmt.Sprintf("https://example.com/posts/%d", i)
		body[i] = docBody(i)
		bodyValid[i] = i%11 != 0 // every eleventh body is null
		if !bodyValid[i] {
			body[i] = nil
		}
	}
	return Batch{Columns: []Column{
		{Data: id},
		{Data: url},
		{Data: body, Valid: bodyValid},
	}}, body, bodyValid
}

func TestBlobRefRoundTrip(t *testing.T) {
	schema := blobSchema(t, TypeBlobRef)
	const n = 900
	batch, body, bodyValid := blobBatch(n)
	// Small row groups so the column spans several groups and the ordinal base
	// math across groups is exercised.
	opts := WriterOptions{RowGroupMaxRows: 200, PageMaxValues: 64}
	mf := writeFile(t, schema, opts, batch)

	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.Header().Flags&FlagHasBlobRegion == 0 {
		t.Fatal("blob region flag not set")
	}

	// Reassemble the body column across groups and compare against the input.
	got := make([][]byte, 0, n)
	var gotValid []bool
	for g := 0; g < r.NumRowGroups(); g++ {
		col, err := r.ReadColumn(g, 2)
		if err != nil {
			t.Fatalf("ReadColumn body group %d: %v", g, err)
		}
		rows := r.RowGroupRows(g)
		got = append(got, col.Data.([][]byte)...)
		gotValid = append(gotValid, mustValid(col, rows)...)
	}
	if len(got) != n {
		t.Fatalf("body rows: got %d want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if gotValid[i] != bodyValid[i] {
			t.Fatalf("row %d validity: got %v want %v", i, gotValid[i], bodyValid[i])
		}
		if !bodyValid[i] {
			continue
		}
		if !bytes.Equal(got[i], body[i]) {
			t.Fatalf("row %d body mismatch: got %d bytes want %d bytes", i, len(got[i]), len(body[i]))
		}
	}
}

// smallRecord builds a short, heavily templated blob value. Each value is too
// small for its own run to learn the shared structure, so a column of these is
// the regime where a shared dictionary pays off.
func smallRecord(i int) []byte {
	return []byte(fmt.Sprintf(
		`{"lang":"en","content_type":"text/html; charset=utf-8","status":200,"robots":"index,follow","id":%d,"shard":%d}`,
		i, i%16))
}

// TestBlobDictPath drives the dictionary branch: many small templated records
// with a small run target make each run short, so the shared dictionary beats
// plain per-run zstd and is kept. The file must carry a dict region and read
// back exactly.
func TestBlobDictPath(t *testing.T) {
	schema := blobSchema(t, TypeBlobRef)
	const n = 3000
	id := make([]int64, n)
	url := make([]string, n)
	body := make([][]byte, n)
	for i := 0; i < n; i++ {
		id[i] = int64(i)
		url[i] = fmt.Sprintf("https://example.com/r/%d", i)
		body[i] = smallRecord(i)
	}
	batch := Batch{Columns: []Column{{Data: id}, {Data: url}, {Data: body}}}
	// A small run target keeps each run too short to learn the boilerplate, which
	// is what makes the shared dictionary win.
	opts := WriterOptions{RowGroupMaxRows: 500, PageMaxValues: 128, BlobRunTargetBytes: 2048}
	mf := writeFile(t, schema, opts, batch)

	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.Header().Flags&FlagHasDictRegion == 0 {
		t.Fatal("dict region flag not set: the dictionary path was not exercised")
	}
	got := make([][]byte, 0, n)
	for g := 0; g < r.NumRowGroups(); g++ {
		col, err := r.ReadColumn(g, 2)
		if err != nil {
			t.Fatalf("ReadColumn group %d: %v", g, err)
		}
		got = append(got, col.Data.([][]byte)...)
	}
	if len(got) != n {
		t.Fatalf("rows: got %d want %d", len(got), n)
	}
	for i := 0; i < n; i++ {
		if !bytes.Equal(got[i], body[i]) {
			t.Fatalf("row %d mismatch: got %q want %q", i, got[i], body[i])
		}
	}
}

func TestBlobRefByteStable(t *testing.T) {
	schema := blobSchema(t, TypeBlobRef)
	batch, _, _ := blobBatch(500)
	opts := WriterOptions{RowGroupMaxRows: 200, PageMaxValues: 64}
	a := writeFile(t, schema, opts, batch)
	b := writeFile(t, schema, opts, batch)
	if !bytes.Equal(a.b, b.b) {
		t.Fatalf("blob writes not byte-stable: %d vs %d bytes", len(a.b), len(b.b))
	}
}

// TestBlobRefBeatsInline checks that separating the body into the blob region
// with a shared trained dictionary produces a smaller file than storing the same
// bodies inline as BYTES, where each row group re-derives its own zstd context.
func TestBlobRefBeatsInline(t *testing.T) {
	const n = 2000
	opts := WriterOptions{RowGroupMaxRows: 200, PageMaxValues: 64}

	sep := blobSchema(t, TypeBlobRef)
	batch, _, _ := blobBatch(n)
	sepFile := writeFile(t, sep, opts, batch)

	inline := blobSchema(t, TypeBytes)
	batch2, _, _ := blobBatch(n)
	inlineFile := writeFile(t, inline, opts, batch2)

	sepSize := len(sepFile.b)
	inlineSize := len(inlineFile.b)
	t.Logf("separated=%d inline=%d ratio=%.3f", sepSize, inlineSize, float64(sepSize)/float64(inlineSize))
	if sepSize >= inlineSize {
		t.Fatalf("blob separation did not shrink the file: separated=%d inline=%d", sepSize, inlineSize)
	}
}
