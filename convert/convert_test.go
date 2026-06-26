package convert

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/tamnd/tatami"
)

// docRow mirrors the shape of a ccrawl markdown shard closely enough to exercise
// every heuristic: an identity column (bloom), a low-cardinality string (dict), a
// large body (blob), the integer family, a bool, and a nullable column.
type docRow struct {
	DocID    string  `parquet:"doc_id"`
	URL      string  `parquet:"url"`
	Host     string  `parquet:"host"`
	Status   int32   `parquet:"status"`
	BodyLen  int64   `parquet:"body_length"`
	OK       bool    `parquet:"ok"`
	Markdown string  `parquet:"markdown"`
	Note     *string `parquet:"note,optional"`
}

func writeParquet(t *testing.T, path string, rows []docRow) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[docRow](f, parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func sampleRows(n int) []docRow {
	rows := make([]docRow, n)
	hosts := []string{"a.example", "b.example", "c.example"}
	for i := range rows {
		body := fmt.Sprintf("# Document %d\n\nThis is a fairly repetitive markdown body that compresses well across the shard. Row %d.\n", i, i)
		rows[i] = docRow{
			DocID:    fmt.Sprintf("%032x", i),
			URL:      fmt.Sprintf("https://%s/page/%d", hosts[i%len(hosts)], i),
			Host:     hosts[i%len(hosts)],
			Status:   200,
			BodyLen:  int64(len(body)),
			OK:       i%2 == 0,
			Markdown: body,
		}
		if i%5 == 0 {
			s := fmt.Sprintf("note-%d", i)
			rows[i].Note = &s
		}
	}
	return rows
}

func TestConvertSchemaHeuristics(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.parquet")
	out := filepath.Join(dir, "out.tatami")
	writeParquet(t, in, sampleRows(1000))

	st, err := File(in, out, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if st.Rows != 1000 {
		t.Fatalf("converted %d rows, want 1000", st.Rows)
	}
	if st.Columns != 8 {
		t.Fatalf("mapped %d columns, want 8", st.Columns)
	}

	r, f, err := tatami.OpenFile(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	byName := map[string]tatami.Field{}
	for _, fld := range r.Schema().Fields {
		byName[fld.Name] = fld
	}
	if got := byName["markdown"]; got.Type != tatami.TypeBlobRef || !got.BlobSeparated {
		t.Fatalf("markdown should be a separated blobref, got %+v", got)
	}
	for _, id := range []string{"doc_id", "url"} {
		if !byName[id].BloomFilter {
			t.Fatalf("%s should carry a bloom filter, got %+v", id, byName[id])
		}
	}
	if got := byName["host"]; got.Type != tatami.TypeString || !got.DictHint {
		t.Fatalf("host should be a dict-hinted string, got %+v", got)
	}
	if byName["status"].Type != tatami.TypeInt32 || byName["body_length"].Type != tatami.TypeInt64 {
		t.Fatalf("integer columns mistyped: %+v %+v", byName["status"], byName["body_length"])
	}
	if !byName["note"].Nullable {
		t.Fatalf("optional note should be nullable, got %+v", byName["note"])
	}
}

func TestConvertRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.parquet")
	out := filepath.Join(dir, "out.tatami")
	want := sampleRows(500)
	writeParquet(t, in, want)
	if _, err := File(in, out, Options{}); err != nil {
		t.Fatal(err)
	}

	r, f, err := tatami.OpenFile(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	cols, err := r.ReadRowGroup(0)
	if err != nil {
		t.Fatal(err)
	}
	// Column order follows the parquet leaf order: doc_id, url, host, status,
	// body_length, ok, markdown, note.
	ids := cols[0].Data.([]string)
	md := cols[6].Data.([][]byte)
	notes := cols[7]
	for i := range want {
		if ids[i] != want[i].DocID {
			t.Fatalf("row %d doc_id %q != %q", i, ids[i], want[i].DocID)
		}
		if string(md[i]) != want[i].Markdown {
			t.Fatalf("row %d markdown body did not round-trip", i)
		}
		// note is null on 4 of every 5 rows; the validity bitmap must agree.
		gotNull := notes.At(i) == nil
		wantNull := want[i].Note == nil
		if gotNull != wantNull {
			t.Fatalf("row %d note nullness %v != %v", i, gotNull, wantNull)
		}
	}
}

func TestConvertOverrides(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.parquet")
	out := filepath.Join(dir, "out.tatami")
	writeParquet(t, in, sampleRows(200))

	// An explicit empty blob list disables body separation; markdown stays an
	// inline string. An explicit bloom list moves the filter to host.
	if _, err := File(in, out, Options{Blob: []string{}, Bloom: []string{"host"}}); err != nil {
		t.Fatal(err)
	}
	r, f, err := tatami.OpenFile(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	byName := map[string]tatami.Field{}
	for _, fld := range r.Schema().Fields {
		byName[fld.Name] = fld
	}
	if byName["markdown"].Type != tatami.TypeString {
		t.Fatalf("disabled blob should leave markdown a string, got %+v", byName["markdown"])
	}
	if !byName["host"].BloomFilter {
		t.Fatalf("host should carry the bloom filter after override, got %+v", byName["host"])
	}
	if byName["doc_id"].BloomFilter {
		t.Fatalf("doc_id should lose its default bloom under an override, got %+v", byName["doc_id"])
	}
}
