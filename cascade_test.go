package tatami

import "testing"

// metaSchema mimics the integer metadata columns of a crawl shard: an HTTP
// status that is almost always 200 (runs), a content length that varies a lot
// (bitpack with a frame of reference), and a fetch timestamp that climbs row by
// row (delta). These are the columns the M1 gate cares about beating Parquet on.
func metaSchema(t *testing.T) *Schema {
	t.Helper()
	s, err := NewSchema(
		Field{Name: "status", Type: TypeInt32},
		Field{Name: "length", Type: TypeInt64},
		Field{Name: "fetched_at", Type: TypeTimestampMicros},
		Field{Name: "flagged", Type: TypeBool},
	)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	return s
}

func metaBatch(n int) Batch {
	status := make([]int32, n)
	length := make([]int64, n)
	fetched := make([]int64, n)
	flagged := make([]bool, n)
	base := int64(1_700_000_000_000_000)
	for i := 0; i < n; i++ {
		switch {
		case i%97 == 0:
			status[i] = 404
		case i%50 == 0:
			status[i] = 301
		default:
			status[i] = 200
		}
		length[i] = 1200 + int64((i*2654435761)%40000)
		fetched[i] = base + int64(i)*250000
		flagged[i] = i%64 == 0
	}
	return Batch{Columns: []Column{
		{Data: status}, {Data: length}, {Data: fetched}, {Data: flagged},
	}}
}

func TestCascadeSelectsAndShrinks(t *testing.T) {
	schema := metaSchema(t)
	const n = 5000
	batch := metaBatch(n)
	mf := writeFile(t, schema, WriterOptions{}, batch)

	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Round-trip every column exactly.
	for c := range schema.Fields {
		col, err := r.ReadColumn(0, c)
		if err != nil {
			t.Fatalf("ReadColumn %d: %v", c, err)
		}
		compareColumn(t, schema.Fields[c].Name, batch.Columns[c], col, n)
	}

	info := r.Info()
	byName := map[string]ColumnStat{}
	for _, cs := range info.Columns {
		byName[cs.Name] = cs
	}

	// status is mostly a single value: the sampler should leave PLAIN behind.
	if got := byName["status"].Encoding; got == EncPlain {
		t.Fatalf("status stayed PLAIN, expected a cascade encoding")
	}
	// fetched_at climbs by a constant step, the textbook DELTA case.
	if got := byName["fetched_at"].Encoding; got != EncDelta {
		t.Fatalf("fetched_at encoding = %v, want DELTA", got)
	}
	// bool columns ride the BITMAP path.
	if got := byName["flagged"].Encoding; got != EncBitmap {
		t.Fatalf("flagged encoding = %v, want BITMAP", got)
	}

	// Every integer column must land below its raw fixed-width PLAIN size even
	// before the block codec runs, which is what lets the file beat Parquet.
	rawWidth := map[string]int64{"status": 4, "length": 8, "fetched_at": 8}
	for name, w := range rawWidth {
		cs := byName[name]
		raw := w * cs.NumValues
		if cs.TotalUncompressed >= raw {
			t.Fatalf("col %s: encoded %d bytes >= raw PLAIN %d", name, cs.TotalUncompressed, raw)
		}
	}
}
