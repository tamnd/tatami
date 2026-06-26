package tatami

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

// memFile is an in-memory io.WriterAt + io.ReaderAt for tests, so a file can be
// written, byte-compared, and read back without touching disk.
type memFile struct{ b []byte }

func (m *memFile) WriteAt(p []byte, off int64) (int, error) {
	end := int(off) + len(p)
	if end > len(m.b) {
		nb := make([]byte, end)
		copy(nb, m.b)
		m.b = nb
	}
	copy(m.b[off:], p)
	return len(p), nil
}

func (m *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(m.b)) {
		return 0, io.EOF
	}
	n := copy(p, m.b[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// allTypesSchema covers every logical type M0 supports, with two nullable
// columns to exercise the validity path.
func allTypesSchema(t *testing.T) *Schema {
	t.Helper()
	s, err := NewSchema(
		Field{Name: "b", Type: TypeBool},
		Field{Name: "i8", Type: TypeInt8},
		Field{Name: "i16", Type: TypeInt16},
		Field{Name: "i32", Type: TypeInt32, Nullable: true},
		Field{Name: "i64", Type: TypeInt64, SortKey: true},
		Field{Name: "u8", Type: TypeUint8},
		Field{Name: "u16", Type: TypeUint16},
		Field{Name: "u32", Type: TypeUint32},
		Field{Name: "u64", Type: TypeUint64},
		Field{Name: "f32", Type: TypeFloat32},
		Field{Name: "f64", Type: TypeFloat64},
		Field{Name: "s", Type: TypeString, Nullable: true},
		Field{Name: "by", Type: TypeBytes},
		Field{Name: "ts", Type: TypeTimestampMicros},
	)
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	return s
}

// genBatch builds n deterministic rows for the all-types schema.
func genBatch(n int) Batch {
	b := make([]bool, n)
	i8 := make([]int8, n)
	i16 := make([]int16, n)
	i32 := make([]int32, n)
	i32v := make([]bool, n)
	i64 := make([]int64, n)
	u8 := make([]uint8, n)
	u16 := make([]uint16, n)
	u32 := make([]uint32, n)
	u64 := make([]uint64, n)
	f32 := make([]float32, n)
	f64 := make([]float64, n)
	s := make([]string, n)
	sv := make([]bool, n)
	by := make([][]byte, n)
	ts := make([]int64, n)
	for i := 0; i < n; i++ {
		b[i] = i%3 == 0
		i8[i] = int8(i % 127)
		i16[i] = int16(i * 7)
		i32[i] = int32(i * 1000)
		i32v[i] = i%5 != 0 // every fifth is null
		i64[i] = int64(i)
		u8[i] = uint8(i % 255)
		u16[i] = uint16(i * 3)
		u32[i] = uint32(i * 100000)
		u64[i] = uint64(i) * 1 << 40
		f32[i] = float32(i) * 0.5
		f64[i] = float64(i) * 1.25
		s[i] = "row-" + string(rune('a'+i%26))
		sv[i] = i%4 != 0
		by[i] = []byte{byte(i), byte(i >> 8), 0xfe}
		ts[i] = int64(1_700_000_000_000_000 + i)
	}
	return Batch{Columns: []Column{
		{Data: b},
		{Data: i8},
		{Data: i16},
		{Data: i32, Valid: i32v},
		{Data: i64},
		{Data: u8},
		{Data: u16},
		{Data: u32},
		{Data: u64},
		{Data: f32},
		{Data: f64},
		{Data: s, Valid: sv},
		{Data: by},
		{Data: ts},
	}}
}

func writeFile(t *testing.T, schema *Schema, opts WriterOptions, batch Batch) *memFile {
	t.Helper()
	mf := &memFile{}
	w, err := NewWriter(mf, schema, opts)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if err := w.Append(batch); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mf
}

// compareColumn checks that two columns hold the same n values with the same
// null pattern, regardless of whether validity is stored as nil or a bitmap.
func compareColumn(t *testing.T, name string, want, got Column, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		wv, gv := want.isValid(i), got.isValid(i)
		if wv != gv {
			t.Fatalf("col %s row %d: validity want=%v got=%v", name, i, wv, gv)
		}
		if !wv {
			continue
		}
		if !reflect.DeepEqual(want.At(i), got.At(i)) {
			t.Fatalf("col %s row %d: value want=%v got=%v", name, i, want.At(i), got.At(i))
		}
	}
}

func TestRoundTripAllTypes(t *testing.T) {
	schema := allTypesSchema(t)
	const n = 200
	batch := genBatch(n)
	mf := writeFile(t, schema, WriterOptions{}, batch)

	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.NumRows() != n {
		t.Fatalf("rows: got %d want %d", r.NumRows(), n)
	}
	for c := range schema.Fields {
		var got Column
		for g := 0; g < r.NumRowGroups(); g++ {
			col, err := r.ReadColumn(g, c)
			if err != nil {
				t.Fatalf("ReadColumn %d: %v", c, err)
			}
			if got.Data == nil {
				got = col
			} else {
				got.Data = appendTyped(schema.Fields[c].Type, got.Data, col.Data)
				if col.Valid != nil || got.Valid != nil {
					got.Valid = append(got.Valid, mustValid(col, lengthOf(col))...)
				}
			}
		}
		compareColumn(t, schema.Fields[c].Name, batch.Columns[c], got, n)
	}
}

func lengthOf(c Column) int {
	n, _ := c.length()
	return n
}

func mustValid(c Column, n int) []bool {
	if c.Valid != nil {
		return c.Valid
	}
	v := make([]bool, n)
	for i := range v {
		v[i] = true
	}
	return v
}

func TestByteStable(t *testing.T) {
	schema := allTypesSchema(t)
	batch := genBatch(150)
	a := writeFile(t, schema, WriterOptions{}, batch)
	b := writeFile(t, schema, WriterOptions{}, batch)
	if !bytes.Equal(a.b, b.b) {
		t.Fatalf("writes not byte-stable: %d vs %d bytes", len(a.b), len(b.b))
	}
}

func TestMultiGroupMultiPage(t *testing.T) {
	schema := allTypesSchema(t)
	const n = 250
	batch := genBatch(n)
	// Tiny limits force several row groups and several pages per chunk.
	opts := WriterOptions{RowGroupMaxRows: 100, PageMaxValues: 30}
	mf := writeFile(t, schema, opts, batch)

	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if r.NumRowGroups() != 3 {
		t.Fatalf("groups: got %d want 3", r.NumRowGroups())
	}
	// Reassemble each column across groups and compare.
	for c := range schema.Fields {
		ft := schema.Fields[c].Type
		got := Column{Data: emptyTyped(ft)}
		anyNull := false
		for g := 0; g < r.NumRowGroups(); g++ {
			col, err := r.ReadColumn(g, c)
			if err != nil {
				t.Fatalf("ReadColumn: %v", err)
			}
			rows := r.RowGroupRows(g)
			got.Data = appendTyped(ft, got.Data, col.Data)
			got.Valid = append(got.Valid, mustValid(col, rows)...)
			if col.Valid != nil {
				anyNull = true
			}
		}
		if !anyNull {
			got.Valid = nil
		}
		compareColumn(t, schema.Fields[c].Name, batch.Columns[c], got, n)
	}
}

func TestCorruptionDetected(t *testing.T) {
	schema := allTypesSchema(t)
	mf := writeFile(t, schema, WriterOptions{}, genBatch(50))

	// Flip a byte in the middle of the data region and expect a page checksum
	// failure when that column is read.
	mid := HeaderSize + 40
	mf.b[mid] ^= 0xff
	r, err := Open(mf, int64(len(mf.b)))
	if err != nil {
		// Corrupting the data region may also trip header/footer reads; either
		// way the file must not read clean.
		return
	}
	failed := false
	for g := 0; g < r.NumRowGroups(); g++ {
		for c := range schema.Fields {
			if _, err := r.ReadColumn(g, c); err != nil {
				failed = true
			}
		}
	}
	if !failed {
		t.Fatal("corrupted file read back clean, checksum did not catch it")
	}
}
