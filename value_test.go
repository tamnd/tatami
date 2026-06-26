package tatami

import "testing"

func TestScalarRoundTrip(t *testing.T) {
	cases := []struct {
		typ LogicalType
		val any
	}{
		{TypeBool, true},
		{TypeBool, false},
		{TypeInt8, int8(-5)},
		{TypeInt16, int16(-300)},
		{TypeInt32, int32(-70000)},
		{TypeInt64, int64(-1 << 40)},
		{TypeUint8, uint8(200)},
		{TypeUint16, uint16(60000)},
		{TypeUint32, uint32(4000000000)},
		{TypeUint64, uint64(1 << 60)},
		{TypeFloat32, float32(3.14)},
		{TypeFloat64, float64(2.718281828)},
		{TypeString, "hello"},
		{TypeBytes, []byte{0, 1, 2, 255}},
		{TypeTimestampMicros, int64(1719360000000000)},
	}
	for _, c := range cases {
		enc := encodeScalar(c.typ, c.val)
		got, err := decodeScalar(c.typ, enc)
		if err != nil {
			t.Fatalf("%s decode: %v", c.typ, err)
		}
		if cmpScalar(c.typ, got, c.val) != 0 {
			t.Fatalf("%s round trip: got %v want %v", c.typ, got, c.val)
		}
	}
}

func TestCmpScalarOrder(t *testing.T) {
	if cmpScalar(TypeInt32, int32(-1), int32(1)) >= 0 {
		t.Fatal("int32 -1 should be less than 1")
	}
	if cmpScalar(TypeUint32, uint32(1), uint32(2)) >= 0 {
		t.Fatal("uint32 1 should be less than 2")
	}
	if cmpScalar(TypeString, "abc", "abd") >= 0 {
		t.Fatal("abc should be less than abd")
	}
	if cmpScalar(TypeBytes, []byte{1}, []byte{1, 0}) >= 0 {
		t.Fatal("shorter prefix should be less")
	}
}

func TestColumnZone(t *testing.T) {
	col := Column{
		Data:  []int32{5, 1, 9, 3},
		Valid: nil,
	}
	z := columnZone(TypeInt32, col, 0, 4)
	if !z.present {
		t.Fatal("zone should be present")
	}
	mn, _ := decodeScalar(TypeInt32, z.min)
	mx, _ := decodeScalar(TypeInt32, z.max)
	if mn.(int32) != 1 || mx.(int32) != 9 {
		t.Fatalf("zone got [%v,%v] want [1,9]", mn, mx)
	}
}

func TestColumnZoneAllNull(t *testing.T) {
	col := Column{
		Data:  []int32{0, 0},
		Valid: []bool{false, false},
	}
	z := columnZone(TypeInt32, col, 0, 2)
	if z.present {
		t.Fatal("an all-null region should have no zone")
	}
}

func TestPredicateEvalTri(t *testing.T) {
	schema, err := NewSchema(Field{Name: "n", Type: TypeInt32})
	if err != nil {
		t.Fatal(err)
	}
	// A region whose n ranges [10, 20].
	z := zoneStat{min: encodeScalar(TypeInt32, int32(10)), max: encodeScalar(TypeInt32, int32(20)), present: true}
	rv := &fakeRegion{zones: map[int]zoneStat{0: z}, rows: 100}

	check := func(p *Pred, want Tri) {
		t.Helper()
		if err := p.bind(schema); err != nil {
			t.Fatal(err)
		}
		if got := p.eval(schema, rv); got != want {
			t.Fatalf("eval got %d want %d", got, want)
		}
	}
	check(Eq("n", int32(5)), triNo)     // below range
	check(Eq("n", int32(15)), triMaybe) // inside range
	check(Lt("n", int32(5)), triNo)     // all >= 10
	check(Lt("n", int32(30)), triYes)   // all < 30
	check(Gt("n", int32(25)), triNo)    // all <= 20
	check(Ge("n", int32(10)), triYes)   // all >= 10
	check(Between("n", int32(0), int32(9)), triNo)
	check(Between("n", int32(0), int32(100)), triYes)
	check(Between("n", int32(15), int32(18)), triMaybe)
}

// fakeRegion is a hand-built regionView for the evaluator unit test.
type fakeRegion struct {
	zones map[int]zoneStat
	nulls map[int]int
	rows  int
}

func (f *fakeRegion) zone(colID int) (zoneStat, bool) {
	z, ok := f.zones[colID]
	return z, ok && z.present
}
func (f *fakeRegion) nullCount(colID int) int              { return f.nulls[colID] }
func (f *fakeRegion) rowCount() int                        { return f.rows }
func (f *fakeRegion) rejectsEq(colID int, enc []byte) bool { return false }
