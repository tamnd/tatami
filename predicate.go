package tatami

import "fmt"

// This file is the pushdown predicate: a small prune-or-keep tree the reader
// evaluates against a region's metadata before it decodes any data. It is not
// an expression language. A leaf is (column, op, value); interior nodes are AND
// and OR. Anything richer (a regex, a computed column) is the caller's job after
// the reader hands back the surviving rows.

// Op is a leaf comparison operator.
type Op uint8

const (
	OpEQ Op = iota
	OpNE
	OpLT
	OpLE
	OpGT
	OpGE
	OpBetween
	OpIsNull
)

// predKind tags a predicate node.
type predKind uint8

const (
	kindLeaf predKind = iota
	kindAnd
	kindOr
)

// Pred is one node of a predicate tree. Build leaves with Eq, Lt, Between, and
// the rest; combine them with And and Or.
type Pred struct {
	kind predKind

	// leaf fields
	col   string
	op    Op
	val   any // the comparison value, or the low bound for Between
	hi    any // the high bound for Between
	colID int // resolved by bind

	// interior fields
	kids []*Pred
}

// Eq builds a column == value leaf.
func Eq(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpEQ, val: val} }

// Ne builds a column != value leaf. It never prunes a region (a region almost
// always holds some other value) but composes inside AND/OR.
func Ne(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpNE, val: val} }

// Lt builds a column < value leaf.
func Lt(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpLT, val: val} }

// Le builds a column <= value leaf.
func Le(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpLE, val: val} }

// Gt builds a column > value leaf.
func Gt(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpGT, val: val} }

// Ge builds a column >= value leaf.
func Ge(col string, val any) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpGE, val: val} }

// Between builds a lo <= column <= hi leaf (both bounds inclusive).
func Between(col string, lo, hi any) *Pred {
	return &Pred{kind: kindLeaf, col: col, op: OpBetween, val: lo, hi: hi}
}

// IsNull builds a column IS NULL leaf.
func IsNull(col string) *Pred { return &Pred{kind: kindLeaf, col: col, op: OpIsNull} }

// And combines children so the result holds only when every child holds.
func And(kids ...*Pred) *Pred { return &Pred{kind: kindAnd, kids: kids} }

// Or combines children so the result holds when any child holds.
func Or(kids ...*Pred) *Pred { return &Pred{kind: kindOr, kids: kids} }

// bind resolves each leaf's column name to an id against the schema and checks
// the value type matches the column. It runs once before a scan.
func (p *Pred) bind(s *Schema) error {
	switch p.kind {
	case kindLeaf:
		id := -1
		for i, f := range s.Fields {
			if f.Name == p.col {
				id = i
				break
			}
		}
		if id < 0 {
			return fmt.Errorf("tatami: predicate names unknown column %q", p.col)
		}
		p.colID = id
		if p.op == OpIsNull {
			return nil
		}
		t := s.Fields[id].Type
		if !scalarMatches(t, p.val) {
			return fmt.Errorf("tatami: predicate on %q (%s) got value of type %T", p.col, t, p.val)
		}
		if p.op == OpBetween && !scalarMatches(t, p.hi) {
			return fmt.Errorf("tatami: between on %q (%s) got high bound of type %T", p.col, t, p.hi)
		}
		return nil
	case kindAnd, kindOr:
		if len(p.kids) == 0 {
			return fmt.Errorf("tatami: empty AND/OR predicate")
		}
		for _, k := range p.kids {
			if err := k.bind(s); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("tatami: bad predicate kind %d", p.kind)
	}
}

// Tri is the three-valued result of evaluating a predicate against a region's
// metadata: the region cannot match, the region definitely all-matches, or it
// might match and must be decoded.
type Tri uint8

const (
	triNo Tri = iota
	triMaybe
	triYes
)

// regionView is what a leaf needs to know about one region (a row group or a
// page) to decide prune-or-keep without decoding it.
type regionView interface {
	// zone returns the column's zone stat and whether one is available.
	zone(colID int) (zoneStat, bool)
	// nullCount returns how many rows of the column are null in the region.
	nullCount(colID int) int
	// rowCount returns the region's total row count.
	rowCount() int
	// rejectsEq reports whether a membership filter definitively rules out the
	// encoded value for the column. False when there is no filter.
	rejectsEq(colID int, enc []byte) bool
}

// eval runs the predicate against a region. The schema gives column types.
func (p *Pred) eval(s *Schema, r regionView) Tri {
	switch p.kind {
	case kindLeaf:
		return p.evalLeaf(s, r)
	case kindAnd:
		res := triYes
		for _, k := range p.kids {
			switch k.eval(s, r) {
			case triNo:
				return triNo
			case triMaybe:
				res = triMaybe
			}
		}
		return res
	case kindOr:
		res := triNo
		for _, k := range p.kids {
			switch k.eval(s, r) {
			case triYes:
				return triYes
			case triMaybe:
				res = triMaybe
			}
		}
		return res
	default:
		return triMaybe
	}
}

func (p *Pred) evalLeaf(s *Schema, r regionView) Tri {
	t := s.Fields[p.colID].Type
	if p.op == OpIsNull {
		if r.nullCount(p.colID) == 0 {
			return triNo
		}
		return triMaybe
	}
	z, ok := r.zone(p.colID)
	if !ok || !z.present {
		// No usable zone (no stat, or an all-null region): cannot prune. An
		// all-null region cannot satisfy a value predicate, but without the stat
		// we conservatively keep it; the row-level filter drops it on decode.
		if !ok {
			return triMaybe
		}
		// present == false means all nulls: a value predicate matches nothing.
		return triNo
	}
	enc := encodeScalar(t, p.val)
	switch p.op {
	case OpEQ:
		if cmpEncoded(t, enc, z.min) < 0 || cmpEncoded(t, enc, z.max) > 0 {
			return triNo
		}
		if r.rejectsEq(p.colID, enc) {
			return triNo
		}
		return triMaybe
	case OpNE:
		// A region is all-excluded only when every value equals val, i.e. the
		// zone is the single point val. Rare, but a cheap win for constant chunks.
		if cmpEncoded(t, z.min, z.max) == 0 && cmpEncoded(t, enc, z.min) == 0 {
			return triNo
		}
		return triMaybe
	case OpLT:
		if cmpEncoded(t, z.min, enc) >= 0 {
			return triNo
		}
		if cmpEncoded(t, z.max, enc) < 0 {
			return triYes
		}
		return triMaybe
	case OpLE:
		if cmpEncoded(t, z.min, enc) > 0 {
			return triNo
		}
		if cmpEncoded(t, z.max, enc) <= 0 {
			return triYes
		}
		return triMaybe
	case OpGT:
		if cmpEncoded(t, z.max, enc) <= 0 {
			return triNo
		}
		if cmpEncoded(t, z.min, enc) > 0 {
			return triYes
		}
		return triMaybe
	case OpGE:
		if cmpEncoded(t, z.max, enc) < 0 {
			return triNo
		}
		if cmpEncoded(t, z.min, enc) >= 0 {
			return triYes
		}
		return triMaybe
	case OpBetween:
		lo := enc
		hi := encodeScalar(t, p.hi)
		if cmpEncoded(t, z.max, lo) < 0 || cmpEncoded(t, hi, z.min) < 0 {
			return triNo
		}
		if cmpEncoded(t, z.min, lo) >= 0 && cmpEncoded(t, z.max, hi) <= 0 {
			return triYes
		}
		return triMaybe
	default:
		return triMaybe
	}
}

// matchRow evaluates the predicate against a single decoded row, the leaf-level
// truth the reader applies to a MAYBE region. cols is indexed by column id and
// holds the decoded column for the region; row is the index within it.
func (p *Pred) matchRow(s *Schema, cols []Column, row int) bool {
	switch p.kind {
	case kindLeaf:
		return p.matchRowLeaf(s, cols, row)
	case kindAnd:
		for _, k := range p.kids {
			if !k.matchRow(s, cols, row) {
				return false
			}
		}
		return true
	case kindOr:
		for _, k := range p.kids {
			if k.matchRow(s, cols, row) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (p *Pred) matchRowLeaf(s *Schema, cols []Column, row int) bool {
	col := cols[p.colID]
	t := s.Fields[p.colID].Type
	if p.op == OpIsNull {
		return !col.isValid(row)
	}
	if !col.isValid(row) {
		return false // null never satisfies a value comparison
	}
	v := scalarAt(t, col, row)
	switch p.op {
	case OpEQ:
		return cmpScalar(t, v, p.val) == 0
	case OpNE:
		return cmpScalar(t, v, p.val) != 0
	case OpLT:
		return cmpScalar(t, v, p.val) < 0
	case OpLE:
		return cmpScalar(t, v, p.val) <= 0
	case OpGT:
		return cmpScalar(t, v, p.val) > 0
	case OpGE:
		return cmpScalar(t, v, p.val) >= 0
	case OpBetween:
		return cmpScalar(t, v, p.val) >= 0 && cmpScalar(t, v, p.hi) <= 0
	default:
		return false
	}
}

// columns returns the set of column ids the predicate references, so the reader
// decodes exactly those to apply the row-level filter.
func (p *Pred) columns(into map[int]struct{}) {
	switch p.kind {
	case kindLeaf:
		into[p.colID] = struct{}{}
	default:
		for _, k := range p.kids {
			k.columns(into)
		}
	}
}

// scalarMatches reports whether v is the Go type a column of type t stores.
func scalarMatches(t LogicalType, v any) bool {
	switch t {
	case TypeBool:
		_, ok := v.(bool)
		return ok
	case TypeInt8:
		_, ok := v.(int8)
		return ok
	case TypeInt16:
		_, ok := v.(int16)
		return ok
	case TypeInt32:
		_, ok := v.(int32)
		return ok
	case TypeInt64, TypeTimestampMicros:
		_, ok := v.(int64)
		return ok
	case TypeUint8:
		_, ok := v.(uint8)
		return ok
	case TypeUint16:
		_, ok := v.(uint16)
		return ok
	case TypeUint32:
		_, ok := v.(uint32)
		return ok
	case TypeUint64:
		_, ok := v.(uint64)
		return ok
	case TypeFloat32:
		_, ok := v.(float32)
		return ok
	case TypeFloat64:
		_, ok := v.(float64)
		return ok
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBytes, TypeBlobRef:
		_, ok := v.([]byte)
		return ok
	default:
		return false
	}
}
