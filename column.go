package tatami

import "fmt"

// Column is a batch of values for one field. Data is a typed Go slice whose
// element type matches the field's logical type:
//
//	BOOL              -> []bool
//	INT8..INT64       -> []int8, []int16, []int32, []int64
//	UINT8..UINT64     -> []uint8, []uint16, []uint32, []uint64
//	FLOAT32/FLOAT64   -> []float32, []float64
//	STRING            -> []string
//	BYTES / BLOBREF   -> [][]byte
//	TIMESTAMP_MICROS  -> []int64
//
// Data is always full length: it has one slot per row including null rows. When
// Valid is non-nil it has the same length, and a false entry marks a null. The
// value stored at a null slot is ignored on write and set to the type's zero on
// read, so callers never have to compact their own slices.
type Column struct {
	Data  any
	Valid []bool
}

// length returns the number of rows in the column, deriving it from the typed
// Data slice. A nil or unsupported Data returns (0, error).
func (c Column) length() (int, error) {
	switch d := c.Data.(type) {
	case []bool:
		return len(d), nil
	case []int8:
		return len(d), nil
	case []int16:
		return len(d), nil
	case []int32:
		return len(d), nil
	case []int64:
		return len(d), nil
	case []uint8:
		return len(d), nil
	case []uint16:
		return len(d), nil
	case []uint32:
		return len(d), nil
	case []uint64:
		return len(d), nil
	case []float32:
		return len(d), nil
	case []float64:
		return len(d), nil
	case []string:
		return len(d), nil
	case [][]byte:
		return len(d), nil
	default:
		return 0, fmt.Errorf("tatami: unsupported column data type %T", c.Data)
	}
}

// matchesType reports whether the Data slice is the right Go type for a logical
// type. Timestamp shares []int64 with INT64.
func (c Column) matchesType(t LogicalType) bool {
	switch t {
	case TypeBool:
		_, ok := c.Data.([]bool)
		return ok
	case TypeInt8:
		_, ok := c.Data.([]int8)
		return ok
	case TypeInt16:
		_, ok := c.Data.([]int16)
		return ok
	case TypeInt32:
		_, ok := c.Data.([]int32)
		return ok
	case TypeInt64, TypeTimestampMicros:
		_, ok := c.Data.([]int64)
		return ok
	case TypeUint8:
		_, ok := c.Data.([]uint8)
		return ok
	case TypeUint16:
		_, ok := c.Data.([]uint16)
		return ok
	case TypeUint32:
		_, ok := c.Data.([]uint32)
		return ok
	case TypeUint64:
		_, ok := c.Data.([]uint64)
		return ok
	case TypeFloat32:
		_, ok := c.Data.([]float32)
		return ok
	case TypeFloat64:
		_, ok := c.Data.([]float64)
		return ok
	case TypeString:
		_, ok := c.Data.([]string)
		return ok
	case TypeBytes, TypeBlobRef:
		_, ok := c.Data.([][]byte)
		return ok
	default:
		return false
	}
}

// isValid reports whether row i is present.
func (c Column) isValid(i int) bool {
	return c.Valid == nil || c.Valid[i]
}

// slice returns the rows [start, start+count) of a column, sharing the backing
// arrays. It is used to cut a large Append into row-group-sized pieces.
func (c Column) slice(start, count int) Column {
	out := Column{}
	switch d := c.Data.(type) {
	case []bool:
		out.Data = d[start : start+count]
	case []int8:
		out.Data = d[start : start+count]
	case []int16:
		out.Data = d[start : start+count]
	case []int32:
		out.Data = d[start : start+count]
	case []int64:
		out.Data = d[start : start+count]
	case []uint8:
		out.Data = d[start : start+count]
	case []uint16:
		out.Data = d[start : start+count]
	case []uint32:
		out.Data = d[start : start+count]
	case []uint64:
		out.Data = d[start : start+count]
	case []float32:
		out.Data = d[start : start+count]
	case []float64:
		out.Data = d[start : start+count]
	case []string:
		out.Data = d[start : start+count]
	case [][]byte:
		out.Data = d[start : start+count]
	}
	if c.Valid != nil {
		out.Valid = c.Valid[start : start+count]
	}
	return out
}

// At returns the value at row i as an any, or nil when the row is null. It is a
// convenience for row-oriented consumers like the cat command; column-oriented
// code should use the typed Data slice directly.
func (c Column) At(i int) any {
	if !c.isValid(i) {
		return nil
	}
	switch d := c.Data.(type) {
	case []bool:
		return d[i]
	case []int8:
		return d[i]
	case []int16:
		return d[i]
	case []int32:
		return d[i]
	case []int64:
		return d[i]
	case []uint8:
		return d[i]
	case []uint16:
		return d[i]
	case []uint32:
		return d[i]
	case []uint64:
		return d[i]
	case []float32:
		return d[i]
	case []float64:
		return d[i]
	case []string:
		return d[i]
	case [][]byte:
		return d[i]
	default:
		return nil
	}
}

// Batch is one Append call: a typed column per schema field, all the same
// length. Index i of every column is row i.
type Batch struct {
	Columns []Column
}

// rows returns the row count of a batch after checking every column agrees.
func (b Batch) rows(s *Schema) (int, error) {
	if len(b.Columns) != len(s.Fields) {
		return 0, fmt.Errorf("tatami: batch has %d columns, schema has %d", len(b.Columns), len(s.Fields))
	}
	n := -1
	for i := range b.Columns {
		f := s.Fields[i]
		if !b.Columns[i].matchesType(f.Type) {
			return 0, fmt.Errorf("tatami: column %q (%s) got data of type %T", f.Name, f.Type, b.Columns[i].Data)
		}
		li, err := b.Columns[i].length()
		if err != nil {
			return 0, err
		}
		if v := b.Columns[i].Valid; v != nil && len(v) != li {
			return 0, fmt.Errorf("tatami: column %q has %d values but %d validity bits", f.Name, li, len(v))
		}
		if !f.Nullable && b.Columns[i].Valid != nil {
			for _, ok := range b.Columns[i].Valid {
				if !ok {
					return 0, fmt.Errorf("tatami: column %q is not nullable but has a null", f.Name)
				}
			}
		}
		if n == -1 {
			n = li
		} else if li != n {
			return 0, fmt.Errorf("tatami: column %q has %d rows, expected %d", f.Name, li, n)
		}
	}
	if n < 0 {
		n = 0
	}
	return n, nil
}
