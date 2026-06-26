package tatami

// columnBuilder accumulates values for one column across Append calls until the
// row group flushes. It keeps a typed growable slice and a lazily-materialized
// validity slice so a column with no nulls carries no validity overhead.
type columnBuilder struct {
	t     LogicalType
	data  any
	valid []bool
	n     int
}

func newColumnBuilder(t LogicalType) *columnBuilder {
	return &columnBuilder{t: t, data: emptyTyped(t)}
}

// emptyTyped returns a fresh empty slice of the Go type for a logical type.
func emptyTyped(t LogicalType) any {
	switch t {
	case TypeBool:
		return []bool{}
	case TypeInt8:
		return []int8{}
	case TypeInt16:
		return []int16{}
	case TypeInt32:
		return []int32{}
	case TypeInt64, TypeTimestampMicros:
		return []int64{}
	case TypeUint8:
		return []uint8{}
	case TypeUint16:
		return []uint16{}
	case TypeUint32:
		return []uint32{}
	case TypeUint64:
		return []uint64{}
	case TypeFloat32:
		return []float32{}
	case TypeFloat64:
		return []float64{}
	case TypeString:
		return []string{}
	case TypeBytes, TypeBlobRef:
		return [][]byte{}
	default:
		return nil
	}
}

// appendBatch appends count rows of src to the builder.
func (b *columnBuilder) appendBatch(src Column, count int) {
	b.data = appendTyped(b.t, b.data, src.Data)
	switch {
	case b.valid == nil && src.Valid == nil:
		// stays all-present
	default:
		if b.valid == nil {
			b.valid = make([]bool, b.n)
			for i := range b.valid {
				b.valid[i] = true
			}
		}
		if src.Valid == nil {
			for i := 0; i < count; i++ {
				b.valid = append(b.valid, true)
			}
		} else {
			b.valid = append(b.valid, src.Valid...)
		}
	}
	b.n += count
}

// column returns the accumulated values as a Column.
func (b *columnBuilder) column() Column {
	return Column{Data: b.data, Valid: b.valid}
}

// reset clears the builder for the next row group.
func (b *columnBuilder) reset() {
	b.data = emptyTyped(b.t)
	b.valid = nil
	b.n = 0
}

// appendTyped appends src (a typed slice) onto dst (a typed slice of the same
// type) and returns the grown slice.
func appendTyped(t LogicalType, dst, src any) any {
	switch t {
	case TypeBool:
		return append(dst.([]bool), src.([]bool)...)
	case TypeInt8:
		return append(dst.([]int8), src.([]int8)...)
	case TypeInt16:
		return append(dst.([]int16), src.([]int16)...)
	case TypeInt32:
		return append(dst.([]int32), src.([]int32)...)
	case TypeInt64, TypeTimestampMicros:
		return append(dst.([]int64), src.([]int64)...)
	case TypeUint8:
		return append(dst.([]uint8), src.([]uint8)...)
	case TypeUint16:
		return append(dst.([]uint16), src.([]uint16)...)
	case TypeUint32:
		return append(dst.([]uint32), src.([]uint32)...)
	case TypeUint64:
		return append(dst.([]uint64), src.([]uint64)...)
	case TypeFloat32:
		return append(dst.([]float32), src.([]float32)...)
	case TypeFloat64:
		return append(dst.([]float64), src.([]float64)...)
	case TypeString:
		return append(dst.([]string), src.([]string)...)
	case TypeBytes, TypeBlobRef:
		return append(dst.([][]byte), src.([][]byte)...)
	default:
		return dst
	}
}

// columnBytes estimates the uncompressed size of count rows of a column, used
// to decide when a row group is full.
func columnBytes(t LogicalType, src Column, count int) int {
	if w, ok := t.fixedWidth(); ok {
		if t == TypeBool {
			return (count + 7) / 8
		}
		return w * count
	}
	switch t {
	case TypeString:
		total := 0
		for _, s := range src.Data.([]string) {
			total += len(s) + 1
		}
		return total
	case TypeBytes, TypeBlobRef:
		total := 0
		for _, v := range src.Data.([][]byte) {
			total += len(v) + 1
		}
		return total
	default:
		return count
	}
}
