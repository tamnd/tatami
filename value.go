package tatami

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// This file is the typed-scalar layer the index rungs share. A zone map, a
// predicate, and the sparse key index all compare single values of a column's
// logical type, and all serialize a bound value into the footer. The functions
// here are the one place that knows how a logical type orders and how it packs
// into bytes, so the rest of the index code stays type-agnostic.
//
// A scalar is held as the same Go type the column's Data slice uses: bool,
// the sized int and uint types, float32/float64, string, or []byte. Comparison
// is the column's total order (numeric for numbers, lexicographic for strings
// and bytes), the same order the sort key uses.

// cmpScalar returns -1, 0, or 1 comparing a and b as values of logical type t.
// Both must be the Go type the column stores for t.
func cmpScalar(t LogicalType, a, b any) int {
	switch t {
	case TypeBool:
		return cmpBool(a.(bool), b.(bool))
	case TypeInt8:
		return cmpInt(int64(a.(int8)), int64(b.(int8)))
	case TypeInt16:
		return cmpInt(int64(a.(int16)), int64(b.(int16)))
	case TypeInt32:
		return cmpInt(int64(a.(int32)), int64(b.(int32)))
	case TypeInt64, TypeTimestampMicros:
		return cmpInt(a.(int64), b.(int64))
	case TypeUint8:
		return cmpUint(uint64(a.(uint8)), uint64(b.(uint8)))
	case TypeUint16:
		return cmpUint(uint64(a.(uint16)), uint64(b.(uint16)))
	case TypeUint32:
		return cmpUint(uint64(a.(uint32)), uint64(b.(uint32)))
	case TypeUint64:
		return cmpUint(a.(uint64), b.(uint64))
	case TypeFloat32:
		return cmpFloat(float64(a.(float32)), float64(b.(float32)))
	case TypeFloat64:
		return cmpFloat(a.(float64), b.(float64))
	case TypeString:
		return bytes.Compare([]byte(a.(string)), []byte(b.(string)))
	case TypeBytes, TypeBlobRef:
		return bytes.Compare(a.([]byte), b.([]byte))
	default:
		return 0
	}
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// scalarAt reads the value at row i of a column as a typed scalar. The caller
// guarantees the row is present.
func scalarAt(t LogicalType, col Column, i int) any {
	switch t {
	case TypeBool:
		return col.Data.([]bool)[i]
	case TypeInt8:
		return col.Data.([]int8)[i]
	case TypeInt16:
		return col.Data.([]int16)[i]
	case TypeInt32:
		return col.Data.([]int32)[i]
	case TypeInt64, TypeTimestampMicros:
		return col.Data.([]int64)[i]
	case TypeUint8:
		return col.Data.([]uint8)[i]
	case TypeUint16:
		return col.Data.([]uint16)[i]
	case TypeUint32:
		return col.Data.([]uint32)[i]
	case TypeUint64:
		return col.Data.([]uint64)[i]
	case TypeFloat32:
		return col.Data.([]float32)[i]
	case TypeFloat64:
		return col.Data.([]float64)[i]
	case TypeString:
		return col.Data.([]string)[i]
	case TypeBytes, TypeBlobRef:
		return col.Data.([][]byte)[i]
	default:
		return nil
	}
}

// encodeScalar packs a scalar into the bytes a footer zone-map or key bound
// stores. Fixed-width numerics use little-endian, matching PLAIN; strings and
// bytes store their raw octets; bool is one byte. Floats store their IEEE bits
// little-endian so decode is exact.
func encodeScalar(t LogicalType, v any) []byte {
	switch t {
	case TypeBool:
		if v.(bool) {
			return []byte{1}
		}
		return []byte{0}
	case TypeInt8:
		return []byte{byte(v.(int8))}
	case TypeUint8:
		return []byte{v.(uint8)}
	case TypeInt16:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, uint16(v.(int16)))
		return b
	case TypeUint16:
		b := make([]byte, 2)
		binary.LittleEndian.PutUint16(b, v.(uint16))
		return b
	case TypeInt32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(v.(int32)))
		return b
	case TypeUint32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v.(uint32))
		return b
	case TypeFloat32:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, math.Float32bits(v.(float32)))
		return b
	case TypeInt64, TypeTimestampMicros:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v.(int64)))
		return b
	case TypeUint64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, v.(uint64))
		return b
	case TypeFloat64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(v.(float64)))
		return b
	case TypeString:
		return []byte(v.(string))
	case TypeBytes, TypeBlobRef:
		return append([]byte(nil), v.([]byte)...)
	default:
		return nil
	}
}

// decodeScalar reverses encodeScalar.
func decodeScalar(t LogicalType, b []byte) (any, error) {
	switch t {
	case TypeBool:
		return len(b) > 0 && b[0] != 0, nil
	case TypeInt8:
		return int8(b[0]), nil
	case TypeUint8:
		return b[0], nil
	case TypeInt16:
		return int16(binary.LittleEndian.Uint16(b)), nil
	case TypeUint16:
		return binary.LittleEndian.Uint16(b), nil
	case TypeInt32:
		return int32(binary.LittleEndian.Uint32(b)), nil
	case TypeUint32:
		return binary.LittleEndian.Uint32(b), nil
	case TypeFloat32:
		return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
	case TypeInt64, TypeTimestampMicros:
		return int64(binary.LittleEndian.Uint64(b)), nil
	case TypeUint64:
		return binary.LittleEndian.Uint64(b), nil
	case TypeFloat64:
		return math.Float64frombits(binary.LittleEndian.Uint64(b)), nil
	case TypeString:
		return string(b), nil
	case TypeBytes, TypeBlobRef:
		return append([]byte(nil), b...), nil
	default:
		return nil, fmt.Errorf("tatami: cannot decode scalar of type %s", t)
	}
}

// zoneStat is a column region's min and max over its present values, stored as
// the type's encoded bytes. present is false for an all-null or empty region,
// whose zone map survives no equality or range predicate.
type zoneStat struct {
	min     []byte
	max     []byte
	present bool
}

// columnZone computes the zone stat over rows [start, start+count) of a column.
// Null rows are skipped; an all-null or empty range yields present == false.
func columnZone(t LogicalType, col Column, start, count int) zoneStat {
	var minV, maxV any
	have := false
	for i := start; i < start+count; i++ {
		if !col.isValid(i) {
			continue
		}
		v := scalarAt(t, col, i)
		if !have {
			minV, maxV = v, v
			have = true
			continue
		}
		if cmpScalar(t, v, minV) < 0 {
			minV = v
		}
		if cmpScalar(t, v, maxV) > 0 {
			maxV = v
		}
	}
	if !have {
		return zoneStat{}
	}
	return zoneStat{min: encodeScalar(t, minV), max: encodeScalar(t, maxV), present: true}
}

// merge folds another zone stat into z, widening the bounds. Either side may be
// absent.
func (z zoneStat) merge(t LogicalType, o zoneStat) zoneStat {
	if !o.present {
		return z
	}
	if !z.present {
		return o
	}
	out := zoneStat{min: z.min, max: z.max, present: true}
	if cmpEncoded(t, o.min, out.min) < 0 {
		out.min = o.min
	}
	if cmpEncoded(t, o.max, out.max) > 0 {
		out.max = o.max
	}
	return out
}

// cmpEncoded compares two encoded scalar bounds of type t. It decodes only for
// types whose byte form is not already order-preserving; strings and bytes
// compare lexicographically as stored.
func cmpEncoded(t LogicalType, a, b []byte) int {
	switch t {
	case TypeString, TypeBytes, TypeBlobRef:
		return bytes.Compare(a, b)
	default:
		av, err := decodeScalar(t, a)
		if err != nil {
			return 0
		}
		bv, err := decodeScalar(t, b)
		if err != nil {
			return 0
		}
		return cmpScalar(t, av, bv)
	}
}
