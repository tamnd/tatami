package tatami

import (
	"encoding/binary"
	"fmt"
	"math"
)

// This file implements PLAIN value serialization, the only encoding in M0. A
// page payload (before the block codec) is an optional validity bitmap followed
// by the PLAIN-encoded present values:
//
//	[ validity bitmap, ceil(num_values/8) bytes ]  (only when nulls present)
//	[ present value 0 ][ present value 1 ]...
//
// Null slots are not stored. The decoder reads the bitmap, decodes exactly
// (num_values - null_count) values, and scatters them back into the present
// positions, leaving null positions at the type's zero value.
//
// Fixed-width values are little-endian. Strings and bytes are each a uvarint
// length followed by that many raw bytes. Bools are bit-packed.

// validityBytes returns the number of bytes a validity bitmap needs for n values.
func validityBytes(n int) int { return (n + 7) / 8 }

// buildValidityMask writes a present-bit bitmap for a page-local present mask of
// count rows. Bit j is set when present[j] is true. It returns a nil bitmap and
// zero when the mask is nil or holds no nulls, so a page with no nulls carries
// no bitmap and clears its nulls_present flag.
func buildValidityMask(present []bool, count int) (bitmap []byte, nullCount int) {
	if present == nil {
		return nil, 0
	}
	bm := make([]byte, validityBytes(count))
	for j := 0; j < count; j++ {
		if present[j] {
			bm[j>>3] |= 1 << uint(j&7)
		} else {
			nullCount++
		}
	}
	if nullCount == 0 {
		return nil, 0
	}
	return bm, nullCount
}

// validAt reports whether row start+j is present, reading the bitmap when one
// was written and treating an absent bitmap as all-present.
func validAt(bitmap []byte, j int) bool {
	if bitmap == nil {
		return true
	}
	return bitmap[j>>3]&(1<<uint(j&7)) != 0
}

// encodePlain appends the PLAIN bytes for the present values of rows
// [start, start+count) of col to dst. It dispatches on the field type.
func encodePlain(dst []byte, t LogicalType, col Column, start, count int, present []bool) []byte {
	switch t {
	case TypeBool:
		vals := col.Data.([]bool)
		// Bit-pack present bools in order.
		bitPos := 0
		var cur byte
		var out []byte
		for j := 0; j < count; j++ {
			if !isPresent(present, j) {
				continue
			}
			if vals[start+j] {
				cur |= 1 << uint(bitPos&7)
			}
			bitPos++
			if bitPos&7 == 0 {
				out = append(out, cur)
				cur = 0
			}
		}
		if bitPos&7 != 0 {
			out = append(out, cur)
		}
		return append(dst, out...)
	case TypeInt8:
		vals := col.Data.([]int8)
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				dst = append(dst, byte(vals[start+j]))
			}
		}
		return dst
	case TypeUint8:
		vals := col.Data.([]uint8)
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				dst = append(dst, vals[start+j])
			}
		}
		return dst
	case TypeInt16:
		vals := col.Data.([]int16)
		var b [2]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint16(b[:], uint16(vals[start+j]))
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeUint16:
		vals := col.Data.([]uint16)
		var b [2]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint16(b[:], vals[start+j])
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeInt32:
		vals := col.Data.([]int32)
		var b [4]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint32(b[:], uint32(vals[start+j]))
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeUint32:
		vals := col.Data.([]uint32)
		var b [4]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint32(b[:], vals[start+j])
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeFloat32:
		vals := col.Data.([]float32)
		var b [4]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint32(b[:], math.Float32bits(vals[start+j]))
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeInt64, TypeTimestampMicros:
		vals := col.Data.([]int64)
		var b [8]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint64(b[:], uint64(vals[start+j]))
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeUint64:
		vals := col.Data.([]uint64)
		var b [8]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint64(b[:], vals[start+j])
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeFloat64:
		vals := col.Data.([]float64)
		var b [8]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				binary.LittleEndian.PutUint64(b[:], math.Float64bits(vals[start+j]))
				dst = append(dst, b[:]...)
			}
		}
		return dst
	case TypeString:
		vals := col.Data.([]string)
		var lb [binary.MaxVarintLen64]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				n := binary.PutUvarint(lb[:], uint64(len(vals[start+j])))
				dst = append(dst, lb[:n]...)
				dst = append(dst, vals[start+j]...)
			}
		}
		return dst
	case TypeBytes, TypeBlobRef:
		vals := col.Data.([][]byte)
		var lb [binary.MaxVarintLen64]byte
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				n := binary.PutUvarint(lb[:], uint64(len(vals[start+j])))
				dst = append(dst, lb[:n]...)
				dst = append(dst, vals[start+j]...)
			}
		}
		return dst
	default:
		panic(fmt.Sprintf("tatami: encodePlain unsupported type %s", t))
	}
}

// isPresent reports whether the j-th row in a page range is present, given the
// per-range present mask (nil means all present).
func isPresent(present []bool, j int) bool {
	return present == nil || present[j]
}

// decodePlain reconstructs num values from PLAIN bytes, scattering present
// values into the positions marked by bitmap. It returns a typed slice for the
// field type. Null positions hold the type's zero value.
func decodePlain(t LogicalType, payload []byte, num int, bitmap []byte) (any, error) {
	switch t {
	case TypeBool:
		out := make([]bool, num)
		bit := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			bytePos := bit >> 3
			if bytePos >= len(payload) {
				return nil, fmt.Errorf("tatami: short bool payload")
			}
			out[i] = payload[bytePos]&(1<<uint(bit&7)) != 0
			bit++
		}
		return out, nil
	case TypeInt8:
		out := make([]int8, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p >= len(payload) {
				return nil, fmt.Errorf("tatami: short int8 payload")
			}
			out[i] = int8(payload[p])
			p++
		}
		return out, nil
	case TypeUint8:
		out := make([]uint8, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p >= len(payload) {
				return nil, fmt.Errorf("tatami: short uint8 payload")
			}
			out[i] = payload[p]
			p++
		}
		return out, nil
	case TypeInt16:
		out := make([]int16, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+2 > len(payload) {
				return nil, fmt.Errorf("tatami: short int16 payload")
			}
			out[i] = int16(binary.LittleEndian.Uint16(payload[p:]))
			p += 2
		}
		return out, nil
	case TypeUint16:
		out := make([]uint16, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+2 > len(payload) {
				return nil, fmt.Errorf("tatami: short uint16 payload")
			}
			out[i] = binary.LittleEndian.Uint16(payload[p:])
			p += 2
		}
		return out, nil
	case TypeInt32:
		out := make([]int32, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+4 > len(payload) {
				return nil, fmt.Errorf("tatami: short int32 payload")
			}
			out[i] = int32(binary.LittleEndian.Uint32(payload[p:]))
			p += 4
		}
		return out, nil
	case TypeUint32:
		out := make([]uint32, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+4 > len(payload) {
				return nil, fmt.Errorf("tatami: short uint32 payload")
			}
			out[i] = binary.LittleEndian.Uint32(payload[p:])
			p += 4
		}
		return out, nil
	case TypeFloat32:
		out := make([]float32, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+4 > len(payload) {
				return nil, fmt.Errorf("tatami: short float32 payload")
			}
			out[i] = math.Float32frombits(binary.LittleEndian.Uint32(payload[p:]))
			p += 4
		}
		return out, nil
	case TypeInt64, TypeTimestampMicros:
		out := make([]int64, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+8 > len(payload) {
				return nil, fmt.Errorf("tatami: short int64 payload")
			}
			out[i] = int64(binary.LittleEndian.Uint64(payload[p:]))
			p += 8
		}
		return out, nil
	case TypeUint64:
		out := make([]uint64, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+8 > len(payload) {
				return nil, fmt.Errorf("tatami: short uint64 payload")
			}
			out[i] = binary.LittleEndian.Uint64(payload[p:])
			p += 8
		}
		return out, nil
	case TypeFloat64:
		out := make([]float64, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			if p+8 > len(payload) {
				return nil, fmt.Errorf("tatami: short float64 payload")
			}
			out[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[p:]))
			p += 8
		}
		return out, nil
	case TypeString:
		out := make([]string, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			l, n := binary.Uvarint(payload[p:])
			if n <= 0 {
				return nil, fmt.Errorf("tatami: bad string length varint")
			}
			p += n
			if p+int(l) > len(payload) {
				return nil, fmt.Errorf("tatami: short string payload")
			}
			out[i] = string(payload[p : p+int(l)])
			p += int(l)
		}
		return out, nil
	case TypeBytes, TypeBlobRef:
		out := make([][]byte, num)
		p := 0
		for i := 0; i < num; i++ {
			if !validAt(bitmap, i) {
				continue
			}
			l, n := binary.Uvarint(payload[p:])
			if n <= 0 {
				return nil, fmt.Errorf("tatami: bad bytes length varint")
			}
			p += n
			if p+int(l) > len(payload) {
				return nil, fmt.Errorf("tatami: short bytes payload")
			}
			v := make([]byte, l)
			copy(v, payload[p:p+int(l)])
			out[i] = v
			p += int(l)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("tatami: decodePlain unsupported type %s", t)
	}
}
