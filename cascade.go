package tatami

import "github.com/tamnd/tatami/encoding"

// This file is the bridge between a column chunk's pages and the physical
// encoding package. On write it gathers the present values of a page into a
// dense slice, runs the greedy sampler over the candidate encodings for the
// column's type, and returns the winner. On read it decodes the dense present
// values and scatters them back into a full typed slice, leaving null slots at
// the zero value. Floats, strings, and bytes stay on the PLAIN path from M0;
// the cascade handles the integer family and bool, where structured redundancy
// (narrow ranges, runs, monotonic steps) pays off before the block codec runs.

// intCandidates is the set of integer encodings the sampler tries on every
// page, on top of the PLAIN floor. Applicability guards inside the encoding
// package decline GROUPVARINT and PFORDELTA when a page does not fit their
// 32-bit (and, for PFORDELTA, 256-value) envelope, so listing them here is
// safe for any column; the sampler simply never picks a declined candidate.
var intCandidates = []encoding.ID{
	encoding.BitpackFOR,
	encoding.Delta,
	encoding.RLE,
	encoding.GroupVarint,
	encoding.PForDelta,
}

// encodePageValues returns the encoding chosen for one page and the value bytes
// that follow the validity bitmap in the page payload. count is the page row
// span starting at start; present is the page-local validity mask (nil when the
// page has no nulls).
func encodePageValues(t LogicalType, col Column, start, count int, present []bool) (Encoding, []byte) {
	switch {
	case t == TypeBool:
		return EncBitmap, encoding.EncodeBitmap(nil, gatherBool(col, start, count, present))
	case isIntegerType(t):
		dense, signed := gatherUint64(t, col, start, count, present)
		best := EncPlain
		bestBytes := encodePlain(nil, t, col, start, count, present)
		for _, id := range intCandidates {
			b, ok := encoding.EncodeInts(id, nil, dense, signed)
			if ok && len(b) < len(bestBytes) {
				best, bestBytes = Encoding(id), b
			}
		}
		return best, bestBytes
	default:
		return EncPlain, encodePlain(nil, t, col, start, count, present)
	}
}

// decodePageValues reconstructs num values from a page's value bytes (the
// payload after the validity bitmap) given the encoding the writer chose.
func decodePageValues(t LogicalType, enc Encoding, body []byte, num int, bitmap []byte) (any, error) {
	if enc == EncPlain {
		return decodePlain(t, body, num, bitmap)
	}
	present := presentCount(bitmap, num)
	switch {
	case t == TypeBool && enc == EncBitmap:
		dense := make([]bool, present)
		if err := encoding.DecodeBitmap(body, dense); err != nil {
			return nil, err
		}
		return scatterBool(dense, num, bitmap), nil
	case isIntegerType(t):
		dense := make([]uint64, present)
		_, signed := signedWidth(t)
		if err := encoding.DecodeInts(encoding.ID(enc), body, dense, signed); err != nil {
			return nil, err
		}
		return scatterUint64(t, dense, num, bitmap), nil
	default:
		return decodePlain(t, body, num, bitmap)
	}
}

// isIntegerType reports whether t is one of the fixed-width integer logical
// types the cascade widens to uint64. Timestamps share the int64 path.
func isIntegerType(t LogicalType) bool {
	switch t {
	case TypeInt8, TypeInt16, TypeInt32, TypeInt64,
		TypeUint8, TypeUint16, TypeUint32, TypeUint64,
		TypeTimestampMicros:
		return true
	default:
		return false
	}
}

// signedWidth reports whether t is read in signed two's-complement order and
// its width in bits. The signed flag drives the frame-of-reference base choice.
func signedWidth(t LogicalType) (width int, signed bool) {
	switch t {
	case TypeInt8:
		return 8, true
	case TypeInt16:
		return 16, true
	case TypeInt32:
		return 32, true
	case TypeInt64, TypeTimestampMicros:
		return 64, true
	case TypeUint8:
		return 8, false
	case TypeUint16:
		return 16, false
	case TypeUint32:
		return 32, false
	case TypeUint64:
		return 64, false
	default:
		return 64, false
	}
}

// gatherUint64 widens the present integer values of rows [start, start+count)
// into a dense []uint64. Signed types are sign-extended so int64 ordering of
// the widened values matches the column's ordering, which keeps the FOR base
// and zig-zag deltas correct.
func gatherUint64(t LogicalType, col Column, start, count int, present []bool) ([]uint64, bool) {
	_, signed := signedWidth(t)
	out := make([]uint64, 0, count)
	switch d := col.Data.(type) {
	case []int8:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(int64(d[start+j])))
			}
		}
	case []int16:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(int64(d[start+j])))
			}
		}
	case []int32:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(int64(d[start+j])))
			}
		}
	case []int64:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(d[start+j]))
			}
		}
	case []uint8:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(d[start+j]))
			}
		}
	case []uint16:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(d[start+j]))
			}
		}
	case []uint32:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, uint64(d[start+j]))
			}
		}
	case []uint64:
		for j := 0; j < count; j++ {
			if isPresent(present, j) {
				out = append(out, d[start+j])
			}
		}
	}
	return out, signed
}

// scatterUint64 builds a full typed slice of num rows, narrowing each dense
// present value back to t's element type and placing it at its present row.
func scatterUint64(t LogicalType, dense []uint64, num int, bitmap []byte) any {
	switch t {
	case TypeInt8:
		out := make([]int8, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = int8(dense[k])
				k++
			}
		}
		return out
	case TypeInt16:
		out := make([]int16, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = int16(dense[k])
				k++
			}
		}
		return out
	case TypeInt32:
		out := make([]int32, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = int32(dense[k])
				k++
			}
		}
		return out
	case TypeInt64, TypeTimestampMicros:
		out := make([]int64, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = int64(dense[k])
				k++
			}
		}
		return out
	case TypeUint8:
		out := make([]uint8, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = uint8(dense[k])
				k++
			}
		}
		return out
	case TypeUint16:
		out := make([]uint16, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = uint16(dense[k])
				k++
			}
		}
		return out
	case TypeUint32:
		out := make([]uint32, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = uint32(dense[k])
				k++
			}
		}
		return out
	case TypeUint64:
		out := make([]uint64, num)
		k := 0
		for i := 0; i < num; i++ {
			if validAt(bitmap, i) {
				out[i] = dense[k]
				k++
			}
		}
		return out
	default:
		return nil
	}
}

// gatherBool collects the present bools of rows [start, start+count) densely.
func gatherBool(col Column, start, count int, present []bool) []bool {
	vals := col.Data.([]bool)
	out := make([]bool, 0, count)
	for j := 0; j < count; j++ {
		if isPresent(present, j) {
			out = append(out, vals[start+j])
		}
	}
	return out
}

// scatterBool spreads dense present bools across num rows by the bitmap.
func scatterBool(dense []bool, num int, bitmap []byte) []bool {
	out := make([]bool, num)
	k := 0
	for i := 0; i < num; i++ {
		if validAt(bitmap, i) {
			out[i] = dense[k]
			k++
		}
	}
	return out
}

// presentCount returns the number of present rows in a page: the set bits of
// the validity bitmap, or all num rows when the page carries no bitmap.
func presentCount(bitmap []byte, num int) int {
	if bitmap == nil {
		return num
	}
	c := 0
	for i := 0; i < num; i++ {
		if validAt(bitmap, i) {
			c++
		}
	}
	return c
}
