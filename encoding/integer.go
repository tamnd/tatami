package encoding

import "fmt"

// This file implements the integer encodings over a dense []uint64 of present
// values: BITPACK_FOR (the substrate), DELTA, RLE, GROUPVARINT, and PFORDELTA.
// The caller widens its typed column into []uint64 and tells each FOR-based
// encoder whether to read the range in signed order. Every encoder round-trips
// for any input, including the empty slice; efficiency is the sampler's job.

// minRun is the run length at or above which RLE emits a run rather than
// literals. Eight is the byte-boundary group size and a safe floor; the sampler
// compares full sizes, so a slightly loose threshold never picks a worse page.
const minRun = 8

// EncodeInts encodes vs with the given encoding and returns the payload and
// whether the encoding applies. GROUPVARINT and PFORDELTA only apply when every
// value fits in 32 bits, and PFORDELTA additionally needs at most 256 values so
// its one-byte exception positions stay in range; the sampler skips them
// otherwise. BITPACK_FOR, DELTA, and RLE apply to any input.
func EncodeInts(id ID, dst []byte, vs []uint64, signed bool) ([]byte, bool) {
	switch id {
	case BitpackFOR:
		return encodeBitpackFOR(dst, vs, signed), true
	case Delta:
		return encodeDelta(dst, vs), true
	case RLE:
		return encodeRLE(dst, vs), true
	case GroupVarint:
		if !fits32(vs) {
			return dst, false
		}
		return encodeGroupVarint(dst, vs), true
	case PForDelta:
		if !fits32(vs) || len(vs) > 256 {
			return dst, false
		}
		return encodePForDelta(dst, vs), true
	default:
		return dst, false
	}
}

// DecodeInts decodes count values (len(out)) of encoding id into out.
func DecodeInts(id ID, src []byte, out []uint64, signed bool) error {
	switch id {
	case BitpackFOR:
		return decodeBitpackFOR(src, out)
	case Delta:
		return decodeDelta(src, out)
	case RLE:
		return decodeRLE(src, out)
	case GroupVarint:
		return decodeGroupVarint(src, out)
	case PForDelta:
		return decodePForDelta(src, out)
	default:
		return fmt.Errorf("tatami/encoding: not an integer encoding: %d", id)
	}
}

func fits32(vs []uint64) bool {
	for _, v := range vs {
		if v > 0xFFFFFFFF {
			return false
		}
	}
	return true
}

// BITPACK_FOR: u8 width, varint FOR base, packed residuals.

func encodeBitpackFOR(dst []byte, vs []uint64, signed bool) []byte {
	if len(vs) == 0 {
		return append(dst, 1)
	}
	base, max := minMax(vs, signed)
	width := minWidth(max - base)
	dst = append(dst, byte(width))
	dst = appendUvarint(dst, base)
	res := make([]uint64, len(vs))
	for i, v := range vs {
		res[i] = v - base
	}
	return packBits(dst, res, width)
}

func decodeBitpackFOR(src []byte, out []uint64) error {
	if len(out) == 0 {
		return nil
	}
	if len(src) < 1 {
		return fmt.Errorf("tatami/encoding: short bitpack_for header")
	}
	width := int(src[0])
	base, n, err := readUvarint(src[1:])
	if err != nil {
		return err
	}
	if _, err := unpackBits(src[1+n:], out, len(out), width); err != nil {
		return err
	}
	for i := range out {
		out[i] += base
	}
	return nil
}

// DELTA: varint first value, then a BITPACK_FOR stream over zig-zag deltas.

func encodeDelta(dst []byte, vs []uint64) []byte {
	if len(vs) == 0 {
		return dst
	}
	dst = appendUvarint(dst, vs[0])
	deltas := make([]uint64, len(vs)-1)
	for i := 1; i < len(vs); i++ {
		deltas[i-1] = zigzag(int64(vs[i] - vs[i-1]))
	}
	return encodeBitpackFOR(dst, deltas, false)
}

func decodeDelta(src []byte, out []uint64) error {
	if len(out) == 0 {
		return nil
	}
	first, n, err := readUvarint(src)
	if err != nil {
		return err
	}
	out[0] = first
	deltas := make([]uint64, len(out)-1)
	if err := decodeBitpackFOR(src[n:], deltas); err != nil {
		return err
	}
	for i := 1; i < len(out); i++ {
		out[i] = out[i-1] + uint64(unzigzag(deltas[i-1]))
	}
	return nil
}

// RLE: u8 width, then alternating run and bitpacked-literal groups, each with a
// control varint whose low bit selects the mode.

func encodeRLE(dst []byte, vs []uint64) []byte {
	if len(vs) == 0 {
		return append(dst, 1)
	}
	_, max := minMax(vs, false)
	width := minWidth(max)
	dst = append(dst, byte(width))
	i := 0
	for i < len(vs) {
		run := runLengthAt(vs, i)
		if run >= minRun {
			dst = appendUvarint(dst, uint64(run)<<1|1)
			dst = packBits(dst, vs[i:i+1], width)
			i += run
			continue
		}
		// Gather literals up to the next qualifying run. The control carries
		// the exact literal count, so interior groups need no padding; each
		// group's packed body is flushed to a byte boundary by packBits.
		j := i
		for j < len(vs) && runLengthAt(vs, j) < minRun {
			j++
		}
		dst = appendUvarint(dst, uint64(j-i)<<1) // low bit 0 selects literal mode
		dst = packBits(dst, vs[i:j], width)
		i = j
	}
	return dst
}

func decodeRLE(src []byte, out []uint64) error {
	if len(src) < 1 {
		return fmt.Errorf("tatami/encoding: short rle header")
	}
	width := int(src[0])
	pos := 1
	i := 0
	for i < len(out) {
		ctrl, k, err := readUvarint(src[pos:])
		if err != nil {
			return err
		}
		pos += k
		if ctrl&1 == 1 {
			runLen := int(ctrl >> 1)
			var one [1]uint64
			n, err := unpackBits(src[pos:], one[:], 1, width)
			if err != nil {
				return err
			}
			pos += n
			for r := 0; r < runLen && i < len(out); r++ {
				out[i] = one[0]
				i++
			}
		} else {
			groupVals := int(ctrl >> 1)
			tmp := make([]uint64, groupVals)
			n, err := unpackBits(src[pos:], tmp, groupVals, width)
			if err != nil {
				return err
			}
			pos += n
			for j := 0; j < groupVals && i < len(out); j++ {
				out[i] = tmp[j]
				i++
			}
		}
	}
	return nil
}

// runLengthAt returns the length of the run of equal values starting at i.
func runLengthAt(vs []uint64, i int) int {
	j := i + 1
	for j < len(vs) && vs[j] == vs[i] {
		j++
	}
	return j - i
}

// GROUPVARINT: four values per control byte, each 2-bit field holding bytes-1.

func encodeGroupVarint(dst []byte, vs []uint64) []byte {
	for i := 0; i < len(vs); i += 4 {
		n := 4
		if rem := len(vs) - i; rem < 4 {
			n = rem
		}
		var ctrl byte
		var lens [4]int
		for j := 0; j < n; j++ {
			l := byteLen(vs[i+j])
			lens[j] = l
			ctrl |= byte(l-1) << uint(2*j)
		}
		dst = append(dst, ctrl)
		for j := 0; j < n; j++ {
			v := vs[i+j]
			for k := 0; k < lens[j]; k++ {
				dst = append(dst, byte(v))
				v >>= 8
			}
		}
	}
	return dst
}

func decodeGroupVarint(src []byte, out []uint64) error {
	pos := 0
	for i := 0; i < len(out); i += 4 {
		n := 4
		if rem := len(out) - i; rem < 4 {
			n = rem
		}
		if pos >= len(src) {
			return fmt.Errorf("tatami/encoding: short groupvarint")
		}
		ctrl := src[pos]
		pos++
		for j := 0; j < n; j++ {
			l := int((ctrl>>uint(2*j))&0x3) + 1
			if pos+l > len(src) {
				return fmt.Errorf("tatami/encoding: short groupvarint value")
			}
			var v uint64
			for b := 0; b < l; b++ {
				v |= uint64(src[pos]) << uint(8*b)
				pos++
			}
			out[i+j] = v
		}
	}
	return nil
}

// byteLen returns the number of bytes (1..4) needed to hold v, which fits in 32
// bits by the time GROUPVARINT is selected.
func byteLen(v uint64) int {
	switch {
	case v < 1<<8:
		return 1
	case v < 1<<16:
		return 2
	case v < 1<<24:
		return 3
	default:
		return 4
	}
}

// PFORDELTA: u8 width, u8 exception count, ascending (pos, uvarint value)
// exceptions, then the low width bits of every value packed.

func encodePForDelta(dst []byte, vs []uint64) []byte {
	w := choosePForWidth(vs)
	limit := mask(w)
	low := make([]uint64, len(vs))
	var exc []byte
	excCount := 0
	for i, v := range vs {
		low[i] = v & limit
		if v > limit {
			excCount++
			exc = append(exc, byte(i))
			exc = appendUvarint(exc, v)
		}
	}
	dst = append(dst, byte(w), byte(excCount))
	dst = append(dst, exc...)
	return packBits(dst, low, w)
}

func decodePForDelta(src []byte, out []uint64) error {
	if len(src) < 2 {
		return fmt.Errorf("tatami/encoding: short pfordelta header")
	}
	w := int(src[0])
	excCount := int(src[1])
	pos := 2
	type patch struct {
		at  int
		val uint64
	}
	patches := make([]patch, excCount)
	for i := 0; i < excCount; i++ {
		if pos >= len(src) {
			return fmt.Errorf("tatami/encoding: short pfordelta exception")
		}
		at := int(src[pos])
		pos++
		v, n, err := readUvarint(src[pos:])
		if err != nil {
			return err
		}
		pos += n
		patches[i] = patch{at, v}
	}
	if _, err := unpackBits(src[pos:], out, len(out), w); err != nil {
		return err
	}
	for _, p := range patches {
		if p.at >= len(out) {
			return fmt.Errorf("tatami/encoding: pfordelta exception out of range")
		}
		out[p.at] = p.val
	}
	return nil
}

// choosePForWidth picks the low-bits width minimizing packed body plus
// exception overhead, the OptPFD search.
func choosePForWidth(vs []uint64) int {
	best, bestW := -1, 1
	for w := 1; w <= 32; w++ {
		limit := mask(w)
		excBytes := 0
		for _, v := range vs {
			if v > limit {
				excBytes += 1 + uvarintLen(v)
			}
		}
		cost := packedLen(len(vs), w) + excBytes
		if best < 0 || cost < best {
			best, bestW = cost, w
		}
	}
	return bestW
}

// uvarintLen returns the number of bytes binary.AppendUvarint would write for v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}
