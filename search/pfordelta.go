package search

// PForDelta (Patched Frame-of-Reference) bit-packs a block of values at a width
// that fits the bulk of the distribution and stores the few values that exceed
// it as patched exceptions. This keeps one outlier frequency - a stop word
// appearing hundreds of times in a document - from forcing the whole block to a
// wide bit width, which plain FOR would do. The codec uses it for the frequency
// stream; doc-id gaps use plain FOR, which the block layout records.

// uvarintLen returns the encoded length of x as an unsigned varint.
func uvarintLen(x uint64) int {
	n := 1
	for x >= 0x80 {
		x >>= 7
		n++
	}
	return n
}

// chooseWidth picks the low-bits width that minimizes the encoded size of vs:
// the packed body at that width plus the exception records for values that do
// not fit. It is the OptPFD-style search over candidate widths.
func chooseWidth(vs []uint32) (width, exceptions int) {
	best, bestW, bestExc := -1, 1, 0
	for w := 1; w <= 32; w++ {
		limit := ^uint32(0)
		if w < 32 {
			limit = uint32(1)<<uint(w) - 1
		}
		exc, excBytes := 0, 0
		for _, v := range vs {
			if v > limit {
				exc++
				excBytes += 1 + uvarintLen(uint64(v))
			}
		}
		cost := packedLen(len(vs), w) + excBytes
		if best < 0 || cost < best {
			best, bestW, bestExc = cost, w, exc
		}
	}
	return bestW, bestExc
}

// appendPForDelta appends vs (a single block, len <= BlockSize) to dst.
// Layout: width byte, exception-count byte, exception records
// (position byte + uvarint full value) in ascending position order, then the
// low-bits packed body.
func appendPForDelta(dst []byte, vs []uint32) []byte {
	w, _ := chooseWidth(vs)
	limit := ^uint32(0)
	if w < 32 {
		limit = uint32(1)<<uint(w) - 1
	}

	low := make([]uint32, len(vs))
	var excPos []byte
	var excCount int
	for i, v := range vs {
		low[i] = v & limit
		if v > limit {
			excCount++
			excPos = append(excPos, byte(i))
			excPos = appendUvarint(excPos, uint64(v))
		}
	}
	dst = append(dst, byte(w), byte(excCount))
	dst = append(dst, excPos...)
	dst = packBits(dst, low, w)
	return dst
}

// readPForDelta decodes len(out) values written by appendPForDelta into out and
// returns the number of bytes consumed.
func readPForDelta(src []byte, out []uint32) int {
	w := int(src[0])
	excCount := int(src[1])
	pos := 2
	type patch struct {
		at  int
		val uint32
	}
	patches := make([]patch, excCount)
	for i := range excCount {
		at := int(src[pos])
		pos++
		v, n := readUvarint(src[pos:])
		pos += n
		patches[i] = patch{at: at, val: uint32(v)}
	}
	pos += unpackBits(src[pos:], out, len(out), w)
	for _, p := range patches {
		out[p.at] = p.val
	}
	return pos
}
