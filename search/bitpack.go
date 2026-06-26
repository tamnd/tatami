package search

// The posting-list codec: 128-document blocks, Frame-of-Reference bit-packing
// for delta-encoded document ids, a group-varint tail for the final under-128
// block, and per-block max metadata for the block-max WAND loop. The decode path
// is branch-free block bit-unpacking with a scalar Go fallback, so the format
// stays architecture-independent while leaving room for a SIMD specialization
// later (Spec 2066, 09-search-scale.md, section 4).

import "math/bits"

// BlockSize is the document count per fixed block. 128 is the Lucene/Tantivy
// block size: large enough that branch-free block decode beats a branch-per-byte
// varint, small enough that a skip lands close to the target.
const BlockSize = 128

// minWidth returns the number of bits needed to represent v, with a floor of 1
// so an all-zero block still round-trips (a width of 0 would encode nothing).
func minWidth(v uint32) int {
	if v == 0 {
		return 1
	}
	return bits.Len32(v)
}

// maxWidth returns the bit width needed for the largest value in vs.
func maxWidth(vs []uint32) int {
	var m uint32
	for _, v := range vs {
		if v > m {
			m = v
		}
	}
	return minWidth(m)
}

// packBits appends len(vs) values, each width bits wide, to dst as a contiguous
// little-endian bit stream. width must be >= the width of every value.
func packBits(dst []byte, vs []uint32, width int) []byte {
	if width == 0 {
		width = 1
	}
	var acc uint64
	var nbits int
	for _, v := range vs {
		acc |= uint64(v) << uint(nbits)
		nbits += width
		for nbits >= 8 {
			dst = append(dst, byte(acc))
			acc >>= 8
			nbits -= 8
		}
	}
	if nbits > 0 {
		dst = append(dst, byte(acc))
	}
	return dst
}

// unpackBits reads count values of the given width from src (the inverse of
// packBits) into out, and returns the number of bytes consumed.
func unpackBits(src []byte, out []uint32, count, width int) int {
	if width == 0 {
		width = 1
	}
	mask := uint64(1)<<uint(width) - 1
	var acc uint64
	var nbits int
	pos := 0
	for i := range count {
		for nbits < width {
			acc |= uint64(src[pos]) << uint(nbits)
			pos++
			nbits += 8
		}
		out[i] = uint32(acc & mask)
		acc >>= uint(width)
		nbits -= width
	}
	return pos
}

// packedLen returns the byte length of count values packed at the given width.
func packedLen(count, width int) int {
	if width == 0 {
		width = 1
	}
	return (count*width + 7) / 8
}

// appendUvarint appends x as a LEB128 unsigned varint.
func appendUvarint(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

// readUvarint reads a LEB128 unsigned varint and returns it with bytes consumed.
func readUvarint(src []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, b := range src {
		if b < 0x80 {
			return x | uint64(b)<<s, i + 1
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, 0
}
