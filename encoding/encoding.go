// Package encoding implements the per-page value encodings that run before the
// block codec, the encode stage of the page pipeline pinned in the format spec.
// It is the heart of tatami's compact story: each encoding removes a kind of
// structured redundancy (narrow ranges, runs, monotonic steps) that a general
// block compressor would otherwise have to rediscover byte by byte.
//
// The package is deliberately physical and self-contained. It does not import
// the root tatami package and knows nothing of logical types or nulls. Integer
// encodings work on a dense []uint64 of present values (the caller widens its
// typed slice and handles nulls one level up), bool works on a []bool, and the
// FOR base is chosen in signed or unsigned order as the caller asks. Every
// encoder round-trips: Decode(Encode(vs)) == vs for any input including the
// empty slice.
package encoding

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// ID is the encoding enum recorded in a page header. The values are pinned by
// the format canon and must never be renumbered; they match tatami.Encoding.
type ID uint8

const (
	Plain       ID = 0
	RLE         ID = 1
	Dictionary  ID = 2
	BitpackFOR  ID = 3
	Delta       ID = 4
	GroupVarint ID = 5
	PForDelta   ID = 6
	FSST        ID = 7
	Bitmap      ID = 8
)

// minMax returns the minimum and maximum of vs. When signed is true the order
// is the two's-complement signed order, so a column of negatives picks the most
// negative value as its frame-of-reference base. vs must be non-empty.
func minMax(vs []uint64, signed bool) (mn, mx uint64) {
	mn, mx = vs[0], vs[0]
	for _, v := range vs[1:] {
		if less(v, mn, signed) {
			mn = v
		}
		if less(mx, v, signed) {
			mx = v
		}
	}
	return mn, mx
}

// less reports whether a < b, in signed two's-complement order when signed.
func less(a, b uint64, signed bool) bool {
	if signed {
		return int64(a) < int64(b)
	}
	return a < b
}

// minWidth returns the number of bits needed to represent v, at least 1 so a
// page of all-equal values (residual 0) still packs one bit per value rather
// than zero, which keeps the unpack loop well defined.
func minWidth(v uint64) int {
	if v == 0 {
		return 1
	}
	return bits.Len64(v)
}

// packBits appends len(vs) values, each width bits wide, as a contiguous
// little-endian bit stream. It writes one value across as many partial-byte
// chunks as it takes, so the pending accumulator never holds more than eight
// bits and widths up to 64 pack without losing the high bits.
func packBits(dst []byte, vs []uint64, width int) []byte {
	if width == 0 {
		return dst
	}
	var acc uint64 // holds nbits pending bits, always < 8 at the top of the loop
	var nbits int
	for _, v := range vs {
		v &= mask(width)
		for b := 0; b < width; {
			free := 8 - nbits
			chunk := width - b
			if chunk > free {
				chunk = free
			}
			acc |= ((v >> uint(b)) & mask(chunk)) << uint(nbits)
			nbits += chunk
			b += chunk
			if nbits == 8 {
				dst = append(dst, byte(acc))
				acc, nbits = 0, 0
			}
		}
	}
	if nbits > 0 {
		dst = append(dst, byte(acc))
	}
	return dst
}

// unpackBits reads count values of the given width into out (the inverse of
// packBits) and returns the number of source bytes consumed.
func unpackBits(src []byte, out []uint64, count, width int) (int, error) {
	if width == 0 {
		for i := 0; i < count; i++ {
			out[i] = 0
		}
		return 0, nil
	}
	var acc uint64 // holds nbits unconsumed bits from src
	var nbits, pos int
	for i := 0; i < count; i++ {
		var v uint64
		for got := 0; got < width; {
			if nbits == 0 {
				if pos >= len(src) {
					return 0, fmt.Errorf("tatami/encoding: short bitpack stream")
				}
				acc = uint64(src[pos])
				pos++
				nbits = 8
			}
			take := width - got
			if take > nbits {
				take = nbits
			}
			v |= (acc & mask(take)) << uint(got)
			acc >>= uint(take)
			nbits -= take
			got += take
		}
		out[i] = v
	}
	return pos, nil
}

// mask returns a width-bit low mask, handling width 64 where 1<<64 overflows.
func mask(width int) uint64 {
	if width >= 64 {
		return ^uint64(0)
	}
	return uint64(1)<<uint(width) - 1
}

// packedLen returns the byte length of count values packed at width bits.
func packedLen(count, width int) int {
	return (count*width + 7) / 8
}

// zigzag maps a signed delta to an unsigned value that stays small for small
// magnitudes of either sign, so FOR bitpacking handles decreases as cheaply as
// increases.
func zigzag(d int64) uint64 {
	return uint64((d << 1) ^ (d >> 63))
}

// unzigzag inverts zigzag.
func unzigzag(z uint64) int64 {
	return int64(z>>1) ^ -int64(z&1)
}

// appendUvarint and readUvarint wrap the standard library so the encodings read
// the same way the rest of the format does.
func appendUvarint(dst []byte, v uint64) []byte {
	return binary.AppendUvarint(dst, v)
}

func readUvarint(src []byte) (uint64, int, error) {
	v, n := binary.Uvarint(src)
	if n <= 0 {
		return 0, 0, fmt.Errorf("tatami/encoding: bad uvarint")
	}
	return v, n, nil
}
