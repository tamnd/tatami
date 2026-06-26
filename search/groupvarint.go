package search

// Group-varint encodes integers four at a time: one control byte holds four
// 2-bit length fields (each 0..3, meaning 1..4 data bytes), followed by the data
// bytes for the group. Decoding reads the control byte once and then copies
// known-length runs, which is branch-free per value - the reason Google's
// VarintGB beats a branch-per-byte LEB128 on the decode hot path. The codec uses
// it for the final under-128 block, where fixed-width FOR packing would waste
// bits on a short run.

// byteLen returns the number of bytes (1..4) needed to hold v.
func byteLen(v uint32) int {
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

// appendGroupVarint appends vs to dst in group-varint framing. Any length is
// allowed; the final group is padded with zero-length (1-byte) values.
func appendGroupVarint(dst []byte, vs []uint32) []byte {
	for i := 0; i < len(vs); i += 4 {
		n := min(4, len(vs)-i)
		var ctrl byte
		lens := [4]int{1, 1, 1, 1}
		for j := range n {
			l := byteLen(vs[i+j])
			lens[j] = l
			ctrl |= byte(l-1) << uint(2*j)
		}
		dst = append(dst, ctrl)
		for j := range n {
			v := vs[i+j]
			for range lens[j] {
				dst = append(dst, byte(v))
				v >>= 8
			}
		}
	}
	return dst
}

// readGroupVarint reads count values written by appendGroupVarint into out and
// returns the number of bytes consumed.
func readGroupVarint(src []byte, out []uint32, count int) int {
	pos := 0
	for i := 0; i < count; i += 4 {
		n := min(4, count-i)
		ctrl := src[pos]
		pos++
		for j := range n {
			l := int((ctrl>>uint(2*j))&0x3) + 1
			var v uint32
			for b := range l {
				v |= uint32(src[pos]) << uint(8*b)
				pos++
			}
			out[i+j] = v
		}
	}
	return pos
}
