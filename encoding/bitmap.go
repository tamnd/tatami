package encoding

import "fmt"

// EncodeBitmap packs bools into ceil(n/8) bytes, bit i of byte i/8 holding
// vs[i] with the least significant bit first. This is the natural encoding for
// a dense boolean column: one bit per value, which zstd then squeezes further
// when the column is mostly one value.
func EncodeBitmap(dst []byte, vs []bool) []byte {
	for i := 0; i < len(vs); i += 8 {
		var b byte
		for j := 0; j < 8 && i+j < len(vs); j++ {
			if vs[i+j] {
				b |= 1 << uint(j)
			}
		}
		dst = append(dst, b)
	}
	return dst
}

// DecodeBitmap reads len(out) bools packed by EncodeBitmap.
func DecodeBitmap(src []byte, out []bool) error {
	need := (len(out) + 7) / 8
	if len(src) < need {
		return fmt.Errorf("tatami/encoding: short bitmap stream")
	}
	for i := range out {
		out[i] = src[i/8]&(1<<uint(i%8)) != 0
	}
	return nil
}
