package encoding

import (
	"fmt"
	"testing"
)

// intEncodings is every integer encoding the cascade can pick, paired with the
// signed flag the FOR base uses. The test drives each one through a battery of
// value patterns and asserts an exact round-trip.
var intEncodings = []struct {
	id     ID
	signed bool
}{
	{BitpackFOR, false},
	{BitpackFOR, true},
	{Delta, false},
	{RLE, false},
	{GroupVarint, false},
	{PForDelta, false},
}

// intCases are the value patterns. Names describe the redundancy each pattern
// exercises so a failure points at the encoding's weak spot.
func intCases() map[string][]uint64 {
	cases := map[string][]uint64{
		"empty":        {},
		"single":       {42},
		"all_equal":    rep(0, 200),
		"all_equal_hi": rep(1<<30, 130),
		"narrow_range": {1000, 1001, 1002, 1000, 1003, 1001, 1002, 1000, 1004, 1002},
		"monotonic":    seq(500, 1, 300),
		"monotonic_2":  seq(0, 7, 300),
		"runs":         append(append(rep(5, 40), rep(9, 50)...), rep(5, 30)...),
		"run_outlier":  statusShape(),
		"sparse_exc":   withExceptions(),
		"max32":        {0, 1, 0xFFFFFFFF, 7, 0xFFFFFFFE, 3},
		"width_64":     {0, ^uint64(0), 1, ^uint64(0) - 1},
		"zero_then_hi": {0, 0, 0, 1 << 40},
	}
	return cases
}

func TestIntEncodingsRoundTrip(t *testing.T) {
	for _, enc := range intEncodings {
		for name, vs := range intCases() {
			t.Run(fmt.Sprintf("%v/%s/signed=%v", enc.id, name, enc.signed), func(t *testing.T) {
				payload, ok := EncodeInts(enc.id, nil, vs, enc.signed)
				if !ok {
					// GROUPVARINT and PFORDELTA decline wide or large inputs;
					// that is expected, not a failure.
					if applies(enc.id, vs) {
						t.Fatalf("encoding %v unexpectedly declined %s", enc.id, name)
					}
					return
				}
				out := make([]uint64, len(vs))
				if err := DecodeInts(enc.id, payload, out, enc.signed); err != nil {
					t.Fatalf("decode: %v", err)
				}
				assertEqualU64(t, vs, out)
			})
		}
	}
}

// applies mirrors EncodeInts's applicability guards so the test can tell a
// legitimate decline from a bug.
func applies(id ID, vs []uint64) bool {
	switch id {
	case GroupVarint:
		return fits32(vs)
	case PForDelta:
		return fits32(vs) && len(vs) <= 256
	default:
		return true
	}
}

func TestBitmapRoundTrip(t *testing.T) {
	cases := map[string][]bool{
		"empty":     {},
		"single":    {true},
		"byte_edge": pattern(8),
		"unaligned": pattern(13),
		"large":     pattern(1000),
	}
	for name, vs := range cases {
		t.Run(name, func(t *testing.T) {
			payload := EncodeBitmap(nil, vs)
			out := make([]bool, len(vs))
			if err := DecodeBitmap(payload, out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			for i := range vs {
				if out[i] != vs[i] {
					t.Fatalf("bit %d: got %v want %v", i, out[i], vs[i])
				}
			}
		})
	}
}

// TestPackBitsWidths checks the bit packer across every width including the
// 64-bit boundary where a naive accumulator would drop the high bits.
func TestPackBitsWidths(t *testing.T) {
	for width := 1; width <= 64; width++ {
		vs := make([]uint64, 17)
		for i := range vs {
			vs[i] = (uint64(i)*0x9E3779B97F4A7C15 + 1) & mask(width)
		}
		packed := packBits(nil, vs, width)
		if got := len(packed); got != packedLen(len(vs), width) {
			t.Fatalf("width %d: packed len %d want %d", width, got, packedLen(len(vs), width))
		}
		out := make([]uint64, len(vs))
		if _, err := unpackBits(packed, out, len(vs), width); err != nil {
			t.Fatalf("width %d: unpack: %v", width, err)
		}
		assertEqualU64(t, vs, out)
	}
}

func TestZigzagRoundTrip(t *testing.T) {
	for _, d := range []int64{0, 1, -1, 2, -2, 1 << 40, -(1 << 40), 1<<63 - 1, -(1 << 62)} {
		if got := unzigzag(zigzag(d)); got != d {
			t.Fatalf("zigzag round-trip: got %d want %d", got, d)
		}
	}
}

func assertEqualU64(t *testing.T, want, got []uint64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("length: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if want[i] != got[i] {
			t.Fatalf("value %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func rep(v uint64, n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = v
	}
	return out
}

func seq(start, step uint64, n int) []uint64 {
	out := make([]uint64, n)
	v := start
	for i := range out {
		out[i] = v
		v += step
	}
	return out
}

func withExceptions() []uint64 {
	out := seq(100, 0, 80)
	for i := range out {
		out[i] = 100 + uint64(i%4)
	}
	out[10] = 50000
	out[40] = 70000
	out[79] = 99999
	return out
}

// statusShape is the long-run, isolated-outlier pattern of an HTTP status
// column. It pins the RLE regression where a single literal between two runs
// was padded to a multiple of 8 and the padding overwrote real values.
func statusShape() []uint64 {
	out := rep(200, 300)
	out[0] = 404
	out[50] = 301
	out[97] = 404
	out[150] = 500
	out[151] = 502
	return out
}

func pattern(n int) []bool {
	out := make([]bool, n)
	for i := range out {
		out[i] = (i*7+3)%5 < 2
	}
	return out
}
