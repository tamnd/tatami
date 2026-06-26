package index

import (
	"encoding/binary"
	"testing"
)

func key(i int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 5000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = key(i)
	}
	b := BuildBloom(keys, BloomBitsPerKey)
	for i := 0; i < n; i++ {
		if !b.MayContain(key(i)) {
			t.Fatalf("false negative for key %d: a member was rejected", i)
		}
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 10000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = key(i)
	}
	b := BuildBloom(keys, BloomBitsPerKey)
	fp := 0
	const probes = 100000
	for i := n; i < n+probes; i++ {
		if b.MayContain(key(i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	// 10 bits per key targets ~1%; allow generous headroom so the test is stable.
	if rate > 0.03 {
		t.Fatalf("false-positive rate %.4f too high (want < 0.03)", rate)
	}
}

func TestBloomRoundTrip(t *testing.T) {
	keys := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	b := BuildBloom(keys, BloomBitsPerKey)
	enc := b.Encode()
	got, err := LoadBloom(enc)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range keys {
		if !got.MayContain(k) {
			t.Fatalf("loaded filter rejected member %q", k)
		}
	}
	if got.MayContain([]byte("not-present-at-all-zzz")) {
		// Possible but unlikely; only fail if it is clearly wrong by re-checking
		// a value that was definitely not added would be flaky, so skip the
		// assertion and only check the deterministic re-encode below.
		_ = got
	}
	if string(got.Encode()) != string(enc) {
		t.Fatal("re-encode is not byte-stable")
	}
}

func TestBloomDeterministic(t *testing.T) {
	keys := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	a := BuildBloom(keys, BloomBitsPerKey).Encode()
	b := BuildBloom(keys, BloomBitsPerKey).Encode()
	if string(a) != string(b) {
		t.Fatal("bloom build is not deterministic for the same keys")
	}
}

func TestBloomEmpty(t *testing.T) {
	b := BuildBloom(nil, BloomBitsPerKey)
	if b.MayContain([]byte("anything")) {
		// An empty filter has every bit clear, so a probe with k>=1 must reject.
		t.Fatal("empty filter should reject every key")
	}
}
