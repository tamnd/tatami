package codec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestNoneRoundTrip(t *testing.T) {
	c := Identity()
	src := []byte("the quick brown fox")
	comp := c.Compress(nil, src)
	out, err := c.Decompress(nil, comp, len(src))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("round trip mismatch: %q", out)
	}
}

func TestZstdRoundTrip(t *testing.T) {
	c, err := Default()
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	src := bytes.Repeat([]byte("abcdefgh"), 1000)
	comp := c.Compress(nil, src)
	if len(comp) >= len(src) {
		t.Fatalf("zstd did not compress: %d >= %d", len(comp), len(src))
	}
	out, err := c.Decompress(nil, comp, len(src))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatal("zstd round trip mismatch")
	}
}

// TestDictCodec checks the ZSTD_DICT codec: a raw content dictionary lets a
// small record carrying shared boilerplate compress far better than plain zstd,
// and it decodes back exactly and deterministically.
func TestDictCodec(t *testing.T) {
	var dict []byte
	for i := 0; i < 200; i++ {
		dict = append(dict, fmt.Sprintf(`{"lang":"en","type":"article","id":%d}`, i)...)
	}
	dc, err := NewZstdDict(dict, DefaultLevel)
	if err != nil {
		t.Fatalf("new dict codec: %v", err)
	}
	if dc.ID() != ZstdDict {
		t.Fatalf("id: got %d want %d", dc.ID(), ZstdDict)
	}
	plain, err := Default()
	if err != nil {
		t.Fatalf("default: %v", err)
	}

	rec := []byte(`{"lang":"en","type":"article","id":424242}`)
	withDict := dc.Compress(nil, rec)
	noDict := plain.Compress(nil, rec)
	if len(withDict) >= len(noDict) {
		t.Fatalf("dictionary did not help a small record: dict=%d plain=%d", len(withDict), len(noDict))
	}

	out, err := dc.Decompress(nil, withDict, len(rec))
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if !bytes.Equal(out, rec) {
		t.Fatalf("dict round trip mismatch: %q", out)
	}

	// Determinism: the same record compresses to the same bytes twice.
	again := dc.Compress(nil, rec)
	if !bytes.Equal(withDict, again) {
		t.Fatal("dict codec not deterministic")
	}
}

func TestEmptyDictRejected(t *testing.T) {
	if _, err := NewZstdDict(nil, zstd.SpeedFastest); err == nil {
		t.Fatal("expected error for empty dictionary")
	}
}
