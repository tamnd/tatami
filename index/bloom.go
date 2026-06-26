// Package index holds the membership filters and the page-index layout a tatami
// file carries to skip data it does not need to read. The structures here are
// self-contained: they take and return bytes and never import the root package,
// so the writer and reader compose them without a cycle.
//
// The bloom filter is the classic Kirsch-Mitzenmacher double-hashing
// construction from kv's lsm/bloom.go, frozen so a filter written by one build
// reads identically in the next. A negative answer is definitive, which is the
// one-sided guarantee the read path leans on: a row group whose filter says no
// is skipped without touching a data page.
package index

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math/bits"
)

// BloomBitsPerKey is the default budget. Ten bits per key gives k = 7 probes and
// roughly a one percent false-positive rate, the same default kv ships.
const BloomBitsPerKey = 10

// Bloom is a built filter over a set of keys. The zero value is not usable;
// build one with BuildBloom or load one with LoadBloom.
type Bloom struct {
	bits []byte // bit array, length a power of two in bits
	k    uint32 // probe count
	mask uint32 // nbits - 1, so a probe is one mask instead of a divide
}

// BuildBloom builds a filter over keys at the given bits-per-key budget. Passing
// bitsPerKey <= 0 uses BloomBitsPerKey. An empty key set yields a tiny filter
// that rejects everything, which is the correct answer for an empty group.
func BuildBloom(keys [][]byte, bitsPerKey int) *Bloom {
	if bitsPerKey <= 0 {
		bitsPerKey = BloomBitsPerKey
	}
	k := uint32(float64(bitsPerKey)*0.69 + 0.5)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	nbits := uint32(len(keys) * bitsPerKey)
	if nbits < 64 {
		nbits = 64
	}
	nbits = nextPow2(nbits)
	b := &Bloom{
		bits: make([]byte, nbits/8),
		k:    k,
		mask: nbits - 1,
	}
	for _, key := range keys {
		h, d := hashPair(key)
		for i := uint32(0); i < k; i++ {
			pos := h & b.mask
			b.bits[pos>>3] |= 1 << (pos & 7)
			h += d
		}
	}
	return b
}

// MayContain reports whether key might be in the set. False is definitive; true
// may be a false positive at the configured rate.
func (b *Bloom) MayContain(key []byte) bool {
	h, d := hashPair(key)
	for i := uint32(0); i < b.k; i++ {
		pos := h & b.mask
		if b.bits[pos>>3]&(1<<(pos&7)) == 0 {
			return false
		}
		h += d
	}
	return true
}

// Encode serializes the filter: k (u32 LE), nbits (u32 LE), then the bit array.
func (b *Bloom) Encode() []byte {
	out := make([]byte, 8+len(b.bits))
	binary.LittleEndian.PutUint32(out[0:4], b.k)
	binary.LittleEndian.PutUint32(out[4:8], b.mask+1)
	copy(out[8:], b.bits)
	return out
}

// LoadBloom parses a filter encoded by Encode.
func LoadBloom(raw []byte) (*Bloom, error) {
	if len(raw) < 8 {
		return nil, fmt.Errorf("index: bloom blob too short: %d bytes", len(raw))
	}
	k := binary.LittleEndian.Uint32(raw[0:4])
	nbits := binary.LittleEndian.Uint32(raw[4:8])
	if nbits == 0 || nbits&(nbits-1) != 0 {
		return nil, fmt.Errorf("index: bloom nbits %d not a power of two", nbits)
	}
	if int(nbits/8) != len(raw)-8 {
		return nil, fmt.Errorf("index: bloom bit array is %d bytes, header says %d", len(raw)-8, nbits/8)
	}
	return &Bloom{
		bits: raw[8:],
		k:    k,
		mask: nbits - 1,
	}, nil
}

// hashPair derives the base hash and the stride for double hashing. The base is
// FNV-1a 32-bit; the stride is a fixed rotate of it, the same derivation kv
// uses, so the constants stay frozen across builds.
func hashPair(key []byte) (h, d uint32) {
	f := fnv.New32a()
	_, _ = f.Write(key)
	h = f.Sum32()
	d = bits.RotateLeft32(h, -17)
	return h, d
}

func nextPow2(v uint32) uint32 {
	if v == 0 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}
