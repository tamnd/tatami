package search

import "math/bits"

// Live is a compact live-docs set: bit i set means document i (a dense docID) is
// live. A delete clears a bit; the posting lists are not touched, so a delete is
// O(1) and space is reclaimed only when the segment is merged or rewritten, the
// Lucene/Scorch model (09-search-scale.md, section 7). It is the openindex
// segment bitset, ported to the search package so the inverted index can carry
// its own liveness. It is not safe for concurrent mutation; reads are safe once
// the segment is sealed.
type Live struct {
	words []uint64
	n     int // number of bits
}

// NewLive returns a set of n documents with every document live, the initial
// state of a freshly built segment.
func NewLive(n int) *Live {
	l := &Live{words: make([]uint64, (n+63)/64), n: n}
	l.setAll()
	return l
}

// setAll marks every bit live and clears the padding past n so Count is exact.
func (l *Live) setAll() {
	for i := range l.words {
		l.words[i] = ^uint64(0)
	}
	if rem := l.n % 64; rem != 0 {
		l.words[len(l.words)-1] = uint64(1)<<uint(rem) - 1
	}
}

// Get reports whether docID is live. An out-of-range id is not live.
func (l *Live) Get(i int) bool {
	if i < 0 || i >= l.n {
		return false
	}
	return l.words[i>>6]&(1<<uint(i&63)) != 0
}

// Clear marks docID deleted. It returns true when this call changed the state,
// so a caller can count distinct deletes and ignore a repeat.
func (l *Live) Clear(i int) bool {
	if i < 0 || i >= l.n {
		return false
	}
	mask := uint64(1) << uint(i&63)
	if l.words[i>>6]&mask == 0 {
		return false
	}
	l.words[i>>6] &^= mask
	return true
}

// Count returns the number of live documents.
func (l *Live) Count() int {
	var c int
	for _, w := range l.words {
		c += bits.OnesCount64(w)
	}
	return c
}

// Len returns the document-id space size (live plus deleted).
func (l *Live) Len() int { return l.n }

// AllLive reports whether no document has been deleted, the common case that
// lets a query skip the liveness filter entirely.
func (l *Live) AllLive() bool { return l.Count() == l.n }

// EncodeLive serializes the bitset as the doc count followed by the raw words, so
// it can ride in the inverted sub-region as a fourth run. The word count is
// implied by n, so only n is framed.
func EncodeLive(l *Live) []byte {
	out := appendUvarint(nil, uint64(l.n))
	for _, w := range l.words {
		var b [8]byte
		b[0] = byte(w)
		b[1] = byte(w >> 8)
		b[2] = byte(w >> 16)
		b[3] = byte(w >> 24)
		b[4] = byte(w >> 32)
		b[5] = byte(w >> 40)
		b[6] = byte(w >> 48)
		b[7] = byte(w >> 56)
		out = append(out, b[:]...)
	}
	return out
}

// DecodeLive reverses Encode. An empty input decodes to a nil set, which the
// inverted index treats as all-live.
func DecodeLive(data []byte) *Live {
	if len(data) == 0 {
		return nil
	}
	n, off := readUvarint(data)
	l := &Live{n: int(n), words: make([]uint64, (int(n)+63)/64)}
	for i := range l.words {
		if off+8 > len(data) {
			break
		}
		l.words[i] = uint64(data[off]) | uint64(data[off+1])<<8 | uint64(data[off+2])<<16 |
			uint64(data[off+3])<<24 | uint64(data[off+4])<<32 | uint64(data[off+5])<<40 |
			uint64(data[off+6])<<48 | uint64(data[off+7])<<56
		off += 8
	}
	return l
}
