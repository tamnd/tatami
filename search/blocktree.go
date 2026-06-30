package search

// blocktree.go is the scale redesign's term dictionary (Spec 2066, scale/03).
//
// SortedDict (terms.go) holds every term string resident in the Go heap. At a
// few hundred thousand terms that is fine; at the 1.4M terms of one real crawl
// shard it is already ~100 MB of heap, and at the 5-10M terms of a 1M-doc
// segment it does not survive a 23 GB machine once a few segments are resident.
//
// BlockTreeDict replaces that with an on-disk block tree behind the same
// Dictionary seam (terms.go). Terms are partitioned into sorted blocks of a
// fixed count, front-coded within a block and zstd-compressed. The only thing
// kept resident is a sparse index: one separator string plus a few integers per
// block. A lookup binary-searches the sparse index, decompresses exactly one
// block, and scans it. A miss is answered inside the block with no posting-store
// seek, so the negative-lookup contract at terms.go:44 holds.
//
// This file keeps the package self-contained against the rest of the module
// (types.go): it imports only the standard library and the same external zstd
// the codec package uses, never another tatami package.

import (
	"encoding/binary"
	"fmt"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// DefaultBlockTreeBlockSize is the number of terms per block. 64 is the tuning
// point from scale/03: large enough that the sparse index is ~1/64 the term
// count, small enough that one block decompresses in well under a microsecond.
const DefaultBlockTreeBlockSize = 64

// blockTreeVersion tags the serialized form so a reader can reject an unknown
// layout instead of misreading it.
const blockTreeVersion = 2

// btZstd is a process-wide deterministic zstd pair for the dictionary blocks,
// single-threaded so the serialized bytes are reproducible, matching the codec
// package's choice (codec/codec.go).
var (
	btZstdOnce sync.Once
	btEnc      *zstd.Encoder
	btDec      *zstd.Decoder
	btZstdErr  error
)

func btZstd() (*zstd.Encoder, *zstd.Decoder, error) {
	btZstdOnce.Do(func() {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1),
			zstd.WithWindowSize(1<<20),
		)
		if err != nil {
			btZstdErr = err
			return
		}
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			btZstdErr = err
			return
		}
		btEnc, btDec = enc, dec
	})
	return btEnc, btDec, btZstdErr
}

// BuildBlockTree serializes a sorted term list into the block-tree byte form.
// The terms must already be in ascending Unicode order, which is what
// InvertedBuilder.Build produces (index.go). blockSize <= 0 selects the default.
//
// The layout is, in order:
//
//	header: version(1) | blockSize(uvarint) | numTerms(uvarint) |
//	        numBlocks(uvarint) | indexLen(uvarint)
//	index:  numBlocks records of { firstTerm | blockOffset | compLen | rawLen }
//	blocks: numBlocks zstd frames, each a front-coded run of blockSize terms
//
// blockOffset is relative to the start of the blocks region, so the reader can
// seek to any block without scanning the ones before it.
func BuildBlockTree(terms []Term, blockSize int) ([]byte, error) {
	if blockSize <= 0 {
		blockSize = DefaultBlockTreeBlockSize
	}
	enc, _, err := btZstd()
	if err != nil {
		return nil, fmt.Errorf("search: block-tree zstd: %w", err)
	}

	numBlocks := (len(terms) + blockSize - 1) / blockSize
	type idxRec struct {
		first   string
		offset  int
		compLen int
		rawLen  int
	}
	recs := make([]idxRec, 0, numBlocks)

	var blocks []byte
	var raw []byte // reusable scratch for one block body
	for start := 0; start < len(terms); start += blockSize {
		end := min(start+blockSize, len(terms))
		raw = raw[:0]
		prev := ""
		for i := start; i < end; i++ {
			t := terms[i].Term
			cp := commonPrefixLen(prev, t)
			suffix := t[cp:]
			raw = binary.AppendUvarint(raw, uint64(cp))
			raw = binary.AppendUvarint(raw, uint64(len(suffix)))
			raw = append(raw, suffix...)
			raw = appendEntry(raw, terms[i].Entry)
			prev = t
		}
		off := len(blocks)
		comp := enc.EncodeAll(raw, nil)
		blocks = append(blocks, comp...)
		recs = append(recs, idxRec{
			first:   terms[start].Term,
			offset:  off,
			compLen: len(comp),
			rawLen:  len(raw),
		})
	}

	var index []byte
	for _, r := range recs {
		index = binary.AppendUvarint(index, uint64(len(r.first)))
		index = append(index, r.first...)
		index = binary.AppendUvarint(index, uint64(r.offset))
		index = binary.AppendUvarint(index, uint64(r.compLen))
		index = binary.AppendUvarint(index, uint64(r.rawLen))
	}

	var out []byte
	out = append(out, blockTreeVersion)
	out = binary.AppendUvarint(out, uint64(blockSize))
	out = binary.AppendUvarint(out, uint64(len(terms)))
	out = binary.AppendUvarint(out, uint64(numBlocks))
	out = binary.AppendUvarint(out, uint64(len(index)))
	out = append(out, index...)
	out = append(out, blocks...)
	return out, nil
}

// appendEntry serializes a dictionary entry, mirroring the term-dictionary run
// in EncodeInverted (index.go) so the two stay semantically identical.
func appendEntry(dst []byte, e Entry) []byte {
	dst = binary.AppendUvarint(dst, uint64(e.DocFreq))
	if e.Singleton {
		dst = append(dst, 1)
		dst = binary.AppendUvarint(dst, uint64(e.SingletonDoc))
		dst = binary.AppendUvarint(dst, uint64(e.SingletonFreq))
	} else {
		dst = append(dst, 0)
		dst = binary.AppendUvarint(dst, uint64(e.PostingOffset))
	}
	return dst
}

// commonPrefixLen returns the length in bytes of the longest shared prefix of a
// and b. Front coding stores only the bytes after this point.
func commonPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// btBlock is one resident sparse-index record: the block's first term and where
// its compressed bytes live in the blocks region.
type btBlock struct {
	first   string
	offset  int
	compLen int
	rawLen  int
}

// BlockTreeDict is a Dictionary backed by a block tree. The sparse index (firsts
// and the per-block offsets) is resident; the compressed blocks stay in the
// backing bytes and decompress on demand into a small cache. It is immutable
// after Open and safe for concurrent readers.
type BlockTreeDict struct {
	blockSize int
	numTerms  int
	blocks    []btBlock
	data      []byte // the blocks region; in M2 this is a window over the mmap

	mu    sync.Mutex
	cache map[int][]byte // blockIdx -> decompressed body, a tiny decode cache
}

// OpenBlockTree parses the serialized form into a queryable dictionary. It reads
// the header and the sparse index into memory and keeps a window over the blocks
// region; it does not decompress any block until a lookup needs one.
func OpenBlockTree(data []byte) (*BlockTreeDict, error) {
	if len(data) < 1 || data[0] != blockTreeVersion {
		return nil, fmt.Errorf("search: block-tree bad version")
	}
	r := &byteReader{b: data, pos: 1}
	blockSize := int(r.uvarint())
	numTerms := int(r.uvarint())
	numBlocks := int(r.uvarint())
	indexLen := int(r.uvarint())
	if r.err != nil {
		return nil, fmt.Errorf("search: block-tree header: %w", r.err)
	}
	indexStart := r.pos
	blocksStart := indexStart + indexLen
	if blocksStart > len(data) {
		return nil, fmt.Errorf("search: block-tree index overruns data")
	}
	ir := &byteReader{b: data[indexStart:blocksStart]}
	blocks := make([]btBlock, 0, numBlocks)
	for range numBlocks {
		fl := int(ir.uvarint())
		first := string(ir.take(fl))
		off := int(ir.uvarint())
		cl := int(ir.uvarint())
		rl := int(ir.uvarint())
		blocks = append(blocks, btBlock{first: first, offset: off, compLen: cl, rawLen: rl})
	}
	if ir.err != nil {
		return nil, fmt.Errorf("search: block-tree index: %w", ir.err)
	}
	return &BlockTreeDict{
		blockSize: blockSize,
		numTerms:  numTerms,
		blocks:    blocks,
		data:      data[blocksStart:],
		cache:     make(map[int][]byte),
	}, nil
}

// Len reports the number of terms.
func (d *BlockTreeDict) Len() int { return d.numTerms }

// ResidentBytes estimates the heap held by the sparse index alone: the separator
// strings plus the per-block integer fields. This is the number that matters at
// scale, because the compressed blocks live in the file (mmap, page cache) and
// are not part of the resident handle. It is reported by the dict-RAM benchmark.
func (d *BlockTreeDict) ResidentBytes() int {
	n := 0
	for _, b := range d.blocks {
		n += len(b.first) + 16 // string bytes plus offset/compLen/rawLen ints
		n += 16                // string header (ptr+len) and slice-element padding
	}
	return n
}

// blockOf returns the index of the block whose key range can contain term: the
// last block whose first term is <= term. It returns -1 when term sorts before
// the first block's first term, which is a definite miss.
func (d *BlockTreeDict) blockOf(term string) int {
	// Largest i with blocks[i].first <= term.
	lo, hi := 0, len(d.blocks)
	for lo < hi {
		mid := (lo + hi) / 2
		if d.blocks[mid].first <= term {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo - 1
}

// decodeBlock returns the decompressed body of block i, caching it so a hot
// block is decompressed once. The cache is intentionally tiny; the page cache,
// not this map, is the real working set in the mmap design (scale/05).
func (d *BlockTreeDict) decodeBlock(i int) ([]byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if b, ok := d.cache[i]; ok {
		return b, nil
	}
	_, dec, err := btZstd()
	if err != nil {
		return nil, err
	}
	rec := d.blocks[i]
	if rec.offset+rec.compLen > len(d.data) {
		return nil, fmt.Errorf("search: block %d overruns blocks region", i)
	}
	comp := d.data[rec.offset : rec.offset+rec.compLen]
	body, err := dec.DecodeAll(comp, make([]byte, 0, rec.rawLen))
	if err != nil {
		return nil, fmt.Errorf("search: decode block %d: %w", i, err)
	}
	// Bound the cache so a scan over every block does not pin them all.
	if len(d.cache) >= 32 {
		for k := range d.cache {
			delete(d.cache, k)
			break
		}
	}
	d.cache[i] = body
	return body, nil
}

// scanBlock walks the front-coded terms of a decompressed block body, calling fn
// for each (term, entry) in order. fn returns false to stop early. It returns an
// error only on a malformed block.
func scanBlock(body []byte, fn func(term string, e Entry) bool) error {
	r := &byteReader{b: body}
	prev := ""
	for r.pos < len(r.b) {
		cp := int(r.uvarint())
		sl := int(r.uvarint())
		suffix := r.take(sl)
		if r.err != nil {
			return r.err
		}
		if cp > len(prev) {
			return fmt.Errorf("search: front-code prefix %d exceeds previous term %d", cp, len(prev))
		}
		term := prev[:cp] + string(suffix)
		var e Entry
		e.DocFreq = int(r.uvarint())
		if r.byte1() == 1 {
			e.Singleton = true
			e.SingletonDoc = DocID(r.uvarint())
			e.SingletonFreq = uint32(r.uvarint())
		} else {
			e.PostingOffset = int64(r.uvarint())
		}
		if r.err != nil {
			return r.err
		}
		if !fn(term, e) {
			return nil
		}
		prev = term
	}
	return nil
}

// Lookup returns the entry for an exact term and whether it exists. It decodes at
// most one block and answers a miss without touching the postings store.
func (d *BlockTreeDict) Lookup(term string) (Entry, bool) {
	i := d.blockOf(term)
	if i < 0 {
		return Entry{}, false
	}
	body, err := d.decodeBlock(i)
	if err != nil {
		return Entry{}, false
	}
	var out Entry
	found := false
	_ = scanBlock(body, func(t string, e Entry) bool {
		if t == term {
			out, found = e, true
			return false
		}
		if t > term {
			return false // sorted: past where term would be, it is absent
		}
		return true
	})
	return out, found
}

// PrefixScan calls fn for every term with the given prefix, in ascending order,
// stopping early if fn returns false. It starts at the block that can hold the
// prefix and streams blocks until a term sorts past the prefix range.
func (d *BlockTreeDict) PrefixScan(prefix string, fn func(term string, e Entry) bool) {
	start := max(d.blockOf(prefix), 0)
	stop := false
	for i := start; i < len(d.blocks) && !stop; i++ {
		body, err := d.decodeBlock(i)
		if err != nil {
			return
		}
		_ = scanBlock(body, func(t string, e Entry) bool {
			if strings.HasPrefix(t, prefix) {
				if !fn(t, e) {
					stop = true
					return false
				}
				return true
			}
			if t > prefix {
				// Past the prefix run. Because terms are sorted, no later term in
				// any later block can carry the prefix once we are strictly past it.
				stop = true
				return false
			}
			return true
		})
	}
}

// compile-time check that BlockTreeDict satisfies the Dictionary seam.
var _ Dictionary = (*BlockTreeDict)(nil)
