package search

// liststore.go is the scale redesign's lazy posting access (Spec 2066, scale/04,
// lever 2).
//
// DecodeInverted used to build one *List per posting list with its whole skip
// table decoded into a block array up front. On a real 1.4M-term crawl shard that
// is 263k lists and 275k block structs, ~22 MiB of resident heap, for a skips run
// that is 2.5 MiB on disk: a 9x blowup that lives for as long as the segment is
// open. With the term dictionary moved off heap by the block tree (blocktree.go),
// this eager skip-table decode is the dominant resident cost, and at 1M docs per
// segment it is what stops several segments fitting one machine (scale/01).
//
// The fix is to keep the postings and skips as their raw byte runs and decode one
// list only when a query asks for it. The store holds a compact offset table into
// each run, built by one sequential scan that skips over the bytes without
// decoding any block. Resident heap drops from the block array to two offset
// slices; the block structs live transiently inside one query.

import "fmt"

// listStore is the seam over a segment's posting lists: random access to list i
// and the list count. The builder and merge path use the eager store, where every
// list is materialized anyway; a decoded segment being served uses the lazy store.
type listStore interface {
	// at returns list i fully formed: payload slice plus decoded skip blocks. It
	// is called per query term and during re-encode, so it must return a List the
	// caller can take a Cursor or MaxFreq from directly.
	at(i int) *List
	// len reports the number of posting lists.
	len() int
}

// eagerLists is the in-memory store: the fully decoded lists the builder produces.
// Build and merge already hold every list resident, so there is nothing to defer.
type eagerLists []*List

func (e eagerLists) at(i int) *List { return e[i] }
func (e eagerLists) len() int       { return len(e) }

// lazyLists is the on-demand store over the raw postings and skips runs. It keeps
// no decoded block array; instead it keeps one byte offset per list into each run
// and rebuilds a *List when at is called. The offset tables are the only resident
// cost, ~16 bytes per list, versus the ~110 bytes per list the eager block array
// and payload-slice headers held.
type lazyLists struct {
	postings []byte
	skips    []byte
	n        int
	pOff     []uint64 // pOff[i] = offset in postings of list i's length prefix
	sOff     []uint64 // sOff[i] = offset in skips of list i's skip record
}

// newLazyLists scans the postings and skips runs once each, recording where every
// list begins, without decoding any payload or block. The two runs must carry the
// same list count, which DecodeInverted already checks, but it is re-checked here
// so the store is safe to build from untrusted bytes.
func newLazyLists(postings, skips []byte) (*lazyLists, error) {
	pc := &byteReader{b: postings}
	nLists := int(pc.uvarint())
	pOff := make([]uint64, nLists)
	for i := range nLists {
		pOff[i] = uint64(pc.pos)
		n := int(pc.uvarint())
		_ = pc.take(n) // skip the payload, do not copy it
	}
	if pc.err != nil {
		return nil, fmt.Errorf("search: scan posting payloads: %w", pc.err)
	}

	sc := &byteReader{b: skips}
	nSkip := int(sc.uvarint())
	if nSkip != nLists {
		return nil, fmt.Errorf("search: %d skip tables for %d posting lists", nSkip, nLists)
	}
	sOff := make([]uint64, nLists)
	for i := range nLists {
		sOff[i] = uint64(sc.pos)
		_ = sc.uvarint()        // numDoc
		nb := int(sc.uvarint()) // block count
		for range nb {
			for range 5 { // firstDoc, lastDoc, maxFreq, offset, count
				_ = sc.uvarint()
			}
		}
	}
	if sc.err != nil {
		return nil, fmt.Errorf("search: scan skip tables: %w", sc.err)
	}

	return &lazyLists{
		postings: postings,
		skips:    skips,
		n:        nLists,
		pOff:     pOff,
		sOff:     sOff,
	}, nil
}

func (z *lazyLists) len() int { return z.n }

// at decodes list i on demand: it slices the payload out of the postings run (no
// copy, the List shares the backing bytes) and decodes the skip blocks out of the
// skips run. It mirrors DecodeInverted's per-list decode exactly, so the lazy and
// eager stores are byte-for-byte equivalent. A malformed run yields an empty List
// rather than a panic, matching the decoder's error discipline.
func (z *lazyLists) at(i int) *List {
	if i < 0 || i >= z.n {
		return &List{}
	}
	pc := &byteReader{b: z.postings, pos: int(z.pOff[i])}
	n := int(pc.uvarint())
	data := pc.take(n)

	sc := &byteReader{b: z.skips, pos: int(z.sOff[i])}
	l := &List{data: data}
	l.numDoc = int(sc.uvarint())
	nb := int(sc.uvarint())
	l.blocks = make([]block, nb)
	for j := range nb {
		l.blocks[j] = block{
			firstDoc: DocID(sc.uvarint()),
			lastDoc:  DocID(sc.uvarint()),
			maxFreq:  uint32(sc.uvarint()),
			offset:   int(sc.uvarint()),
			count:    int(sc.uvarint()),
		}
	}
	if pc.err != nil || sc.err != nil {
		return &List{}
	}
	return l
}

// residentBytes estimates the heap the lazy store holds: the two offset tables.
// The raw runs it points at are the file (mmap, page cache) in the scale design,
// not resident heap. Reported by the skip-table RAM benchmark.
func (z *lazyLists) residentBytes() int {
	return len(z.pOff)*8 + len(z.sOff)*8
}
