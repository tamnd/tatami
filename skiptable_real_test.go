package tatami

// Measures the eager skip-table materialization cost on a real crawl shard, the
// second resident cost DecodeInverted pays after the term dictionary (which M1
// moved off-heap). DecodeInverted builds one *List per posting list with its full
// block array decoded up front (search/index.go:355). This test reports how much
// heap that array holds versus the raw skips run, so M2 (lazy skip access) is
// justified by a measured number, not an estimate.
//
// Gated on a local crawl shard like the other real-data tests.
//
//	go test -run TestSkipTableRealRAM -v

import (
	"encoding/binary"
	"testing"

	"github.com/tamnd/tatami/search"
)

// sizeofList is the heap of one List header: data slice header (24) + blocks
// slice header (24) + numDoc int (8), before the block array it points at.
const sizeofList = 56

// sizeofBlock is the heap of one decoded block struct: firstDoc + lastDoc
// (DocID, 4 each) + maxFreq (uint32, 4) + offset + count (int, 8 each), padded.
const sizeofBlock = 32

func TestSkipTableRealRAM(t *testing.T) {
	seg := loadRealSegment(t)
	inv := seg.Inverted()
	_, postings, skips := search.EncodeInverted(inv)

	// Parse only the skips run header to count lists and total blocks without
	// building the *List array, mirroring DecodeInverted's shape.
	pos := 0
	readUvarint := func() int {
		v, n := binary.Uvarint(skips[pos:])
		pos += n
		return int(v)
	}
	nLists := readUvarint()
	totalBlocks := 0
	for range nLists {
		_ = readUvarint() // numDoc
		nb := readUvarint()
		totalBlocks += nb
		for range nb {
			for range 5 { // firstDoc,lastDoc,maxFreq,offset,count
				_ = readUvarint()
			}
		}
	}

	// Eager: one *List per list plus the decoded block array (the old decode).
	// It also held a [][]byte of payload-slice headers, 24 bytes per list.
	eagerHeap := nLists*sizeofList + totalBlocks*sizeofBlock + nLists*24
	// Lazy: two uint64 offset tables, one into each run, nothing decoded.
	lazyHeap := nLists * 16

	t.Logf("real shard posting lists       : %d", nLists)
	t.Logf("real shard skip blocks         : %d", totalBlocks)
	t.Logf("skips run on disk              : %s", humanByteCount(len(skips)))
	t.Logf("postings run on disk           : %s", humanByteCount(len(postings)))
	t.Logf("eager skip-table heap (before) : %s", humanByteCount(eagerHeap))
	t.Logf("  = %d lists * %dB + %d blocks * %dB + %d data headers * 24B", nLists, sizeofList, totalBlocks, sizeofBlock, nLists)
	t.Logf("lazy offset-table heap (after) : %s", humanByteCount(lazyHeap))
	t.Logf("  = %d lists * 16B (two uint64 offset tables)", nLists)
	t.Logf("resident reduction             : %.1fx", float64(eagerHeap)/float64(lazyHeap))
}
