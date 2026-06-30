package tatami

// Real-data measurement for M0: the block-tree term dictionary wired into the
// on-disk segment format (Spec 2066, scale/03). M1 proved the dictionary in
// isolation and M2 proved the lazy posting store; M0 is what makes a segment
// read off disk actually serve from them. This test opens a real on-disk search
// segment and measures the two costs M6 flagged at the 1M tier: open wall time
// and the resident heap an open handle holds. The legacy flat dictionary is
// reconstructed from the same terms to quote the before number, not estimate it.
//
// It is gated on a local crawl shard like the other *_realdata tests, so CI
// skips it cleanly. Run it with:
//
//	go test -run TestM0FormatBlockTree -v

import (
	"runtime"
	"testing"
	"time"

	"github.com/tamnd/tatami/search"
)

// TestM0FormatBlockTree confirms a written search segment carries the block-tree
// flag, that reopening it takes the block-tree decode path, and reports the open
// time and resident heap against the flat-dictionary baseline.
func TestM0FormatBlockTree(t *testing.T) {
	seg := loadRealSegment(t) // writes the segment to realSegPath and opens it once
	t.Logf("segment: %d docs, %d terms", seg.NumDocs(), seg.NumTerms())

	// The on-disk file must advertise the block-tree dictionary so a reader picks
	// the right decode path without sniffing the run bytes.
	r, f, err := OpenFile(realSegPath)
	if err != nil {
		t.Fatal(err)
	}
	if r.header.Flags&FlagBlockTreeDict == 0 {
		t.Fatal("written search segment is missing FlagBlockTreeDict")
	}
	if r.header.Flags&FlagRoleSearchSeg == 0 {
		t.Fatal("written search segment is missing FlagRoleSearchSeg")
	}
	_ = f.Close()

	// The reopened dictionary must be the block tree, not the resident SortedDict.
	if _, ok := seg.Inverted().Dict().(*search.BlockTreeDict); !ok {
		t.Fatalf("Dict() is %T, want *search.BlockTreeDict on the open path", seg.Inverted().Dict())
	}

	// Open wall time: reopen the file fresh a few times and average. This is the
	// cost M6 measured at 5.75 s for the 1M tier when decode still built the whole
	// SortedDict; the block tree only parses the sparse index here.
	const opens = 5
	var total time.Duration
	for range opens {
		start := time.Now()
		s, err := OpenSearch(realSegPath)
		total += time.Since(start)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	}
	t.Logf("block-tree open: %v avg over %d opens", total/opens, opens)

	// Resident heap of the open handle's term dictionary. Pull the term list off
	// the open segment, build the legacy flat SortedDict the pre-M0 open path
	// materialized, and measure its live retained heap with a HeapAlloc delta
	// around a forced GC. The block tree's resident cost is its sparse index, which
	// it accounts exactly via ResidentBytes; a MemStats delta cannot resolve ~1 MiB
	// against the churn of building and compressing the blocks, so the precise
	// accessor is the honest number (the compressed blocks live in the file, not
	// the heap).
	var terms []search.Term
	seg.Inverted().Dict().PrefixScan("", func(term string, e search.Entry) bool {
		terms = append(terms, search.Term{Term: term, Entry: e})
		return true
	})

	flatHeap := int(retainedHeap(func() any { return search.NewSortedDict(terms) }))

	// Time the two dictionary build steps in isolation: constructing the flat
	// SortedDict is the dominant work the pre-M0 open path did over every term,
	// while opening the block tree only parses the sparse index. This is the
	// open-time lever, separated from the rest of OpenSearch.
	flatStart := time.Now()
	_ = search.NewSortedDict(terms)
	flatBuild := time.Since(flatStart)

	data, err := search.BuildBlockTree(terms, search.DefaultBlockTreeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	btStart := time.Now()
	bt, err := search.OpenBlockTree(data)
	if err != nil {
		t.Fatal(err)
	}
	btOpen := time.Since(btStart)
	btHeap := bt.ResidentBytes()

	t.Logf("flat SortedDict construct (pre-M0 open work) : %v", flatBuild)
	t.Logf("block-tree open (M0 open work)               : %v", btOpen)

	t.Logf("flat SortedDict resident (pre-M0 open path) : %s (live heap)", humanByteCount(flatHeap))
	t.Logf("block-tree resident (M0 open path)          : %s (sparse index)", humanByteCount(btHeap))
	t.Logf("block-tree dictionary on disk               : %s (file-backed, not heap)", humanByteCount(len(data)))
	if btHeap > 0 {
		t.Logf("resident reduction on the open path         : %.1fx", float64(flatHeap)/float64(btHeap))
	}
}

// retainedHeap returns the live heap bytes the value built by mk retains: it
// forces a GC with the value held, reads HeapAlloc, then forces a GC after the
// value is dropped and reads again, returning the difference. The value is kept
// alive across the first read with runtime.KeepAlive.
func retainedHeap(mk func() any) uint64 {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	v := mk()

	runtime.GC()
	var held runtime.MemStats
	runtime.ReadMemStats(&held)
	runtime.KeepAlive(v)

	if held.HeapAlloc < before.HeapAlloc {
		return 0
	}
	return held.HeapAlloc - before.HeapAlloc
}
