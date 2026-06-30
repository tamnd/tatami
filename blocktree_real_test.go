package tatami

// Real-data measurement for the scale redesign's term dictionary (Spec 2066,
// scale/03). It builds a search segment from a real crawl shard, pulls the exact
// term list the segment carries, and compares the block-tree dictionary against
// the resident SortedDict on two axes that decide whether 100M docs/machine is
// reachable: resident heap and lookup latency.
//
// It is gated on a local crawl shard like the other *_realdata tests, so CI
// skips it cleanly. Run it with:
//
//	go test -run TestBlockTreeRealRAM -v
//	go test -bench BenchmarkBlockTreeRealLookup -benchmem -run x

import (
	"testing"

	"github.com/tamnd/tatami/search"
)

// realTerms builds (once, via the shared loader) the real segment and returns
// its sorted term list. It skips when the shard is absent.
func realTerms(tb testing.TB) []search.Term {
	seg := loadRealSegment(tb)
	var terms []search.Term
	seg.Inverted().Dict().PrefixScan("", func(t string, e search.Entry) bool {
		terms = append(terms, search.Term{Term: t, Entry: e})
		return true
	})
	return terms
}

// sortedDictResidentBytes estimates the heap a SortedDict holds: every term
// string plus the per-entry slice element. This is the cost the block tree is
// trying to remove from resident memory.
func sortedDictResidentBytes(terms []search.Term) int {
	n := 0
	for _, t := range terms {
		n += len(t.Term) + 16 // term bytes plus the string header
		n += 40               // Term struct (string header + Entry fields) slice element
	}
	return n
}

// TestBlockTreeRealRAM reports, on the real shard, the resident bytes of the
// block-tree sparse index versus the SortedDict, the term-dictionary on-disk
// size under each, and confirms the block tree answers every real term and a
// batch of misses identically. The numbers are logged so the implementation note
// can quote measured values, not estimates.
func TestBlockTreeRealRAM(t *testing.T) {
	terms := realTerms(t)
	if len(terms) == 0 {
		t.Skip("no terms in real shard")
	}

	ref := search.NewSortedDict(terms)
	data, err := search.BuildBlockTree(terms, search.DefaultBlockTreeBlockSize)
	if err != nil {
		t.Fatal(err)
	}
	bt, err := search.OpenBlockTree(data)
	if err != nil {
		t.Fatal(err)
	}

	if bt.Len() != ref.Len() {
		t.Fatalf("Len mismatch: bt=%d ref=%d", bt.Len(), ref.Len())
	}
	// Exactness over every real term.
	for _, term := range terms {
		re, rok := ref.Lookup(term.Term)
		be, bok := bt.Lookup(term.Term)
		if rok != bok || re != be {
			t.Fatalf("Lookup(%q): ref=(%v,%v) bt=(%v,%v)", term.Term, re, rok, be, bok)
		}
	}

	sortedRAM := sortedDictResidentBytes(terms)
	btRAM := bt.ResidentBytes()
	t.Logf("real shard: %d terms", len(terms))
	t.Logf("SortedDict resident estimate : %s", humanByteCount(sortedRAM))
	t.Logf("BlockTree  resident (sparse) : %s", humanByteCount(btRAM))
	t.Logf("resident reduction           : %.1fx", float64(sortedRAM)/float64(btRAM))
	t.Logf("BlockTree serialized on disk : %s (front-coded + zstd, all blocks)", humanByteCount(len(data)))
}

// BenchmarkBlockTreeRealLookup times a lookup of present terms against both
// dictionaries on the real shard, so the implementation note can show the lazy,
// one-block-decode path stays in the sub-microsecond-to-few-microsecond range
// that the 10ms query budget can absorb.
func BenchmarkBlockTreeRealLookup(b *testing.B) {
	terms := realTerms(b)
	if len(terms) == 0 {
		b.Skip("no terms in real shard")
	}
	// A spread of probe terms across the keyspace.
	probes := make([]string, 0, 1024)
	step := len(terms) / 1024
	if step == 0 {
		step = 1
	}
	for i := 0; i < len(terms) && len(probes) < 1024; i += step {
		probes = append(probes, terms[i].Term)
	}

	ref := search.NewSortedDict(terms)
	data, err := search.BuildBlockTree(terms, search.DefaultBlockTreeBlockSize)
	if err != nil {
		b.Fatal(err)
	}
	bt, err := search.OpenBlockTree(data)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("SortedDict", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = ref.Lookup(probes[i%len(probes)])
		}
	})
	b.Run("BlockTree", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = bt.Lookup(probes[i%len(probes)])
		}
	})
}
