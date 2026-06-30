package search

// Differential and property tests for the block-tree term dictionary. The
// contract is simple: over the same sorted term list, BlockTreeDict must answer
// Lookup, PrefixScan, and Len identically to SortedDict, which terms.go already
// trusts as the reference. The real-data RAM and lookup numbers live in the root
// package (blocktree_real_test.go) where the crawl shard is available.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

// makeTerms builds n deterministic, distinct terms with a realistic mix of
// shared prefixes (so front coding is exercised) and entry shapes (singletons
// and posting-offset terms).
func makeTerms(n int, seed int64) []Term {
	rng := rand.New(rand.NewSource(seed))
	prefixes := []string{"", "a", "ab", "abc", "data", "http", "www", "z", "zz", "machine", "learn"}
	set := make(map[string]struct{}, n)
	terms := make([]Term, 0, n)
	for len(terms) < n {
		p := prefixes[rng.Intn(len(prefixes))]
		t := fmt.Sprintf("%s%x", p, rng.Int63())
		if _, ok := set[t]; ok {
			continue
		}
		set[t] = struct{}{}
		e := Entry{DocFreq: 1 + rng.Intn(1000)}
		if e.DocFreq == 1 {
			e.Singleton = true
			e.SingletonDoc = DocID(rng.Intn(1 << 20))
			e.SingletonFreq = uint32(1 + rng.Intn(50))
		} else {
			e.PostingOffset = int64(len(terms))
		}
		terms = append(terms, Term{Term: t, Entry: e})
	}
	sort.Slice(terms, func(i, j int) bool { return terms[i].Term < terms[j].Term })
	return terms
}

func TestBlockTreeMatchesSortedDict(t *testing.T) {
	for _, n := range []int{0, 1, 2, 63, 64, 65, 1000, 50000} {
		for _, bs := range []int{1, 16, 64, 128} {
			terms := makeTerms(n, int64(n*131+bs))
			ref := NewSortedDict(terms)
			data, err := BuildBlockTree(terms, bs)
			if err != nil {
				t.Fatalf("n=%d bs=%d build: %v", n, bs, err)
			}
			bt, err := OpenBlockTree(data)
			if err != nil {
				t.Fatalf("n=%d bs=%d open: %v", n, bs, err)
			}
			if bt.Len() != ref.Len() {
				t.Fatalf("n=%d bs=%d Len: bt=%d ref=%d", n, bs, bt.Len(), ref.Len())
			}
			// Every present term resolves to the identical entry.
			for _, term := range terms {
				re, rok := ref.Lookup(term.Term)
				be, bok := bt.Lookup(term.Term)
				if rok != bok || re != be {
					t.Fatalf("n=%d bs=%d Lookup(%q): ref=(%v,%v) bt=(%v,%v)", n, bs, term.Term, re, rok, be, bok)
				}
			}
			// A batch of definite misses resolves to a miss in both.
			for i := range 200 {
				miss := fmt.Sprintf("MISS-%d-no-such-term", i)
				_, rok := ref.Lookup(miss)
				_, bok := bt.Lookup(miss)
				if rok || bok {
					t.Fatalf("n=%d bs=%d Lookup(%q) should miss: ref=%v bt=%v", n, bs, miss, rok, bok)
				}
			}
		}
	}
}

func TestBlockTreePrefixScanMatchesSortedDict(t *testing.T) {
	terms := makeTerms(20000, 99)
	ref := NewSortedDict(terms)
	data, err := BuildBlockTree(terms, 64)
	if err != nil {
		t.Fatal(err)
	}
	bt, err := OpenBlockTree(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"", "a", "ab", "abc", "data", "z", "zz", "machine", "nomatch", "http", "www"} {
		var refOut, btOut []string
		ref.PrefixScan(prefix, func(term string, _ Entry) bool { refOut = append(refOut, term); return true })
		bt.PrefixScan(prefix, func(term string, _ Entry) bool { btOut = append(btOut, term); return true })
		if len(refOut) != len(btOut) {
			t.Fatalf("PrefixScan(%q): ref=%d btr=%d terms", prefix, len(refOut), len(btOut))
		}
		for i := range refOut {
			if refOut[i] != btOut[i] {
				t.Fatalf("PrefixScan(%q) #%d: ref=%q bt=%q", prefix, i, refOut[i], btOut[i])
			}
		}
	}
}

func TestBlockTreePrefixScanEarlyStop(t *testing.T) {
	terms := makeTerms(5000, 7)
	data, err := BuildBlockTree(terms, 32)
	if err != nil {
		t.Fatal(err)
	}
	bt, err := OpenBlockTree(data)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	bt.PrefixScan("a", func(string, Entry) bool { count++; return count < 3 })
	if count != 3 {
		t.Fatalf("early stop: expected 3 calls, got %d", count)
	}
}
