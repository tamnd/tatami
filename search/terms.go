package search

// The segment term dictionary maps a term to its posting-list pointer and
// statistics and supports ordered enumeration for prefix and range scans.
//
// In production this is a finite state transducer per field (the Vellum/Lucene
// model) built by Daciuk incremental minimal-acyclic construction over the
// segment's already-sorted terms, fronting an on-disk block tree. That machinery
// is the swap target; the seam is the Dictionary interface, and SortedDict is
// the in-memory reference it is tested against. The two properties the rest of
// the engine leans on are pinned here: ordered enumeration (so prefix and range
// scans are a contiguous walk) and a negative lookup that needs no posting-store
// seek (09-search-scale.md, section 4).

import (
	"sort"
	"strings"
)

// Entry is what the dictionary returns for a term: its document frequency and
// how to reach its postings. When DocFreq == 1 the single document id is stored
// inline (Singleton), so a one-document term costs no separate posting list -
// the SingletonDocID optimization. At web scale the long tail of one-document
// terms (typos, hashes, unique tokens) is enormous, so this saves the per-list
// overhead across millions of terms.
type Entry struct {
	DocFreq      int
	Singleton    bool
	SingletonDoc DocID
	// SingletonFreq is the term frequency in the single document, carried inline
	// so the optimization preserves scoring inputs, not just the doc id;
	// meaningful only when Singleton.
	SingletonFreq uint32
	// PostingOffset indexes the term's encoded posting list in the segment's
	// postings store; meaningful only when !Singleton.
	PostingOffset int64
}

// Dictionary is the term-dictionary seam. Implementations are immutable after
// construction and safe for concurrent readers.
type Dictionary interface {
	// Lookup returns the entry for an exact term and whether it exists. A miss
	// must not require touching the postings store.
	Lookup(term string) (Entry, bool)
	// PrefixScan calls fn for every term with the given prefix, in ascending term
	// order, stopping early if fn returns false.
	PrefixScan(prefix string, fn func(term string, e Entry) bool)
	// Len reports the number of terms.
	Len() int
}

// Term is one term and its entry, used when building a SortedDict.
type Term struct {
	Term  string
	Entry Entry
}

// SortedDict is the reference dictionary: a slice of terms kept in ascending
// Unicode order, with binary-search lookup and contiguous prefix scans. It
// mirrors the ordering contract of the FST without the suffix compression.
type SortedDict struct {
	terms []Term
}

// NewSortedDict builds a dictionary from terms, which need not be pre-sorted; it
// sorts a copy. Duplicate terms keep the last entry, matching a builder that
// overwrites.
func NewSortedDict(terms []Term) *SortedDict {
	cp := make([]Term, len(terms))
	copy(cp, terms)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Term < cp[j].Term })
	// Collapse duplicates, keeping the last write.
	out := cp[:0]
	for i, t := range cp {
		if i > 0 && t.Term == out[len(out)-1].Term {
			out[len(out)-1] = t
			continue
		}
		out = append(out, t)
	}
	return &SortedDict{terms: out}
}

// search returns the index of term or the insertion point.
func (d *SortedDict) search(term string) int {
	return sort.Search(len(d.terms), func(i int) bool { return d.terms[i].Term >= term })
}

// Lookup returns the entry for an exact term. The binary search answers a miss
// without any posting-store access, matching the FST's negative-lookup property.
func (d *SortedDict) Lookup(term string) (Entry, bool) {
	i := d.search(term)
	if i < len(d.terms) && d.terms[i].Term == term {
		return d.terms[i].Entry, true
	}
	return Entry{}, false
}

// PrefixScan walks every term sharing prefix in ascending order. With the terms
// sorted, the matches are one contiguous run starting at the prefix's insertion
// point, so the scan stops at the first term that no longer carries the prefix.
func (d *SortedDict) PrefixScan(prefix string, fn func(term string, e Entry) bool) {
	for i := d.search(prefix); i < len(d.terms); i++ {
		if !strings.HasPrefix(d.terms[i].Term, prefix) {
			return
		}
		if !fn(d.terms[i].Term, d.terms[i].Entry) {
			return
		}
	}
}

// Len reports the number of terms.
func (d *SortedDict) Len() int { return len(d.terms) }

// Terms returns the dictionary entries in ascending term order, the form the
// inverted-region serializer walks.
func (d *SortedDict) Terms() []Term { return d.terms }
