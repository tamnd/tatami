package search

// This file ties the codec, the dictionary, and the scorer into a buildable,
// serializable, queryable inverted index: the in-memory builder that assigns
// dense doc ids and accumulates per-term postings, the three-run serialization
// of the inverted sub-region (term dictionary, posting payloads, skip tables),
// and the query-side index that turns a term into a cursor and runs the top-k
// loop (09-search-scale.md, sections 4 and 5).

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// InvertedBuilder accumulates documents into a term-to-postings map with dense,
// sequential doc ids assigned in add order. It is the build half of a segment:
// AddDocument records one document's per-term frequencies, and Build seals the
// dictionary and the posting lists.
type InvertedBuilder struct {
	numDocs int
	terms   map[string][]Posting
}

// NewInvertedBuilder returns an empty builder.
func NewInvertedBuilder() *InvertedBuilder {
	return &InvertedBuilder{terms: make(map[string][]Posting)}
}

// AddDocument records the term frequencies of one document and returns the dense
// doc id assigned to it. Ids are assigned 0..N-1 in add order, so appending each
// term's posting keeps every list ascending by doc id, which is what Encode
// requires. A term with a zero frequency is ignored.
func (b *InvertedBuilder) AddDocument(termFreqs map[string]uint32) DocID {
	id := DocID(b.numDocs)
	for t, f := range termFreqs {
		if f == 0 {
			continue
		}
		b.terms[t] = append(b.terms[t], Posting{Doc: id, Frequency: f})
	}
	b.numDocs++
	return id
}

// NumDocs reports how many documents have been added.
func (b *InvertedBuilder) NumDocs() int { return b.numDocs }

// Build seals the accumulated postings into an immutable inverted index. A
// one-document term is stored as an inline singleton; every other term gets an
// encoded posting list. Terms are walked in ascending order so the output is
// deterministic.
func (b *InvertedBuilder) Build() (*Inverted, error) {
	keys := make([]string, 0, len(b.terms))
	for t := range b.terms {
		keys = append(keys, t)
	}
	sort.Strings(keys)

	termList := make([]Term, 0, len(keys))
	var lists []*List
	for _, t := range keys {
		ps := b.terms[t]
		if len(ps) == 1 {
			termList = append(termList, Term{Term: t, Entry: Entry{
				DocFreq:       1,
				Singleton:     true,
				SingletonDoc:  ps[0].Doc,
				SingletonFreq: ps[0].Frequency,
			}})
			continue
		}
		l, err := Encode(ps)
		if err != nil {
			return nil, fmt.Errorf("search: encode term %q: %w", t, err)
		}
		idx := len(lists)
		lists = append(lists, l)
		termList = append(termList, Term{Term: t, Entry: Entry{
			DocFreq:       len(ps),
			PostingOffset: int64(idx),
		}})
	}
	return &Inverted{dict: NewSortedDict(termList), lists: lists, numDocs: b.numDocs, live: NewLive(b.numDocs)}, nil
}

// Inverted is an immutable, queryable inverted index: a term dictionary, the
// posting lists it points at, the document count, and a live-docs bitset. The
// posting lists are immutable; only the live bitset mutates, on a delete. It is
// safe for concurrent readers but not for a reader racing a Delete.
type Inverted struct {
	dict    *SortedDict
	lists   []*List
	numDocs int
	live    *Live
}

// NumDocs returns the dense doc-id space size N, the input to IDF. It counts
// deleted documents too, because IDF is defined over the segment as built; a
// merge is what actually removes deleted documents from N.
func (inv *Inverted) NumDocs() int { return inv.numDocs }

// LiveDocs returns the number of documents not yet deleted.
func (inv *Inverted) LiveDocs() int {
	if inv.live == nil {
		return inv.numDocs
	}
	return inv.live.Count()
}

// NumDeleted returns the number of deleted documents.
func (inv *Inverted) NumDeleted() int { return inv.numDocs - inv.LiveDocs() }

// IsLive reports whether a dense doc id is still live.
func (inv *Inverted) IsLive(d DocID) bool {
	if inv.live == nil {
		return int(d) >= 0 && int(d) < inv.numDocs
	}
	return inv.live.Get(int(d))
}

// Delete marks a dense doc id deleted. It returns true when the call changed the
// state. The posting lists are untouched; the document is filtered at query time
// and dropped at the next merge (09-search-scale.md, section 7).
func (inv *Inverted) Delete(d DocID) bool {
	if inv.live == nil {
		inv.live = NewLive(inv.numDocs)
	}
	return inv.live.Clear(int(d))
}

// Live returns the live-docs bitset, or nil when none was attached (treated as
// all-live).
func (inv *Inverted) Live() *Live { return inv.live }

// SetLive attaches a live-docs bitset, used when reloading a segment whose
// deletions were persisted in the inverted sub-region.
func (inv *Inverted) SetLive(l *Live) { inv.live = l }

// PerDocFreqs reconstructs, for every live document, its term-to-frequency map by
// walking every posting list once. It is the input to a merge, which re-adds each
// live document to a fresh builder so the merged segment's postings are rebuilt
// from scratch with new dense ids (09-search-scale.md, section 7). Deleted
// documents are omitted, so the merged segment reclaims their space.
func (inv *Inverted) PerDocFreqs() []map[string]uint32 {
	perDoc := make([]map[string]uint32, inv.numDocs)
	for _, t := range inv.dict.Terms() {
		cur, _, ok := inv.Postings(t.Term)
		if !ok {
			continue
		}
		for cur.Next() {
			d := int(cur.Doc())
			if !inv.IsLive(cur.Doc()) {
				continue
			}
			if perDoc[d] == nil {
				perDoc[d] = make(map[string]uint32)
			}
			perDoc[d][t.Term] = cur.Freq()
		}
	}
	return perDoc
}

// NumTerms returns the number of distinct terms.
func (inv *Inverted) NumTerms() int { return inv.dict.Len() }

// Dict exposes the term dictionary for prefix and range scans.
func (inv *Inverted) Dict() *SortedDict { return inv.dict }

// Postings returns a cursor over a term's posting list and its document
// frequency, or ok=false when the term is absent. A singleton term is
// materialized into a one-posting list on demand, so the caller sees a uniform
// cursor regardless of how the term was stored.
func (inv *Inverted) Postings(term string) (*Cursor, int, bool) {
	e, ok := inv.dict.Lookup(term)
	if !ok {
		return nil, 0, false
	}
	if e.Singleton {
		l, err := Encode([]Posting{{Doc: e.SingletonDoc, Frequency: e.SingletonFreq}})
		if err != nil {
			return nil, 0, false
		}
		return l.Cursor(), 1, true
	}
	if e.PostingOffset < 0 || int(e.PostingOffset) >= len(inv.lists) {
		return nil, 0, false
	}
	return inv.lists[e.PostingOffset].Cursor(), e.DocFreq, true
}

// MaxFreq returns a term's maximum in-document frequency, the input to its WAND
// upper bound. A singleton's max is its single frequency.
func (inv *Inverted) MaxFreq(term string) uint32 {
	e, ok := inv.dict.Lookup(term)
	if !ok {
		return 0
	}
	if e.Singleton {
		return e.SingletonFreq
	}
	if int(e.PostingOffset) >= len(inv.lists) {
		return 0
	}
	return inv.lists[e.PostingOffset].MaxFreq()
}

// Search runs the block-max WAND top-k over the disjunction of the query terms,
// scored by single-field BM25 with the shared k1 saturation. It returns the hits
// highest score first. Terms absent from the segment are skipped.
func (inv *Inverted) Search(terms []string, k int) []Hit {
	col := Collection{N: inv.numDocs}
	var inputs []TermInput
	for _, t := range terms {
		cur, df, ok := inv.Postings(t)
		if !ok {
			continue
		}
		idf := col.IDF(df)
		inputs = append(inputs, TermInput{
			Cursor:  cur,
			Scorer:  NewBM25Scorer(idf, DefaultK1),
			MaxFreq: inv.MaxFreq(t),
		})
	}
	// When nothing is deleted, take the plain path so the common case pays no
	// per-document liveness check.
	if inv.live == nil || inv.live.AllLive() {
		return WAND(inputs, k)
	}
	return WANDFilter(inputs, k, func(d DocID) bool { return inv.live.Get(int(d)) })
}

// EncodeInverted serializes an inverted index into the three byte runs of the
// inverted sub-region: the term dictionary, the concatenated posting payloads,
// and the per-list skip tables. The three runs are kept separate so a query can
// hold the small, hot dictionary and skip tables resident while the large
// posting payloads page in on demand (09-search-scale.md, section 4).
func EncodeInverted(inv *Inverted) (termDict, postings, skips []byte) {
	// Term dictionary run.
	td := binary.AppendUvarint(nil, uint64(inv.dict.Len()))
	for _, t := range inv.dict.Terms() {
		td = binary.AppendUvarint(td, uint64(len(t.Term)))
		td = append(td, t.Term...)
		td = binary.AppendUvarint(td, uint64(t.Entry.DocFreq))
		if t.Entry.Singleton {
			td = append(td, 1)
			td = binary.AppendUvarint(td, uint64(t.Entry.SingletonDoc))
			td = binary.AppendUvarint(td, uint64(t.Entry.SingletonFreq))
		} else {
			td = append(td, 0)
			td = binary.AppendUvarint(td, uint64(t.Entry.PostingOffset))
		}
	}

	// Posting payloads run.
	pp := binary.AppendUvarint(nil, uint64(len(inv.lists)))
	for _, l := range inv.lists {
		pp = binary.AppendUvarint(pp, uint64(len(l.data)))
		pp = append(pp, l.data...)
	}

	// Skip tables run.
	sk := binary.AppendUvarint(nil, uint64(len(inv.lists)))
	for _, l := range inv.lists {
		sk = binary.AppendUvarint(sk, uint64(l.numDoc))
		sk = binary.AppendUvarint(sk, uint64(len(l.blocks)))
		for _, b := range l.blocks {
			sk = binary.AppendUvarint(sk, uint64(b.firstDoc))
			sk = binary.AppendUvarint(sk, uint64(b.lastDoc))
			sk = binary.AppendUvarint(sk, uint64(b.maxFreq))
			sk = binary.AppendUvarint(sk, uint64(b.offset))
			sk = binary.AppendUvarint(sk, uint64(b.count))
		}
	}
	return td, pp, sk
}

// DecodeInverted reconstructs an inverted index from the three runs that
// EncodeInverted produced plus the segment's document count. It pairs each
// posting payload with its skip table by position, the same index the dictionary
// entries reference.
func DecodeInverted(termDict, postings, skips []byte, numDocs int) (*Inverted, error) {
	// Posting payloads.
	pc := &byteReader{b: postings}
	nLists := int(pc.uvarint())
	datas := make([][]byte, nLists)
	for i := 0; i < nLists; i++ {
		n := int(pc.uvarint())
		datas[i] = pc.take(n)
	}
	if pc.err != nil {
		return nil, fmt.Errorf("search: decode posting payloads: %w", pc.err)
	}

	// Skip tables, paired with the payloads.
	sc := &byteReader{b: skips}
	nSkip := int(sc.uvarint())
	if nSkip != nLists {
		return nil, fmt.Errorf("search: %d skip tables for %d posting lists", nSkip, nLists)
	}
	lists := make([]*List, nLists)
	for i := 0; i < nLists; i++ {
		l := &List{data: datas[i]}
		l.numDoc = int(sc.uvarint())
		nb := int(sc.uvarint())
		l.blocks = make([]block, nb)
		for j := 0; j < nb; j++ {
			l.blocks[j] = block{
				firstDoc: DocID(sc.uvarint()),
				lastDoc:  DocID(sc.uvarint()),
				maxFreq:  uint32(sc.uvarint()),
				offset:   int(sc.uvarint()),
				count:    int(sc.uvarint()),
			}
		}
		lists[i] = l
	}
	if sc.err != nil {
		return nil, fmt.Errorf("search: decode skip tables: %w", sc.err)
	}

	// Term dictionary.
	tc := &byteReader{b: termDict}
	nTerms := int(tc.uvarint())
	termList := make([]Term, 0, nTerms)
	for i := 0; i < nTerms; i++ {
		tl := int(tc.uvarint())
		term := string(tc.take(tl))
		e := Entry{DocFreq: int(tc.uvarint())}
		if tc.byte1() == 1 {
			e.Singleton = true
			e.SingletonDoc = DocID(tc.uvarint())
			e.SingletonFreq = uint32(tc.uvarint())
		} else {
			e.PostingOffset = int64(tc.uvarint())
		}
		termList = append(termList, Term{Term: term, Entry: e})
	}
	if tc.err != nil {
		return nil, fmt.Errorf("search: decode term dictionary: %w", tc.err)
	}
	return &Inverted{dict: NewSortedDict(termList), lists: lists, numDocs: numDocs}, nil
}

// byteReader is a tiny forward reader over a byte slice, the decode mirror of the
// binary.AppendUvarint encoders above.
type byteReader struct {
	b   []byte
	pos int
	err error
}

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b[r.pos:])
	if n <= 0 {
		r.err = fmt.Errorf("bad uvarint at offset %d", r.pos)
		return 0
	}
	r.pos += n
	return v
}

func (r *byteReader) byte1() byte {
	if r.err != nil || r.pos >= len(r.b) {
		if r.err == nil {
			r.err = fmt.Errorf("truncated run")
		}
		return 0
	}
	v := r.b[r.pos]
	r.pos++
	return v
}

func (r *byteReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.b) {
		r.err = fmt.Errorf("run slice overruns body")
		return nil
	}
	out := r.b[r.pos : r.pos+n]
	r.pos += n
	return out
}
