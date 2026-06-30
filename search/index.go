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
	return &Inverted{dict: NewSortedDict(termList), lists: eagerLists(lists), numDocs: b.numDocs, live: NewLive(b.numDocs)}, nil
}

// Inverted is an immutable, queryable inverted index: a term dictionary, the
// posting lists it points at, the document count, and a live-docs bitset. The
// posting lists are immutable; only the live bitset mutates, on a delete. It is
// safe for concurrent readers but not for a reader racing a Delete.
type Inverted struct {
	dict    Dictionary
	lists   listStore
	numDocs int
	live    *Live
}

// eachTerm walks every dictionary entry in ascending term order. It is the seam-
// generic form of SortedDict.Terms(): a prefix scan with an empty prefix visits
// every term, so the builder's resident SortedDict and a decoded segment's on-disk
// block tree both enumerate without materializing the whole term list (scale/03).
func eachTerm(d Dictionary, fn func(term string, e Entry) bool) {
	d.PrefixScan("", fn)
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
	eachTerm(inv.dict, func(term string, _ Entry) bool {
		cur, _, ok := inv.Postings(term)
		if !ok {
			return true
		}
		for cur.Next() {
			d := int(cur.Doc())
			if !inv.IsLive(cur.Doc()) {
				continue
			}
			if perDoc[d] == nil {
				perDoc[d] = make(map[string]uint32)
			}
			perDoc[d][term] = cur.Freq()
		}
		return true
	})
	return perDoc
}

// NumTerms returns the number of distinct terms.
func (inv *Inverted) NumTerms() int { return inv.dict.Len() }

// Dict exposes the term dictionary for prefix and range scans. It is the seam
// type, so a decoded segment can hand back the on-disk block tree and a builder
// can hand back the resident SortedDict behind the same interface.
func (inv *Inverted) Dict() Dictionary { return inv.dict }

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
	if e.PostingOffset < 0 || int(e.PostingOffset) >= inv.lists.len() {
		return nil, 0, false
	}
	return inv.lists.at(int(e.PostingOffset)).Cursor(), e.DocFreq, true
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
	if int(e.PostingOffset) >= inv.lists.len() {
		return 0
	}
	return inv.lists.at(int(e.PostingOffset)).MaxFreq()
}

// GlobalStats supplies the corpus-wide statistics that make a shard's scores
// comparable across shards: the total live document count N over every shard and
// the global document frequency of a term, summed over every shard. A shard
// scored with global stats produces scores a broker can rank directly against
// other shards' scores, which is what makes a pruned, early-terminating
// cross-shard top-k exact rather than best-effort (12-distributed-serving.md).
// A RoutingIndex satisfies this interface.
type GlobalStats interface {
	// NumDocs is N, the live document count across the whole collection.
	NumDocs() int
	// DocFreq is the term's document frequency summed across every shard.
	DocFreq(term string) int
}

// Search runs the block-max WAND top-k over the disjunction of the query terms,
// scored by single-field BM25 with the shared k1 saturation, using this shard's
// own document count for IDF. It returns the hits highest score first. Terms
// absent from the segment are skipped.
func (inv *Inverted) Search(terms []string, k int) []Hit {
	return inv.SearchWith(terms, k, nil)
}

// SearchWith is Search with the IDF of every term computed from the supplied
// global stats rather than this shard's local document count. A nil stats falls
// back to per-shard IDF, which is what a single-segment query wants. A broker
// serving many shards passes the same global stats to every shard so the scores
// it merges are on one scale, the precondition the cross-shard top-k merge and
// its early termination rely on (12-distributed-serving.md). The MaxFreq each
// term contributes to its WAND upper bound stays shard-local, because it bounds
// the frequencies in this shard's own postings; only the IDF goes global.
func (inv *Inverted) SearchWith(terms []string, k int, stats GlobalStats) []Hit {
	col := Collection{N: inv.numDocs}
	if stats != nil {
		col = Collection{N: stats.NumDocs()}
	}
	var inputs []TermInput
	for _, t := range terms {
		cur, df, ok := inv.Postings(t)
		if !ok {
			continue
		}
		if stats != nil {
			df = stats.DocFreq(t)
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

// EachTerm calls fn for every term in the dictionary with its document frequency
// and its maximum in-document frequency, in ascending term order. It is the input
// to the routing builder, which folds each shard's terms into the broker's
// term-to-shards map (12-distributed-serving.md). It reads only the dictionary
// and skip tables, never a posting payload, so building a routing index over many
// shards stays cheap.
func (inv *Inverted) EachTerm(fn func(term string, df int, maxFreq uint32)) {
	eachTerm(inv.dict, func(term string, e Entry) bool {
		var mf uint32
		if e.Singleton {
			mf = e.SingletonFreq
		} else if int(e.PostingOffset) < inv.lists.len() {
			mf = inv.lists.at(int(e.PostingOffset)).MaxFreq()
		}
		fn(term, e.DocFreq, mf)
		return true
	})
}

// EncodeInverted serializes an inverted index into the three byte runs of the
// inverted sub-region: the term dictionary, the concatenated posting payloads,
// and the per-list skip tables. The three runs are kept separate so a query can
// hold the small, hot dictionary and skip tables resident while the large
// posting payloads page in on demand (09-search-scale.md, section 4).
func EncodeInverted(inv *Inverted) (termDict, postings, skips []byte) {
	// Term dictionary run: the on-disk block tree (scale/03, M0). The entries are
	// already in ascending order, which is what BuildBlockTree requires, so a
	// decoded segment serves lookups from the block tree's sparse index instead of
	// materializing every term string the way the old flat run forced.
	terms := make([]Term, 0, inv.dict.Len())
	eachTerm(inv.dict, func(t string, e Entry) bool {
		terms = append(terms, Term{Term: t, Entry: e})
		return true
	})
	// BuildBlockTree's only error path is zstd encoder initialization, a process-
	// wide condition the codec also depends on; if it ever fires the term run is
	// left empty and DecodeInverted reports it rather than the writer panicking.
	td, _ := BuildBlockTree(terms, DefaultBlockTreeBlockSize)

	// Posting payloads run.
	nLists := inv.lists.len()
	pp := binary.AppendUvarint(nil, uint64(nLists))
	for i := range nLists {
		l := inv.lists.at(i)
		pp = binary.AppendUvarint(pp, uint64(len(l.data)))
		pp = append(pp, l.data...)
	}

	// Skip tables run.
	sk := binary.AppendUvarint(nil, uint64(nLists))
	for i := range nLists {
		l := inv.lists.at(i)
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
// entries reference. blockTree selects the term-dictionary decoder: true for the
// on-disk block tree this writer now emits (scale/03, M0), false for the legacy
// flat uvarint run a pre-M0 segment carries. The caller reads the choice from the
// segment header's FlagBlockTreeDict bit, never by sniffing the run bytes, because
// the block-tree version byte collides with a small flat-format term count.
func DecodeInverted(termDict, postings, skips []byte, numDocs int, blockTree bool) (*Inverted, error) {
	// Posting payloads and skip tables stay as their raw runs; the lazy store
	// scans them once to find each list's offset and decodes a list only when a
	// query asks for it (scale/04, lever 2). This is the heap win: the eager
	// decode held the whole block array resident, ~22 MiB on a real shard.
	lists, err := newLazyLists(postings, skips)
	if err != nil {
		return nil, err
	}

	var dict Dictionary
	if blockTree {
		// The block tree keeps only a sparse index resident and pages each 64-term
		// block in on demand, so opening a 20M-term segment no longer materializes
		// every term string into the heap (scale/03).
		bt, err := OpenBlockTree(termDict)
		if err != nil {
			return nil, fmt.Errorf("search: open block-tree dictionary: %w", err)
		}
		dict = bt
	} else {
		d, err := decodeFlatDict(termDict)
		if err != nil {
			return nil, err
		}
		dict = d
	}
	return &Inverted{dict: dict, lists: lists, numDocs: numDocs}, nil
}

// decodeFlatDict reads the original flat uvarint term-dictionary run into a
// resident SortedDict. It is kept for segments written before M0 wired the block
// tree into the format; new segments take the block-tree path in DecodeInverted.
func decodeFlatDict(termDict []byte) (*SortedDict, error) {
	tc := &byteReader{b: termDict}
	nTerms := int(tc.uvarint())
	termList := make([]Term, 0, nTerms)
	for range nTerms {
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
	return NewSortedDict(termList), nil
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
