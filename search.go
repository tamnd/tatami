package tatami

// This file is the search-segment role: it turns a batch of documents into a
// tatami file with the role bit set, a forward store laid out as columns indexed
// by dense docID, and an inverted sub-region in the index region. The codec, the
// term dictionary, the scorer, and the block-max WAND loop all live in the
// self-contained search/ subpackage; this file is the bridge that writes those
// bytes into the container and reads them back to answer a query (Spec 2066,
// 09-search-scale.md).
//
// The forward store is genuine tatami columns: url, title, a blob-separated body,
// and the per-field length norms. Because dense docIDs are assigned in add order,
// the row index of every column equals the docID, so fetching a hit's stored
// fields is a direct columnar read with no global lookup, the whole point of the
// dense-id design (09-search-scale.md, sections 2 and 3).

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/tamnd/tatami/search"
)

// Search-segment forward-store column names. The order here is the schema order,
// so a column's index is stable. Column 3 is the variable one: a full-document
// segment stores the blob-separated body there, a search-only segment stores a
// short snippet string in the same slot, so every other column index is identical
// across the two shapes and the reader tells them apart by the column's type.
const (
	colDocID     = "doc_id"
	colURL       = "url"
	colTitle     = "title"
	colBody      = "body"
	colSnippet   = "snippet"
	colNormBody  = "norm_body"
	colNormTitle = "norm_title"
	colNormAnchr = "norm_anchor"
	colNormURL   = "norm_url"
)

// colVariable is the index of the body-or-snippet column, the one slot whose type
// differs between a full-document segment and a search-only segment.
const colVariable = 3

// searchSchema is the forward-store schema of a search segment. doc_id is the
// durable global identity (sha256(url)) and carries a bloom filter for by-id
// lookup; url and title are short strings; the four norm columns are the per-field
// lengths BM25F reads. The middle column depends on the segment shape: a
// full-document segment blob-separates the body, a search-only segment keeps a
// short snippet string instead and never stores the body at all
// (13-search-only-and-scale.md).
func searchSchema(snippet bool) *Schema {
	mid := Field{Name: colBody, Type: TypeBlobRef, BlobSeparated: true}
	if snippet {
		mid = Field{Name: colSnippet, Type: TypeString}
	}
	return &Schema{Fields: []Field{
		{Name: colDocID, Type: TypeString, BloomFilter: true},
		{Name: colURL, Type: TypeString},
		{Name: colTitle, Type: TypeString},
		mid,
		{Name: colNormBody, Type: TypeUint16},
		{Name: colNormTitle, Type: TypeUint16},
		{Name: colNormAnchr, Type: TypeUint16},
		{Name: colNormURL, Type: TypeUint16},
	}}
}

// DefaultSnippetRunes is the snippet length a search-only builder keeps when its
// options leave the length zero. It is a lead excerpt long enough to render a
// result row, far short of the body it replaces.
const DefaultSnippetRunes = 200

// SearchDoc is one document handed to a SearchBuilder. DocID is the stable global
// identity; URL, Title, and Body are the stored fields and the text that is
// tokenized and inverted. Anchor text, when the link graph supplies it, goes in
// Anchor.
type SearchDoc struct {
	DocID  string
	URL    string
	Title  string
	Body   string
	Anchor string
}

// SearchBuilder accumulates documents and seals them into a search segment. It
// assigns dense docIDs in add order, tokenizes each field, accumulates the
// term-to-postings map, and records the per-field length norms. The build is
// in-memory; a streaming builder is a later milestone (matching M4/M5, which
// defer streaming k-way merge).
//
// In search-only mode the builder never retains the body: it tokenizes the body
// into the inverted index, keeps a short snippet for the result row, and drops the
// text. That is what lets a search-only segment cost a fraction of a
// full-document one on disk and in memory (13-search-only-and-scale.md).
type SearchBuilder struct {
	docs []SearchDoc
	inv  *search.InvertedBuilder
	// norms[docID] is the per-field token count, capped to uint16.
	norms [][search.NumFields]uint16
	// snippet mode: snippets[docID] holds the lead excerpt and docs[docID].Body is
	// blanked, so the body never lives past the indexing of one document.
	snippet      bool
	snippetRunes int
	snippets     []string
	// bigrams, when non-nil, accumulates the shard's adjacent-pair statistics for
	// the phrase routing sidecar: each ordered pair's document frequency and its
	// maximum in-document adjacency count. It is built only when the caller asks for
	// it, so a builder that never feeds a phrase router pays nothing (07-routing-latency.md,
	// section 4.1).
	bigrams map[search.BigramKey]*bigramStat
	// bigramKeep, when non-nil, bounds the capture to these pairs. The phrase router
	// only ever routes on a tracked set of adjacencies, never on every pair in the
	// corpus (07-routing-latency.md, section 4.1 calls the full set an unbounded
	// dictionary), so a builder handed that set records only those pairs and skips the
	// rest at the source. That turns the shard-wide pair map from millions of entries
	// over web text into the handful the router will ask for, which is the memory peak
	// of a bigram-capturing build. A nil keep set (the default) captures every pair,
	// the shape a caller that does not know its query set ahead of time still gets.
	bigramKeep map[search.BigramKey]bool
}

// bigramStat is one shard's running count for an adjacent pair: how many documents
// hold the adjacency and the most times it occurs in any single one of them.
type bigramStat struct {
	df      uint32
	maxFreq uint32
}

// SearchBuilderOptions selects the segment shape. Snippet turns on the
// search-only forward store: no body is stored, only a SnippetRunes-long lead
// excerpt per document. SnippetRunes defaults to DefaultSnippetRunes.
type SearchBuilderOptions struct {
	Snippet      bool
	SnippetRunes int
	// Bigrams turns on adjacent-pair capture for the phrase routing sidecar. It is
	// off by default because only a corpus that will answer phrase queries needs it,
	// and capturing it costs a per-document pass over adjacent tokens and a shard-wide
	// pair dictionary (07-routing-latency.md, section 4.1).
	Bigrams bool
	// BigramKeep bounds the capture to these pairs when non-nil. The sidecar the build
	// writes only carries the pairs the query set routes on, so with the keep set in
	// hand the builder records just those and never grows the shard-wide dictionary of
	// every adjacency in the corpus. That dictionary, held resident per in-flight
	// shard, is the memory peak of a sharded bigram build; bounding it at the source is
	// what lets a 10M or 100M shard build fit a box with tens of GB. It takes effect
	// only with Bigrams set; nil keeps the capture-everything behavior.
	BigramKeep map[search.BigramKey]bool
}

// NewSearchBuilder returns an empty full-document builder, the shape that stores
// the body blob-separated.
func NewSearchBuilder() *SearchBuilder {
	return &SearchBuilder{inv: search.NewInvertedBuilder()}
}

// NewSearchBuilderWith returns a builder of the shape the options select. A
// search-only builder indexes every document's body into the postings exactly as
// the full-document builder does, so the two produce identical retrieval, and
// differs only in what it keeps in the forward store.
func NewSearchBuilderWith(opts SearchBuilderOptions) *SearchBuilder {
	b := &SearchBuilder{inv: search.NewInvertedBuilder(), snippet: opts.Snippet}
	if opts.Snippet {
		b.snippetRunes = opts.SnippetRunes
		if b.snippetRunes <= 0 {
			b.snippetRunes = DefaultSnippetRunes
		}
	}
	if opts.Bigrams {
		b.bigrams = map[search.BigramKey]*bigramStat{}
		b.bigramKeep = opts.BigramKeep
	}
	return b
}

// Add tokenizes a document, records its per-field term frequencies and length
// norms, and assigns it the next dense docID. A term's posting frequency is its
// total count across all fields; the per-field counts survive as the length norms
// that a BM25F re-rank reads (09-search-scale.md, section 5).
func (b *SearchBuilder) Add(doc SearchDoc) {
	var norm [search.NumFields]uint16
	freqs := map[string]uint32{}
	// docBigrams counts this document's adjacent pairs, only when bigram capture is
	// on. Adjacency is within a field: each index() call starts with no predecessor,
	// so the last token of one field never pairs with the first token of the next.
	var docBigrams map[search.BigramKey]uint32
	if b.bigrams != nil {
		docBigrams = map[search.BigramKey]uint32{}
	}
	index := func(text string, f search.Field) {
		n := 0
		var prev string
		havePrev := false
		for _, tok := range tokenize(text) {
			freqs[tok]++
			n++
			if docBigrams != nil {
				if havePrev {
					key := search.BigramKey{A: prev, B: tok}
					// With a keep set the builder tracks only the pairs the query set
					// routes on, so an adjacency outside it never enters the per-document
					// map or the shard dictionary. That is what keeps the pair map bounded
					// to the query set instead of growing to every adjacency in the shard.
					if b.bigramKeep == nil || b.bigramKeep[key] {
						docBigrams[key]++
					}
				}
				prev, havePrev = tok, true
			}
		}
		norm[f] = capU16(n)
	}
	index(doc.Body, search.FieldBody)
	index(doc.Title, search.FieldTitle)
	index(doc.Anchor, search.FieldAnchor)
	index(doc.URL, search.FieldURL)

	// Fold this document's pairs into the shard dictionary: each pair the document
	// holds adds one to that pair's document frequency and lifts its shard maxFreq to
	// the document's adjacency count if that count is a new high.
	for key, cnt := range docBigrams {
		bs := b.bigrams[key]
		if bs == nil {
			bs = &bigramStat{}
			b.bigrams[key] = bs
		}
		bs.df++
		if cnt > bs.maxFreq {
			bs.maxFreq = cnt
		}
	}

	b.inv.AddDocument(freqs)
	b.norms = append(b.norms, norm)
	if b.snippet {
		b.snippets = append(b.snippets, makeSnippet(doc.Body, b.snippetRunes))
		doc.Body = "" // the body is indexed; do not carry it past this point
	}
	b.docs = append(b.docs, doc)
}

// EachBigram iterates the shard's adjacent pairs with each pair's document
// frequency and maximum in-document adjacency count, so a BigramRoutingBuilder can
// fold this shard into the phrase routing sidecar. It satisfies search.BigramSource.
// A builder created without the Bigrams option captured nothing and iterates zero
// pairs.
func (b *SearchBuilder) EachBigram(fn func(a, bb string, df int, maxFreq uint32)) {
	for key, bs := range b.bigrams {
		fn(key.A, key.B, int(bs.df), bs.maxFreq)
	}
}

// NumDocs reports how many documents have been added.
func (b *SearchBuilder) NumDocs() int { return len(b.docs) }

// Write seals the segment to path: it writes the forward columns through the
// normal tatami writer, attaches the serialized inverted sub-region, and closes
// the file with the role bit set.
func (b *SearchBuilder) Write(path string, opts WriterOptions) error {
	inv, err := b.inv.Build()
	if err != nil {
		return err
	}
	schema := searchSchema(b.snippet)
	w, f, err := Create(path, schema, opts)
	if err != nil {
		return err
	}
	cols := b.forwardColumns()
	if err := w.Append(Batch{Columns: cols}); err != nil {
		_ = w.Close()
		_ = f.Close()
		return err
	}
	td, pp, sk := search.EncodeInverted(inv)
	// A freshly built segment has no deletions, so it carries no live-docs run; a
	// reader treats its absence as all-live. Deletions are made on the open
	// segment and materialized at the next merge, which drops them.
	w.AttachInverted(td, pp, sk, nil, uint64(inv.NumTerms()), uint64(inv.NumDocs()))
	if err := w.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// forwardColumns materializes the forward store as typed columns in docID order.
// The middle column is the body blob run for a full-document segment or the
// snippet string for a search-only segment, matching searchSchema.
func (b *SearchBuilder) forwardColumns() []Column {
	n := len(b.docs)
	docID := make([]string, n)
	url := make([]string, n)
	title := make([]string, n)
	nb := make([]uint16, n)
	nt := make([]uint16, n)
	na := make([]uint16, n)
	nu := make([]uint16, n)
	for i, d := range b.docs {
		docID[i] = d.DocID
		url[i] = d.URL
		title[i] = d.Title
		nb[i] = b.norms[i][search.FieldBody]
		nt[i] = b.norms[i][search.FieldTitle]
		na[i] = b.norms[i][search.FieldAnchor]
		nu[i] = b.norms[i][search.FieldURL]
	}
	var mid Column
	if b.snippet {
		snip := make([]string, n)
		copy(snip, b.snippets)
		mid = Column{Data: snip}
	} else {
		body := make([][]byte, n)
		for i, d := range b.docs {
			body[i] = []byte(d.Body)
		}
		mid = Column{Data: body}
	}
	return []Column{
		{Data: docID}, {Data: url}, {Data: title}, mid,
		{Data: nb}, {Data: nt}, {Data: na}, {Data: nu},
	}
}

// makeSnippet builds a result-row excerpt from a document body: it collapses every
// run of whitespace to a single space, trims the ends, and keeps the first
// maxRunes runes, cutting back to the last word boundary so the excerpt does not
// end mid-word. It appends a plain three-dot marker when it truncated. The body is
// already indexed by the time this runs, so the snippet is purely for display.
func makeSnippet(body string, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = DefaultSnippetRunes
	}
	var sb strings.Builder
	space := false
	count := 0
	for _, r := range body {
		if unicode.IsSpace(r) {
			space = true
			continue
		}
		if space && sb.Len() > 0 {
			sb.WriteByte(' ')
			count++
			if count >= maxRunes {
				break
			}
		}
		space = false
		sb.WriteRune(r)
		count++
		if count >= maxRunes {
			break
		}
	}
	s := sb.String()
	if count < maxRunes {
		return s
	}
	// Truncated: cut back to the last word boundary so the tail is a whole word,
	// then mark the cut.
	if i := strings.LastIndexByte(s, ' '); i > 0 {
		s = s[:i]
	}
	return s + "..."
}

// SearchSegment is a read-only view of a search-segment file: the tatami reader
// for the forward store plus the decoded inverted index for retrieval. It is the
// served handle.
//
// It is safe for concurrent queries. The retrieval path (SearchTermsWith) is
// reentrant: the WAND loop allocates its posting cursors per call and the scorer
// is a value type, so two goroutines searching the same segment never share
// mutable state. The stored-field path lazily caches the small display columns
// per row group; those caches are guarded by mu, with the column read done
// outside the lock so concurrent fetches do not serialize on I/O. Delete mutates
// the live bitset in place and is therefore not safe to run while queries are in
// flight; a served segment is treated as immutable (14-serving.md).
type SearchSegment struct {
	r          *Reader
	f          *os.File
	inv        *search.Inverted
	groupFirst []uint64 // firstRow of each row group, for docID -> group mapping
	snippet    bool     // true when column 3 is a snippet string, not a body blob

	mu           sync.RWMutex
	urlCache     map[int][]string
	titleCache   map[int][]string
	snippetCache map[int][]string
	idCache      map[int][]string  // doc_id column per group, for cross-segment dedup
	docIndex     map[string]uint32 // global doc_id -> dense docID, built lazily for deletes
}

// groupStrings returns the string column col for row group g, reading it once and
// caching it. The read happens outside the lock so concurrent first-touches of
// different groups do not serialize, and a lost race on the same group just
// discards the duplicate read. After warmup every call is a read-locked map hit,
// so thousands of concurrent fetches against one segment run without contending.
func (s *SearchSegment) groupStrings(cache map[int][]string, g, col int) ([]string, error) {
	s.mu.RLock()
	v, ok := cache[g]
	s.mu.RUnlock()
	if ok {
		return v, nil
	}
	c, err := s.r.ReadColumn(g, col)
	if err != nil {
		return nil, err
	}
	data := c.Data.([]string)
	s.mu.Lock()
	if existing, ok := cache[g]; ok {
		data = existing
	} else {
		cache[g] = data
	}
	s.mu.Unlock()
	return data, nil
}

// SearchResult is one ranked hit: the dense docID, the stable global doc_id, the
// stored url, title, and snippet, and the relevance score. Snippet is empty on a
// full-document segment, which stores the body rather than a precomputed excerpt.
// DocID is the durable identity an aggregator dedups and tie-breaks on so a
// fleet-wide merge orders documents exactly as a single broker would.
type SearchResult struct {
	Doc     uint32
	DocID   string
	URL     string
	Title   string
	Snippet string
	Score   float32
}

// OpenSearch opens a search-segment file and decodes its inverted sub-region. It
// errors if the file does not carry the search-segment role.
func OpenSearch(path string) (*SearchSegment, error) {
	r, f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}
	if r.header.Flags&FlagRoleSearchSeg == 0 {
		_ = f.Close()
		return nil, fmt.Errorf("tatami: %s is not a search segment", path)
	}
	d := r.meta.invert
	if !d.present {
		_ = f.Close()
		return nil, fmt.Errorf("tatami: %s has the search role but no inverted descriptor", path)
	}
	td, err := r.readIndexRecord(d.termDictOff)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	pp, err := r.readIndexRecord(d.postingsOff)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sk, err := r.readIndexRecord(d.skipsOff)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	blockTree := r.header.Flags&FlagBlockTreeDict != 0
	inv, err := search.DecodeInverted(td, pp, sk, int(d.numDocs), blockTree)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if d.liveLen > 0 {
		lv, err := r.readIndexRecord(d.liveOff)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		inv.SetLive(search.DecodeLive(lv))
	}
	first := make([]uint64, r.NumRowGroups())
	for g := range first {
		first[g] = r.meta.groups[g].firstRow
	}
	snippet := false
	if sc := r.Schema(); sc != nil && len(sc.Fields) > colVariable {
		snippet = sc.Fields[colVariable].Type == TypeString
	}
	return &SearchSegment{
		r:            r,
		f:            f,
		inv:          inv,
		groupFirst:   first,
		snippet:      snippet,
		urlCache:     map[int][]string{},
		titleCache:   map[int][]string{},
		snippetCache: map[int][]string{},
		idCache:      map[int][]string{},
	}, nil
}

// Close releases the underlying file.
func (s *SearchSegment) Close() error { return s.f.Close() }

// NumDocs returns the dense doc-id space size, including deleted documents. It
// is the N that IDF is defined over until a merge removes the deletions.
func (s *SearchSegment) NumDocs() int { return s.inv.NumDocs() }

// LiveDocs returns the number of documents not yet deleted, the size class the
// merge policy tiers on.
func (s *SearchSegment) LiveDocs() int { return s.inv.LiveDocs() }

// NumDeleted returns the number of deleted documents.
func (s *SearchSegment) NumDeleted() int { return s.inv.NumDeleted() }

// NumTerms returns the distinct term count.
func (s *SearchSegment) NumTerms() int { return s.inv.NumTerms() }

// SnippetOnly reports whether this is a search-only segment: one that stores a
// snippet for display and not the document body.
func (s *SearchSegment) SnippetOnly() bool { return s.snippet }

// DeleteDense marks a dense doc id deleted in the in-memory live bitset. The
// deletion is honored by every later query on this open segment and is
// materialized (the document dropped) at the next merge. It is not written back
// to the immutable file; durable deletes across a reopen are a tombstone-sidecar
// refinement (09-search-scale.md, section 7). It returns true when the call
// changed the state.
func (s *SearchSegment) DeleteDense(d uint32) bool {
	return s.inv.Delete(search.DocID(d))
}

// Delete marks the document with the given global doc_id deleted, resolving it to
// its dense id through a lazily built index over the doc_id column. It returns
// false when the doc_id is not in this segment.
func (s *SearchSegment) Delete(docID string) (bool, error) {
	if s.docIndex == nil {
		if err := s.buildDocIndex(); err != nil {
			return false, err
		}
	}
	dense, ok := s.docIndex[docID]
	if !ok {
		return false, nil
	}
	return s.DeleteDense(dense), nil
}

// buildDocIndex reads the doc_id column across every row group and maps each
// global doc_id to its dense id, so a delete keyed by the stable identity can
// find the ephemeral one.
func (s *SearchSegment) buildDocIndex() error {
	idx := make(map[string]uint32, s.inv.NumDocs())
	for g := 0; g < s.r.NumRowGroups(); g++ {
		col, err := s.r.ReadColumn(g, 0) // doc_id
		if err != nil {
			return err
		}
		ids := col.Data.([]string)
		base := uint32(s.groupFirst[g])
		for i, id := range ids {
			idx[id] = base + uint32(i)
		}
	}
	s.docIndex = idx
	return nil
}

// Inverted exposes the decoded inverted index for retrieval-only callers (the
// latency benchmark measures this path without the stored-field fetch).
func (s *SearchSegment) Inverted() *search.Inverted { return s.inv }

// Query tokenizes a query string and returns the top-k document ids and scores
// from the block-max WAND loop, without fetching stored fields. This is the hot
// retrieval path the <10ms target is measured against.
func (s *SearchSegment) Query(query string, k int) []search.Hit {
	return s.inv.Search(tokenize(query), k)
}

// SearchTermsWith runs the block-max WAND loop over already-tokenized query terms
// with corpus-wide statistics, so a broker serving many shards scores every shard
// against the same global IDF and the partial top-k lists it merges are on one
// scale. Passing nil stats falls back to this shard's local IDF, which is the
// single-segment path. This is the entry the Cluster broker drives
// (12-distributed-serving.md).
func (s *SearchSegment) SearchTermsWith(terms []string, k int, stats search.GlobalStats) []search.Hit {
	return s.inv.SearchWith(terms, k, stats)
}

// SearchTermsSeeded is SearchTermsWith carrying a cross-shard score floor: the
// broker passes its running k-th best score so this shard prunes documents that
// cannot enter the global top-k before it scores them. A seed of 0 is identical to
// SearchTermsWith. The exactness of the merged top-k is preserved because a valid
// seed is a lower bound on the final global k-th, so no document the answer needs
// is ever pruned (scale/07, M5).
func (s *SearchSegment) SearchTermsSeeded(terms []string, k int, stats search.GlobalStats, seed search.Score) []search.Hit {
	return s.inv.SearchSeeded(terms, k, stats, seed)
}

// Search runs the top-k retrieval and then fetches the url and title of each
// surviving document from the forward columns, the full query-to-results path.
func (s *SearchSegment) Search(query string, k int) ([]SearchResult, error) {
	hits := s.Query(query, k)
	out := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		f, err := s.storedFields(uint32(h.Doc))
		if err != nil {
			return nil, err
		}
		id, err := s.globalDocID(uint32(h.Doc))
		if err != nil {
			return nil, err
		}
		out = append(out, SearchResult{Doc: uint32(h.Doc), DocID: id, URL: f.url, Title: f.title, Snippet: f.snippet, Score: float32(h.Score)})
	}
	return out, nil
}

// stored holds the forward fields of one hit: the display columns a result row
// needs. snippet is empty on a full-document segment.
type stored struct {
	url, title, snippet string
}

// storedFields returns the display fields of a dense docID. It maps the id to its
// row group, reads the small url/title columns (and the snippet column on a
// search-only segment) once per group, and caches them, so a top-k fetch over one
// group costs a fixed number of column reads regardless of k. A full-document
// segment leaves the snippet empty; its body is blob-separated and fetched only
// when a caller asks for it directly.
func (s *SearchSegment) storedFields(docID uint32) (stored, error) {
	g := s.groupOf(docID)
	if g < 0 {
		return stored{}, fmt.Errorf("tatami: docID %d out of range", docID)
	}
	off := int(uint64(docID) - s.groupFirst[g])
	urls, err := s.groupStrings(s.urlCache, g, 1) // url
	if err != nil {
		return stored{}, err
	}
	titles, err := s.groupStrings(s.titleCache, g, 2) // title
	if err != nil {
		return stored{}, err
	}
	if off < 0 || off >= len(urls) {
		return stored{}, fmt.Errorf("tatami: docID %d offset out of range in group %d", docID, g)
	}
	out := stored{url: urls[off], title: titles[off]}
	if s.snippet {
		snips, err := s.groupStrings(s.snippetCache, g, colVariable) // snippet
		if err != nil {
			return stored{}, err
		}
		if off < len(snips) {
			out.snippet = snips[off]
		}
	}
	return out, nil
}

// globalDocID returns the stable doc_id of a dense docID, reading the doc_id
// column once per group and caching it. The serving layer uses it to dedup the
// same page surfaced by more than one segment after a recrawl.
func (s *SearchSegment) globalDocID(docID uint32) (string, error) {
	g := s.groupOf(docID)
	if g < 0 {
		return "", fmt.Errorf("tatami: docID %d out of range", docID)
	}
	off := int(uint64(docID) - s.groupFirst[g])
	ids, err := s.groupStrings(s.idCache, g, 0) // doc_id
	if err != nil {
		return "", err
	}
	if off < 0 || off >= len(ids) {
		return "", fmt.Errorf("tatami: docID %d offset out of range in group %d", docID, g)
	}
	return ids[off], nil
}

// groupOf returns the row group holding a dense docID, or -1 when out of range.
func (s *SearchSegment) groupOf(docID uint32) int {
	target := uint64(docID)
	g := sort.Search(len(s.groupFirst), func(i int) bool { return s.groupFirst[i] > target })
	if g == 0 {
		return -1
	}
	return g - 1
}

// tokenize lowercases text and splits it into maximal runs of letters and
// digits. It is the indexer's analyzer and the query analyzer, kept identical so
// a query token matches an indexed token.
func tokenize(text string) []string {
	var toks []string
	var b strings.Builder
	flush := func() {
		if b.Len() > 0 {
			toks = append(toks, b.String())
			b.Reset()
		}
	}
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
		} else {
			flush()
		}
	}
	flush()
	return toks
}

// capU16 clamps a count to the uint16 range so a very long field still fits the
// norm column.
func capU16(n int) uint16 {
	if n > 0xffff {
		return 0xffff
	}
	return uint16(n)
}
