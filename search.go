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
	"unicode"

	"github.com/tamnd/tatami/search"
)

// Search-segment forward-store column names. The order here is the schema order,
// so a column's index is stable.
const (
	colDocID     = "doc_id"
	colURL       = "url"
	colTitle     = "title"
	colBody      = "body"
	colNormBody  = "norm_body"
	colNormTitle = "norm_title"
	colNormAnchr = "norm_anchor"
	colNormURL   = "norm_url"
)

// searchSchema is the fixed forward-store schema of a search segment. doc_id is
// the durable global identity (sha256(url)) and carries a bloom filter for
// by-id lookup; url and title are short strings; body is blob-separated; the four
// norm columns are the per-field lengths BM25F reads.
func searchSchema() *Schema {
	return &Schema{Fields: []Field{
		{Name: colDocID, Type: TypeString, BloomFilter: true},
		{Name: colURL, Type: TypeString},
		{Name: colTitle, Type: TypeString},
		{Name: colBody, Type: TypeBlobRef, BlobSeparated: true},
		{Name: colNormBody, Type: TypeUint16},
		{Name: colNormTitle, Type: TypeUint16},
		{Name: colNormAnchr, Type: TypeUint16},
		{Name: colNormURL, Type: TypeUint16},
	}}
}

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
type SearchBuilder struct {
	docs []SearchDoc
	inv  *search.InvertedBuilder
	// norms[docID] is the per-field token count, capped to uint16.
	norms [][search.NumFields]uint16
}

// NewSearchBuilder returns an empty builder.
func NewSearchBuilder() *SearchBuilder {
	return &SearchBuilder{inv: search.NewInvertedBuilder()}
}

// Add tokenizes a document, records its per-field term frequencies and length
// norms, and assigns it the next dense docID. A term's posting frequency is its
// total count across all fields; the per-field counts survive as the length norms
// that a BM25F re-rank reads (09-search-scale.md, section 5).
func (b *SearchBuilder) Add(doc SearchDoc) {
	var norm [search.NumFields]uint16
	freqs := map[string]uint32{}
	index := func(text string, f search.Field) {
		n := 0
		for _, tok := range tokenize(text) {
			freqs[tok]++
			n++
		}
		norm[f] = capU16(n)
	}
	index(doc.Body, search.FieldBody)
	index(doc.Title, search.FieldTitle)
	index(doc.Anchor, search.FieldAnchor)
	index(doc.URL, search.FieldURL)

	b.inv.AddDocument(freqs)
	b.norms = append(b.norms, norm)
	b.docs = append(b.docs, doc)
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
	schema := searchSchema()
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
	w.AttachInverted(td, pp, sk, uint64(inv.NumTerms()), uint64(inv.NumDocs()))
	if err := w.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// forwardColumns materializes the forward store as typed columns in docID order.
func (b *SearchBuilder) forwardColumns() []Column {
	n := len(b.docs)
	docID := make([]string, n)
	url := make([]string, n)
	title := make([]string, n)
	body := make([][]byte, n)
	nb := make([]uint16, n)
	nt := make([]uint16, n)
	na := make([]uint16, n)
	nu := make([]uint16, n)
	for i, d := range b.docs {
		docID[i] = d.DocID
		url[i] = d.URL
		title[i] = d.Title
		body[i] = []byte(d.Body)
		nb[i] = b.norms[i][search.FieldBody]
		nt[i] = b.norms[i][search.FieldTitle]
		na[i] = b.norms[i][search.FieldAnchor]
		nu[i] = b.norms[i][search.FieldURL]
	}
	return []Column{
		{Data: docID}, {Data: url}, {Data: title}, {Data: body},
		{Data: nb}, {Data: nt}, {Data: na}, {Data: nu},
	}
}

// SearchSegment is a read-only view of a search-segment file: the tatami reader
// for the forward store plus the decoded inverted index for retrieval. It is the
// served handle.
type SearchSegment struct {
	r          *Reader
	f          *os.File
	inv        *search.Inverted
	groupFirst []uint64 // firstRow of each row group, for docID -> group mapping
	urlCache   map[int][]string
	titleCache map[int][]string
}

// SearchResult is one ranked hit: the dense docID, its stored url and title, and
// the relevance score.
type SearchResult struct {
	Doc   uint32
	URL   string
	Title string
	Score float32
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
	inv, err := search.DecodeInverted(td, pp, sk, int(d.numDocs))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	first := make([]uint64, r.NumRowGroups())
	for g := range first {
		first[g] = r.meta.groups[g].firstRow
	}
	return &SearchSegment{
		r:          r,
		f:          f,
		inv:        inv,
		groupFirst: first,
		urlCache:   map[int][]string{},
		titleCache: map[int][]string{},
	}, nil
}

// Close releases the underlying file.
func (s *SearchSegment) Close() error { return s.f.Close() }

// NumDocs returns the dense doc-id space size.
func (s *SearchSegment) NumDocs() int { return s.inv.NumDocs() }

// NumTerms returns the distinct term count.
func (s *SearchSegment) NumTerms() int { return s.inv.NumTerms() }

// Inverted exposes the decoded inverted index for retrieval-only callers (the
// latency benchmark measures this path without the stored-field fetch).
func (s *SearchSegment) Inverted() *search.Inverted { return s.inv }

// Query tokenizes a query string and returns the top-k document ids and scores
// from the block-max WAND loop, without fetching stored fields. This is the hot
// retrieval path the <10ms target is measured against.
func (s *SearchSegment) Query(query string, k int) []search.Hit {
	return s.inv.Search(tokenize(query), k)
}

// Search runs the top-k retrieval and then fetches the url and title of each
// surviving document from the forward columns, the full query-to-results path.
func (s *SearchSegment) Search(query string, k int) ([]SearchResult, error) {
	hits := s.Query(query, k)
	out := make([]SearchResult, 0, len(hits))
	for _, h := range hits {
		url, title, err := s.storedFields(uint32(h.Doc))
		if err != nil {
			return nil, err
		}
		out = append(out, SearchResult{Doc: uint32(h.Doc), URL: url, Title: title, Score: float32(h.Score)})
	}
	return out, nil
}

// storedFields returns the url and title of a dense docID. It maps the id to its
// row group, reads the small url/title columns once per group, and caches them,
// so a top-k fetch over one group costs two column reads regardless of k. Bodies
// (blob-separated, large) are fetched only when a caller asks; snippet
// generation is a later milestone.
func (s *SearchSegment) storedFields(docID uint32) (url, title string, err error) {
	g := s.groupOf(docID)
	if g < 0 {
		return "", "", fmt.Errorf("tatami: docID %d out of range", docID)
	}
	off := int(uint64(docID) - s.groupFirst[g])
	urls, ok := s.urlCache[g]
	if !ok {
		col, err := s.r.ReadColumn(g, 1) // url
		if err != nil {
			return "", "", err
		}
		urls = col.Data.([]string)
		s.urlCache[g] = urls
	}
	titles, ok := s.titleCache[g]
	if !ok {
		col, err := s.r.ReadColumn(g, 2) // title
		if err != nil {
			return "", "", err
		}
		titles = col.Data.([]string)
		s.titleCache[g] = titles
	}
	if off < 0 || off >= len(urls) {
		return "", "", fmt.Errorf("tatami: docID %d offset out of range in group %d", docID, g)
	}
	return urls[off], titles[off], nil
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
