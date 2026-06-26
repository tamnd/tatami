// Package search is tatami's search-segment engine: the posting-list codec, the
// term dictionary, the BM25F scorer, and the block-max WAND retrieval loop that
// turn a tatami file with the role bit set into a full-text search segment
// (Spec 2066, 09-search-scale.md).
//
// The codec stack is lifted, mostly verbatim, from tamnd/openindex, which
// already shipped a working search engine with this exact posting-list codec, an
// FST-backed term dictionary seam, a tiered merge policy, and a block-max WAND
// loop. Re-expressing that model inside tatami's container means openindex's
// index packages become tatami's inverted sub-region: one codec, one dictionary,
// one scorer, one retrieval loop, shared between build, merge, and query.
//
// This package is self-contained: it imports nothing else in the module, so the
// format library and the container writer can both depend on it without a cycle.
// The domain primitives below are the shared vocabulary, kept in one place so no
// part of the engine invents its own.
package search

import "fmt"

// DocID is a per-segment sequential document identifier. It is dense within a
// segment so it can index directly into the forward columns, and it is
// reassigned on merge. A DocID is meaningless outside the segment that minted
// it; the durable external identity of a page is its doc_id = sha256(url), which
// the document-store role keeps as a column (09-search-scale.md, section 2).
type DocID uint32

// Score is a relevance score. Scores are only ever compared within a single
// result set; their absolute magnitude is not portable across scorers or
// segments.
type Score float32

// Posting is one entry in a term's posting list: the document the term occurs in
// and how many times. Frequencies live in a separate stream from the doc-id
// gaps, so a document-frequency scan never pays to decode them.
type Posting struct {
	Doc       DocID
	Frequency uint32
}

// Field names a logical text field of a document for BM25F weighted scoring. The
// set is fixed at index time so a field maps to a stable small integer in the
// segment.
type Field uint8

const (
	FieldBody Field = iota
	FieldTitle
	FieldAnchor // inlink anchor text aggregated from the link graph
	FieldURL
	numFields
)

// String returns the canonical lowercase field name used in config and logs.
func (f Field) String() string {
	switch f {
	case FieldBody:
		return "body"
	case FieldTitle:
		return "title"
	case FieldAnchor:
		return "anchor"
	case FieldURL:
		return "url"
	default:
		return fmt.Sprintf("field(%d)", uint8(f))
	}
}

// NumFields is the count of scorable fields, sized for the fixed-length per-field
// norms array in the forward store and the BM25F field-weight vector.
const NumFields = int(numFields)
