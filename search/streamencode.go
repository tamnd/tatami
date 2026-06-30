package search

// streamencode.go is the writer-side counterpart to EncodeInverted for the
// streaming, bounded-RSS build (Spec 2066, scale/06, M3). EncodeInverted walks a
// fully built *Inverted and serializes its three runs in one shot, which means
// the whole inverted index must be resident first. StreamEncoder instead accepts
// terms one at a time, in ascending term order, each with its complete posting
// list, and appends to the three runs incrementally. The external-merge build
// feeds it the merged term stream, so the index is never held whole in the raw
// map form the in-memory builder uses.
//
// The byte layout it produces is identical to EncodeInverted's, term for term,
// because both assign each non-singleton term a dense posting index in ascending
// term order and write the payloads in that same order. That identity is what the
// M3 differential test asserts: a streamed segment and an in-memory segment over
// the same documents are byte-for-byte equal.

import "encoding/binary"

// StreamEncoder accumulates the three inverted runs (term dictionary, posting
// payloads, skip tables) from an ascending stream of terms. It mirrors
// EncodeInverted exactly: singletons inline into the dictionary entry, every
// other term gets a posting payload and a skip record at the next dense index.
//
// The resident cost is the encoded output, not the raw posting map: the posting
// payloads are bit-packed (~2 bytes per posting) where the in-memory builder's
// map holds ~16 bytes per posting with slice and map overhead, so the merge phase
// holds the index in its compact on-disk form, never the map.
type StreamEncoder struct {
	terms  []Term // entries only (no posting bytes), block-tree'd at Finish
	ppBody []byte // concatenated len-prefixed posting payloads, non-singletons
	skBody []byte // concatenated skip records, non-singletons
	nLists uint64 // number of non-singleton posting lists appended so far
}

// NewStreamEncoder returns an empty encoder.
func NewStreamEncoder() *StreamEncoder { return &StreamEncoder{} }

// Add appends one term and its complete posting list. The terms must arrive in
// ascending Unicode order and each list's postings in ascending docid order,
// which is what the external merge yields. A one-posting term is inlined as a
// singleton, matching InvertedBuilder.Build (index.go); every other term is
// encoded with the same Encode path the in-memory builder uses, so the payload
// bytes are identical.
func (e *StreamEncoder) Add(term string, ps []Posting) error {
	if len(ps) == 1 {
		e.terms = append(e.terms, Term{Term: term, Entry: Entry{
			DocFreq:       1,
			Singleton:     true,
			SingletonDoc:  ps[0].Doc,
			SingletonFreq: ps[0].Frequency,
		}})
		return nil
	}
	l, err := Encode(ps)
	if err != nil {
		return err
	}
	idx := e.nLists
	e.nLists++
	e.terms = append(e.terms, Term{Term: term, Entry: Entry{
		DocFreq:       len(ps),
		PostingOffset: int64(idx),
	}})

	// Posting payload run, mirroring EncodeInverted's per-list framing.
	e.ppBody = binary.AppendUvarint(e.ppBody, uint64(len(l.data)))
	e.ppBody = append(e.ppBody, l.data...)

	// Skip-table run, mirroring EncodeInverted's per-block framing.
	e.skBody = binary.AppendUvarint(e.skBody, uint64(l.numDoc))
	e.skBody = binary.AppendUvarint(e.skBody, uint64(len(l.blocks)))
	for _, b := range l.blocks {
		e.skBody = binary.AppendUvarint(e.skBody, uint64(b.firstDoc))
		e.skBody = binary.AppendUvarint(e.skBody, uint64(b.lastDoc))
		e.skBody = binary.AppendUvarint(e.skBody, uint64(b.maxFreq))
		e.skBody = binary.AppendUvarint(e.skBody, uint64(b.offset))
		e.skBody = binary.AppendUvarint(e.skBody, uint64(b.count))
	}
	return nil
}

// NumTerms reports how many terms have been added.
func (e *StreamEncoder) NumTerms() int { return len(e.terms) }

// Finish serializes the accumulated runs. The term dictionary is the block tree
// over every entry (the same form EncodeInverted now writes, scale/03 M0); the
// posting and skip runs get their list-count prefix prepended, so the bytes equal
// what EncodeInverted produces for the same terms in the same order.
func (e *StreamEncoder) Finish() (termDict, postings, skips []byte, err error) {
	td, err := BuildBlockTree(e.terms, DefaultBlockTreeBlockSize)
	if err != nil {
		return nil, nil, nil, err
	}
	pp := binary.AppendUvarint(nil, e.nLists)
	pp = append(pp, e.ppBody...)
	sk := binary.AppendUvarint(nil, e.nLists)
	sk = append(sk, e.skBody...)
	return td, pp, sk, nil
}
