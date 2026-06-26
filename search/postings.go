package search

import "errors"

// block is the per-block skip and impact metadata kept uncompressed so the
// block-max WAND loop can read a block's bound without decoding it. firstDoc and
// lastDoc drive skipping; maxFreq is the impact bound; offset locates the
// payload; count is 128 except for the tail (09-search-scale.md, section 4).
type block struct {
	firstDoc DocID  // absolute id of the block's first doc, for skip
	lastDoc  DocID  // absolute id of the block's last doc (skip target)
	maxFreq  uint32 // largest frequency in the block, the impact bound
	offset   int    // byte offset of the block's payload in data
	count    int    // docs in the block (BlockSize, or fewer for the tail)
}

// List is an encoded posting list: the compressed block payloads plus the
// uncompressed per-block skip table. It is immutable and safe for concurrent
// readers, each of which takes its own Cursor.
type List struct {
	data   []byte
	blocks []block
	numDoc int
}

// NumDocs returns the document frequency of the term this list belongs to.
func (l *List) NumDocs() int { return l.numDoc }

// MaxFreq returns the largest frequency anywhere in the list, the input to a
// term's global score upper bound in the WAND retrieval loop.
func (l *List) MaxFreq() uint32 {
	var m uint32
	for _, b := range l.blocks {
		if b.maxFreq > m {
			m = b.maxFreq
		}
	}
	return m
}

// Encode builds a List from postings that MUST be sorted by ascending DocID.
// Doc ids are delta-encoded and Frame-of-Reference bit-packed in full 128-doc
// blocks; the final partial block uses group-varint; frequencies use PForDelta.
// It returns an error on unsorted or duplicate input, since a posting list with
// out-of-order docs would silently corrupt intersection.
func Encode(ps []Posting) (*List, error) {
	for i := 1; i < len(ps); i++ {
		if ps[i].Doc <= ps[i-1].Doc {
			return nil, errors.New("search: postings not strictly ascending by DocID")
		}
	}
	l := &List{numDoc: len(ps)}
	for start := 0; start < len(ps); start += BlockSize {
		end := min(start+BlockSize, len(ps))
		l.encodeBlock(ps[start:end])
	}
	return l, nil
}

func (l *List) encodeBlock(ps []Posting) {
	b := block{
		firstDoc: ps[0].Doc,
		lastDoc:  ps[len(ps)-1].Doc,
		count:    len(ps),
		offset:   len(l.data),
	}
	gaps := make([]uint32, len(ps)-1)
	freqs := make([]uint32, len(ps))
	for i, p := range ps {
		freqs[i] = p.Frequency
		if p.Frequency > b.maxFreq {
			b.maxFreq = p.Frequency
		}
		if i > 0 {
			gaps[i-1] = uint32(p.Doc - ps[i-1].Doc)
		}
	}

	// Per-block header travels in the skip table (firstDoc/lastDoc/maxFreq), so
	// the payload is just the gap and frequency streams.
	if b.count == BlockSize {
		w := maxWidth(gaps)
		l.data = append(l.data, byte(w))
		l.data = packBits(l.data, gaps, w)
	} else {
		// Tail block: group-varint the gaps rather than pay fixed-width padding.
		l.data = appendGroupVarint(l.data, gaps)
	}
	l.data = appendPForDelta(l.data, freqs)
	l.blocks = append(l.blocks, b)
}

// Cursor walks a List one document at a time and supports block-skipping advance
// for intersection and block-max WAND. It is not safe for concurrent use; each
// goroutine takes its own via List.Cursor.
type Cursor struct {
	l        *List
	blockIdx int
	inBlock  int // index within the decoded block
	docs     [BlockSize]DocID
	freqs    [BlockSize]uint32
	decoded  bool
	// shallowIdx is the block pointer used by AdvanceShallow/BlockMax* only. It is
	// kept separate from blockIdx so reading a block's bound never disturbs the
	// decode/iteration position (the Lucene shallow-vs-deep advance split).
	shallowIdx int
}

// Cursor returns a fresh cursor positioned before the first document. Call Next
// to advance to the first posting.
func (l *List) Cursor() *Cursor {
	return &Cursor{l: l, blockIdx: -1}
}

// Done reports whether the cursor has advanced past the last document.
func (c *Cursor) Done() bool { return c.blockIdx >= len(c.l.blocks) }

// Doc returns the current document id; valid only when !Done and after Next.
func (c *Cursor) Doc() DocID { return c.docs[c.inBlock] }

// Freq returns the current document's term frequency.
func (c *Cursor) Freq() uint32 { return c.freqs[c.inBlock] }

// Next advances to the next document and reports whether one exists.
func (c *Cursor) Next() bool {
	if c.blockIdx < 0 {
		c.blockIdx = 0
		if len(c.l.blocks) == 0 {
			c.blockIdx = 0
			return false
		}
		c.decodeBlock()
		c.inBlock = 0
		return true
	}
	c.inBlock++
	if c.inBlock >= c.l.blocks[c.blockIdx].count {
		c.blockIdx++
		if c.Done() {
			return false
		}
		c.decodeBlock()
		c.inBlock = 0
	}
	return true
}

// NextGEQ advances to the first document with id >= target and returns it, or
// reports Done if none. It uses the skip table to jump whole blocks whose
// lastDoc is below target without decoding them - the advance(target) fast path.
func (c *Cursor) NextGEQ(target DocID) (DocID, bool) {
	if c.Done() {
		return 0, false
	}
	if c.blockIdx < 0 {
		c.blockIdx = 0
	}
	// Skip whole blocks whose last doc is still below the target.
	for c.blockIdx < len(c.l.blocks) && c.l.blocks[c.blockIdx].lastDoc < target {
		c.blockIdx++
		c.decoded = false
	}
	if c.Done() {
		return 0, false
	}
	if !c.decoded {
		c.decodeBlock()
		c.inBlock = 0
	}
	for c.inBlock < c.l.blocks[c.blockIdx].count {
		if c.docs[c.inBlock] >= target {
			return c.docs[c.inBlock], true
		}
		c.inBlock++
	}
	// Target is past this block; recurse into the next block.
	c.blockIdx++
	c.decoded = false
	return c.NextGEQ(target)
}

// BlockMaxFreq returns the maximum frequency in the shallow-positioned block,
// the input to the block-max WAND impact bound. AdvanceShallow positions the
// block; this reads its bound without decoding the block payload.
func (c *Cursor) BlockMaxFreq() uint32 { return c.l.blocks[c.shallowIdx].maxFreq }

// BlockLastDoc returns the last document id of the shallow-positioned block, the
// right edge the WAND loop compares its pivot against.
func (c *Cursor) BlockLastDoc() DocID { return c.l.blocks[c.shallowIdx].lastDoc }

// AdvanceShallow moves the shallow block pointer to the block that could contain
// target, without decoding it and without disturbing the iteration position, so
// the caller can read BlockMaxFreq/BlockLastDoc cheaply before deciding whether
// the block is worth a full decode.
func (c *Cursor) AdvanceShallow(target DocID) {
	for c.shallowIdx < len(c.l.blocks)-1 && c.l.blocks[c.shallowIdx].lastDoc < target {
		c.shallowIdx++
	}
}

func (c *Cursor) decodeBlock() {
	b := c.l.blocks[c.blockIdx]
	src := c.l.data[b.offset:]
	c.docs[0] = b.firstDoc
	gaps := make([]uint32, b.count-1)
	var pos int
	if b.count == BlockSize {
		w := int(src[0])
		pos = 1
		pos += unpackBits(src[1:], gaps, b.count-1, w)
	} else {
		pos = readGroupVarint(src, gaps, b.count-1)
	}
	for i := 1; i < b.count; i++ {
		c.docs[i] = c.docs[i-1] + DocID(gaps[i-1])
	}
	readPForDelta(src[pos:], c.freqs[:b.count])
	c.decoded = true
}
