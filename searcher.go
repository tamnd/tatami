package tatami

// One segment is one file; web scale is many segments. An Index fans a query out
// to every segment it holds, runs the per-segment block-max WAND loop, and merges
// the partial top-k lists into one global top-k, deduplicating a page that more
// than one segment carries after a recrawl by its stable doc_id
// (09-search-scale.md, section 6). This is the leaf-node serving model: a leaf
// loads the files its manifest names and serves them as one index.
//
// The fan-out here is sequential across segments because each segment's loop is
// already microseconds on a real shard; a leaf with a deadline budget can run the
// segments concurrently and load-shed the slowest, which the spec's aggregator
// tier does. The dedup and global merge are the parts that must be correct.

import (
	"sort"

	"github.com/tamnd/tatami/search"
)

// Index is a searchable set of open search segments served as one logical index.
// It owns the segments it opens through OpenIndex and closes them together.
type Index struct {
	segs  []*SearchSegment
	owned bool // true when OpenIndex opened the files, so Close closes them
}

// NewIndex wraps already-open segments into one index. The caller keeps ownership
// of the segments; Close does not close them.
func NewIndex(segs []*SearchSegment) *Index {
	return &Index{segs: segs}
}

// OpenIndex opens every segment file in paths and serves them as one index. Close
// closes all of them. A failure to open any file closes the ones already opened
// and returns the error.
func OpenIndex(paths []string) (*Index, error) {
	segs := make([]*SearchSegment, 0, len(paths))
	for _, p := range paths {
		seg, err := OpenSearch(p)
		if err != nil {
			for _, s := range segs {
				_ = s.Close()
			}
			return nil, err
		}
		segs = append(segs, seg)
	}
	return &Index{segs: segs, owned: true}, nil
}

// Close closes the segments if this index opened them.
func (ix *Index) Close() error {
	if !ix.owned {
		return nil
	}
	var first error
	for _, s := range ix.segs {
		if err := s.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Segments returns the underlying segments, for stats and merge selection.
func (ix *Index) Segments() []*SearchSegment { return ix.segs }

// NumDocs sums the live documents across every segment.
func (ix *Index) NumDocs() int {
	var n int
	for _, s := range ix.segs {
		n += s.LiveDocs()
	}
	return n
}

// scored is one hit on its way through the global merge: its score, the segment
// that produced it, and its dense id within that segment.
type scored struct {
	score   float32
	seg     int
	dense   uint32
	globalI string
}

// Search runs the query against every segment, merges the partial results into a
// global top-k, and dedups by stable doc_id, keeping the highest-scoring copy of
// a page that appears in more than one segment. It fetches the url and title of
// each surviving hit from the segment that produced it. To keep dedup correct it
// pulls more than k candidates per segment when there are several segments, since
// duplicates collapse and could otherwise leave fewer than k distinct results.
func (ix *Index) Search(query string, k int) ([]SearchResult, error) {
	if k <= 0 {
		return nil, nil
	}
	// Over-fetch per segment so post-dedup we still have k distinct pages.
	perSeg := k
	if len(ix.segs) > 1 {
		perSeg = k * 2
	}

	var cands []scored
	for si, seg := range ix.segs {
		hits := seg.Query(query, perSeg)
		for _, h := range hits {
			id, err := seg.globalDocID(uint32(h.Doc))
			if err != nil {
				return nil, err
			}
			cands = append(cands, scored{
				score:   float32(h.Score),
				seg:     si,
				dense:   uint32(h.Doc),
				globalI: id,
			})
		}
	}

	// Global rank by (score desc, doc_id asc) so the order is deterministic.
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].score != cands[j].score {
			return cands[i].score > cands[j].score
		}
		return cands[i].globalI < cands[j].globalI
	})

	out := make([]SearchResult, 0, k)
	seen := make(map[string]struct{}, k)
	for _, c := range cands {
		if _, dup := seen[c.globalI]; dup {
			continue
		}
		seen[c.globalI] = struct{}{}
		f, err := ix.segs[c.seg].storedFields(c.dense)
		if err != nil {
			return nil, err
		}
		out = append(out, SearchResult{Doc: c.dense, DocID: c.globalI, URL: f.url, Title: f.title, Snippet: f.snippet, Score: c.score})
		if len(out) == k {
			break
		}
	}
	return out, nil
}

// Query is the retrieval-only path across all segments: the global top-k of dense
// ids and scores without the stored-field fetch, tagged with the segment each hit
// came from. It is what the latency benchmark times.
func (ix *Index) Query(query string, k int) []IndexHit {
	if k <= 0 {
		return nil
	}
	perSeg := k
	if len(ix.segs) > 1 {
		perSeg = k * 2
	}
	var cands []IndexHit
	for si, seg := range ix.segs {
		for _, h := range seg.Query(query, perSeg) {
			cands = append(cands, IndexHit{Segment: si, Doc: uint32(h.Doc), Score: float32(h.Score)})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Score != cands[j].Score {
			return cands[i].Score > cands[j].Score
		}
		if cands[i].Segment != cands[j].Segment {
			return cands[i].Segment < cands[j].Segment
		}
		return cands[i].Doc < cands[j].Doc
	})
	if len(cands) > k {
		cands = cands[:k]
	}
	return cands
}

// IndexHit is a scored hit tagged with the segment that produced it.
type IndexHit struct {
	Segment int
	Doc     uint32
	Score   float32
}

// SelectMerge applies the tiered merge policy to the index's segments and returns
// the segment indices the policy chose to merge, or nil when none qualify.
func (ix *Index) SelectMerge(p search.MergePolicy) []int {
	stats := make([]search.SegmentStat, len(ix.segs))
	for i, s := range ix.segs {
		stats[i] = s
	}
	return p.Select(stats)
}
