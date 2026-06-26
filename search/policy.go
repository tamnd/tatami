package search

import "sort"

// MergePolicy decides which segments to fold together. It is the tiered policy
// from openindex, itself Lucene's tuned defaults: small segments are merged into
// a logarithmic staircase, tiny segments are floored so they do not form a long
// tail, and a maximum merged size caps the cost of any single merge. A segment
// whose deleted fraction crosses a threshold is rewritten on its own to reclaim
// the space. Merging is I/O-intensive and must be rate-limited by the caller so
// it never starves serving; this type only chooses the candidates
// (09-search-scale.md, section 7).
type MergePolicy struct {
	// SegmentsPerTier is how many segments of roughly equal size accumulate
	// before they are eligible to merge.
	SegmentsPerTier int
	// MaxMergeAtOnce caps how many segments one merge consumes.
	MaxMergeAtOnce int
	// FloorSegmentDocs rounds tiny segments up to one size class so a long tail of
	// single-doc segments does not defeat tiering.
	FloorSegmentDocs int
	// MaxMergedDocs caps the document count of a produced segment.
	MaxMergedDocs int
	// DeletesPctAllowed triggers a rewrite of a segment whose deleted fraction
	// exceeds this percentage even if tiering would not.
	DeletesPctAllowed int
}

// DefaultMergePolicy returns the pinned tiered defaults from openindex.
func DefaultMergePolicy() MergePolicy {
	return MergePolicy{
		SegmentsPerTier:   10,
		MaxMergeAtOnce:    10,
		FloorSegmentDocs:  2000,
		MaxMergedDocs:     50_000_000,
		DeletesPctAllowed: 33,
	}
}

// SegmentStat is the slice of a segment the policy needs: its built document
// count and how many of those are still live. Anything that can report both
// (an Inverted, an open search segment, a manifest member) can be tiered.
type SegmentStat interface {
	NumDocs() int
	LiveDocs() int
}

// flooredSize is a segment's size class for tiering: its live-doc count rounded
// up to the floor, so segments below the floor compare equal.
func (p MergePolicy) flooredSize(s SegmentStat) int {
	n := s.LiveDocs()
	if n < p.FloorSegmentDocs {
		return p.FloorSegmentDocs
	}
	return n
}

// tooManyDeletes reports whether a segment's deleted fraction has crossed the
// reclaim threshold, which makes it a rewrite candidate on its own.
func (p MergePolicy) tooManyDeletes(s SegmentStat) bool {
	if s.NumDocs() == 0 {
		return false
	}
	deleted := s.NumDocs() - s.LiveDocs()
	return deleted*100 >= s.NumDocs()*p.DeletesPctAllowed
}

// Select chooses the next batch of segment indices to merge, or nil if none
// qualify. A delete-driven rewrite takes priority: it reclaims space a tiering
// pass would leave stranded in a large segment, and is returned as a singleton
// batch. Otherwise, if the collection has at least SegmentsPerTier segments, it
// merges up to MaxMergeAtOnce of the smallest, bounded by MaxMergedDocs. It
// returns indices into the input slice so the caller maps them back to files.
func (p MergePolicy) Select(segs []SegmentStat) []int {
	if len(segs) == 0 {
		return nil
	}
	for i, s := range segs {
		if p.tooManyDeletes(s) {
			return []int{i}
		}
	}
	if len(segs) < p.SegmentsPerTier {
		return nil
	}
	order := make([]int, len(segs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return p.flooredSize(segs[order[a]]) < p.flooredSize(segs[order[b]])
	})
	var batch []int
	var total int
	for _, idx := range order {
		if len(batch) >= p.MaxMergeAtOnce {
			break
		}
		if total+segs[idx].LiveDocs() > p.MaxMergedDocs && len(batch) > 0 {
			break
		}
		batch = append(batch, idx)
		total += segs[idx].LiveDocs()
	}
	if len(batch) < 2 {
		return nil
	}
	return batch
}
