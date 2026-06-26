package search

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// TestLiveBitsetRoundTrip checks the live-docs bitset: a fresh set is all live,
// clears stick and are idempotent, the count tracks, and the serialized form
// reloads identically.
func TestLiveBitsetRoundTrip(t *testing.T) {
	const n = 1000
	l := NewLive(n)
	if !l.AllLive() || l.Count() != n {
		t.Fatalf("fresh set should be all live: count=%d", l.Count())
	}
	deleted := []int{0, 1, 63, 64, 65, 511, 999}
	for _, d := range deleted {
		if !l.Clear(d) {
			t.Fatalf("first clear of %d should change state", d)
		}
		if l.Clear(d) {
			t.Fatalf("second clear of %d should be a no-op", d)
		}
	}
	if l.Count() != n-len(deleted) {
		t.Fatalf("count=%d want %d", l.Count(), n-len(deleted))
	}
	for _, d := range deleted {
		if l.Get(d) {
			t.Fatalf("doc %d should be deleted", d)
		}
	}

	enc := EncodeLive(l)
	got := DecodeLive(enc)
	if got.Len() != l.Len() || got.Count() != l.Count() {
		t.Fatalf("reloaded count=%d len=%d want %d/%d", got.Count(), got.Len(), l.Count(), l.Len())
	}
	for i := 0; i < n; i++ {
		if got.Get(i) != l.Get(i) {
			t.Fatalf("doc %d liveness differs after reload", i)
		}
	}
	if DecodeLive(nil) != nil {
		t.Fatal("empty input should decode to nil (all-live)")
	}
}

// TestWANDFilterMatchesBruteForce is the deletion-correctness gate: with a live
// filter, the top-k must equal an exhaustive scan that scores every document
// under the same scorer and then drops the deleted ones. N is unchanged by a
// delete, so scores are unchanged; only membership in the result changes.
func TestWANDFilterMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(98765))
	for trial := 0; trial < 40; trial++ {
		const numDocs = 4000
		numTerms := 3 + rng.Intn(5)
		termPostings := make([][]Posting, numTerms)
		termFreqByDoc := make([]map[int]uint32, numTerms)
		for ti := range termPostings {
			termFreqByDoc[ti] = map[int]uint32{}
			for d := 0; d < numDocs; d++ {
				if rng.Float64() < 0.12 {
					f := uint32(1 + rng.Intn(15))
					termPostings[ti] = append(termPostings[ti], Posting{Doc: DocID(d), Frequency: f})
					termFreqByDoc[ti][d] = f
				}
			}
			if len(termPostings[ti]) == 0 {
				termPostings[ti] = []Posting{{Doc: DocID(rng.Intn(numDocs)), Frequency: 1}}
				termFreqByDoc[ti][int(termPostings[ti][0].Doc)] = 1
			}
		}
		lists := make([]*List, numTerms)
		for ti := range termPostings {
			l, err := Encode(termPostings[ti])
			if err != nil {
				t.Fatal(err)
			}
			lists[ti] = l
		}

		// Delete a random tenth of the documents.
		live := NewLive(numDocs)
		for d := 0; d < numDocs; d++ {
			if rng.Float64() < 0.1 {
				live.Clear(d)
			}
		}

		col := Collection{N: numDocs}
		scorers := make([]bm25Scorer, numTerms)
		inputs := make([]TermInput, numTerms)
		for ti := range lists {
			idf := col.IDF(len(termPostings[ti]))
			scorers[ti] = bm25Scorer{idf: idf, k1: DefaultK1}
			inputs[ti] = TermInput{Cursor: lists[ti].Cursor(), Scorer: scorers[ti], MaxFreq: lists[ti].MaxFreq()}
		}

		k := 1 + rng.Intn(20)
		got := WANDFilter(inputs, k, func(d DocID) bool { return live.Get(int(d)) })

		type sd struct {
			doc   DocID
			score Score
		}
		var all []sd
		for d := 0; d < numDocs; d++ {
			if !live.Get(d) {
				continue
			}
			var s Score
			for ti := range scorers {
				if f, ok := termFreqByDoc[ti][d]; ok {
					s += scorers[ti].Score(f)
				}
			}
			if s > 0 {
				all = append(all, sd{DocID(d), s})
			}
		}
		sort.Slice(all, func(i, j int) bool {
			if all[i].score != all[j].score {
				return all[i].score > all[j].score
			}
			return all[i].doc < all[j].doc
		})
		want := all
		if len(want) > k {
			want = want[:k]
		}
		if len(got) != len(want) {
			t.Fatalf("trial %d: got %d hits, want %d", trial, len(got), len(want))
		}
		for i := range want {
			if got[i].Doc != want[i].doc {
				t.Fatalf("trial %d rank %d: doc %d want %d (deleted leaked? %v)", trial, i, got[i].Doc, want[i].doc, !live.Get(int(got[i].Doc)))
			}
			if math.Abs(float64(got[i].Score-want[i].score)) > 1e-4 {
				t.Fatalf("trial %d rank %d: score %v want %v", trial, i, got[i].Score, want[i].score)
			}
		}
	}
}

// fakeStat is a SegmentStat for exercising the merge policy without building real
// segments.
type fakeStat struct{ num, live int }

func (f fakeStat) NumDocs() int  { return f.num }
func (f fakeStat) LiveDocs() int { return f.live }

// TestMergePolicySelect checks the tiered policy: nothing merges below the tier
// threshold, the smallest run is chosen once it is reached, and a segment past
// the delete threshold is rewritten on its own ahead of tiering.
func TestMergePolicySelect(t *testing.T) {
	p := DefaultMergePolicy()

	// Below SegmentsPerTier with no deletes: nothing to do.
	few := []SegmentStat{fakeStat{1000, 1000}, fakeStat{1000, 1000}}
	if got := p.Select(few); got != nil {
		t.Fatalf("expected no merge below tier threshold, got %v", got)
	}

	// Exactly SegmentsPerTier equal segments: merge up to MaxMergeAtOnce.
	many := make([]SegmentStat, p.SegmentsPerTier)
	for i := range many {
		many[i] = fakeStat{1000, 1000}
	}
	got := p.Select(many)
	if len(got) != p.MaxMergeAtOnce {
		t.Fatalf("expected %d segments selected, got %d", p.MaxMergeAtOnce, len(got))
	}

	// A segment past the delete threshold is selected alone, ahead of tiering.
	withDeletes := make([]SegmentStat, p.SegmentsPerTier)
	for i := range withDeletes {
		withDeletes[i] = fakeStat{1000, 1000}
	}
	withDeletes[3] = fakeStat{1000, 600} // 40 percent deleted, over the 33 percent gate
	got = p.Select(withDeletes)
	if len(got) != 1 || got[0] != 3 {
		t.Fatalf("expected singleton rewrite of segment 3, got %v", got)
	}

	// The smallest segments tier first: build a staircase and check the floor run
	// is what gets chosen.
	stair := []SegmentStat{
		fakeStat{100000, 100000}, fakeStat{100000, 100000},
	}
	for i := 0; i < p.SegmentsPerTier; i++ {
		stair = append(stair, fakeStat{3000, 3000})
	}
	got = p.Select(stair)
	if len(got) == 0 {
		t.Fatal("expected a merge of the small tier")
	}
	for _, idx := range got {
		if stair[idx].LiveDocs() != 3000 {
			t.Fatalf("policy chose a large segment (idx %d, %d docs) over the small tier", idx, stair[idx].LiveDocs())
		}
	}
}
