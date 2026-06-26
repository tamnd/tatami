package search

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// TestBitpackRoundTrip exercises the FOR bit-packing at every width on a full
// 128-value block, the path full posting blocks take for doc-id gaps.
func TestBitpackRoundTrip(t *testing.T) {
	for width := 1; width <= 32; width++ {
		vals := make([]uint32, BlockSize)
		var maxv uint32 = 1<<uint(width) - 1
		if width == 32 {
			maxv = ^uint32(0)
		}
		for i := range vals {
			vals[i] = uint32(i*2654435761) & maxv
		}
		w := maxWidth(vals)
		packed := packBits(nil, vals, w)
		if len(packed) != packedLen(len(vals), w) {
			t.Fatalf("width %d: packed %d bytes, want %d", width, len(packed), packedLen(len(vals), w))
		}
		out := make([]uint32, len(vals))
		if n := unpackBits(packed, out, len(vals), w); n != len(packed) {
			t.Fatalf("width %d: consumed %d of %d bytes", width, n, len(packed))
		}
		for i := range vals {
			if out[i] != vals[i] {
				t.Fatalf("width %d index %d: got %d want %d", width, i, out[i], vals[i])
			}
		}
	}
}

// TestGroupVarintRoundTrip checks the tail-block codec across lengths that are
// not multiples of four, the awkward cases the group framing must pad.
func TestGroupVarintRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 7, 71, 127} {
		vals := make([]uint32, n)
		for i := range vals {
			vals[i] = uint32(i) * 9973
		}
		enc := appendGroupVarint(nil, vals)
		out := make([]uint32, n)
		if got := readGroupVarint(enc, out, n); got != len(enc) {
			t.Fatalf("n=%d consumed %d of %d", n, got, len(enc))
		}
		for i := range vals {
			if out[i] != vals[i] {
				t.Fatalf("n=%d index %d: got %d want %d", n, i, out[i], vals[i])
			}
		}
	}
}

// TestPForDeltaPatching checks that a single large outlier is stored as an
// exception instead of widening the whole block, which is the codec's reason to
// exist for frequency streams.
func TestPForDeltaPatching(t *testing.T) {
	vals := make([]uint32, BlockSize)
	for i := range vals {
		vals[i] = 2 + uint32(i%3)
	}
	vals[53] = 4000 // the stop-word outlier
	w, exc := chooseWidth(vals)
	if exc != 1 {
		t.Fatalf("expected 1 exception, got %d at width %d", exc, w)
	}
	enc := appendPForDelta(nil, vals)
	out := make([]uint32, len(vals))
	readPForDelta(enc, out)
	for i := range vals {
		if out[i] != vals[i] {
			t.Fatalf("index %d: got %d want %d", i, out[i], vals[i])
		}
	}
}

// TestListEncodeCursor builds a posting list that spans a full block and a tail,
// then walks it with Next and probes NextGEQ skips.
func TestListEncodeCursor(t *testing.T) {
	const n = BlockSize + 72
	ps := make([]Posting, n)
	doc := DocID(1000)
	for i := range ps {
		doc += DocID(1 + i%5)
		ps[i] = Posting{Doc: doc, Frequency: uint32(1 + i%7)}
	}
	l, err := Encode(ps)
	if err != nil {
		t.Fatal(err)
	}
	if l.NumDocs() != n {
		t.Fatalf("NumDocs %d want %d", l.NumDocs(), n)
	}

	// Full forward walk reproduces every posting.
	c := l.Cursor()
	for i := 0; i < n; i++ {
		if !c.Next() {
			t.Fatalf("ran out at %d", i)
		}
		if c.Doc() != ps[i].Doc || c.Freq() != ps[i].Frequency {
			t.Fatalf("index %d: got (%d,%d) want (%d,%d)", i, c.Doc(), c.Freq(), ps[i].Doc, ps[i].Frequency)
		}
	}
	if c.Next() {
		t.Fatal("cursor did not terminate")
	}

	// NextGEQ lands on the first doc >= target, including a target inside the tail.
	c2 := l.Cursor()
	target := ps[n-10].Doc
	got, ok := c2.NextGEQ(target)
	if !ok || got != target {
		t.Fatalf("NextGEQ(%d) = (%d,%v)", target, got, ok)
	}
	// A target above the last doc reports done.
	c3 := l.Cursor()
	if _, ok := c3.NextGEQ(ps[n-1].Doc + 1); ok {
		t.Fatal("NextGEQ past the end should be done")
	}
}

// TestEncodeRejectsUnsorted guards the intersection-correctness invariant.
func TestEncodeRejectsUnsorted(t *testing.T) {
	if _, err := Encode([]Posting{{Doc: 5}, {Doc: 5}}); err == nil {
		t.Fatal("duplicate doc id should be rejected")
	}
	if _, err := Encode([]Posting{{Doc: 5}, {Doc: 3}}); err == nil {
		t.Fatal("descending doc id should be rejected")
	}
}

// TestWANDMatchesBruteForce is the retrieval-correctness gate: block-max WAND
// must return exactly the same top-k (scores and order) as an exhaustive scan
// under the same scorer. It runs many random corpora and queries so the skipping
// logic is exercised across list shapes.
func TestWANDMatchesBruteForce(t *testing.T) {
	rng := rand.New(rand.NewSource(20660926))
	for trial := 0; trial < 50; trial++ {
		const numDocs = 5000
		numTerms := 3 + rng.Intn(6)
		// Build per-term postings.
		termPostings := make([][]Posting, numTerms)
		termFreqByDoc := make([]map[int]uint32, numTerms)
		for ti := range termPostings {
			termFreqByDoc[ti] = map[int]uint32{}
			for d := 0; d < numDocs; d++ {
				if rng.Float64() < 0.1 {
					f := uint32(1 + rng.Intn(20))
					termPostings[ti] = append(termPostings[ti], Posting{Doc: DocID(d), Frequency: f})
					termFreqByDoc[ti][d] = f
				}
			}
		}
		lists := make([]*List, numTerms)
		for ti := range termPostings {
			if len(termPostings[ti]) == 0 {
				termPostings[ti] = []Posting{{Doc: DocID(rng.Intn(numDocs)), Frequency: 1}}
				termFreqByDoc[ti][int(termPostings[ti][0].Doc)] = 1
			}
			l, err := Encode(termPostings[ti])
			if err != nil {
				t.Fatal(err)
			}
			lists[ti] = l
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
		got := WAND(inputs, k)

		// Brute force: score every doc, take top-k by (score desc, doc asc).
		type sd struct {
			doc   DocID
			score Score
		}
		var all []sd
		for d := 0; d < numDocs; d++ {
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
			t.Fatalf("trial %d: WAND returned %d hits, want %d", trial, len(got), len(want))
		}
		for i := range want {
			if got[i].Doc != want[i].doc {
				t.Fatalf("trial %d rank %d: doc %d want %d", trial, i, got[i].Doc, want[i].doc)
			}
			if math.Abs(float64(got[i].Score-want[i].score)) > 1e-4 {
				t.Fatalf("trial %d rank %d: score %v want %v", trial, i, got[i].Score, want[i].score)
			}
		}
	}
}

// TestBM25FFielding checks the fielded scorer's saturation and field weighting:
// the same raw frequency in the title outscores it in the body, and a term
// stuffed in one field saturates rather than scaling linearly.
func TestBM25FFielding(t *testing.T) {
	col := Collection{N: 1000}
	for f := range col.AvgFieldLen {
		col.AvgFieldLen[f] = 100
	}
	s := NewScorer(DefaultParams(), col)
	idf := col.IDF(50)

	var titleTF, bodyTF [NumFields]uint32
	titleTF[FieldTitle] = 3
	bodyTF[FieldBody] = 3
	fieldLen := [NumFields]uint32{100, 100, 100, 100}

	title := s.ScoreTerm(idf, titleTF, fieldLen)
	body := s.ScoreTerm(idf, bodyTF, fieldLen)
	if !(title > body) {
		t.Fatalf("title weight should beat body: title=%v body=%v", title, body)
	}

	// Saturation: doubling the frequency less than doubles the score.
	var tf1, tf2 [NumFields]uint32
	tf1[FieldBody] = 2
	tf2[FieldBody] = 4
	low := float64(s.ScoreTerm(idf, tf1, fieldLen))
	high := float64(s.ScoreTerm(idf, tf2, fieldLen))
	if high >= 2*low {
		t.Fatalf("score should saturate: f=2 -> %v, f=4 -> %v", low, high)
	}
}

// TestInvertedBuildSearchRoundTrip builds an index, serializes the sub-region,
// reloads it, and checks that search results survive the round trip unchanged.
func TestInvertedBuildSearchRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	vocab := []string{"tatami", "search", "columnar", "index", "crawl", "segment", "posting", "block"}
	b := NewInvertedBuilder()
	for d := 0; d < 3000; d++ {
		tf := map[string]uint32{}
		for _, w := range vocab {
			if rng.Float64() < 0.2 {
				tf[w] = uint32(1 + rng.Intn(5))
			}
		}
		// A unique term per doc exercises the singleton path.
		tf[uniqueTerm(d)] = 1
		b.AddDocument(tf)
	}
	inv, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}

	td, pp, sk := EncodeInverted(inv)
	got, err := DecodeInverted(td, pp, sk, inv.NumDocs())
	if err != nil {
		t.Fatal(err)
	}
	if got.NumTerms() != inv.NumTerms() {
		t.Fatalf("reloaded %d terms, want %d", got.NumTerms(), inv.NumTerms())
	}

	query := []string{"tatami", "search", "index"}
	want := inv.Search(query, 10)
	reloaded := got.Search(query, 10)
	if len(want) != len(reloaded) {
		t.Fatalf("reloaded %d hits, want %d", len(reloaded), len(want))
	}
	for i := range want {
		if want[i] != reloaded[i] {
			t.Fatalf("rank %d: reloaded %+v want %+v", i, reloaded[i], want[i])
		}
	}

	// The singleton path resolves to exactly one document.
	if _, df, ok := got.Postings(uniqueTerm(42)); !ok || df != 1 {
		t.Fatalf("singleton lookup: df=%d ok=%v", df, ok)
	}
}

func uniqueTerm(d int) string {
	const hex = "0123456789abcdef"
	b := []byte("u-00000000")
	for i := 0; i < 8; i++ {
		b[9-i] = hex[(d>>(4*i))&0xf]
	}
	return string(b)
}
