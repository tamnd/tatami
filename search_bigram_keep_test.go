package tatami

import (
	"testing"

	"github.com/tamnd/tatami/search"
)

// TestBigramKeepBoundsCapture proves the BigramKeep option records exactly the pairs
// in the keep set and nothing else, with the same document frequency and maximum
// in-document count a full capture gives those pairs. The point of the option is
// memory: a full capture grows a shard-wide dictionary of every adjacency in the
// corpus, which is the peak of a sharded phrase build, while the router only ever
// asks for a bounded set of pairs. So the bounded builder must be a faithful subset,
// never a different count, or the phrase route it feeds would shift.
func TestBigramKeepBoundsCapture(t *testing.T) {
	docs := []SearchDoc{
		{DocID: "1", Body: "open source software is free software"},
		{DocID: "2", Body: "machine learning model and open source model"},
		{DocID: "3", Body: "free software open source open source"},
	}

	// A full, unbounded capture is the oracle: every adjacency with its true df and
	// maxFreq.
	full := NewSearchBuilderWith(SearchBuilderOptions{Bigrams: true})
	for _, d := range docs {
		full.Add(d)
	}
	oracle := map[search.BigramKey]bgrPair{}
	full.EachBigram(func(a, b string, df int, mf uint32) {
		oracle[search.BigramKey{A: a, B: b}] = bgrPair{a: a, b: b, df: df, maxFreq: mf}
	})

	// Keep two pairs: one common ("open"->"source", present in every doc, twice in doc
	// 3) and one rarer ("free"->"software"). Both must come back with the oracle's
	// counts; every other pair must be absent.
	keep := map[search.BigramKey]bool{
		{A: "open", B: "source"}:   true,
		{A: "free", B: "software"}: true,
	}
	bounded := NewSearchBuilderWith(SearchBuilderOptions{Bigrams: true, BigramKeep: keep})
	for _, d := range docs {
		bounded.Add(d)
	}
	got := map[search.BigramKey]bgrPair{}
	bounded.EachBigram(func(a, b string, df int, mf uint32) {
		key := search.BigramKey{A: a, B: b}
		if !keep[key] {
			t.Fatalf("bounded capture yielded pair %q->%q outside the keep set", a, b)
		}
		got[key] = bgrPair{a: a, b: b, df: df, maxFreq: mf}
	})

	if len(got) != len(keep) {
		t.Fatalf("bounded capture yielded %d pairs, want the %d kept", len(got), len(keep))
	}
	for key := range keep {
		want, ok := oracle[key]
		if !ok {
			t.Fatalf("keep set names pair %q->%q that the corpus does not hold", key.A, key.B)
		}
		g := got[key]
		if g.df != want.df || g.maxFreq != want.maxFreq {
			t.Fatalf("pair %q->%q bounded df=%d maxFreq=%d, want df=%d maxFreq=%d",
				key.A, key.B, g.df, g.maxFreq, want.df, want.maxFreq)
		}
	}

	// A nil keep set is unchanged: it still captures every pair.
	if len(oracle) <= len(keep) {
		t.Fatalf("oracle captured %d pairs, expected more than the %d kept so the bound is meaningful", len(oracle), len(keep))
	}
}
