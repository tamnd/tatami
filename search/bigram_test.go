package search

import (
	"reflect"
	"testing"
)

// fakeBigramShard is a BigramSource with hand-set adjacencies, so the phrase
// routing tests exercise the sidecar without building a real segment.
type fakeBigramShard struct {
	pairs map[BigramKey]fakeTerm
}

func (f fakeBigramShard) EachBigram(fn func(a, b string, df int, maxFreq uint32)) {
	for _, k := range sortedBigramKeys(f.pairs) {
		ft := f.pairs[k]
		fn(k.A, k.B, ft.df, ft.maxFreq)
	}
}

func sortedBigramKeys(m map[BigramKey]fakeTerm) []BigramKey {
	out := make([]BigramKey, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && lessBigram(out[j], out[j-1]); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func lessBigram(a, b BigramKey) bool {
	if a.A != b.A {
		return a.A < b.A
	}
	return a.B < b.B
}

// fakeStats is the minimal GlobalStats a phrase route needs: only NumDocs is read,
// since the bound uses each pair's own global df, not a per-term df.
type fakeStats struct{ n int }

func (f fakeStats) NumDocs() int       { return f.n }
func (f fakeStats) DocFreq(string) int { return 0 }
func (f fakeStats) ShardDocs(int) int  { return 0 }

func buildBigrams(shards []fakeBigramShard) *BigramRouting {
	b := NewBigramRoutingBuilder()
	for sid, s := range shards {
		b.AddShard(uint32(sid), s)
	}
	return b.Build()
}

func bk(a, b string) BigramKey { return BigramKey{A: a, B: b} }

// TestPhraseRouteNarrowsToAdjacency is the core property: a phrase routes only to
// the shards that hold its adjacency, not the union of the shards holding its
// words. Here both shards hold "open" and "source" as words, but only shard 0 holds
// them adjacent, so the phrase routes to shard 0 alone.
func TestPhraseRouteNarrowsToAdjacency(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 12, maxFreq: 3}}},
		{pairs: map[BigramKey]fakeTerm{bk("source", "code"): {df: 4, maxFreq: 1}}},
	})

	bounds, covered := br.RoutePhrase([]string{"open", "source"}, fakeStats{n: 1000})
	if !covered {
		t.Fatal("route should be covered: the only adjacency is tracked")
	}
	if len(bounds) != 1 || bounds[0].Shard != 0 {
		t.Fatalf("phrase routed to %v, want shard 0 only", bounds)
	}
	if bounds[0].Bound <= 0 {
		t.Fatalf("bound = %v, want positive", bounds[0].Bound)
	}
}

// TestPhraseRouteUnionAcrossAdjacencies checks a three-word phrase routes to the
// union of its two adjacencies' shards, which is what keeps the route exact: a
// document matching the whole phrase holds both adjacencies, so its shard is in the
// union under either pair.
func TestPhraseRouteUnionAcrossAdjacencies(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 5, maxFreq: 2}}},
		{pairs: map[BigramKey]fakeTerm{bk("source", "software"): {df: 7, maxFreq: 2}}},
		{pairs: map[BigramKey]fakeTerm{bk("free", "lunch"): {df: 3, maxFreq: 1}}},
	})

	bounds, covered := br.RoutePhrase([]string{"open", "source", "software"}, fakeStats{n: 1000})
	if !covered {
		t.Fatal("route should be covered: both adjacencies are tracked")
	}
	got := map[uint32]bool{}
	for _, b := range bounds {
		got[b.Shard] = true
	}
	if !got[0] || !got[1] || got[2] {
		t.Fatalf("phrase routed to %v, want shards {0,1} (not 2)", bounds)
	}
}

// TestPhraseRouteUncoveredFallsBack checks the covered flag: when an adjacency is
// not tracked, the route cannot prove which shards hold it, so covered is false and
// the caller must fall back to the bag route.
func TestPhraseRouteUncoveredFallsBack(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 5, maxFreq: 2}}},
	})

	_, covered := br.RoutePhrase([]string{"open", "source", "software"}, fakeStats{n: 1000})
	if covered {
		t.Fatal("route should be uncovered: the (source, software) adjacency is untracked")
	}
}

// TestPhraseRouteSingleWord checks that a one-word query has no adjacency, so the
// phrase path reports covered false and the caller falls back to the bag route.
func TestPhraseRouteSingleWord(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 5, maxFreq: 2}}},
	})
	bounds, covered := br.RoutePhrase([]string{"alpha"}, fakeStats{n: 10})
	if covered {
		t.Fatal("a single word has no adjacency, so covered must be false")
	}
	if bounds != nil {
		t.Fatalf("single-word route returned %v, want nil", bounds)
	}
}

// TestBigramRoutingAsSource checks that a BigramRouting folds into a higher-level
// bigram builder as one shard, the building block of the box-level phrase summary
// (scale/11 lever four). A summary built over two per-box sidecars must report the
// sum of a pair's per-box document frequency as its own df, and route a phrase to a
// box using that box's ceiling adjacency frequency, the max over its shards, so a
// box's phrase bound stays a true upper bound on any phrase score in the box.
func TestBigramRoutingAsSource(t *testing.T) {
	// Box 0: two shards, "open source" reaching maxFreq 3 then 7 across them, plus an
	// adjacency "box zero" that lives only in this box.
	box0 := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 10, maxFreq: 3}, bk("box", "zero"): {df: 2, maxFreq: 1}}},
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 8, maxFreq: 7}}},
	})
	// Box 1: one shard with "open source" only.
	box1 := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 5, maxFreq: 4}}},
	})

	// Fold each box's sidecar in as one shard: the box is a BigramSource through
	// EachBigram, so the box-level summary is built by the same builder one level up.
	b := NewBigramRoutingBuilder()
	b.AddShard(0, box0)
	b.AddShard(1, box1)
	summary := b.Build()

	// open,source: box0 df 10+8=18, box1 df 5, fleet 23.
	if got := summary.PairDocFreq("open", "source"); got != 23 {
		t.Fatalf("summary PairDocFreq(open,source) = %d, want 23", got)
	}
	if got := summary.PairDocFreq("box", "zero"); got != 2 {
		t.Fatalf("summary PairDocFreq(box,zero) = %d, want 2", got)
	}

	// The box-unique adjacency routes to box 0 alone, the phrase-level analogue of a
	// term living in one box.
	if bounds, covered := summary.RoutePhrase([]string{"box", "zero"}, fakeStats{n: 150}); !covered || len(bounds) != 1 || bounds[0].Shard != 0 {
		t.Fatalf("box,zero routed to %v (covered=%v), want box 0 only", bounds, covered)
	}

	// open,source routes to both boxes, box 0 first because its ceiling adjacency
	// frequency (max over shards, 7) beats box 1's 4, and box 0's bound must use that
	// ceiling scored with the fleet IDF, so it upper-bounds any phrase score in box 0.
	bounds, covered := summary.RoutePhrase([]string{"open", "source"}, fakeStats{n: 150})
	if !covered || len(bounds) != 2 {
		t.Fatalf("open,source routed to %v (covered=%v), want 2 boxes", bounds, covered)
	}
	if bounds[0].Shard != 0 || bounds[1].Shard != 1 {
		t.Fatalf("box order = %+v, want box 0 then 1", bounds)
	}
	col := Collection{N: 150}
	sc := bm25Scorer{idf: col.IDF(23), k1: DefaultK1}
	if want := sc.MaxScore(7); bounds[0].Bound != want {
		t.Fatalf("box 0 bound = %v, want %v (fleet IDF times ceiling freq 7)", bounds[0].Bound, want)
	}
}

func TestBigramRoutingRoundTrip(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{
			bk("open", "source"):     {df: 5, maxFreq: 2},
			bk("source", "software"): {df: 7, maxFreq: 3},
		}},
		{pairs: map[BigramKey]fakeTerm{bk("open", "source"): {df: 1, maxFreq: 1}}},
	})

	got, err := DecodeBigramRouting(EncodeBigramRouting(br))
	if err != nil {
		t.Fatal(err)
	}
	if got.NumPairs() != br.NumPairs() {
		t.Fatalf("decoded %d pairs, want %d", got.NumPairs(), br.NumPairs())
	}
	if got.PairDocFreq("open", "source") != 6 {
		t.Fatalf("PairDocFreq(open,source) = %d, want 6", got.PairDocFreq("open", "source"))
	}
	a, _ := br.RoutePhrase([]string{"open", "source"}, fakeStats{n: 100})
	b, _ := got.RoutePhrase([]string{"open", "source"}, fakeStats{n: 100})
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("route differs after roundtrip: %v vs %v", a, b)
	}
}

func TestBigramKeepCommon(t *testing.T) {
	br := buildBigrams([]fakeBigramShard{
		{pairs: map[BigramKey]fakeTerm{
			bk("the", "data"): {df: 50, maxFreq: 4}, // both common
			bk("data", "xqz"): {df: 2, maxFreq: 1},  // xqz is rare
		}},
	})
	uni := BuildRouting([]RoutingSource{
		fakeShard{live: 100, terms: map[string]fakeTerm{
			"the":  {df: 80, maxFreq: 9},
			"data": {df: 40, maxFreq: 6},
			"xqz":  {df: 2, maxFreq: 1},
		}},
	})

	kept := br.KeepCommon(uni, 10)
	if kept.NumPairs() != 1 {
		t.Fatalf("KeepCommon kept %d pairs, want 1 (only the,data)", kept.NumPairs())
	}
	if kept.PairDocFreq("the", "data") != 50 {
		t.Fatalf("kept the,data df = %d, want 50", kept.PairDocFreq("the", "data"))
	}
	if kept.PairDocFreq("data", "xqz") != 0 {
		t.Fatalf("data,xqz should have been dropped")
	}
}
