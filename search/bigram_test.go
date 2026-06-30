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
