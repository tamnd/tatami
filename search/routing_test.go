package search

import (
	"reflect"
	"testing"
)

// fakeShard is a RoutingSource with hand-set facts, so the routing tests exercise
// the index without building a real inverted segment.
type fakeShard struct {
	live  int
	terms map[string]fakeTerm
}

type fakeTerm struct {
	df      int
	maxFreq uint32
}

func (f fakeShard) LiveDocs() int { return f.live }

func (f fakeShard) EachTerm(fn func(term string, df int, maxFreq uint32)) {
	// Deterministic order is not required of EachTerm, but a stable order keeps
	// the test's encode roundtrip easy to reason about.
	for _, t := range sortedKeys(f.terms) {
		ft := f.terms[t]
		fn(t, ft.df, ft.maxFreq)
	}
}

func sortedKeys(m map[string]fakeTerm) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// insertion sort, small maps
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func TestRoutingBuildStats(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 100, terms: map[string]fakeTerm{
			"alpha": {df: 40, maxFreq: 5},
			"beta":  {df: 10, maxFreq: 2},
		}},
		fakeShard{live: 50, terms: map[string]fakeTerm{
			"alpha": {df: 20, maxFreq: 9},
			"gamma": {df: 5, maxFreq: 1},
		}},
	})

	if ri.NumDocs() != 150 {
		t.Fatalf("NumDocs = %d, want 150", ri.NumDocs())
	}
	if ri.NumShards() != 2 {
		t.Fatalf("NumShards = %d, want 2", ri.NumShards())
	}
	if got := ri.DocFreq("alpha"); got != 60 {
		t.Fatalf("DocFreq(alpha) = %d, want 60", got)
	}
	if got := ri.DocFreq("beta"); got != 10 {
		t.Fatalf("DocFreq(beta) = %d, want 10", got)
	}
	if got := ri.DocFreq("absent"); got != 0 {
		t.Fatalf("DocFreq(absent) = %d, want 0", got)
	}
	if got := ri.ShardDocs(1); got != 50 {
		t.Fatalf("ShardDocs(1) = %d, want 50", got)
	}
}

// TestRouteOrderAndBound checks that Route returns only shards holding a query
// term, ordered by a descending bound, and that the bound equals the global-IDF
// impact the broker scores against.
func TestRouteOrderAndBound(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 100, terms: map[string]fakeTerm{"alpha": {df: 40, maxFreq: 3}}},
		fakeShard{live: 100, terms: map[string]fakeTerm{"alpha": {df: 40, maxFreq: 9}}},
		fakeShard{live: 100, terms: map[string]fakeTerm{"beta": {df: 5, maxFreq: 2}}},
	})

	got := ri.Route([]string{"alpha"})
	if len(got) != 2 {
		t.Fatalf("Route(alpha) returned %d shards, want 2", len(got))
	}
	// Shard 1 has the higher max frequency, so it must sort first.
	if got[0].Shard != 1 || got[1].Shard != 0 {
		t.Fatalf("Route order = %+v, want shard 1 then 0", got)
	}
	if !(got[0].Bound > got[1].Bound) {
		t.Fatalf("bounds not descending: %+v", got)
	}

	// The bound must equal globalIDF(alpha) * sat(maxFreq) for that shard.
	col := Collection{N: 300}
	sc := bm25Scorer{idf: col.IDF(80), k1: DefaultK1}
	if want := sc.MaxScore(9); got[0].Bound != want {
		t.Fatalf("bound = %v, want %v", got[0].Bound, want)
	}
}

// TestRouteRepeatedTermDoublesBound checks that a term repeated in the query adds
// its bound twice, matching how a shard scores the repeated token, so the bound
// stays a true upper bound.
func TestRouteRepeatedTermDoublesBound(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 10, terms: map[string]fakeTerm{"alpha": {df: 4, maxFreq: 3}}},
	})
	once := ri.Route([]string{"alpha"})
	twice := ri.Route([]string{"alpha", "alpha"})
	if len(once) != 1 || len(twice) != 1 {
		t.Fatalf("unexpected shard counts: once=%d twice=%d", len(once), len(twice))
	}
	if twice[0].Bound != 2*once[0].Bound {
		t.Fatalf("repeated term bound = %v, want %v", twice[0].Bound, 2*once[0].Bound)
	}
}

// TestRouteMultiTermAccumulates checks that a shard's bound sums the contributions
// of every distinct query term it holds.
func TestRouteMultiTermAccumulates(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 100, terms: map[string]fakeTerm{
			"alpha": {df: 40, maxFreq: 3},
			"beta":  {df: 10, maxFreq: 2},
		}},
	})
	a := ri.Route([]string{"alpha"})[0].Bound
	b := ri.Route([]string{"beta"})[0].Bound
	both := ri.Route([]string{"alpha", "beta"})[0].Bound
	if both != a+b {
		t.Fatalf("combined bound = %v, want %v", both, a+b)
	}
}

func TestRoutingEncodeRoundtrip(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 100, terms: map[string]fakeTerm{
			"alpha": {df: 40, maxFreq: 5},
			"beta":  {df: 10, maxFreq: 2},
		}},
		fakeShard{live: 50, terms: map[string]fakeTerm{
			"alpha": {df: 20, maxFreq: 9},
			"gamma": {df: 5, maxFreq: 1},
		}},
	})

	enc := EncodeRouting(ri)
	back, err := DecodeRouting(enc)
	if err != nil {
		t.Fatalf("DecodeRouting: %v", err)
	}
	if back.NumDocs() != ri.NumDocs() || back.NumShards() != ri.NumShards() {
		t.Fatalf("totals differ after roundtrip")
	}
	if !reflect.DeepEqual(back.shardDocs, ri.shardDocs) {
		t.Fatalf("shardDocs differ: %v vs %v", back.shardDocs, ri.shardDocs)
	}
	if !reflect.DeepEqual(back, ri) {
		t.Fatalf("flat routing differs after roundtrip")
	}

	// Re-encoding the decoded index must reproduce the same bytes (deterministic).
	if enc2 := EncodeRouting(back); !reflect.DeepEqual(enc, enc2) {
		t.Fatalf("re-encode not byte-identical")
	}
}

func TestDecodeRoutingTruncated(t *testing.T) {
	ri := BuildRouting([]RoutingSource{
		fakeShard{live: 10, terms: map[string]fakeTerm{"alpha": {df: 4, maxFreq: 3}}},
	})
	enc := EncodeRouting(ri)
	if _, err := DecodeRouting(enc[:len(enc)/2]); err == nil {
		t.Fatalf("expected error decoding truncated routing index")
	}
}
