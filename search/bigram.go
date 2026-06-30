package search

// A bag-of-words query of common terms fans out to nearly every shard, because a
// common term lives in nearly every shard and the union of three such terms is
// the union of "most" with "most" with "most", which is "nearly all"
// (07-routing-latency.md, section 1.2). The unigram routing index cannot tighten
// that: it knows a shard holds "open" and holds "software", so it must route
// there in case some document holds both. What it does not know is whether any
// document holds them adjacent.
//
// The bigram routing sidecar carries that adjacency. For an ordered pair (a, b)
// it records, per shard, how many documents hold a immediately before b. A phrase
// "open source software" then routes to the union of the shards holding its
// adjacencies ("open source", "source software") rather than the union of the
// shards holding its words, and an adjacency is far rarer than either of its
// words. A shard that holds the words but never the adjacency provably holds no
// document the phrase can match, so dropping it is exact for phrase semantics
// (section 4.1).
//
// It is a sidecar parallel to the unigram routing index, not mixed into it, so a
// broker that never runs a phrase query never builds or loads it and pays nothing
// (section 4.1.1). The structure mirrors termRoute keyed on an ordered pair, and
// its encode/decode mirror EncodeRouting/DecodeRouting.

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// bigramPosting is one shard's entry for an ordered adjacent pair: the shard id,
// how many of the shard's documents hold the adjacency, and the maximum number of
// times the adjacency occurs in any single one of them. maxFreq plays the same
// role for a phrase bound that a term's shard maxFreq plays for a word bound.
type bigramPosting struct {
	shard   uint32
	df      uint32
	maxFreq uint32
}

// bigramRoute is an ordered pair's fleet-wide document frequency and the per-shard
// postings that sum to it, kept ascending by shard id, mirroring termRoute.
type bigramRoute struct {
	globalDF uint64
	shards   []bigramPosting
}

// BigramKey is an ordered adjacent term pair: A is immediately followed by B in a
// document, within a single field. It keys the phrase routing sidecar and is a
// valid map key because both members are strings.
type BigramKey struct {
	A, B string
}

// BigramRouting routes a phrase to the shards that hold its adjacencies. It is the
// phrase-query counterpart to RoutingIndex: where RoutingIndex routes a word to
// the shards holding the word, BigramRouting routes an adjacent pair to the shards
// holding the pair adjacent. The corpus statistics (N and per-pair global df) come
// from the unigram routing path through a GlobalStats, so a phrase bound is on the
// same IDF scale as a word bound and the cross-shard merge stays exact.
type BigramRouting struct {
	pairs map[BigramKey]*bigramRoute
}

// BigramSource is what one shard exposes to the bigram routing builder: an
// iteration over its adjacent pairs with each pair's document frequency and
// maximum in-document adjacency count. A SearchBuilder satisfies it through
// EachBigram when bigram capture is on.
type BigramSource interface {
	EachBigram(func(a, b string, df int, maxFreq uint32))
}

// BigramRoutingBuilder folds shards into a bigram sidecar one at a time. Unlike
// the unigram RoutingBuilder it does not assign shard ids; the caller passes the
// same shard id the unigram RoutingBuilder assigned for that shard, so a phrase
// route and a bag route name the same shard set.
type BigramRoutingBuilder struct {
	pairs map[BigramKey]*bigramRoute
}

// NewBigramRoutingBuilder returns an empty bigram routing builder.
func NewBigramRoutingBuilder() *BigramRoutingBuilder {
	return &BigramRoutingBuilder{pairs: make(map[BigramKey]*bigramRoute)}
}

// AddShard folds one shard's adjacent-pair dictionary into the sidecar under sid.
// sid must be the id the unigram RoutingBuilder assigned the same shard, because a
// phrase route returns shard ids the cluster maps to file paths exactly as a bag
// route does. Pairs are appended in shard-add order, so the per-pair postings stay
// ascending by shard id, the invariant RoutePhrase and the encoder rely on.
func (b *BigramRoutingBuilder) AddShard(sid uint32, src BigramSource) {
	src.EachBigram(func(a, bb string, df int, maxFreq uint32) {
		if df <= 0 {
			return
		}
		key := BigramKey{A: a, B: bb}
		br := b.pairs[key]
		if br == nil {
			br = &bigramRoute{}
			b.pairs[key] = br
		}
		br.globalDF += uint64(df)
		br.shards = append(br.shards, bigramPosting{shard: sid, df: uint32(df), maxFreq: maxFreq})
	})
}

// Build seals the accumulated shards into a bigram routing sidecar.
func (b *BigramRoutingBuilder) Build() *BigramRouting {
	return &BigramRouting{pairs: b.pairs}
}

// KeepCommon drops every pair either of whose members has a global unigram
// document frequency below minDF, leaving only common-word adjacencies. A pair of
// two rare words already routes the phrase to a handful of shards through the rare
// member, so storing it does not tighten the route; dropping it bounds the sidecar
// to a small multiple of the unigram footprint (section 4.1.1). minDF <= 0 keeps
// every pair, the default the differential test runs against. The unigram df comes
// from the routing index built over the same shards.
func (br *BigramRouting) KeepCommon(unigram *RoutingIndex, minDF int) *BigramRouting {
	if minDF <= 0 {
		return br
	}
	kept := make(map[BigramKey]*bigramRoute, len(br.pairs))
	for key, route := range br.pairs {
		if unigram.DocFreq(key.A) >= minDF && unigram.DocFreq(key.B) >= minDF {
			kept[key] = route
		}
	}
	return &BigramRouting{pairs: kept}
}

// NumPairs is how many distinct adjacent pairs the sidecar tracks.
func (br *BigramRouting) NumPairs() int { return len(br.pairs) }

// PairDocFreq is an ordered pair's document frequency summed across every shard,
// or zero when no shard holds the adjacency.
func (br *BigramRouting) PairDocFreq(a, b string) int {
	if r, ok := br.pairs[BigramKey{A: a, B: b}]; ok {
		return int(r.globalDF)
	}
	return 0
}

// PhraseAdjacencies returns the ordered adjacent pairs of a token stream: for
// tokens t0 t1 t2 it returns (t0,t1) and (t1,t2). A phrase of fewer than two
// tokens has no adjacency and returns nil, the signal a caller uses to fall back
// to bag routing for a single-word query.
func PhraseAdjacencies(terms []string) []BigramKey {
	if len(terms) < 2 {
		return nil
	}
	pairs := make([]BigramKey, 0, len(terms)-1)
	for i := 0; i+1 < len(terms); i++ {
		pairs = append(pairs, BigramKey{A: terms[i], B: terms[i+1]})
	}
	return pairs
}

// RoutePhrase routes a phrase to the shards that hold at least one of its
// adjacencies, ordered by a descending upper bound on the phrase score they can
// contribute, with the bound computed from the pair's global IDF so it is
// comparable across shards. The second return value reports whether the route is
// exact: it is true when every adjacency in the phrase is tracked, so a shard
// absent from the route provably holds no matching document; it is false when an
// adjacency is untracked (filtered out by KeepCommon), in which case the caller
// must fall back to bag routing to stay exact.
//
// The bound mirrors RouteWith one structure over: per adjacency it sums
// idf(pair) * MaxScore(maxFreq) across the pair's shards, where idf(pair) uses the
// pair's global df. The union across adjacencies is what keeps the route exact for
// a multi-word phrase: a document matching "a b c" holds both adjacencies, so its
// shard is in the (a,b) set and in the (b,c) set, so it survives the union.
func (br *BigramRouting) RoutePhrase(terms []string, stats GlobalStats) ([]ShardBound, bool) {
	pairs := PhraseAdjacencies(terms)
	if len(pairs) == 0 {
		return nil, false
	}
	col := Collection{N: stats.NumDocs()}
	bounds := make(map[uint32]Score)
	covered := true
	for _, p := range pairs {
		route, ok := br.pairs[p]
		if !ok {
			// The adjacency is not tracked, so this sidecar cannot prove which shards
			// hold it. The route can no longer be exact; the caller falls back to bag
			// routing, which is always exact.
			covered = false
			continue
		}
		sc := bm25Scorer{idf: col.IDF(int(route.globalDF)), k1: DefaultK1}
		for _, sp := range route.shards {
			bounds[sp.shard] += sc.MaxScore(sp.maxFreq)
		}
	}
	out := make([]ShardBound, 0, len(bounds))
	for s, b := range bounds {
		out = append(out, ShardBound{Shard: s, Bound: b})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Bound != out[j].Bound {
			return out[i].Bound > out[j].Bound
		}
		return out[i].Shard < out[j].Shard
	})
	return out, covered
}

// EncodeBigramRouting serializes a bigram sidecar into a self-describing byte run,
// uvarint-framed throughout, mirroring EncodeRouting: the pair count, then each
// pair as its two front-of-frame strings, its global frequency, and its ascending
// per-shard postings. Pairs are written in sorted key order so the encoding is
// deterministic.
func EncodeBigramRouting(br *BigramRouting) []byte {
	keys := make([]BigramKey, 0, len(br.pairs))
	for k := range br.pairs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].A != keys[j].A {
			return keys[i].A < keys[j].A
		}
		return keys[i].B < keys[j].B
	})
	out := binary.AppendUvarint(nil, uint64(len(keys)))
	for _, k := range keys {
		r := br.pairs[k]
		out = binary.AppendUvarint(out, uint64(len(k.A)))
		out = append(out, k.A...)
		out = binary.AppendUvarint(out, uint64(len(k.B)))
		out = append(out, k.B...)
		out = binary.AppendUvarint(out, r.globalDF)
		out = binary.AppendUvarint(out, uint64(len(r.shards)))
		for _, sp := range r.shards {
			out = binary.AppendUvarint(out, uint64(sp.shard))
			out = binary.AppendUvarint(out, uint64(sp.df))
			out = binary.AppendUvarint(out, uint64(sp.maxFreq))
		}
	}
	return out
}

// DecodeBigramRouting reconstructs a bigram sidecar from the bytes
// EncodeBigramRouting wrote.
func DecodeBigramRouting(b []byte) (*BigramRouting, error) {
	r := &byteReader{b: b}
	nPairs := int(r.uvarint())
	pairs := make(map[BigramKey]*bigramRoute, nPairs)
	for i := 0; i < nPairs; i++ {
		al := int(r.uvarint())
		a := string(r.take(al))
		bl := int(r.uvarint())
		bb := string(r.take(bl))
		route := &bigramRoute{globalDF: r.uvarint()}
		ns := int(r.uvarint())
		route.shards = make([]bigramPosting, ns)
		for j := 0; j < ns; j++ {
			route.shards[j] = bigramPosting{
				shard:   uint32(r.uvarint()),
				df:      uint32(r.uvarint()),
				maxFreq: uint32(r.uvarint()),
			}
		}
		pairs[BigramKey{A: a, B: bb}] = route
	}
	if r.err != nil {
		return nil, fmt.Errorf("search: decode bigram routing: %w", r.err)
	}
	return &BigramRouting{pairs: pairs}, nil
}
