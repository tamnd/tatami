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
// (section 4.1.1). It is stored flat and pointer-free, mirroring RoutingIndex: the
// pairs live as one sorted byte blob with an offset table, and the per-shard
// postings live as parallel column arrays a pair id slices, so a sidecar of tens
// of millions of pairs costs the collector almost nothing.

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// bigramPosting is one shard's entry for an ordered adjacent pair during a build:
// the shard id, how many of the shard's documents hold the adjacency, and the
// maximum number of times the adjacency occurs in any single one of them. maxFreq
// plays the same role for a phrase bound that a term's shard maxFreq plays for a
// word bound.
type bigramPosting struct {
	shard   uint32
	df      uint32
	maxFreq uint32
}

// bigramRoute is an ordered pair's fleet-wide document frequency and the per-shard
// postings that sum to it during a build, kept ascending by shard id.
type bigramRoute struct {
	globalDF uint64
	shards   []bigramPosting
}

// BigramKey is an ordered adjacent term pair: A is immediately followed by B in a
// document, within a single field. It keys the phrase routing sidecar during a
// build and is a valid map key because both members are strings.
type BigramKey struct {
	A, B string
}

// BigramRouting routes a phrase to the shards that hold its adjacencies. It is the
// phrase-query counterpart to RoutingIndex: where RoutingIndex routes a word to
// the shards holding the word, BigramRouting routes an adjacent pair to the shards
// holding the pair adjacent. The corpus statistics (N and per-pair global df) come
// from the unigram routing path through a GlobalStats, so a phrase bound is on the
// same IDF scale as a word bound and the cross-shard merge stays exact.
//
// It is stored flat and pointer-free. keyBlob holds every pair's bytes, A then B,
// in ascending (A, B) order; keyOff (len numPairs+1) marks each pair's start and
// aLen the length of its A member, so pair i is A = keyBlob[keyOff[i]:keyOff[i]+
// aLen[i]] and B = keyBlob[keyOff[i]+aLen[i]:keyOff[i+1]]. postOff (len numPairs+1)
// slices the posting columns exactly as the unigram index does.
type BigramRouting struct {
	keyBlob   []byte
	keyOff    []uint32
	aLen      []uint32
	globalDF  []uint64
	postOff   []uint64
	postShard []uint32
	postDf    []uint32
	postMax   []uint32
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
// route and a bag route name the same shard set. It accumulates into a map and
// compacts to the flat form in Build.
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

// Build seals the accumulated shards into a flat bigram sidecar and releases the
// builder's map for the collector.
func (b *BigramRoutingBuilder) Build() *BigramRouting {
	keys := make([]BigramKey, 0, len(b.pairs))
	var totalPost, totalBytes int
	for k, r := range b.pairs {
		keys = append(keys, k)
		totalPost += len(r.shards)
		totalBytes += len(k.A) + len(k.B)
	}
	sortBigramKeys(keys)
	br := newBigramFlat(len(keys), totalPost, totalBytes)
	var po uint64
	for i, k := range keys {
		r := b.pairs[k]
		br.keyOff[i] = uint32(len(br.keyBlob))
		br.aLen[i] = uint32(len(k.A))
		br.keyBlob = append(br.keyBlob, k.A...)
		br.keyBlob = append(br.keyBlob, k.B...)
		br.globalDF[i] = r.globalDF
		br.postOff[i] = po
		for _, sp := range r.shards {
			br.postShard[po] = sp.shard
			br.postDf[po] = sp.df
			br.postMax[po] = sp.maxFreq
			po++
		}
	}
	br.keyOff[len(keys)] = uint32(len(br.keyBlob))
	br.postOff[len(keys)] = po
	b.pairs = nil
	return br
}

func newBigramFlat(nPairs, nPost, nBytes int) *BigramRouting {
	return &BigramRouting{
		keyBlob:   make([]byte, 0, nBytes),
		keyOff:    make([]uint32, nPairs+1),
		aLen:      make([]uint32, nPairs),
		globalDF:  make([]uint64, nPairs),
		postOff:   make([]uint64, nPairs+1),
		postShard: make([]uint32, nPost),
		postDf:    make([]uint32, nPost),
		postMax:   make([]uint32, nPost),
	}
}

func sortBigramKeys(keys []BigramKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].A != keys[j].A {
			return keys[i].A < keys[j].A
		}
		return keys[i].B < keys[j].B
	})
}

// numPairs is how many distinct adjacent pairs the sidecar tracks.
func (br *BigramRouting) numPairs() int {
	if len(br.keyOff) == 0 {
		return 0
	}
	return len(br.keyOff) - 1
}

// aAt and bAt return pair i's members as sub-slices of the blob, no allocation.
func (br *BigramRouting) aAt(i int) []byte {
	s := br.keyOff[i]
	return br.keyBlob[s : s+br.aLen[i]]
}

func (br *BigramRouting) bAt(i int) []byte {
	s := br.keyOff[i] + br.aLen[i]
	return br.keyBlob[s:br.keyOff[i+1]]
}

// lookup resolves a pair to its id by binary search over the sorted blob,
// comparing A then B without allocating.
func (br *BigramRouting) lookup(a, b string) (int, bool) {
	n := br.numPairs()
	i := sort.Search(n, func(i int) bool {
		if c := cmpBytesStr(br.aAt(i), a); c != 0 {
			return c > 0
		}
		return cmpBytesStr(br.bAt(i), b) >= 0
	})
	if i < n && cmpBytesStr(br.aAt(i), a) == 0 && cmpBytesStr(br.bAt(i), b) == 0 {
		return i, true
	}
	return -1, false
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
	n := br.numPairs()
	keep := make([]int, 0, n)
	var totalPost, totalBytes int
	for i := 0; i < n; i++ {
		a := string(br.aAt(i))
		b := string(br.bAt(i))
		if unigram.DocFreq(a) >= minDF && unigram.DocFreq(b) >= minDF {
			keep = append(keep, i)
			totalPost += int(br.postOff[i+1] - br.postOff[i])
			totalBytes += len(a) + len(b)
		}
	}
	out := newBigramFlat(len(keep), totalPost, totalBytes)
	var po uint64
	for idx, i := range keep {
		out.keyOff[idx] = uint32(len(out.keyBlob))
		out.aLen[idx] = br.aLen[i]
		out.keyBlob = append(out.keyBlob, br.aAt(i)...)
		out.keyBlob = append(out.keyBlob, br.bAt(i)...)
		out.globalDF[idx] = br.globalDF[i]
		out.postOff[idx] = po
		for j := br.postOff[i]; j < br.postOff[i+1]; j++ {
			out.postShard[po] = br.postShard[j]
			out.postDf[po] = br.postDf[j]
			out.postMax[po] = br.postMax[j]
			po++
		}
	}
	out.keyOff[len(keep)] = uint32(len(out.keyBlob))
	out.postOff[len(keep)] = po
	return out
}

// NumPairs is how many distinct adjacent pairs the sidecar tracks.
func (br *BigramRouting) NumPairs() int { return br.numPairs() }

// PairDocFreq is an ordered pair's document frequency summed across every shard,
// or zero when no shard holds the adjacency.
func (br *BigramRouting) PairDocFreq(a, b string) int {
	if i, ok := br.lookup(a, b); ok {
		return int(br.globalDF[i])
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
		i, ok := br.lookup(p.A, p.B)
		if !ok {
			// The adjacency is not tracked, so this sidecar cannot prove which shards
			// hold it. The route can no longer be exact; the caller falls back to bag
			// routing, which is always exact.
			covered = false
			continue
		}
		sc := bm25Scorer{idf: col.IDF(int(br.globalDF[i])), k1: DefaultK1}
		for j := br.postOff[i]; j < br.postOff[i+1]; j++ {
			bounds[br.postShard[j]] += sc.MaxScore(br.postMax[j])
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
// per-shard postings. Pairs are already in sorted key order, so the encoding is
// deterministic.
func EncodeBigramRouting(br *BigramRouting) []byte {
	n := br.numPairs()
	out := binary.AppendUvarint(nil, uint64(n))
	for i := 0; i < n; i++ {
		a := br.aAt(i)
		b := br.bAt(i)
		out = binary.AppendUvarint(out, uint64(len(a)))
		out = append(out, a...)
		out = binary.AppendUvarint(out, uint64(len(b)))
		out = append(out, b...)
		out = binary.AppendUvarint(out, br.globalDF[i])
		s, e := br.postOff[i], br.postOff[i+1]
		out = binary.AppendUvarint(out, e-s)
		for j := s; j < e; j++ {
			out = binary.AppendUvarint(out, uint64(br.postShard[j]))
			out = binary.AppendUvarint(out, uint64(br.postDf[j]))
			out = binary.AppendUvarint(out, uint64(br.postMax[j]))
		}
	}
	return out
}

// DecodeBigramRouting reconstructs a flat bigram sidecar from the bytes
// EncodeBigramRouting wrote. The pairs arrive in ascending order, so it fills the
// flat columns in one pass without sorting.
func DecodeBigramRouting(b []byte) (*BigramRouting, error) {
	r := &byteReader{b: b}
	nPairs := int(r.uvarint())
	br := &BigramRouting{
		keyOff:   make([]uint32, nPairs+1),
		aLen:     make([]uint32, nPairs),
		globalDF: make([]uint64, nPairs),
		postOff:  make([]uint64, nPairs+1),
	}
	var po uint64
	for i := 0; i < nPairs; i++ {
		al := int(r.uvarint())
		br.keyOff[i] = uint32(len(br.keyBlob))
		br.aLen[i] = uint32(al)
		br.keyBlob = append(br.keyBlob, r.take(al)...)
		bl := int(r.uvarint())
		br.keyBlob = append(br.keyBlob, r.take(bl)...)
		br.globalDF[i] = r.uvarint()
		br.postOff[i] = po
		ns := int(r.uvarint())
		for j := 0; j < ns; j++ {
			br.postShard = append(br.postShard, uint32(r.uvarint()))
			br.postDf = append(br.postDf, uint32(r.uvarint()))
			br.postMax = append(br.postMax, uint32(r.uvarint()))
			po++
		}
	}
	br.keyOff[nPairs] = uint32(len(br.keyBlob))
	br.postOff[nPairs] = po
	if r.err != nil {
		return nil, fmt.Errorf("search: decode bigram routing: %w", r.err)
	}
	return br, nil
}
