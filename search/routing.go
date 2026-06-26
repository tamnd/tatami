package search

// A collection of a hundred thousand shards cannot answer a query by fanning out
// to every shard: even at a few hundred microseconds each the fan-out is seconds,
// and a process cannot hold that many files open. The routing index is what makes
// the cost scale with the number of shards that can actually contribute to the
// top-k rather than the number of shards that exist (12-distributed-serving.md).
//
// It is a map from a term to the shards that contain it, with each shard's
// document frequency and maximum in-document frequency for that term. Two things
// fall out of it. First, a term's global document frequency is the sum of its
// per-shard frequencies, so a broker computes a corpus-wide IDF without opening a
// shard. Second, the best score a shard can contribute to a query is bounded by
// the sum over the query terms of globalIDF(t) times the saturated shard-maximum
// frequency; ranking shards by that bound and walking them in descending order
// lets the broker stop the moment the next shard's bound cannot beat the current
// k-th best score. Because the bound is a true upper bound on anything the shard
// could score, that early stop is safe and the pruned top-k is exact.
//
// The structure holds only the dictionary-level facts (df and a single max
// frequency per term per shard), never posting payloads, so it is far smaller
// than the shards it routes and is meant to live resident in the broker's memory
// or be loaded from a persisted sidecar.

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// shardPosting is one shard's entry for a term: the shard id, the term's document
// frequency in that shard, and its maximum in-document frequency there.
type shardPosting struct {
	shard   uint32
	df      uint32
	maxFreq uint32
}

// termRoute is a term's global document frequency and the per-shard postings that
// sum to it, kept ascending by shard id.
type termRoute struct {
	globalDF uint64
	shards   []shardPosting
}

// RoutingIndex maps a term to the shards that hold it and carries the per-shard
// live-document counts and the collection total. It satisfies GlobalStats, so it
// is both the structure that picks which shards to visit and the source of the
// global IDF those shards are scored with.
type RoutingIndex struct {
	terms     map[string]*termRoute
	shardDocs []int // live documents per shard, indexed by shard id
	totalDocs int
}

// ShardBound names a shard the broker should consider and the upper bound on the
// score any document in it can contribute to the query, computed with global IDF
// so the bound is comparable across shards.
type ShardBound struct {
	Shard uint32
	Bound Score
}

// RoutingBuilder folds shards into a routing index one at a time, assigning each
// AddShard call the next shard id, so a builder over a directory of files holds
// only one shard's dictionary in flight rather than all of them. The shard ids it
// assigns match the order shards are added, which a broker relies on to map a
// routed shard id back to its file path.
type RoutingBuilder struct {
	terms     map[string]*termRoute
	shardDocs []int
	totalDocs int
}

// NewRoutingBuilder returns an empty builder.
func NewRoutingBuilder() *RoutingBuilder {
	return &RoutingBuilder{terms: make(map[string]*termRoute)}
}

// RoutingSource is what one shard exposes to the routing builder: its live
// document count and an iteration over its terms with each term's document
// frequency and maximum in-document frequency. An Inverted satisfies it through
// LiveDocs and EachTerm.
type RoutingSource interface {
	LiveDocs() int
	EachTerm(func(term string, df int, maxFreq uint32))
}

// AddShard folds one shard into the index under the next shard id and returns
// that id. It reads only the shard's dictionary, so the per-shard cost is the
// term count, not the posting bytes.
func (b *RoutingBuilder) AddShard(src RoutingSource) uint32 {
	sid := uint32(len(b.shardDocs))
	live := src.LiveDocs()
	b.shardDocs = append(b.shardDocs, live)
	b.totalDocs += live
	src.EachTerm(func(term string, df int, maxFreq uint32) {
		tr := b.terms[term]
		if tr == nil {
			tr = &termRoute{}
			b.terms[term] = tr
		}
		tr.globalDF += uint64(df)
		tr.shards = append(tr.shards, shardPosting{shard: sid, df: uint32(df), maxFreq: maxFreq})
	})
	return sid
}

// Build seals the accumulated shards into a routing index.
func (b *RoutingBuilder) Build() *RoutingIndex {
	return &RoutingIndex{terms: b.terms, shardDocs: b.shardDocs, totalDocs: b.totalDocs}
}

// BuildRouting builds a routing index over a fixed set of shards, assigning shard
// ids in slice order. It is the convenience wrapper around RoutingBuilder for
// callers that already hold every shard's source.
func BuildRouting(shards []RoutingSource) *RoutingIndex {
	b := NewRoutingBuilder()
	for _, s := range shards {
		b.AddShard(s)
	}
	return b.Build()
}

// NumDocs is N, the live document count across the whole collection.
func (ri *RoutingIndex) NumDocs() int { return ri.totalDocs }

// NumShards is how many shards the index routes.
func (ri *RoutingIndex) NumShards() int { return len(ri.shardDocs) }

// NumTerms is the distinct term count across the collection.
func (ri *RoutingIndex) NumTerms() int { return len(ri.terms) }

// DocFreq is a term's document frequency summed across every shard, or zero when
// no shard holds it.
func (ri *RoutingIndex) DocFreq(term string) int {
	if tr, ok := ri.terms[term]; ok {
		return int(tr.globalDF)
	}
	return 0
}

// ShardDocs is the live document count of one shard, or zero when the id is out
// of range.
func (ri *RoutingIndex) ShardDocs(shard int) int {
	if shard < 0 || shard >= len(ri.shardDocs) {
		return 0
	}
	return ri.shardDocs[shard]
}

// EachPosting calls fn for every (term, shard) posting in the index, with the
// term's document frequency and maximum in-document frequency in that shard. It is
// for measurement and for re-serializing into a different layout; the order is the
// index's internal map order and is not stable.
func (ri *RoutingIndex) EachPosting(fn func(term string, shard, df, maxFreq uint32)) {
	for term, tr := range ri.terms {
		for _, sp := range tr.shards {
			fn(term, sp.shard, sp.df, sp.maxFreq)
		}
	}
}

// Route returns the shards that hold at least one query term, ordered by a
// descending upper bound on the score they can contribute, with the bound
// computed from global IDF so it is comparable across shards. A broker walks
// these in order and stops as soon as a shard's bound falls below the current
// k-th best score (12-distributed-serving.md).
//
// The query terms are taken as given, repeats included, so the bound matches what
// a shard's SearchWith would score for the same token stream: a term that appears
// twice in the query contributes its bound twice, exactly as the shard scores it
// twice. That keeps the bound a true upper bound and the early stop exact.
func (ri *RoutingIndex) Route(terms []string) []ShardBound {
	return ri.RouteWith(terms, ri)
}

// RouteWith is Route with the corpus statistics supplied from outside rather than
// taken from this index. An aggregator over many leaves passes fleet-wide N and
// per-term document frequency here, so a leaf's shard bounds are computed against
// the same IDF every other leaf scores with. That keeps the bound a true upper
// bound on the fleet-scale score, so the cross-leaf top-k stays exact and the
// per-leaf pruning stays safe (13-search-only-and-scale.md). Passing the index
// itself reproduces Route, the single-broker path.
func (ri *RoutingIndex) RouteWith(terms []string, stats GlobalStats) []ShardBound {
	col := Collection{N: stats.NumDocs()}
	bounds := make(map[uint32]Score)
	for _, t := range terms {
		tr := ri.terms[t]
		if tr == nil {
			continue
		}
		sc := bm25Scorer{idf: col.IDF(stats.DocFreq(t)), k1: DefaultK1}
		for _, sp := range tr.shards {
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
	return out
}

// EncodeRouting serializes a routing index into a single self-describing byte run
// so a broker can persist it as a sidecar and reload it without re-scanning every
// shard's dictionary. The layout is uvarint-framed throughout: the collection
// total, the per-shard live counts, then each term with its global frequency and
// its ascending per-shard postings.
func EncodeRouting(ri *RoutingIndex) []byte {
	out := binary.AppendUvarint(nil, uint64(ri.totalDocs))
	out = binary.AppendUvarint(out, uint64(len(ri.shardDocs)))
	for _, d := range ri.shardDocs {
		out = binary.AppendUvarint(out, uint64(d))
	}
	// Terms in sorted order so the encoding is deterministic.
	keys := make([]string, 0, len(ri.terms))
	for t := range ri.terms {
		keys = append(keys, t)
	}
	sort.Strings(keys)
	out = binary.AppendUvarint(out, uint64(len(keys)))
	for _, t := range keys {
		tr := ri.terms[t]
		out = binary.AppendUvarint(out, uint64(len(t)))
		out = append(out, t...)
		out = binary.AppendUvarint(out, tr.globalDF)
		out = binary.AppendUvarint(out, uint64(len(tr.shards)))
		for _, sp := range tr.shards {
			out = binary.AppendUvarint(out, uint64(sp.shard))
			out = binary.AppendUvarint(out, uint64(sp.df))
			out = binary.AppendUvarint(out, uint64(sp.maxFreq))
		}
	}
	return out
}

// DecodeRouting reconstructs a routing index from the bytes EncodeRouting wrote.
func DecodeRouting(b []byte) (*RoutingIndex, error) {
	r := &byteReader{b: b}
	total := int(r.uvarint())
	nShards := int(r.uvarint())
	shardDocs := make([]int, nShards)
	for i := range shardDocs {
		shardDocs[i] = int(r.uvarint())
	}
	nTerms := int(r.uvarint())
	terms := make(map[string]*termRoute, nTerms)
	for i := 0; i < nTerms; i++ {
		tl := int(r.uvarint())
		term := string(r.take(tl))
		tr := &termRoute{globalDF: r.uvarint()}
		ns := int(r.uvarint())
		tr.shards = make([]shardPosting, ns)
		for j := 0; j < ns; j++ {
			tr.shards[j] = shardPosting{
				shard:   uint32(r.uvarint()),
				df:      uint32(r.uvarint()),
				maxFreq: uint32(r.uvarint()),
			}
		}
		terms[term] = tr
	}
	if r.err != nil {
		return nil, fmt.Errorf("search: decode routing index: %w", r.err)
	}
	return &RoutingIndex{terms: terms, shardDocs: shardDocs, totalDocs: total}, nil
}
