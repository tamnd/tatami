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
//
// The resident form is flat and pointer-free (07-routing-latency.md, the off-heap
// routing note). A map[string]*termRoute over tens of millions of terms is tens of
// gigabytes of pointer-rich memory the garbage collector must trace every cycle,
// which at 10M documents turned a serving box into either second-long GC pauses or
// an out-of-memory kill. The index instead holds its terms as one sorted byte blob
// with an offset table, and its per-shard postings as parallel column arrays in
// compressed-sparse-row form: a term id indexes a slice of the postShard, postDf,
// and postMax columns. The collector sees a handful of slice headers and traces
// nothing inside them, so a routing index of any size costs the GC almost nothing,
// and the same flat columns memory-map directly for an off-heap load.

import (
	"encoding/binary"
	"fmt"
	"sort"
)

// shardPosting is one shard's entry for a term during a build: the shard id, the
// term's document frequency in that shard, and its maximum in-document frequency
// there. It lives only in the builder's accumulator; the sealed index holds the
// same facts as flat columns.
type shardPosting struct {
	shard   uint32
	df      uint32
	maxFreq uint32
}

// termRoute is a term's global document frequency and the per-shard postings that
// sum to it during a build, kept ascending by shard id.
type termRoute struct {
	globalDF uint64
	shards   []shardPosting
}

// RoutingIndex maps a term to the shards that hold it and carries the per-shard
// live-document counts and the collection total. It satisfies GlobalStats, so it
// is both the structure that picks which shards to visit and the source of the
// global IDF those shards are scored with.
//
// It is stored flat and pointer-free. termBlob holds every term's bytes in
// ascending order, termOff its per-term start offsets (len numTerms+1, so term i
// is termBlob[termOff[i]:termOff[i+1]]). globalDF is the per-term collection
// frequency. postOff (len numTerms+1) slices the posting columns: term i's
// postings are postShard/postDf/postMax over [postOff[i]:postOff[i+1]], ascending
// by shard id. A term id is the index a binary search over termBlob resolves.
type RoutingIndex struct {
	termBlob  []byte
	termOff   []uint32
	globalDF  []uint64
	postOff   []uint64
	postShard []uint32
	postDf    []uint32
	postMax   []uint32
	shardDocs []int // live documents per shard, indexed by shard id
	totalDocs int
	// closeFn releases the memory map when the index was loaded from a routing.bin
	// (routing_file.go, scale/11 lever three), and is nil for an in-heap index built
	// or decoded into ordinary slices. The aliased columns point into the mapping, so
	// they must not be used after Close.
	closeFn func() error
}

// Close releases any resource the index holds. For an mmap-backed index it unmaps
// the routing.bin, after which the columns must not be read. For an in-heap index
// it is a no-op, so a caller can Close unconditionally.
func (ri *RoutingIndex) Close() error {
	if ri.closeFn == nil {
		return nil
	}
	err := ri.closeFn()
	ri.closeFn = nil
	return err
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
// routed shard id back to its file path. It accumulates into a map and compacts to
// the flat form in Build; the map is the transient build cost, not the resident
// serving cost.
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

// Build seals the accumulated shards into a flat routing index and releases the
// builder's map so the collector can reclaim it, leaving only the pointer-free
// columns resident.
func (b *RoutingBuilder) Build() *RoutingIndex {
	keys := make([]string, 0, len(b.terms))
	var totalPost, totalBytes int
	for t, tr := range b.terms {
		keys = append(keys, t)
		totalPost += len(tr.shards)
		totalBytes += len(t)
	}
	sort.Strings(keys)

	ri := &RoutingIndex{
		termBlob:  make([]byte, 0, totalBytes),
		termOff:   make([]uint32, len(keys)+1),
		globalDF:  make([]uint64, len(keys)),
		postOff:   make([]uint64, len(keys)+1),
		postShard: make([]uint32, totalPost),
		postDf:    make([]uint32, totalPost),
		postMax:   make([]uint32, totalPost),
		shardDocs: b.shardDocs,
		totalDocs: b.totalDocs,
	}
	var po uint64
	for i, t := range keys {
		tr := b.terms[t]
		ri.termOff[i] = uint32(len(ri.termBlob))
		ri.termBlob = append(ri.termBlob, t...)
		ri.globalDF[i] = tr.globalDF
		ri.postOff[i] = po
		for _, sp := range tr.shards {
			ri.postShard[po] = sp.shard
			ri.postDf[po] = sp.df
			ri.postMax[po] = sp.maxFreq
			po++
		}
		// Release this term's postings backing array as we copy it out, so the map's
		// payload shrinks while the flat columns grow instead of both being resident
		// at once. At tens of millions of terms that halves the compaction's peak, the
		// difference between fitting the build in memory and not.
		delete(b.terms, t)
	}
	ri.termOff[len(keys)] = uint32(len(ri.termBlob))
	ri.postOff[len(keys)] = po
	b.terms = nil
	return ri
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

// numTerms is the distinct term count, the number of entries in the flat columns.
func (ri *RoutingIndex) numTerms() int {
	if len(ri.termOff) == 0 {
		return 0
	}
	return len(ri.termOff) - 1
}

// termAt returns term i's bytes as a sub-slice of the blob, no allocation.
func (ri *RoutingIndex) termAt(i int) []byte {
	return ri.termBlob[ri.termOff[i]:ri.termOff[i+1]]
}

// lookup resolves a term to its id by binary search over the sorted blob, or
// reports absence. It compares the stored bytes against the query string without
// allocating a string per probe.
func (ri *RoutingIndex) lookup(term string) (int, bool) {
	n := ri.numTerms()
	i := sort.Search(n, func(i int) bool { return cmpBytesStr(ri.termAt(i), term) >= 0 })
	if i < n && cmpBytesStr(ri.termAt(i), term) == 0 {
		return i, true
	}
	return -1, false
}

// cmpBytesStr compares a byte slice to a string the way bytes.Compare would, with
// no allocation, returning -1, 0, or 1.
func cmpBytesStr(b []byte, s string) int {
	n := len(b)
	if len(s) < n {
		n = len(s)
	}
	for i := 0; i < n; i++ {
		if b[i] != s[i] {
			if b[i] < s[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(b) < len(s):
		return -1
	case len(b) > len(s):
		return 1
	default:
		return 0
	}
}

// NumDocs is N, the live document count across the whole collection.
func (ri *RoutingIndex) NumDocs() int { return ri.totalDocs }

// NumShards is how many shards the index routes.
func (ri *RoutingIndex) NumShards() int { return len(ri.shardDocs) }

// NumTerms is the distinct term count across the collection.
func (ri *RoutingIndex) NumTerms() int { return ri.numTerms() }

// DocFreq is a term's document frequency summed across every shard, or zero when
// no shard holds it.
func (ri *RoutingIndex) DocFreq(term string) int {
	if i, ok := ri.lookup(term); ok {
		return int(ri.globalDF[i])
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
// for measurement and for re-serializing into a different layout; the order is
// ascending by term then by shard id.
func (ri *RoutingIndex) EachPosting(fn func(term string, shard, df, maxFreq uint32)) {
	n := ri.numTerms()
	for i := 0; i < n; i++ {
		term := string(ri.termAt(i))
		for j := ri.postOff[i]; j < ri.postOff[i+1]; j++ {
			fn(term, ri.postShard[j], ri.postDf[j], ri.postMax[j])
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
		i, ok := ri.lookup(t)
		if !ok {
			continue
		}
		sc := bm25Scorer{idf: col.IDF(stats.DocFreq(t)), k1: DefaultK1}
		for j := ri.postOff[i]; j < ri.postOff[i+1]; j++ {
			bounds[ri.postShard[j]] += sc.MaxScore(ri.postMax[j])
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
// its ascending per-shard postings, terms in ascending order.
func EncodeRouting(ri *RoutingIndex) []byte {
	out := binary.AppendUvarint(nil, uint64(ri.totalDocs))
	out = binary.AppendUvarint(out, uint64(len(ri.shardDocs)))
	for _, d := range ri.shardDocs {
		out = binary.AppendUvarint(out, uint64(d))
	}
	n := ri.numTerms()
	out = binary.AppendUvarint(out, uint64(n))
	for i := 0; i < n; i++ {
		t := ri.termAt(i)
		out = binary.AppendUvarint(out, uint64(len(t)))
		out = append(out, t...)
		out = binary.AppendUvarint(out, ri.globalDF[i])
		s, e := ri.postOff[i], ri.postOff[i+1]
		out = binary.AppendUvarint(out, e-s)
		for j := s; j < e; j++ {
			out = binary.AppendUvarint(out, uint64(ri.postShard[j]))
			out = binary.AppendUvarint(out, uint64(ri.postDf[j]))
			out = binary.AppendUvarint(out, uint64(ri.postMax[j]))
		}
	}
	return out
}

// DecodeRouting reconstructs a flat routing index from the bytes EncodeRouting
// wrote. The terms are already in ascending order on the wire, so it fills the
// flat columns in one pass without sorting.
func DecodeRouting(b []byte) (*RoutingIndex, error) {
	r := &byteReader{b: b}
	total := int(r.uvarint())
	nShards := int(r.uvarint())
	shardDocs := make([]int, nShards)
	for i := range shardDocs {
		shardDocs[i] = int(r.uvarint())
	}
	nTerms := int(r.uvarint())
	ri := &RoutingIndex{
		termOff:   make([]uint32, nTerms+1),
		globalDF:  make([]uint64, nTerms),
		postOff:   make([]uint64, nTerms+1),
		shardDocs: shardDocs,
		totalDocs: total,
	}
	var po uint64
	for i := 0; i < nTerms; i++ {
		tl := int(r.uvarint())
		ri.termOff[i] = uint32(len(ri.termBlob))
		ri.termBlob = append(ri.termBlob, r.take(tl)...)
		ri.globalDF[i] = r.uvarint()
		ri.postOff[i] = po
		ns := int(r.uvarint())
		for j := 0; j < ns; j++ {
			ri.postShard = append(ri.postShard, uint32(r.uvarint()))
			ri.postDf = append(ri.postDf, uint32(r.uvarint()))
			ri.postMax = append(ri.postMax, uint32(r.uvarint()))
			po++
		}
	}
	if nTerms >= 0 {
		ri.termOff[nTerms] = uint32(len(ri.termBlob))
		ri.postOff[nTerms] = po
	}
	if r.err != nil {
		return nil, fmt.Errorf("search: decode routing index: %w", r.err)
	}
	return ri, nil
}
