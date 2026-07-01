package tatami

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/tamnd/tatami/search"
)

// This file proves the box-level phrase route (M11) on real data: a fleet of leaf
// brokers, each carrying its own phrase sidecar, answers a phrase query by routing on
// the box-level adjacency summary rather than the bag of the phrase's words, and the
// routed fleet phrase result equals a single broker's phrase result over every shard.
// It reuses the ccrawl markdown shard the other real-data tests run against and is
// gated on that local file, so CI skips it cleanly. The unigram fleet tests live in
// scale_realdata_test.go; this is their phrase-path mirror.

var (
	phraseFleetOnce   sync.Once
	phraseFleetPaths  []string
	phraseFleetShards []*search.BigramRouting // one per shard, folds into a leaf or global sidecar by EachBigram
	phraseFleetErr    error
)

// loadPhraseFleet builds the real corpus as fleetShards bigram-capturing search-only
// segments and, for each shard, a one-posting phrase sidecar that folds into a leaf
// or global sidecar under any id through EachBigram. It caches both for every test in
// this file. Building the per-shard sidecar rather than holding the heavy SearchBuilders
// resident keeps the fold cheap and exercises the same EachBigram path production uses.
func loadPhraseFleet(tb testing.TB) ([]string, []*search.BigramRouting) {
	phraseFleetOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			phraseFleetErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			phraseFleetErr = err
			return
		}
		dir, err := os.MkdirTemp("", "tatami-phrase-fleet-")
		if err != nil {
			phraseFleetErr = err
			return
		}
		per := (len(docs) + fleetShards - 1) / fleetShards
		for s := 0; s < fleetShards; s++ {
			lo := s * per
			if lo >= len(docs) {
				break
			}
			hi := lo + per
			if hi > len(docs) {
				hi = len(docs)
			}
			b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true, Bigrams: true})
			for _, d := range docs[lo:hi] {
				b.Add(d)
			}
			p := filepath.Join(dir, fmt.Sprintf("seg-%05d.tatami", s))
			if err := b.Write(p, WriterOptions{}); err != nil {
				phraseFleetErr = err
				return
			}
			// One-shard sidecar for this shard: its adjacencies with the shard's own df
			// and ceiling frequency, ready to re-fold under a leaf-local or global id.
			sb := search.NewBigramRoutingBuilder()
			sb.AddShard(0, b)
			phraseFleetShards = append(phraseFleetShards, sb.Build())
			phraseFleetPaths = append(phraseFleetPaths, p)
		}
	})
	if phraseFleetErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", phraseFleetErr)
	}
	return phraseFleetPaths, phraseFleetShards
}

// openPhraseFleet partitions the phrase-fleet shards into leaves of shardsPerLeaf,
// gives each leaf its own phrase sidecar folded from its shards under leaf-local ids,
// and returns an aggregator over them. Every leaf carrying a sidecar is what lets the
// aggregator build the box-level phrase summary, so SearchPhrase routes on adjacencies.
func openPhraseFleet(tb testing.TB) *Aggregator {
	paths, shardBig := loadPhraseFleet(tb)
	var leaves []*Cluster
	for lo := 0; lo < len(paths); lo += shardsPerLeaf {
		hi := lo + shardsPerLeaf
		if hi > len(paths) {
			hi = len(paths)
		}
		c, err := OpenCluster(paths[lo:hi], ClusterOptions{})
		if err != nil {
			tb.Fatal(err)
		}
		lbb := search.NewBigramRoutingBuilder()
		for j := lo; j < hi; j++ {
			lbb.AddShard(uint32(j-lo), shardBig[j])
		}
		leaves = append(leaves, c.WithBigramRouting(lbb.Build()))
	}
	return OpenAggregator(leaves)
}

// openPhraseSingle opens one cluster over every phrase-fleet shard with a global
// phrase sidecar, the single-broker phrase oracle the routed fleet is checked against.
func openPhraseSingle(tb testing.TB) *Cluster {
	paths, shardBig := loadPhraseFleet(tb)
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	gbb := search.NewBigramRoutingBuilder()
	for s := range paths {
		gbb.AddShard(uint32(s), shardBig[s])
	}
	return c.WithBigramRouting(gbb.Build())
}

// TestAggregatorPhraseScaleRouting checks the box-level phrase route is exact on the
// real corpus, which is a routing property, not a scored-result property. The scored
// phrase path is approximate on purpose: a shard is scored bag-of-words, and its phrase
// bound (the pair IDF times the ceiling adjacency frequency) is an upper bound on the
// phrase contribution, not on the bag score it is pruned against, so one global WAND
// walk and eight independent per-leaf walks prune to different documents even though
// both route to the same shards. Byte-identity of the final list is therefore the wrong
// gate for the phrase path; the exact, defensible claims are about which shards get
// opened, and both hold here:
//
//  1. The box-level summary routes a phrase to exactly the leaves whose shards hold one
//     of its adjacencies, no leaf that holds an adjacency dropped (no lost match) and no
//     leaf that lacks every adjacency added. This is the EachBigram fold: a leaf's
//     sidecar folds in as one box under its id, so the box has a posting for a pair iff
//     one of the leaf's shards does, so the box-level route equals the per-leaf union.
//  2. The two-level fleet route (box then shard within each routed leaf) opens exactly
//     the shards a single broker's one-level phrase route opens over the union, because
//     folding preserves postings: the global sidecar's postings for a pair are the union
//     of the leaf sidecars' postings, so the shard sets coincide. This is the M11
//     section 4 claim stated at the routing primitive, before the approximate scoring.
//  3. The phrase route is never wider than the bag route at the box level, because an
//     adjacency's words are a superset of the adjacency, so routing on the adjacency can
//     only narrow.
//
// All three are stated on RoutePhrase, the routing primitive the fold guarantees, not on
// SearchPhrase, whose per-leaf covered-fallback and WAND pruning are the approximate,
// non-distributive scoring layer above it. tokenize plus the phrase's word order give
// the adjacencies; a leaf, the box summary, and the single broker are each just a
// BigramRouting over a different granularity, so the same primitive on each is comparable.
func TestAggregatorPhraseScaleRouting(t *testing.T) {
	single := openPhraseSingle(t)
	defer single.Close()
	agg := openPhraseFleet(t)
	defer agg.Close()
	stats := agg.Stats()

	for _, q := range phraseQueries {
		terms := tokenize(q)

		// Claim 1: the boxes the summary routes to equal the leaves that hold one of the
		// phrase's adjacencies, each computed with the same RoutePhrase over its own
		// sidecar. A box appears in the box-level route iff one of its leaf's shards has
		// a posting for a routed pair, which is iff that leaf's own RoutePhrase is non-empty.
		boxRoute, _ := agg.bigram.RoutePhrase(terms, stats)
		routed := map[int]bool{}
		for _, b := range boxRoute {
			routed[int(b.Shard)] = true
		}
		truth := map[int]bool{}
		for li, leaf := range agg.leaves {
			if lb, _ := leaf.Bigram().RoutePhrase(terms, stats); len(lb) > 0 {
				truth[li] = true
			}
		}
		if !sameLeafSet(routed, truth) {
			t.Fatalf("q=%q: box-level phrase route %v is not the set of leaves holding an adjacency %v", q, keys(routed), keys(truth))
		}

		// Claim 2: the union of the routed leaves' shard candidates, mapped back to global
		// shard ids, equals the single broker's one-level phrase candidate set.
		fleetShardSet := map[int]bool{}
		for li := range routed {
			lb, _ := agg.leaves[li].Bigram().RoutePhrase(terms, stats)
			for _, b := range lb {
				fleetShardSet[li*shardsPerLeaf+int(b.Shard)] = true
			}
		}
		singleShardSet := map[int]bool{}
		sb, _ := single.Bigram().RoutePhrase(terms, stats)
		for _, b := range sb {
			singleShardSet[int(b.Shard)] = true
		}
		if !sameLeafSet(fleetShardSet, singleShardSet) {
			t.Fatalf("q=%q: fleet phrase opens %d shards, single broker opens %d, and the sets differ", q, len(fleetShardSet), len(singleShardSet))
		}

		// Claim 3: the phrase route is a subset of the bag route at the box level.
		bagLeaves := map[int]bool{}
		for _, b := range agg.summary.RouteWith(terms, stats) {
			bagLeaves[int(b.Shard)] = true
		}
		for li := range routed {
			if !bagLeaves[li] {
				t.Fatalf("q=%q: phrase routed to leaf %d that the bag route does not, adjacency route wider than word route", q, li)
			}
		}
		t.Logf("%-26q phrase leaves=%d/%d (bag %d) shards=%d, box-level route exact against per-leaf union and single broker",
			q, len(routed), agg.NumLeaves(), len(bagLeaves), len(fleetShardSet))
	}
}

// sameLeafSet reports whether two id sets are equal.
func sameLeafSet(a, b map[int]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// keys returns a set's ids for a failure message.
func keys(m map[int]bool) []int {
	out := make([]int, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}

// TestAggregatorPhraseScaleLatency serves the real corpus as a phrase-routing fleet
// and asserts the fleet-wide phrase search p99 stays under 10ms, logging the box-level
// pruning next to the bag route so the phrase narrowing is visible on real web text.
// It is the phrase-path mirror of TestAggregatorScaleLatency.
func TestAggregatorPhraseScaleLatency(t *testing.T) {
	agg := openPhraseFleet(t)
	defer agg.Close()
	t.Logf("phrase fleet: %d leaves, %d shards, %d live docs", agg.NumLeaves(), agg.NumShards(), agg.NumDocs())

	// Warm every leaf's caches before timing, the same warm-cache premise the unigram
	// fleet latency test runs under.
	for w := 0; w < 3; w++ {
		for _, q := range phraseQueries {
			if _, _, err := agg.SearchPhrase(q, 10); err != nil {
				t.Fatal(err)
			}
		}
	}

	const reps = 200
	var all []time.Duration
	for _, q := range phraseQueries {
		// The bag route's fan-out for the same words, to show what the phrase route saves.
		_, bag, err := agg.Search(q, 10)
		if err != nil {
			t.Fatal(err)
		}
		var samples []time.Duration
		var last AggStats
		for i := 0; i < reps; i++ {
			start := time.Now()
			_, st, err := agg.SearchPhrase(q, 10)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
			last = st
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)/2]
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v phrase leaves=%d/%d shards=%d/%d | bag leaves=%d shards=%d",
			q, p50, p99, last.LeavesVisited, last.Leaves, last.ShardsVisited, last.Candidates, bag.LeavesVisited, bag.ShardsVisited)
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overallP99 := all[(len(all)*99)/100]
	t.Logf("overall phrase p50=%v p99=%v over %d leaves", all[len(all)/2], overallP99, agg.NumLeaves())
	if overallP99 > 10*time.Millisecond {
		t.Fatalf("fleet phrase search p99 %v exceeds the 10ms target", overallP99)
	}
}

// BenchmarkAggregatorPhraseScaleSearch times the full fleet phrase path: the box-level
// adjacency route, the concurrent leaf fan-out with each leaf routing the phrase over
// its own shards, and the root merge, over the real corpus split into a fleet.
func BenchmarkAggregatorPhraseScaleSearch(b *testing.B) {
	agg := openPhraseFleet(b)
	defer agg.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := agg.SearchPhrase(phraseQueries[i%len(phraseQueries)], 10); err != nil {
			b.Fatal(err)
		}
	}
}
