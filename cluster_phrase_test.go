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

// This file proves the phrase-routing fan-out lever on real data (07-routing-latency.md,
// section 4.1). A bag-of-words query of common terms unions into nearly every shard;
// the same terms as a phrase route only to the shards holding the adjacency, which is
// far fewer. The test asserts two things on the real ccrawl corpus: the phrase route
// is exact (it equals the set of shards that actually hold one of the phrase's
// adjacencies, computed by brute force) and it is far narrower than the bag route, and
// it stays under the 10ms budget. Like the other real-data tests it is gated on a local
// shard file, so CI skips it cleanly.

var (
	phraseClusterOnce  sync.Once
	phraseClusterC     *Cluster
	phraseClusterDocs  []SearchDoc
	phraseClusterRange [][2]int // per shard [lo, hi) into phraseClusterDocs
	phraseClusterErr   error
)

// loadPhraseCluster splits the real shard into many small shards exactly as
// loadScaleCluster does, but builds each segment with bigram capture on and folds a
// bigram routing sidecar in the same shard-id order, so the cluster can answer phrase
// queries. It keeps the documents and per-shard ranges resident so the test can
// recompute adjacency presence by brute force.
func loadPhraseCluster(tb testing.TB) (*Cluster, []SearchDoc, [][2]int) {
	phraseClusterOnce.Do(func() {
		src := shardPath()
		if _, err := os.Stat(src); err != nil {
			phraseClusterErr = err
			return
		}
		docs, err := readMarkdownShard(src)
		if err != nil {
			phraseClusterErr = err
			return
		}
		dir, err := os.MkdirTemp("", "tatami-phrase-")
		if err != nil {
			phraseClusterErr = err
			return
		}
		per := (len(docs) + numClusterShards - 1) / numClusterShards
		var paths []string
		var ranges [][2]int
		bb := search.NewBigramRoutingBuilder()
		for s := 0; s < numClusterShards; s++ {
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
				phraseClusterErr = err
				return
			}
			bb.AddShard(uint32(len(paths)), b)
			paths = append(paths, p)
			ranges = append(ranges, [2]int{lo, hi})
		}
		c, err := OpenCluster(paths, ClusterOptions{CacheSize: clusterCacheCap})
		if err != nil {
			phraseClusterErr = err
			return
		}
		phraseClusterC = c.WithBigramRouting(bb.Build())
		phraseClusterDocs = docs
		phraseClusterRange = ranges
	})
	if phraseClusterErr != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", phraseClusterErr)
	}
	return phraseClusterC, phraseClusterDocs, phraseClusterRange
}

// shardsHoldingPhrase is the brute-force ground truth: the set of shard ids holding at
// least one document in which the phrase's tokens occur adjacent in some field. It
// tokenizes with the same analyzer the indexer uses, so it checks the routing sidecar
// against the same notion of adjacency the build captured.
func shardsHoldingPhrase(query string, docs []SearchDoc, ranges [][2]int) map[uint32]bool {
	terms := tokenize(query)
	pairs := search.PhraseAdjacencies(terms)
	out := map[uint32]bool{}
	if len(pairs) == 0 {
		return out
	}
	for sid, r := range ranges {
		for _, d := range docs[r[0]:r[1]] {
			if docHoldsAnyPair(d, pairs) {
				out[uint32(sid)] = true
				break
			}
		}
	}
	return out
}

// docHoldsAnyPair reports whether any of the pairs occurs adjacent within a single
// field of the document, the same within-field adjacency the builder captures.
func docHoldsAnyPair(d SearchDoc, pairs []search.BigramKey) bool {
	for _, text := range []string{d.Body, d.Title, d.Anchor, d.URL} {
		toks := tokenize(text)
		for i := 0; i+1 < len(toks); i++ {
			for _, p := range pairs {
				if toks[i] == p.A && toks[i+1] == p.B {
					return true
				}
			}
		}
	}
	return false
}

// The multi-word phrase queries come from the shared phraseQueries set in
// serve_scale_test.go.

// TestClusterPhraseRouteExact asserts the phrase route equals the brute-force set of
// shards holding the adjacency, on every phrase query, for the real corpus. This is
// the exactness guarantee: the route never drops a shard that could hold a phrase
// match, and never opens one that cannot.
func TestClusterPhraseRouteExact(t *testing.T) {
	c, docs, ranges := loadPhraseCluster(t)
	for _, q := range phraseQueries {
		terms := tokenize(q)
		routed, covered := c.bigram.RoutePhrase(terms, c.routing)
		if !covered {
			t.Fatalf("%q: every adjacency should be tracked in keep-all mode", q)
		}
		got := map[uint32]bool{}
		for _, sb := range routed {
			got[sb.Shard] = true
		}
		want := shardsHoldingPhrase(q, docs, ranges)
		if len(got) != len(want) {
			t.Fatalf("%q: routed to %d shards, brute force found %d", q, len(got), len(want))
		}
		for s := range want {
			if !got[s] {
				t.Fatalf("%q: brute force shard %d missing from the phrase route", q, s)
			}
		}
	}
}

// TestClusterPhraseFanout reports, per phrase query, how many shards the bag route
// visits versus the phrase route, and asserts the phrase route is no wider than the
// bag route (it is the adjacency-narrowed subset). The log is the headline: a common
// phrase that bags into most shards routes to a fraction of them.
func TestClusterPhraseFanout(t *testing.T) {
	c, _, _ := loadPhraseCluster(t)
	for _, q := range phraseQueries {
		_, bagStats, err := c.QueryWith(q, 10, c.routing)
		if err != nil {
			t.Fatal(err)
		}
		_, phStats, err := c.QueryPhraseWith(q, 10, c.routing)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%-26q bag candidates=%-4d phrase candidates=%-4d (%.0f%% of bag)",
			q, bagStats.Candidates, phStats.Candidates,
			100*float64(phStats.Candidates)/float64(max1(bagStats.Candidates)))
		if phStats.Candidates > bagStats.Candidates {
			t.Fatalf("%q: phrase route %d wider than bag route %d", q, phStats.Candidates, bagStats.Candidates)
		}
	}
}

// TestClusterPhraseLatency asserts the phrase retrieval p99 stays under 10ms on the
// real corpus, the same budget the bag path holds.
func TestClusterPhraseLatency(t *testing.T) {
	c, _, _ := loadPhraseCluster(t)
	// Warm the routed shards into the open-segment cache first: the 10ms budget is a
	// warm-serving claim (07-routing-latency.md, section 1.3), so a query that finds
	// its shards resident runs the posting walk from memory with no cold decode on the
	// path. The cold first-touch cost is measured separately by the scale test.
	for _, q := range phraseQueries {
		for i := 0; i < 5; i++ {
			if _, _, err := c.QueryPhrase(q, 10); err != nil {
				t.Fatal(err)
			}
		}
	}

	const reps = 200
	var all []time.Duration
	for _, q := range phraseQueries {
		var samples []time.Duration
		var last QueryStats
		for i := 0; i < reps; i++ {
			start := time.Now()
			_, st, err := c.QueryPhrase(q, 10)
			if err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
			last = st
		}
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p99 := samples[(len(samples)*99)/100]
		t.Logf("%-26q p50=%-10v p99=%-10v visited=%d/%d", q, samples[len(samples)/2], p99, last.Visited, last.Candidates)
		all = append(all, samples...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	overall := all[(len(all)*99)/100]
	t.Logf("overall phrase p99=%v", overall)
	if overall > 10*time.Millisecond {
		t.Fatalf("phrase retrieval p99 %v exceeds the 10ms target", overall)
	}
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
