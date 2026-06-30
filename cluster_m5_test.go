package tatami

import (
	"sort"
	"testing"
	"time"
)

// This file covers M5, cross-shard threshold sharing (scale/07). The broker fills
// the global top-k from the highest-bound shards first, then carries its running
// k-th best score as a seed into every later shard so that shard prunes documents
// that cannot enter the global answer before it scores them. The two things worth
// proving are that it stays exact (the seed never changes the result) and that it
// pays off (the seeded query does less per-shard work, so p99 falls), both on the
// real sharded corpus loadScaleCluster builds.

// withSeed runs fn with the cluster's threshold sharing forced to the given state
// and restores it after, so a single shared cluster can be A/B'd in one test.
func withSeed(c *Cluster, on bool, fn func()) {
	prev := c.noSeed
	c.noSeed = !on
	defer func() { c.noSeed = prev }()
	fn()
}

// TestM5SeededEqualsUnseeded is the exactness gate for threshold sharing: for every
// benchmark query and every k, the routed result with the seed on must equal the
// routed result with the seed off, hit for hit and score for score. This is the
// A/B form of the proof, complementary to TestClusterScaleExact which compares the
// seeded broker against a full fan-out: here the only thing that changes between
// the two runs is the seed, so any divergence is the seed's fault and nothing else.
func TestM5SeededEqualsUnseeded(t *testing.T) {
	c, _ := loadScaleCluster(t)
	for _, q := range benchQueries {
		for _, k := range []int{1, 10, 50, 100} {
			var seeded, plain []ClusterHit
			withSeed(c, true, func() {
				got, _, err := c.Query(q, k)
				if err != nil {
					t.Fatal(err)
				}
				seeded = got
			})
			withSeed(c, false, func() {
				got, _, err := c.Query(q, k)
				if err != nil {
					t.Fatal(err)
				}
				plain = got
			})
			if !sameHits(seeded, plain) {
				t.Fatalf("query %q k=%d: seeded result differs from unseeded\n seeded %d hits\n plain  %d hits", q, k, len(seeded), len(plain))
			}
		}
	}
}

// TestM5SearchSeededEqualsUnseeded is the same exactness A/B for the stored-field
// path (Cluster.Search), which seeds with the running k-th best distinct score and
// over-fetches per shard. The dedup and over-fetch interact with the seed, so it
// gets its own gate: the deduplicated, ranked result must be identical with the
// seed on and off.
func TestM5SearchSeededEqualsUnseeded(t *testing.T) {
	c, _ := loadScaleCluster(t)
	for _, q := range benchQueries {
		for _, k := range []int{1, 10, 50} {
			var seeded, plain []SearchResult
			withSeed(c, true, func() {
				got, _, err := c.Search(q, k)
				if err != nil {
					t.Fatal(err)
				}
				seeded = got
			})
			withSeed(c, false, func() {
				got, _, err := c.Search(q, k)
				if err != nil {
					t.Fatal(err)
				}
				plain = got
			})
			if len(seeded) != len(plain) {
				t.Fatalf("query %q k=%d: seeded returned %d results, unseeded %d", q, k, len(seeded), len(plain))
			}
			for i := range seeded {
				if seeded[i].DocID != plain[i].DocID || seeded[i].Score != plain[i].Score {
					t.Fatalf("query %q k=%d: result %d differs: seeded %s/%v vs plain %s/%v",
						q, k, i, seeded[i].DocID, seeded[i].Score, plain[i].DocID, plain[i].Score)
				}
			}
		}
	}
}

// TestM5ThresholdSharingLatency measures the payoff: it times each benchmark query
// through the routed broker with the seed on and off, on the same warm cache, and
// reports per-query p50/p99 for both plus the shard fan-out. Threshold sharing does
// not change which shards the bound walk visits, so the fan-out is identical; the
// win is lower per-shard WAND work, which shows up as lower p99. The seeded p99 is
// the number the <10ms gate is held against.
func TestM5ThresholdSharingLatency(t *testing.T) {
	c, _ := loadScaleCluster(t)
	t.Logf("cluster: %d shards, %d live docs", c.NumShards(), c.NumDocs())

	const reps = 300
	const k = 10

	bench := func(on bool, q string) (p50, p99 time.Duration, visited, candidates int) {
		var samples []time.Duration
		withSeed(c, on, func() {
			// One warm pass so both arms time a hot cache, not a cold open.
			_, st, _ := c.Query(q, k)
			candidates, visited = st.Candidates, st.Visited
			for i := 0; i < reps; i++ {
				start := time.Now()
				_, _, err := c.Query(q, k)
				if err != nil {
					t.Fatal(err)
				}
				samples = append(samples, time.Since(start))
			}
		})
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		return samples[len(samples)/2], samples[(len(samples)*99)/100], visited, candidates
	}

	var seededAll, plainAll []time.Duration
	for _, q := range benchQueries {
		offP50, offP99, offVis, cand := bench(false, q)
		onP50, onP99, onVis, _ := bench(true, q)
		if onVis != offVis {
			t.Errorf("query %q: seed changed fan-out %d -> %d, it must not", q, offVis, onVis)
		}
		t.Logf("%-26q seed-off p50=%-9v p99=%-9v | seed-on p50=%-9v p99=%-9v | shards %d/%d",
			q, offP50.Round(time.Microsecond), offP99.Round(time.Microsecond),
			onP50.Round(time.Microsecond), onP99.Round(time.Microsecond), onVis, cand)

		withSeed(c, true, func() {
			for i := 0; i < reps; i++ {
				start := time.Now()
				_, _, _ = c.Query(q, k)
				seededAll = append(seededAll, time.Since(start))
			}
		})
		withSeed(c, false, func() {
			for i := 0; i < reps; i++ {
				start := time.Now()
				_, _, _ = c.Query(q, k)
				plainAll = append(plainAll, time.Since(start))
			}
		})
	}

	sort.Slice(seededAll, func(i, j int) bool { return seededAll[i] < seededAll[j] })
	sort.Slice(plainAll, func(i, j int) bool { return plainAll[i] < plainAll[j] })
	seededP99 := seededAll[(len(seededAll)*99)/100]
	plainP99 := plainAll[(len(plainAll)*99)/100]
	t.Logf("overall seed-off p50=%v p99=%v | seed-on p50=%v p99=%v",
		plainAll[len(plainAll)/2].Round(time.Microsecond), plainP99.Round(time.Microsecond),
		seededAll[len(seededAll)/2].Round(time.Microsecond), seededP99.Round(time.Microsecond))
	if seededP99 > 10*time.Millisecond {
		t.Fatalf("seeded routed retrieval p99 %v exceeds the 10ms target", seededP99)
	}
}
