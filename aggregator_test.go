package tatami

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/tamnd/tatami/search"
)

// These tests prove the aggregator tier against a single-broker oracle: fanning a
// query out to several leaf Clusters and merging must return exactly what one
// Cluster over every shard returns. They run on synthetic segments so CI needs no
// crawl data (13-search-only-and-scale.md).

// partition splits paths into nLeaves contiguous groups and opens a Cluster over
// each, returning the leaves. A contiguous split mirrors how a real fleet assigns
// shard ranges to leaves.
func partition(t *testing.T, paths []string, nLeaves, cacheSize int) []*Cluster {
	t.Helper()
	per := (len(paths) + nLeaves - 1) / nLeaves
	var leaves []*Cluster
	for i := 0; i < nLeaves; i++ {
		lo := i * per
		if lo >= len(paths) {
			break
		}
		hi := lo + per
		if hi > len(paths) {
			hi = len(paths)
		}
		c, err := OpenCluster(paths[lo:hi], ClusterOptions{CacheSize: cacheSize})
		if err != nil {
			t.Fatal(err)
		}
		leaves = append(leaves, c)
	}
	return leaves
}

func sameResults(a, b []SearchResult) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].DocID != b[i].DocID || a[i].Score != b[i].Score {
			return false
		}
	}
	return true
}

// TestAggregatorExactVsSingleCluster checks that the fleet top-k equals a single
// broker's top-k over the same shards, across leaf counts. The aggregator scores
// every leaf against fleet-wide statistics and merges with the same total order,
// so pruning per leaf and merging across leaves changes nothing.
func TestAggregatorExactVsSingleCluster(t *testing.T) {
	dir := t.TempDir()
	hot := []int{100, 90, 80, 70, 5, 4, 3, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	paths := clusterCorpus(t, dir, hot)

	single, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()

	for _, nLeaves := range []int{1, 2, 4, 7, 20} {
		leaves := partition(t, paths, nLeaves, 0)
		agg := OpenAggregator(leaves)
		for _, q := range []string{"alpha", "common", "common alpha", "missing"} {
			for _, k := range []int{1, 3, 10, 25} {
				want, _, err := single.Search(q, k)
				if err != nil {
					t.Fatal(err)
				}
				got, st, err := agg.Search(q, k)
				if err != nil {
					t.Fatal(err)
				}
				if !sameResults(got, want) {
					t.Fatalf("leaves=%d q=%q k=%d:\n got  %+v\n want %+v", nLeaves, q, k, got, want)
				}
				if st.Leaves != len(leaves) {
					t.Fatalf("leaves=%d q=%q: AggStats.Leaves=%d", nLeaves, q, st.Leaves)
				}
			}
		}
		_ = agg.Close()
	}
}

// TestAggregatorRoutesToBoxes checks the lever-four win: the box-level summary routes
// a query to the leaves that hold it and the fan-out skips the rest, so a term living
// in one leaf's shards visits one leaf while a term in every leaf still visits every
// leaf. The routing is what prunes, so the result stays exact alongside, asserted
// against the single-broker oracle.
func TestAggregatorRoutesToBoxes(t *testing.T) {
	dir := t.TempDir()
	// Twenty shards, every shard holds "common"; each shard's filler docs carry
	// per-shard unique terms, so a unique term lives in exactly one shard, hence one
	// leaf once the shards are split into leaves.
	hot := make([]int, 20)
	for i := range hot {
		hot[i] = 1
	}
	paths := clusterCorpus(t, dir, hot)

	single, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()

	leaves := partition(t, paths, 4, 0) // 4 leaves of 5 shards
	agg := OpenAggregator(leaves)
	defer agg.Close()

	// A term in every shard fans out to every leaf.
	_, cs, err := agg.Search("common", 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("common: visited %d of %d leaves", cs.LeavesVisited, cs.Leaves)
	if cs.LeavesVisited != agg.NumLeaves() {
		t.Fatalf("common should visit all %d leaves, visited %d", agg.NumLeaves(), cs.LeavesVisited)
	}

	// A term unique to shard 2 (in leaf 0, shards 0-4) visits exactly one leaf.
	const uniq = "unique0203" // shard 02, filler doc 03
	_, us, err := agg.Search(uniq, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%s: visited %d of %d leaves", uniq, us.LeavesVisited, us.Leaves)
	if us.LeavesVisited != 1 {
		t.Fatalf("a shard-unique term should visit 1 leaf, visited %d", us.LeavesVisited)
	}

	// The routing did not change the answer, for a per-leaf term and a fleet-wide one.
	for _, q := range []string{uniq, "common", "common alpha"} {
		for _, k := range []int{1, 5, 25} {
			want, _, err := single.Search(q, k)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := agg.Search(q, k)
			if err != nil {
				t.Fatal(err)
			}
			if !sameResults(got, want) {
				t.Fatalf("q=%q k=%d: routed fleet differs from single broker\n got  %+v\n want %+v", q, k, got, want)
			}
		}
	}
}

// TestAggregatorPhraseRoutesToBoxes checks the box-level phrase route: a phrase whose
// words are common but whose adjacency is rare fans out to every leaf as a bag of
// words, but routes only to the leaves holding the adjacency as a phrase, the fleet
// analogue of a Cluster's own phrase routing (M5) one level up. Every shard holds
// "open" and "source" as words but only two shards, falling in two leaves, hold them
// adjacent, so the bag route visits all four leaves and the phrase route visits two,
// with the answer exact against a single-broker phrase oracle.
func TestAggregatorPhraseRoutesToBoxes(t *testing.T) {
	dir := t.TempDir()
	const nShards, nLeaves = 20, 4
	adj := map[int]bool{2: true, 13: true} // shards holding the "open source" adjacency, in leaves 0 and 2

	var paths []string
	builders := make([]*SearchBuilder, nShards)
	for s := 0; s < nShards; s++ {
		b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true, Bigrams: true})
		for i := 0; i < 10; i++ {
			// "open" and "source" appear as words but not adjacent, so the bag route of
			// "open source" hits this shard while the phrase route does not.
			b.Add(SearchDoc{
				DocID: fmt.Sprintf("doc-%02d-%02d", s, i),
				URL:   fmt.Sprintf("https://shard%02d.example/%d", s, i),
				Title: fmt.Sprintf("shard %d doc %d", s, i),
				Body:  "open alpha source beta",
			})
		}
		if adj[s] {
			b.Add(SearchDoc{
				DocID: fmt.Sprintf("doc-%02d-phrase", s),
				URL:   fmt.Sprintf("https://shard%02d.example/phrase", s),
				Title: fmt.Sprintf("shard %d phrase", s),
				Body:  "open source software",
			})
		}
		p := filepath.Join(dir, fmt.Sprintf("seg-%03d.tatami", s))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatal(err)
		}
		builders[s] = b
		paths = append(paths, p)
	}

	// Single-broker phrase oracle: one cluster over every shard with a global sidecar.
	single, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	gbb := search.NewBigramRoutingBuilder()
	for s := range paths {
		gbb.AddShard(uint32(s), builders[s])
	}
	single = single.WithBigramRouting(gbb.Build())

	// Partition into four leaves of five shards, each leaf carrying its own sidecar
	// over its shards under leaf-local ids, exactly as a real box would.
	per := nShards / nLeaves
	var leaves []*Cluster
	for lf := 0; lf < nLeaves; lf++ {
		lo := lf * per
		hi := lo + per
		lc, err := OpenCluster(paths[lo:hi], ClusterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		lbb := search.NewBigramRoutingBuilder()
		for j := lo; j < hi; j++ {
			lbb.AddShard(uint32(j-lo), builders[j])
		}
		leaves = append(leaves, lc.WithBigramRouting(lbb.Build()))
	}
	agg := OpenAggregator(leaves)
	defer agg.Close()

	// Bag route: both words live in every shard, so the fan-out hits every leaf.
	_, bag, err := agg.Search("open source", 10)
	if err != nil {
		t.Fatal(err)
	}
	if bag.LeavesVisited != agg.NumLeaves() {
		t.Fatalf("bag route should visit all %d leaves, visited %d", agg.NumLeaves(), bag.LeavesVisited)
	}

	// Phrase route: the adjacency lives in shards 2 and 13, in leaves 0 and 2, so the
	// fan-out visits exactly those two leaves and fewer shards than the bag route.
	_, ph, err := agg.SearchPhrase("open source", 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("open source: bag visited %d leaves/%d shards, phrase visited %d leaves/%d shards",
		bag.LeavesVisited, bag.ShardsVisited, ph.LeavesVisited, ph.ShardsVisited)
	if ph.LeavesVisited != 2 {
		t.Fatalf("phrase route should visit 2 leaves, visited %d", ph.LeavesVisited)
	}
	if ph.ShardsVisited >= bag.ShardsVisited {
		t.Fatalf("phrase route visited %d shards, bag route %d; phrase should visit fewer", ph.ShardsVisited, bag.ShardsVisited)
	}

	// The routing did not change the answer, for a rare adjacency, a longer phrase, and
	// a common adjacency, each checked against the single-broker phrase oracle.
	for _, q := range []string{"open source", "open source software", "source beta"} {
		for _, k := range []int{1, 5, 25} {
			want, _, err := single.SearchPhrase(q, k)
			if err != nil {
				t.Fatal(err)
			}
			got, _, err := agg.SearchPhrase(q, k)
			if err != nil {
				t.Fatal(err)
			}
			if !sameResults(got, want) {
				t.Fatalf("q=%q k=%d: routed fleet phrase differs from single broker\n got  %+v\n want %+v", q, k, got, want)
			}
		}
	}
}

// TestAggregatorFleetStats checks that the aggregator's fleet statistics are the
// sum across leaves, the property that makes a leaf's IDF identical to a single
// broker's and the cross-leaf merge exact.
func TestAggregatorFleetStats(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{10, 20, 30, 40, 50, 60})
	single, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()

	leaves := partition(t, paths, 3, 0)
	agg := OpenAggregator(leaves)
	defer agg.Close()

	if agg.NumDocs() != single.NumDocs() {
		t.Fatalf("fleet NumDocs=%d, single=%d", agg.NumDocs(), single.NumDocs())
	}
	if agg.NumShards() != single.NumShards() {
		t.Fatalf("fleet NumShards=%d, single=%d", agg.NumShards(), single.NumShards())
	}
	for _, term := range []string{"alpha", "common", "filler1", "missing"} {
		if got, want := agg.Stats().DocFreq(term), single.Routing().DocFreq(term); got != want {
			t.Fatalf("DocFreq(%q)=%d, single=%d", term, got, want)
		}
	}
}

// TestAggregatorDedupAcrossLeaves checks that a page surfaced by more than one leaf
// after a recrawl collapses to one result keyed by its stable doc_id, the same
// dedup the single broker does within itself.
func TestAggregatorDedupAcrossLeaves(t *testing.T) {
	dir := t.TempDir()
	// Two leaves, each holding a shard that contains the very same document.
	mk := func(name string) string {
		b := NewSearchBuilder()
		b.Add(SearchDoc{DocID: "shared-doc", URL: "https://example.com/shared", Title: "Shared", Body: "alpha alpha beta"})
		for i := 0; i < 5; i++ {
			b.Add(SearchDoc{DocID: fmt.Sprintf("%s-%d", name, i), URL: fmt.Sprintf("https://example.com/%s/%d", name, i), Title: "x", Body: "alpha filler"})
		}
		p := filepath.Join(dir, name+".tatami")
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatal(err)
		}
		return p
	}
	leftPaths := []string{mk("left")}
	rightPaths := []string{mk("right")}
	left, err := OpenCluster(leftPaths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := OpenCluster(rightPaths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	agg := OpenAggregator([]*Cluster{left, right})
	defer agg.Close()

	res, _, err := agg.Search("alpha beta", 10)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, r := range res {
		if r.DocID == "shared-doc" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("shared-doc appeared %d times across the fleet, want 1: %+v", count, res)
	}
}

// TestAggregatorConcurrentQueries runs many queries at once to shake out a data
// race in the fan-out. It is meaningful under the race detector.
func TestAggregatorConcurrentQueries(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{50, 40, 30, 20, 10, 5, 4, 3, 2, 1})
	leaves := partition(t, paths, 4, 0)
	agg := OpenAggregator(leaves)
	defer agg.Close()

	queries := []string{"alpha", "common", "common alpha", "filler3"}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, _, err := agg.Search(queries[i%len(queries)], 10); err != nil {
				t.Errorf("query error: %v", err)
			}
		}(i)
	}
	wg.Wait()
}
