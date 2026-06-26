package tatami

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
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
