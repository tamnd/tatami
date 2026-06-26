package tatami

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/tamnd/tatami/search"
)

// These tests prove the broker against a brute-force oracle: a Cluster that prunes
// shards by their routing bound must return the same top-k a walk over every shard
// returns. They use synthetic segments so the corpus is controlled and the test
// runs in CI without crawl data (12-distributed-serving.md).

// clusterCorpus writes nShards synthetic search segments into a temp dir and
// returns their paths. Every shard holds the term "common" so a query on it routes
// to all shards, and each shard carries one document with a controlled number of
// "alpha" occurrences so the per-shard impact bounds form a steep gradient, the
// condition that makes pruning observable. Shard i's hot document repeats "alpha"
// hot[i] times.
func clusterCorpus(t *testing.T, dir string, hot []int) []string {
	t.Helper()
	var paths []string
	for s, h := range hot {
		b := NewSearchBuilder()
		// One hot document with h repeats of alpha, plus filler docs each with a
		// single alpha and the shared common term, so every shard is a candidate.
		body := "common"
		for i := 0; i < h; i++ {
			body += " alpha"
		}
		b.Add(SearchDoc{
			DocID: fmt.Sprintf("doc-%02d-hot", s),
			URL:   fmt.Sprintf("https://shard%02d.example/hot", s),
			Title: fmt.Sprintf("shard %d hot", s),
			Body:  body,
		})
		for i := 0; i < 20; i++ {
			b.Add(SearchDoc{
				DocID: fmt.Sprintf("doc-%02d-%02d", s, i),
				URL:   fmt.Sprintf("https://shard%02d.example/%d", s, i),
				Title: fmt.Sprintf("shard %d doc %d", s, i),
				Body:  fmt.Sprintf("common alpha filler%d unique%02d%02d", i, s, i),
			})
		}
		p := filepath.Join(dir, fmt.Sprintf("seg-%03d.tatami", s))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatalf("write segment %d: %v", s, err)
		}
		paths = append(paths, p)
	}
	return paths
}

// bruteForceQuery answers a query by visiting every shard with the global routing
// stats and merging, with no bound pruning. It is the oracle the pruned Cluster
// must match exactly. The merge order mirrors Cluster.Query.
func bruteForceQuery(t *testing.T, paths []string, routing *search.RoutingIndex, query string, k int) []ClusterHit {
	t.Helper()
	terms := tokenize(query)
	var cands []ClusterHit
	for s, p := range paths {
		seg, err := OpenSearch(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		for _, h := range seg.SearchTermsWith(terms, k, routing) {
			cands = append(cands, ClusterHit{Shard: s, Doc: uint32(h.Doc), Score: float32(h.Score)})
		}
		_ = seg.Close()
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].Score != cands[j].Score {
			return cands[i].Score > cands[j].Score
		}
		if cands[i].Shard != cands[j].Shard {
			return cands[i].Shard < cands[j].Shard
		}
		return cands[i].Doc < cands[j].Doc
	})
	if len(cands) > k {
		cands = cands[:k]
	}
	return cands
}

func TestClusterQueryExactVsBruteForce(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{100, 90, 80, 1, 1, 1, 1, 1, 1, 1, 1, 1})
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, query := range []string{"alpha", "common", "common alpha", "missingterm"} {
		for _, k := range []int{1, 3, 5, 10, 25} {
			got, _, err := c.Query(query, k)
			if err != nil {
				t.Fatal(err)
			}
			want := bruteForceQuery(t, paths, c.Routing(), query, k)
			if !sameHits(got, want) {
				t.Fatalf("query %q k=%d:\n got  %+v\n want %+v", query, k, got, want)
			}
		}
	}
}

// TestClusterEarlyTermination checks that on a selective query the broker visits
// far fewer shards than hold the term, while still returning the exact top-k.
func TestClusterEarlyTermination(t *testing.T) {
	dir := t.TempDir()
	// A steep gradient: three hot shards, then a long tail of single-alpha shards.
	hot := []int{100, 90, 80, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	paths := clusterCorpus(t, dir, hot)
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got, st, err := c.Query("alpha", 3)
	if err != nil {
		t.Fatal(err)
	}
	if st.Candidates != len(hot) {
		t.Fatalf("Candidates = %d, want %d (every shard holds alpha)", st.Candidates, len(hot))
	}
	if st.Visited >= st.Candidates {
		t.Fatalf("Visited = %d did not prune any of %d candidates", st.Visited, st.Candidates)
	}
	t.Logf("alpha k=3: visited %d of %d shards, threshold=%v", st.Visited, st.Candidates, st.Threshold)

	want := bruteForceQuery(t, paths, c.Routing(), "alpha", 3)
	if !sameHits(got, want) {
		t.Fatalf("pruned result is not exact:\n got  %+v\n want %+v", got, want)
	}
}

// TestClusterSearchExactVsBruteForce checks the full query-to-results path with
// stored-field fetch and dedup against an oracle, including url and title.
func TestClusterSearchExactVsBruteForce(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{100, 90, 80, 70, 1, 1, 1, 1})
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, k := range []int{1, 5, 10} {
		got, _, err := c.Search("alpha", k)
		if err != nil {
			t.Fatal(err)
		}
		want := bruteForceSearch(t, paths, c.Routing(), "alpha", k)
		if len(got) != len(want) {
			t.Fatalf("k=%d: got %d results, want %d", k, len(got), len(want))
		}
		for i := range got {
			if got[i].URL != want[i].URL || got[i].Title != want[i].Title || got[i].Score != want[i].Score {
				t.Fatalf("k=%d rank %d: got %+v want %+v", k, i, got[i], want[i])
			}
		}
	}
}

// bruteForceSearch is the dedup oracle for Cluster.Search: visit every shard, keep
// the best copy of each stable doc_id, take the top-k, fetch fields.
func bruteForceSearch(t *testing.T, paths []string, routing *search.RoutingIndex, query string, k int) []SearchResult {
	t.Helper()
	terms := tokenize(query)
	perShard := k
	if len(paths) > 1 {
		perShard = k * 2
	}
	type cand struct {
		score float32
		path  string
		dense uint32
		id    string
	}
	best := map[string]cand{}
	for _, p := range paths {
		seg, err := OpenSearch(p)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range seg.SearchTermsWith(terms, perShard, routing) {
			id, err := seg.globalDocID(uint32(h.Doc))
			if err != nil {
				t.Fatal(err)
			}
			c := cand{score: float32(h.Score), path: p, dense: uint32(h.Doc), id: id}
			cur, ok := best[id]
			if !ok || c.score > cur.score || (c.score == cur.score && c.id < cur.id) {
				best[id] = c
			}
		}
		_ = seg.Close()
	}
	ranked := make([]cand, 0, len(best))
	for _, c := range best {
		ranked = append(ranked, c)
	}
	sort.Slice(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		return ranked[i].id < ranked[j].id
	})
	if len(ranked) > k {
		ranked = ranked[:k]
	}
	out := make([]SearchResult, 0, len(ranked))
	for _, rc := range ranked {
		seg, err := OpenSearch(rc.path)
		if err != nil {
			t.Fatal(err)
		}
		url, title, err := seg.storedFields(rc.dense)
		if err != nil {
			t.Fatal(err)
		}
		_ = seg.Close()
		out = append(out, SearchResult{Doc: rc.dense, URL: url, Title: title, Score: rc.score})
	}
	return out
}

// TestClusterCacheEviction checks that a cache smaller than the number of routed
// shards keeps only the cap open while still answering exactly.
func TestClusterCacheEviction(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12})
	const cap = 3
	c, err := OpenCluster(paths, ClusterOptions{CacheSize: cap})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// "common" is in every shard with a flat bound, so the walk visits them all and
	// the cache must evict down to the cap.
	got, st, err := c.Query("common", 50)
	if err != nil {
		t.Fatal(err)
	}
	if st.Visited != len(paths) {
		t.Fatalf("Visited = %d, want %d (flat bound visits every shard)", st.Visited, len(paths))
	}
	if c.CacheLen() > cap {
		t.Fatalf("cache holds %d segments, over cap %d", c.CacheLen(), cap)
	}
	want := bruteForceQuery(t, paths, c.Routing(), "common", 50)
	if !sameHits(got, want) {
		t.Fatalf("eviction changed results:\n got  %+v\n want %+v", got, want)
	}
}

// TestClusterRoutingSidecarRoundtrip checks that a broker rebuilt from a persisted
// routing sidecar answers identically to one that scanned the shards.
func TestClusterRoutingSidecarRoundtrip(t *testing.T) {
	dir := t.TempDir()
	paths := clusterCorpus(t, dir, []int{100, 50, 10, 1, 1})
	c1, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	enc := search.EncodeRouting(c1.Routing())
	routing, err := search.DecodeRouting(enc)
	if err != nil {
		t.Fatal(err)
	}
	c2 := OpenClusterWithRouting(paths, routing, ClusterOptions{})
	defer c2.Close()

	for _, k := range []int{1, 5, 20} {
		a, _, err := c1.Query("alpha", k)
		if err != nil {
			t.Fatal(err)
		}
		b, _, err := c2.Query("alpha", k)
		if err != nil {
			t.Fatal(err)
		}
		if !sameHits(a, b) {
			t.Fatalf("k=%d sidecar broker differs:\n built %+v\n side  %+v", k, a, b)
		}
	}
}

func sameHits(a, b []ClusterHit) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
