package tatami

import (
	"fmt"
	"path/filepath"
	"testing"
)

// These tests prove lever two of the partitioning redesign (scale/12): assigning
// documents to shards by content rather than by crawl order does not change what a
// query retrieves or the score it retrieves it at (it is exact, the same property
// coarsening has), and it shrinks the fan-out a topical query pays, because the
// topic's vocabulary concentrates in a few shards instead of spreading across all
// of them. Both are properties of the partition alone, so they hold on a small
// synthetic topical corpus in CI without any crawl data.

// topicalDocs builds nTopics latent topics, each with a private vocabulary, and
// perTopic documents per topic. Every document also carries a globally common word
// and a unique term. The documents are emitted round-robin across topics, so the
// crawl order maximally mixes them: a contiguous partition puts a little of every
// topic in every shard, which is the wide-fan-out case clustering is meant to beat.
func topicalDocs(nTopics, perTopic int) []SearchDoc {
	var docs []SearchDoc
	total := nTopics * perTopic
	for i := range total {
		topic := i % nTopics
		// A topic's private vocabulary: five words no other topic uses.
		w := func(n int) string { return fmt.Sprintf("t%dw%d", topic, n) }
		body := "common " + w(0) + " " + w(1) + " " + w(2) + " " + w(3)
		body += fmt.Sprintf(" uniq%04d", i)
		docs = append(docs, SearchDoc{
			DocID: fmt.Sprintf("doc-%04d", i),
			URL:   fmt.Sprintf("https://ex/%04d", i),
			Title: fmt.Sprintf("doc %d topic %d", i, topic),
			Body:  body,
		})
	}
	return docs
}

// buildClustered partitions docs into k content shards with the clusterer, writes
// each non-empty shard under a tagged name, and opens a broker over them. It
// returns the broker and the per-shard document counts, so a test can check the
// partition stayed size-balanced. The document set is identical to the crawl-order
// build; only which shard each document lands in changes.
func buildClustered(t *testing.T, dir, tag string, docs []SearchDoc, k int) (*Cluster, []int) {
	t.Helper()
	sample := make([][]string, len(docs))
	for i, d := range docs {
		sample[i] = tokenize(d.Body)
	}
	cl := FitClusterer(sample, ClusterPlanOptions{Shards: k, Dims: 256, Iters: 25, Seed: 1, Slack: 0.15})
	cl.SetCapacity(len(docs), 0.15)

	buckets := make([][]SearchDoc, k)
	for _, d := range docs {
		s := cl.Assign(tokenize(d.Body))
		buckets[s] = append(buckets[s], d)
	}

	var paths []string
	var sizes []int
	for s := range buckets {
		if len(buckets[s]) == 0 {
			continue
		}
		b := NewSearchBuilder()
		for _, d := range buckets[s] {
			b.Add(d)
		}
		p := filepath.Join(dir, fmt.Sprintf("%s-%03d.tatami", tag, s))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatalf("write %s shard %d: %v", tag, s, err)
		}
		paths = append(paths, p)
		sizes = append(sizes, len(buckets[s]))
	}
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatalf("open %s cluster: %v", tag, err)
	}
	return c, sizes
}

// TestContentClusteringIsExact checks that partitioning the same documents by
// content returns the same documents at the same scores as partitioning them by
// crawl order. Global BM25 statistics come from the routing index, which sums
// per-term document frequency across shards, so they do not depend on how the
// documents are partitioned, and neither does the ranking. The comparison is at
// corpus-size k, so both brokers return every match and no top-k boundary tie,
// whose resolution is partition-relative, can make membership differ.
func TestContentClusteringIsExact(t *testing.T) {
	dir := t.TempDir()
	docs := topicalDocs(6, 40) // 240 docs across 6 topics

	crawl := buildPartitioned(t, dir, "crawl", docs, 6) // topics mixed in every shard
	defer crawl.Close()
	clustered, _ := buildClustered(t, dir, "clustered", docs, 6) // one topic per shard
	defer clustered.Close()

	if crawl.NumDocs() != clustered.NumDocs() {
		t.Fatalf("doc counts differ: crawl %d, clustered %d", crawl.NumDocs(), clustered.NumDocs())
	}

	queries := []string{
		"common",            // in every document, routes everywhere either way
		"t2w0",              // one topical word
		"t2w0 t2w1",         // a topical phrase
		"t0w0 t3w2",         // a cross-topic pair
		"uniq0007",          // a unique term, one document
		"common t4w1 t4w3",  // common word plus a topic
		"t5w0 t5w1 t5w2",    // three words of one topic
		"nonexistentterm42", // matches nothing
	}
	for _, q := range queries {
		a := rankedURLScores(t, crawl, q, len(docs))
		b := rankedURLScores(t, clustered, q, len(docs))
		if len(a) != len(b) {
			t.Fatalf("query %q: crawl returned %d docs, clustered %d", q, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("query %q rank %d: crawl %+v vs clustered %+v", q, i, a[i], b[i])
			}
		}
	}
}

// TestContentClusteringShrinksFanout checks the benefit: a topical query touches
// fewer shards under a content partition than under a crawl-order one, because the
// topic's vocabulary concentrates in a few shards instead of spreading across all
// of them. QueryStats.Candidates is the shards holding a query term and Visited is
// the shards actually scored, so both report the fan-out. The result must stay
// identical, which is asserted alongside so the win is not a lost document.
func TestContentClusteringShrinksFanout(t *testing.T) {
	dir := t.TempDir()
	docs := topicalDocs(6, 40)

	crawl := buildPartitioned(t, dir, "crawl", docs, 6)
	defer crawl.Close()
	clustered, sizes := buildClustered(t, dir, "clustered", docs, 6)
	defer clustered.Close()

	if clustered.NumShards() < 2 {
		t.Fatalf("clustering collapsed to %d shards, no fan-out to compare", clustered.NumShards())
	}
	t.Logf("clustered shard sizes: %v", sizes)

	const q = "t2w0 t2w1" // both words belong to topic 2
	cc, cs, err := crawl.Query(q, 10)
	if err != nil {
		t.Fatal(err)
	}
	kc, ks, err := clustered.Query(q, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("query %q: crawl candidates=%d visited=%d, clustered candidates=%d visited=%d",
		q, cs.Candidates, cs.Visited, ks.Candidates, ks.Visited)

	if !(ks.Candidates < cs.Candidates) {
		t.Fatalf("clustering did not shrink candidate shards: crawl %d, clustered %d",
			cs.Candidates, ks.Candidates)
	}
	if ks.Visited > cs.Visited {
		t.Fatalf("clustering increased visited shards: crawl %d, clustered %d",
			cs.Visited, ks.Visited)
	}

	// The fan-out win must not have changed the answer: same documents, same scores.
	a := rankedURLScores(t, crawl, q, len(docs))
	b := rankedURLScores(t, clustered, q, len(docs))
	if len(a) != len(b) {
		t.Fatalf("result size changed: crawl %d, clustered %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("rank %d changed: crawl %+v vs clustered %+v", i, a[i], b[i])
		}
	}
	// Both routes retrieve the topic-2 documents that hold both words.
	if len(cc) == 0 || len(kc) == 0 {
		t.Fatalf("expected hits from both routes, got crawl %d clustered %d", len(cc), len(kc))
	}
}

// TestContentClusteringBalanced checks the partition stays size-balanced: the
// capacity constraint keeps every shard within ceil(N/k)*(1+slack) documents, so
// the fan-out win does not come from a lopsided partition that would cost more per
// shard on the WAND loop than it saves in shard count.
func TestContentClusteringBalanced(t *testing.T) {
	dir := t.TempDir()
	docs := topicalDocs(8, 50) // 400 docs across 8 topics
	k := 8

	clustered, sizes := buildClustered(t, dir, "bal", docs, k)
	defer clustered.Close()

	mean := (len(docs) + k - 1) / k
	capPerShard := int(float64(mean)*1.15 + 0.9999) // ceil(mean * (1+slack))
	for s, n := range sizes {
		if n > capPerShard {
			t.Fatalf("shard %d holds %d docs, over the cap %d (mean %d, slack 0.15)",
				s, n, capPerShard, mean)
		}
	}
	total := 0
	for _, n := range sizes {
		total += n
	}
	if total != len(docs) {
		t.Fatalf("clustered partition lost documents: %d of %d", total, len(docs))
	}
}
