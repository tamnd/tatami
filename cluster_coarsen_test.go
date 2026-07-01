package tatami

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"
)

// These tests prove lever one of the partitioning redesign (scale/11): coarsening
// the shards, that is, putting the same documents into fewer, larger shards, does
// not change what a query retrieves or the score it retrieves it at, and it shrinks
// the routing index because a term common to many shards collapses to fewer
// postings. Both are properties of the partition alone, so they hold on a small
// synthetic corpus in CI without any crawl data.

// coarsenDocs builds n synthetic documents whose bodies draw from a small shared
// vocabulary, so common terms appear in many documents (and, once partitioned,
// many shards) while each document also carries a unique term. That mix is what
// makes the routing index have postings to collapse when the shards coarsen.
func coarsenDocs(n int) []SearchDoc {
	vocab := []string{"alpha", "beta", "gamma", "delta", "common", "epsilon", "zeta"}
	docs := make([]SearchDoc, n)
	for i := range docs {
		body := "common"
		for j := range 4 {
			body += " " + vocab[(i*7+j*3)%len(vocab)]
		}
		body += fmt.Sprintf(" unique%03d", i)
		docs[i] = SearchDoc{
			DocID: fmt.Sprintf("doc-%03d", i),
			URL:   fmt.Sprintf("https://ex/%03d", i),
			Title: fmt.Sprintf("doc %d", i),
			Body:  body,
		}
	}
	return docs
}

// buildPartitioned writes docs into nShards contiguous shards under a tagged name
// so two granularities can live in one dir, then opens a broker over them. The
// document set is identical across granularities; only the shard boundaries move.
func buildPartitioned(t *testing.T, dir, tag string, docs []SearchDoc, nShards int) *Cluster {
	t.Helper()
	per := (len(docs) + nShards - 1) / nShards
	var paths []string
	for s := 0; s*per < len(docs); s++ {
		lo := s * per
		hi := min(lo+per, len(docs))
		b := NewSearchBuilder()
		for _, d := range docs[lo:hi] {
			b.Add(d)
		}
		p := filepath.Join(dir, fmt.Sprintf("%s-%03d.tatami", tag, s))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatalf("write %s shard %d: %v", tag, s, err)
		}
		paths = append(paths, p)
	}
	c, err := OpenCluster(paths, ClusterOptions{})
	if err != nil {
		t.Fatalf("open %s cluster: %v", tag, err)
	}
	return c
}

// routingPostings counts the (term, shard) postings the routing index holds, the
// footprint that coarsening collapses.
func routingPostings(c *Cluster) int {
	n := 0
	c.Routing().EachPosting(func(string, uint32, uint32, uint32) { n++ })
	return n
}

// urlScore is a retrieved document reduced to the two things that must be
// invariant to the partition: which document (its URL) and its score. Doc ids and
// shard ids are partition-relative, so they are deliberately not compared.
type urlScore struct {
	url   string
	score float32
}

func rankedURLScores(t *testing.T, c *Cluster, query string, k int) []urlScore {
	t.Helper()
	res, _, err := c.Search(query, k)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]urlScore, len(res))
	for i, r := range res {
		out[i] = urlScore{r.URL, r.Score}
	}
	// Neutralize tie-break order, which is partition-relative, by sorting on the
	// invariant keys. Ties at the k-th boundary are avoided by asking for every
	// match (k = corpus size at the call site), so this only reorders equal-score
	// runs, it never changes membership.
	sort.Slice(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		return out[i].url < out[j].url
	})
	return out
}

// TestCoarseningIsExact checks that regrouping the same documents into fewer, larger
// shards returns the same documents at the same scores. Global BM25 statistics come
// from the routing index, which sums per-term document frequency across shards, so
// they do not depend on how the documents are partitioned, and neither does the
// ranking. The comparison is at corpus-size k, so both brokers return every match
// and no top-k boundary tie can make membership differ.
func TestCoarseningIsExact(t *testing.T) {
	dir := t.TempDir()
	docs := coarsenDocs(120)

	fine := buildPartitioned(t, dir, "fine", docs, 40) // ~3 docs per shard
	defer fine.Close()
	coarse := buildPartitioned(t, dir, "coarse", docs, 4) // ~30 docs per shard
	defer coarse.Close()

	if fine.NumDocs() != coarse.NumDocs() {
		t.Fatalf("doc counts differ: fine %d, coarse %d", fine.NumDocs(), coarse.NumDocs())
	}
	if !(fine.NumShards() > coarse.NumShards()) {
		t.Fatalf("expected fine (%d) to have more shards than coarse (%d)", fine.NumShards(), coarse.NumShards())
	}

	for _, q := range []string{"alpha", "common", "common alpha", "beta gamma", "unique007", "delta epsilon zeta"} {
		f := rankedURLScores(t, fine, q, len(docs))
		c := rankedURLScores(t, coarse, q, len(docs))
		if len(f) != len(c) {
			t.Fatalf("query %q: fine returned %d docs, coarse %d", q, len(f), len(c))
		}
		for i := range f {
			if f[i] != c[i] {
				t.Fatalf("query %q rank %d: fine %+v vs coarse %+v", q, i, f[i], c[i])
			}
		}
	}
}

// TestCoarseningShrinksRouting checks the resource win: fewer shards means a term
// present in many shards has fewer postings, so the routing index is smaller while
// still covering the same vocabulary and the same documents.
func TestCoarseningShrinksRouting(t *testing.T) {
	dir := t.TempDir()
	docs := coarsenDocs(200)

	fine := buildPartitioned(t, dir, "fine", docs, 50)
	defer fine.Close()
	coarse := buildPartitioned(t, dir, "coarse", docs, 5)
	defer coarse.Close()

	if fine.Routing().NumTerms() != coarse.Routing().NumTerms() {
		t.Fatalf("vocabulary changed with granularity: fine %d terms, coarse %d",
			fine.Routing().NumTerms(), coarse.Routing().NumTerms())
	}
	fp, cp := routingPostings(fine), routingPostings(coarse)
	if !(cp < fp) {
		t.Fatalf("coarsening did not shrink routing postings: fine %d, coarse %d", fp, cp)
	}
	t.Logf("routing postings: fine %d shards -> %d, coarse %d shards -> %d (%.1fx smaller)",
		fine.NumShards(), fp, coarse.NumShards(), cp, float64(fp)/float64(cp))
}
