---
title: "Merging and serving at scale"
description: "Delete documents from a sealed segment, fold many small segments into large ones with a tiered merge, and serve a whole fleet of segments behind one query that still answers in under a millisecond."
weight: 50
---

One segment is one file. Web scale is many segments, with documents that get deleted and re-crawled over time. This guide covers the three operations that keep a search index servable as it grows: deleting, merging, and serving many segments at once.

## Delete documents

A segment is sealed, so a delete cannot edit the posting lists in place. Instead it clears a bit in a live-docs bitset, one bit per document, all set when the segment opens. The delete is O(1) and the space is reclaimed at the next merge.

```go
seg, err := tatami.OpenSearch("index.tatami")
if err != nil {
	log.Fatal(err)
}
defer seg.Close()

removed, err := seg.Delete("doc-1") // by stable DocID
if err != nil {
	log.Fatal(err)
}
fmt.Printf("removed=%v, %d deleted of %d\n", removed, seg.NumDeleted(), seg.NumDocs())
```

A deleted document never appears in a result. The retrieval loop honors the bitset through a keep predicate, and it stays exact: a delete never raises a score, so the top-k threshold only ever rises from live documents and the block-max skip stays valid. The survivors keep their relative order.

## Merge segments

A tiered merge folds many small segments into one larger segment, drops the deleted documents, and re-derives the dense document ids in one ascending pass so the rebuilt posting lists stay sorted. The inputs are left untouched; the caller retires them after a successful merge.

```go
segs := []*tatami.SearchSegment{seg0, seg1, seg2}
if err := tatami.MergeSegments(segs, "merged.tatami", tatami.WriterOptions{}); err != nil {
	log.Fatal(err)
}
```

The merge rebuilds the postings, skip tables, and term dictionary from scratch. Because it concatenates the inputs in order and reassigns dense ids in one pass, the result is byte-identical in retrieval to a single segment built over the same documents in the same order. The merged segment carries no deletions; the tombstones are gone.

## Decide what to merge

The tiered policy decides which segments are worth merging. It prefers any single segment that is more than a third tombstones (worth compacting on its own), then merges the smallest filled tier, which keeps the segment count logarithmic in the corpus size rather than linear.

```go
ix, err := tatami.OpenIndex([]string{"s0.tatami", "s1.tatami", "s2.tatami"})
if err != nil {
	log.Fatal(err)
}
defer ix.Close()

batch := ix.SelectMerge(search.DefaultMergePolicy())
if len(batch) > 0 {
	var toMerge []*tatami.SearchSegment
	for _, i := range batch {
		toMerge = append(toMerge, ix.Segments()[i])
	}
	_ = tatami.MergeSegments(toMerge, "merged.tatami", tatami.WriterOptions{})
}
```

The default policy uses the constants the format was tuned for: ten segments per tier, at most ten merged at once, a two-thousand document floor, a fifty-million document ceiling on a merged segment, and a thirty-three percent delete threshold.

## Serve many segments as one index

An `Index` holds a set of open segments and serves them as one. A query fans out to every segment, runs each one's retrieval loop, merges the partial top-k lists into one global top-k, and deduplicates a page that more than one segment carries (after a re-crawl) by its stable `DocID`, keeping the highest-scoring copy.

```go
results, err := ix.Search("open source software", 10)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	fmt.Printf("%.3f  %s\n", r.Score, r.URL)
}
```

Because duplicates collapse, the index over-fetches per segment when it holds more than one, so after dedup there are still enough distinct results. Scores across segments are best-effort comparable, since each segment computes its own document-count statistic, so the global order is a merge of per-segment exact top-k rather than the exact top-k of a hypothetical monolith. At a leaf holding segments of similar size and vocabulary, that is close.

## The numbers at scale

Splitting the same Common Crawl shard, 20246 documents, into twenty segments and serving them through one Index, querying two hundred times per keyword:

| Query | p50 | p99 |
|-------|-----|-----|
| `the` (every segment has a long list) | 451 us | 478 us |
| `contact us` | 344 us | 409 us |
| `open source software` | 261 us | 874 us |
| `python` (short lists) | 15 us | 39 us |
| overall, mixed set | 230 us | 465 us |

The overall p99 of 465 microseconds is more than twenty times under the ten-millisecond goal, fanning out to twenty segments and merging a global top-k on every query. The single-segment path on the same data is a 237 microsecond p99; the difference is the price of the fan-out and the per-query merge, and it leaves enough headroom that a leaf could hold an order of magnitude more segments and still answer inside the budget.

An `Index` reaches the segments one machine can hold and merges per-segment top-k lists best-effort. To serve a fleet of shards with exact cross-shard ranking, a routed broker that visits only the shards a query needs, and an aggregator over many brokers, see [distributed serving at scale](/guides/distributed-serving/). To run that broker over HTTP, see [serving search over HTTP](/guides/serving-over-http/).

## Where to go next

- For the routed broker, exact cross-shard ranking, search-only segments, and the aggregator tier, see [distributed serving at scale](/guides/distributed-serving/).
- For the inverted region and the live-docs record on disk, see the [file-format reference](/reference/file-format/).
- For the full type list, see the [Go API reference](/reference/go-api/).
