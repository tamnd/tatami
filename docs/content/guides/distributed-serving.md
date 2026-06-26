---
title: "Distributed serving at scale"
description: "Serve a fan of cold shards behind one query with a routed pruned broker, drop the document body to ship search-only segments, and fan a query across leaf brokers with an aggregator for an exact fleet-wide top-k."
weight: 60
---

An `Index` serves a handful of segments by fanning out to all of them. That stops working when the corpus is the web: a fan-out that opens every shard and visits every posting list does not fit in one machine's memory and does not answer in time. This guide covers the three pieces that take search from many segments to a fleet of them: a broker that visits only the shards a query needs, exact ranking across those shards, and an aggregator that merges many brokers into one.

## Serve a fan of shards with a broker

A `Cluster` is a broker over a directory of shards. It keeps only a bounded working set of segments open at a time, and for each query it consults a routing index to visit only the shards that can contribute to the top-k, in the order most likely to fill it.

```go
cluster, err := tatami.OpenCluster(paths, tatami.ClusterOptions{CacheSize: 128})
if err != nil {
	log.Fatal(err)
}
defer cluster.Close()

results, stats, err := cluster.Search("open source software", 10)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("visited %d of %d shards\n", stats.Visited, stats.Candidates)
for _, r := range results {
	fmt.Printf("%.3f  %s\n", r.Score, r.URL)
}
```

The routing index maps each term to the shards that hold it, with a per-shard impact bound: the most any document in that shard could score for the query. The broker walks the shards in descending bound order and stops the moment the next shard's bound cannot beat the current k-th best score. Because the ranking bound is a true upper bound, this never drops a result that a full fan-out would have returned: the pruned answer is byte-identical to visiting every shard, it just visits far fewer. On a real shard split into 254 shards, a selective keyword query prunes to nine or ten candidate shards; a broad three-term phrase that genuinely spans the corpus visits more, and the bound still keeps the broker from touching shards that cannot win.

The `CacheSize` bounds how many segments stay open at once, so the open-file and decoded-index memory is the cache cap, not the shard count. Size it to hold the working set the queries actually touch, because a cache below that set thrashes: each cold visit decodes a whole inverted region again. The working set is the union of the shards the queries *visit*, which is larger than the set that produces a top result.

## Rank exactly across shards

Splitting a corpus into shards splits the statistics that ranking depends on. BM25 weighs a term by how rare it is across the whole corpus, but a single shard only knows its own slice. Left alone, each shard would score the same document differently, and a merged top-k would be approximate.

The broker fixes this with global statistics injection: it scores every shard against one corpus-wide document count and one per-term document frequency, summed across the shards, rather than each shard's local view. The routing index carries those fleet-wide counts, so the same structure that prunes the walk also supplies the statistics that make the scores comparable. The result is that the merged top-k equals the top-k of a single index built over every shard, exactly, not approximately.

## Fan a query across brokers with an aggregator

One broker reaches the shards one machine can hold. A fleet is more shards than that, so brokers are arranged in a tree: an `Aggregator` sits above a set of leaf brokers, fans a query out to all of them concurrently, and merges their results.

```go
leaves := []*tatami.Cluster{leaf0, leaf1, leaf2 /* ... */}
agg, err := tatami.OpenAggregator(leaves)
if err != nil {
	log.Fatal(err)
}

results, stats, err := agg.Search("machine learning model", 10)
if err != nil {
	log.Fatal(err)
}
fmt.Printf("%d leaves, %d shards visited\n", stats.Leaves, stats.ShardsVisited)
```

Each leaf scores against fleet-wide statistics, the same trick one level up: the aggregator sums every leaf's document count and per-term frequency so every leaf ranks on one IDF scale. It then merges the leaves' partial top-k lists, deduplicating a page that more than one leaf carries (after a re-crawl) by its stable id and keeping the best-scoring copy. Because a document in the global top-k cannot be beaten by more than k documents anywhere, it survives in its own leaf's top-k, so the merged answer is exact, identical to a single broker over every shard.

The fan-out is concurrent, so the latency is the slowest leaf plus a small merge, not the sum of the leaves. The only part of the cost that grows with the fleet is the single root merging every leaf's list; past a point that merge alone would blow the budget, so the tree adds tiers of aggregators, each merging a bounded fan-in. On a real shard split across eight leaves, the per-leaf p99 is about 1.2 milliseconds and stays flat as the fleet grows; modelling a hundred thousand shards with a 64-way fan-out, a two-tier tree projects a fleet p99 of about 1.3 milliseconds, inside the ten-millisecond goal.

## Drop the body with a search-only segment

A search segment indexes the document body, but once the postings are built the body is dead weight for serving: a result row shows a url, a title, and a snippet, not the whole page. A search-only segment keeps the snippet and drops the body.

```go
b := tatami.NewSearchBuilderWith(tatami.SearchBuilderOptions{Snippet: true})
b.Add(tatami.SearchDoc{
	DocID: "doc-1",
	URL:   "https://example.com/a",
	Title: "An example page",
	Body:  longBodyText,
})
if err := b.Write("segment.tatami", tatami.WriterOptions{}); err != nil {
	log.Fatal(err)
}
```

The builder tokenises the body and builds the postings exactly as a full-document segment does, then computes a short lead snippet and drops the body before writing. Retrieval is byte-identical to a full-document segment, because the index is the same; only the stored forward column changes, from a body blob to a snippet string. A reader tells the two shapes apart by that column's type alone, with no new format flag, and `SearchSegment.SnippetOnly` reports which it has. On the production ccrawl shard the search-only segment is 46.26 MiB against the full-document segment's 80.94 MiB, 42.8 percent smaller, which is the body blob that is no longer there.

A merge keeps the shape consistent: `MergeSegments` derives the output shape from its inputs and refuses to mix a snippet segment with a full-document one, so a tier is all one or all the other.

## Where to go next

- To run a broker over HTTP with admission control and a per-request deadline, see [serving search over HTTP](/guides/serving-over-http/).
- For the single-process `Index` and the tiered merge underneath all of this, see [merging and serving at scale](/guides/merging-and-serving/).
- For the exported types, see the [Go API reference](/reference/go-api/).
