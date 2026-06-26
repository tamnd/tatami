---
title: "Searching a corpus"
description: "Build a search segment from documents, run keyword queries with BM25 ranking and block-max WAND retrieval, and fetch the matching documents from the same file in microseconds."
weight: 40
---

A tatami file with the search role set is both the document store and the inverted index. This guide builds one from a set of documents, runs keyword queries against it, and reads the matching documents back, all from one file.

## Build a search segment

A `SearchBuilder` takes documents, tokenizes their fields, accumulates the term-to-postings map, and seals the result into a search-segment file. A `SearchDoc` carries a stable id, a url, a title, a body, and optional anchor text.

```go
b := tatami.NewSearchBuilder()
b.Add(tatami.SearchDoc{
	DocID: "doc-1",
	URL:   "https://example.com/python",
	Title: "Getting started with Python",
	Body:  "Python is a high level programming language ...",
})
b.Add(tatami.SearchDoc{
	DocID: "doc-2",
	URL:   "https://example.com/go",
	Title: "A tour of Go",
	Body:  "Go is a statically typed compiled language ...",
})

if err := b.Write("index.tatami", tatami.WriterOptions{}); err != nil {
	log.Fatal(err)
}
```

The builder assigns each document a dense id in add order, the row index in the forward store. That dense id is what makes fetching a hit's fields a direct columnar read. The `DocID` you supply is the stable identity (a crawl typically uses the sha256 of the url), kept as a column and used to deduplicate a page that more than one segment carries.

## Run a query

Open the segment and search. `Search` returns the top-k results already resolved to their url, title, and score; `Query` is the retrieval-only hot path that returns dense ids and scores without the field fetch.

```go
seg, err := tatami.OpenSearch("index.tatami")
if err != nil {
	log.Fatal(err)
}
defer seg.Close()

results, err := seg.Search("python language", 10)
if err != nil {
	log.Fatal(err)
}
for _, r := range results {
	fmt.Printf("%.3f  %s  %s\n", r.Score, r.URL, r.Title)
}
```

The query is tokenized the same way the documents were, so case and word boundaries match. The result is the exact top-k under the scorer, ordered by score.

## How ranking and retrieval work

Retrieval is block-max WAND over the posting lists. The lists are stored in 128-document blocks with frame-of-reference bit-packing on the gaps and a per-list skip table, so the cursor can stride over a block whose best possible score cannot beat the current k-th best without decoding it. The scorer driving the traversal is BM25 with length normalization turned off, which is monotonic in term frequency, so the block-max bound is a true upper bound and the top-k is provably exact, identical to an exhaustive scan.

The full BM25F scorer, with field weights (body 1.0, title 3.0, anchor 2.0, url 1.5) and per-field length saturation, is available for an optional re-rank of the candidate set, which is why the segment stores a length norm per field per document.

## The numbers

On a real Common Crawl markdown shard, 20246 documents and 1.4 million distinct terms, built into one segment and queried two hundred times per keyword:

| Query | p50 | p99 |
|-------|-----|-----|
| `the` (most common word) | 129 us | 149 us |
| `contact us` | 200 us | 349 us |
| `open source software` | 88 us | 104 us |
| `python` (rare, short list) | 4 us | 10 us |
| overall, mixed set | 76 us | 237 us |

The overall p99 of 237 microseconds is more than forty times under the ten-millisecond goal, on a single core, on real crawl text. The full query-to-results path, including the columnar fetch of each hit's url and title, averages about 91 microseconds per query, because the field fetch is two cached column reads rather than a per-hit lookup.

## Where to go next

- To delete documents, fold segments together, and serve many at once, see [merging and serving at scale](/guides/merging-and-serving/).
- For the inverted region's on-disk shape, see the [file-format reference](/reference/file-format/).
