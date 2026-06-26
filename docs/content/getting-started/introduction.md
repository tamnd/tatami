---
title: "Introduction"
description: "Why a crawl corpus and its search index belong in one file, and how tatami puts them there."
weight: 10
---

A web crawl produces two artifacts that almost never live together. The first is the corpus: hundreds of millions of pages, each with a url, a fetch date, a status, a content type, and a body of text. The natural home for that is a columnar store like Parquet, where similar values sit next to each other and compress well, and where a scan can read one column without touching the rest. The second is the search index: the inverted lists that map a word to the documents that contain it, plus the statistics that rank them. That lives in a search engine, which keeps its own files in its own format with its own copy of the text.

Keeping the two apart has a real cost. The text is stored twice. Two systems have to agree on which documents exist. Building the index means reading the whole corpus out of one format and writing it into another. tatami starts from the observation that both artifacts are columnar tables over the same documents, and puts them in one file.

## One file, two roles

A `.tatami` file is a self-describing mat of compressed columns. A header bit decides which of two roles it plays.

A **document store** holds the crawled columns as written: url, status, date, content type, and a body. This is the cold side, the compact archive of what was crawled. You scan it, project a few columns out of it, or look a single row up by key.

A **search segment** is the hot side. It sets the role bit and adds an inverted region beside the columns: a term dictionary, the posting lists, and the skip tables that make them fast to traverse. The forward columns are still there, indexed by a dense document id, so a query that finds the top documents can fetch their url and title from the same file with a direct columnar read. No separate engine, no second copy of the text.

The two roles share one layout and one reader. A document-store file written before the search role existed opens and reads exactly the same way; the new capability is invisible to a reader that does not ask for it.

## What makes a file small

tatami leans on a few ideas to beat a per-group format on disk.

- **An encoding cascade.** Every page is encoded with the cheapest scheme that fits its values: bit-packing with a frame of reference, delta for sorted runs, run-length for repeats, a dictionary for low-cardinality strings, and FSST for high-cardinality strings like urls. The encoded bytes are then compressed with zstd.
- **A shared dictionary.** A trained dictionary spans every row group in the file rather than being rebuilt per group, so a column like `content_type` or `crawl_date`, which repeats the same handful of values across the whole shard, collapses to almost nothing.
- **Blob separation.** The large payloads, a markdown body or raw html, are written to their own region and referenced from the column by an offset, the WiscKey idea of keeping big values out of the main scan path. A query over the metadata columns never reads a byte of body.

On a real Common Crawl markdown shard, the result is a file about a quarter smaller than the same data as zstd Parquet, with every body byte preserved.

## What makes a query fast

The footer is written last and carries the full schema and the byte offset of every column chunk, so opening a file is one read of the tail. From there the reader seeks straight to the columns a query names. Before it decodes anything it prunes: zone maps give the min and max of every chunk, an optional bloom filter answers membership, and a sparse primary-key index turns a point lookup into a bounded seek that touches one page index and one data page regardless of file size.

For the search role, retrieval is block-max WAND over a posting codec ported from [openindex](https://github.com/tamnd/openindex): 128-document blocks, frame-of-reference bit-packing on the gaps, and per-list skip tables that let the cursor stride over a block without decoding it. The scorer driving the traversal is monotonic, so the top-k it returns is exact, identical to an exhaustive scan, while still skipping most of the work. On the same Common Crawl shard, 20246 documents and 1.4 million terms, a keyword query returns with a p99 of 237 microseconds, more than forty times under the ten-millisecond goal the format was built to hit.

## From one file to a corpus

One file is a shard; a crawl is many shards. A `tatami.manifest` catalogs a directory of files into one logical collection, carrying a rollup of each file's key range and zone statistics so a query can skip whole files before opening them. On the search side, a tiered merge folds many small segments into fewer large ones, drops deleted documents, and re-derives the dense document ids in one pass, the way a log-structured search index stays servable as it grows. Served across many segments at once, fan-out keyword retrieval still answers in well under a millisecond.

Past the segments one machine can hold, a broker serves a fan of cold shards behind one query, consulting a routing index so it visits only the shards that can contribute and scoring them against global statistics so the merged top-k is exact, not approximate. A search-only segment drops the document body once the postings are built, keeping a snippet in its place, so a search tier ships a fraction of the bytes. An aggregator fans a query across many brokers and merges an exact fleet-wide top-k, and `tatami serve` puts an HTTP server over a broker that answers thousands of concurrent queries without a shared lock, with a smart cache that keeps the working set warm and the memory bounded. The serving stack is its own pair of guides: [distributed serving at scale](/guides/distributed-serving/) and [serving search over HTTP](/guides/serving-over-http/).

Next: [install tatami](/getting-started/installation/).
