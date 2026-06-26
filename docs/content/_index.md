---
title: "tatami"
description: "tatami (畳) is a single-file columnar storage format for web-scale crawl and search. It stores crawled documents as compressed columns and doubles the same file as an inverted search index, with keyword queries that return in well under ten milliseconds."
heroTitle: "One file, a corpus and its search index"
heroLead: "tatami stores crawled documents as compressed columns and, with one header bit flipped, doubles the same file as a search segment. Convert a Parquet shard, scan it with column projection, or serve keyword queries off the same bytes in microseconds."
heroPrimaryURL: "/getting-started/quick-start/"
heroPrimaryText: "Get started"
---

A web crawl ends up in two places that rarely share a file. The raw documents sit in a columnar store like Parquet, good for scans and cheap to keep, and the search index sits in a separate engine that has its own files, its own format, and its own copy of the text. You pay to store the corpus twice and you maintain two systems to keep them in step.

tatami (畳, the woven straw mat that tiles a Japanese floor) folds both into one file. A `.tatami` file is a mat of compressed columns: it stores the crawled documents compactly and reads them back fast with column projection, and when a header bit is set it also carries an inverted index over those same columns. The cold document store and the hot keyword index are one layout and one reader.

```bash
# Convert a crawl shard, then look at what is inside.
tatami convert shard.parquet shard.tatami
tatami inspect shard.tatami
```

## What it does

- **Stores columns compactly.** Each column is encoded with the cheapest scheme that fits (bit-packing, delta, run-length, dictionary, FSST for strings) and then block-compressed with zstd. A shared trained dictionary spans every row group, which is where it pulls ahead of a per-group format.
- **Separates the big payloads.** Markdown bodies and other large blobs are written to their own region, WiscKey-style, so a scan over the metadata columns never reads a byte of body it does not need.
- **Reads only what you touch.** The footer is written last and carries the full schema and every column offset, so opening a file is one tail read and then seeks straight to the columns a query asks for. Zone maps, bloom filters, and a sparse primary-key index prune row groups and pages before any decode.
- **Doubles as a search index.** Flip a header bit and the same file gains an inverted region with a posting codec, BM25F scoring, and block-max WAND retrieval. On a real Common Crawl shard, keyword queries return with a p99 of 237 microseconds.
- **Scales to many files.** A manifest stitches thousands of files into one logical collection for cross-file pruning, and a tiered merge folds small search segments into large ones. Served across twenty segments, fan-out keyword retrieval stays at a p99 of 465 microseconds.
- **Serves a fleet behind one query.** A routed broker visits only the shards a query needs and ranks them exactly against global statistics, an aggregator merges many brokers into one fleet-wide top-k, and `tatami serve` answers thousands of concurrent queries over HTTP without a shared lock. Single-keyword serving holds a p99 of about 1.5 milliseconds at over 31,000 queries per second on a real shard, with the memory bounded by a smart cache.

## Built for the fleet

tatami is the storage layer for a crawl-to-search pipeline, not a standalone product. A crawler like [ami](https://github.com/tamnd/ami) or a corpus like [Common Crawl through ccrawl-cli](https://github.com/tamnd/ccrawl-cli) writes pages into many `.tatami` files; a manifest groups them; a reader serves point lookups, column scans, and keyword queries off the same bytes. The `convert` command bridges existing Parquet output into the format without a producer change, so an existing crawl gains blob separation, shared dictionaries, and pruning structures for free.

## Where to go next

- New here? Start with the [introduction](/getting-started/introduction/), then the [quick start](/getting-started/quick-start/).
- Want to install it? See [installation](/getting-started/installation/).
- Looking for a specific job? The [guides](/guides/) cover writing and reading files from Go, converting crawl shards, managing a collection, building and serving a search index, serving a fleet of shards behind one query, and running the broker over HTTP.
- Need the full surface? The [CLI reference](/reference/cli/), the [file-format reference](/reference/file-format/), and the [Go API reference](/reference/go-api/) are exhaustive.
