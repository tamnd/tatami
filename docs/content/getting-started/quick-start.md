---
title: "Quick start"
description: "Convert a crawl shard to tatami, inspect its layout, read rows back as JSONL, and see the size it saves, in one short session."
weight: 30
---

This walk-through starts from a Parquet crawl shard, the kind [ami](https://github.com/tamnd/ami) or [ccrawl-cli](https://github.com/tamnd/ccrawl-cli) writes, and ends with a tatami file you can inspect and read. If you do not have a shard handy, any Parquet file with a few string and integer columns works.

## 1. Convert a shard

```bash
tatami convert shard.parquet shard.tatami
```

The converter reads the Parquet schema, maps each column to a tatami type, and applies a few defaults: it separates a `markdown`, `body`, or `html` column into the blob region, builds a bloom filter on identity columns like `doc_id` and `url`, and hints the remaining strings toward the shared dictionary. It streams a batch of rows at a time, so memory stays bounded no matter how large the shard is. When it finishes it prints both sizes:

```
shard.parquet  54638084 bytes
shard.tatami   40390281 bytes  (0.74x)
```

That is the document-store role: a compact archive of the crawl, about a quarter smaller than the zstd Parquet it came from.

## 2. Look inside

```bash
tatami inspect shard.tatami
```

```
file:    shard.tatami
version: 1.1
rows:    20246
groups:  1
role:    document-store
size:    40390281 compressed / 152843776 uncompressed (0.26x)
columns:
  doc_id     string  enc=fsst  codec=zstd      values=20246 nulls=0 bloom=20246
  url        string  enc=fsst  codec=zstd      values=20246 nulls=0 bloom=20246
  markdown   blobref codec=zstd_dict           values=20246 nulls=0 blob=38.5MB
  crawl_date string  enc=dict  codec=zstd      values=20246 nulls=0
```

`inspect` reads only the header and footer, so it is instant even on a large file. It reports the role, the per-column encoding and codec the cascade chose, the null counts, and any index structures (bloom filters, the blob region, the sort key).

## 3. Read rows back

`cat` streams rows to standard output as JSONL, one object per line. Project the columns you want and cap the row count:

```bash
tatami cat shard.tatami --columns doc_id,url --limit 3
```

```json
{"doc_id":"4f1e...","url":"https://example.com/a"}
{"doc_id":"9b2c...","url":"https://example.com/b"}
{"doc_id":"d7a0...","url":"https://example.com/c"}
```

Because the format is columnar, projecting two columns reads only those two columns off disk. The body, sitting in its own region, is never touched.

## 4. Catalog a directory of shards

A crawl is many shards. Point a collection at a directory and add files to it:

```bash
tatami collection add ./corpus shard0.tatami shard1.tatami
tatami collection list ./corpus
```

The manifest records each file's key range and a rollup of its zone statistics, so a query against the collection can skip whole files before opening them.

## 5. Serve search over HTTP

Once a directory holds search segments, serve them over HTTP:

```bash
tatami serve ./segments --addr :8080
curl 'localhost:8080/search?q=open+source+software&k=10'
```

The broker answers each query without a shared lock, so one process handles many concurrent requests, and a smart cache keeps the working set resident.

## What next

- To write and read files from your own Go code, see [writing and reading files](/guides/writing-and-reading/).
- To build a keyword search index over a corpus and serve it, see [searching a corpus](/guides/searching-a-corpus/).
- To serve a fleet of shards behind one query, see [distributed serving at scale](/guides/distributed-serving/) and [serving search over HTTP](/guides/serving-over-http/).
- For every command and flag, see the [CLI reference](/reference/cli/).
