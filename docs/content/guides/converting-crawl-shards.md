---
title: "Converting crawl shards"
description: "Bring existing Parquet crawl output into tatami with the convert command, control which columns are separated, filtered, and dictionary-hinted, and see why the file comes out smaller."
weight: 20
---

Most crawls already write Parquet. The `convert` command re-encodes a Parquet shard as tatami without a producer change, so an existing corpus gains blob separation, a shared dictionary, and pruning structures it did not have before. This is the bridge that lets [ami](https://github.com/tamnd/ami) and [ccrawl-cli](https://github.com/tamnd/ccrawl-cli) output drop into the format as is.

## Convert a shard

```bash
tatami convert shard.parquet shard.tatami
```

The converter reads the Parquet leaf schema, maps each column to a tatami logical type (a UTF-8 byte array becomes a string, an unsigned integer logical type becomes an unsigned column, a timestamp logical type becomes a microsecond timestamp), and rejects nested or repeated columns. It reads and appends a batch of rows at a time, so a shard of any size converts in bounded memory. When it finishes it prints the size both ways.

## Control the layout

Three flags override the heuristics, and each takes a comma-separated column list.

| Flag | Default | Meaning |
|------|---------|---------|
| `--blob` | `markdown,body,html` | Columns to separate into the blob region |
| `--bloom` | `doc_id,url,digest` | Columns to build a membership filter on |
| `--dict` | all other strings | String columns to hint toward the shared dictionary |
| `--batch` | `4096` | Rows to read and append at a time |

For example, to separate a `raw_html` column as well and skip the bloom filters:

```bash
tatami convert shard.parquet shard.tatami --blob markdown,raw_html --bloom ""
```

Passing an empty list to a flag disables that heuristic; leaving a flag off keeps its default.

## Why the file is smaller

On a real Common Crawl markdown shard, `CC-MAIN-2026-25/000000.parquet`, the conversion turns 54638084 bytes into 40390281, about 26 percent smaller, with every markdown body preserved byte for byte. Three levers account for it.

- **Blob separation.** The markdown column, 107 MB of bodies, moves to its own region as 105 packed runs sharing one trained dictionary, dropping to 38.5 MB. Just as important, the metadata columns no longer have body bytes interleaved, so a scan over them is pure metadata.
- **A shared dictionary.** Columns that repeat a small set of values across the whole shard collapse: `crawl_date` goes from 222706 bytes to 334, `content_type` from 250156 to 9253, because the dictionary spans every row group instead of being rebuilt per group.
- **The integer cascade.** Numeric columns are bit-packed and delta-encoded before compression, so a status code or a length column costs a few bits per value.

A shard with no large body column, like an ami capture file that stores WARC pointers rather than text, has no blob lever to pull and shrinks more modestly, on the order of a couple of percent, from the dictionary and the cascade alone.

## Then catalog it

A converted shard carries no sort key, because it comes out in producer or WARC order rather than sorted on a column. It still prunes well in a collection through its zone-map rollup and bloom filters; it just prunes by value range and membership rather than by a key range. Catalog converted shards into a collection the same way as any other file:

```bash
tatami collection add ./corpus shard.tatami
```

## Where to go next

- To group many converted shards into one dataset, see [managing a collection](/guides/managing-a-collection/).
- To build a keyword index over the converted text, see [searching a corpus](/guides/searching-a-corpus/).
