---
title: "Release notes"
description: "What changed in each tatami release."
weight: 40
---

The authoritative, commit-level history lives in [`CHANGELOG.md`](https://github.com/tamnd/tatami/blob/main/CHANGELOG.md) and on the [releases page](https://github.com/tamnd/tatami/releases). This page summarises each version.

## v0.1.0

The first release. tatami is a single-file columnar storage format that stores a crawl corpus compactly and doubles the same file as a keyword search index. It ships the full container, the encoding and compression stack, the indexing and pruning structures, the collection layer, the Parquet bridge, and the search-segment role, proven on real Common Crawl data.

- **The container.** A fixed 64-byte header, row groups of column chunks, an optional blob, dictionary, and index region, and a self-describing footer written last so a reader learns the whole layout from one tail read. The magic `TAT1` sits at both ends, every page header is uncompressed so a reader can stride over pages without decoding, and a CRC32C guards every page and the footer.
- **An encoding cascade.** Each page is encoded with the cheapest scheme that fits (bit-packing with a frame of reference, delta, run-length, dictionary, group-varint, PForDelta, FSST for strings, bitmaps) and then compressed with zstd. The choice is per page and deterministic, so the same input produces a byte-identical file.
- **Blob separation and shared dictionaries.** Large payloads like markdown bodies are written to their own region and referenced by offset, so a metadata scan never reads a body. A trained dictionary spans every row group rather than being rebuilt per group, which is where the format pulls ahead of a per-group layout.
- **Indexing and pruning.** Zone maps on every chunk, opt-in bloom filters, and a sparse primary-key index on a sorted file. A scan pushes a predicate down to the group and page level; a lookup is a bounded seek that reads one page index and one data page regardless of file size.
- **Collections.** A `tatami.manifest` catalogs a directory of files into one logical dataset, with a rollup of each file's key range and zone statistics so a query prunes whole files before opening them. Add, list, compact from the CLI; scan, look up, and merge from the Go API.
- **The Parquet bridge.** `tatami convert` re-encodes an existing Parquet crawl shard as tatami without a producer change. On a real Common Crawl markdown shard the file comes out about 26 percent smaller than the zstd Parquet source, with every body preserved byte for byte.
- **The search-segment role.** A header bit turns a file into a search index: an inverted region over the forward columns, a posting codec with block-max WAND retrieval, BM25 ranking with an exact top-k, and a forward fetch that reads a hit's url and title with two cached column reads. On a real shard, 20246 documents and 1.4 million terms, keyword queries return with a p99 of 237 microseconds.
- **Tiered merge and serving at scale.** Deletions clear a bit in a live-docs bitset and are honored at query time without rewriting a sealed file. A tiered merge folds small segments into large ones and drops the deleted documents. An `Index` serves many segments behind one query with a global top-k and stable-id dedup; split across twenty segments, fan-out keyword retrieval stays at a p99 of 465 microseconds.
- **Packaged everywhere.** Archives for Linux, macOS, Windows, and FreeBSD on amd64 and arm64, `.deb`/`.rpm`/`.apk` packages, a multi-arch GHCR image, Homebrew and Scoop entries, checksums, SBOMs, and a cosign signature.
