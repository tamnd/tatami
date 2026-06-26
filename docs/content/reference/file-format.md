---
title: "File format"
description: "The on-disk layout of a tatami file, region by region: the header, row groups, the blob and dictionary and index regions, the footer, and the trailer."
weight: 20
---

A `.tatami` file is a self-describing container. It is laid out so a reader learns the whole structure from one read of the tail, and so a query reads only the bytes it needs. This page describes the layout. The format library is the source of truth; this is the shape it writes.

## Conventions

- The magic marker `TAT1` sits at both the start and the end of every file.
- Fixed-width integers are little-endian. Variable-length integers are unsigned LEB128.
- A CRC32C checksum guards every page payload and the footer.
- The current format version is 1.1.

## Top-level layout

```
+--------------------------------------------------+
| Header (64 bytes, fixed)                         |
+--------------------------------------------------+
| Row group 0                                      |
|   column chunk 0 (pages)                         |
|   column chunk 1 (pages)                         |
|   ...                                            |
| Row group 1                                      |
| ...                                              |
+--------------------------------------------------+
| Blob region        (optional)                    |
+--------------------------------------------------+
| Dictionary region  (optional)                    |
+--------------------------------------------------+
| Index region       (optional)                    |
+--------------------------------------------------+
| Footer (schema + all offsets)                    |
+--------------------------------------------------+
| footer_len (uint32) | footer_crc32c (uint32)     |
+--------------------------------------------------+
| TAT1 (end magic)                                 |
+--------------------------------------------------+
```

The footer is written last, so the writer can stream the body without knowing the final offsets up front, and the reader can find everything from the tail: read the end magic and the two trailer words, seek back by `footer_len`, verify `footer_crc32c`, and decode the footer.

## Header

A fixed 64-byte block at offset zero. It carries the start magic, the major and minor version, a flags word, a file UUID, a creation timestamp, and a creator id. The flags word records which optional regions are present and which role the file plays.

| Flag | Bit | Meaning |
|------|-----|---------|
| `FlagSorted` | 0 | The file is sorted on its sort-key column |
| `FlagHasBlobRegion` | 1 | A blob region is present |
| `FlagHasDictRegion` | 2 | A dictionary region is present |
| `FlagHasIndexRegion` | 3 | An index region is present |
| `FlagRoleSearchSeg` | 4 | The file is a search segment, not a document store |

A reader that does not understand a flagged region simply does not read it, which is how a document-store file written before the search role existed still opens unchanged.

## Row groups, chunks, and pages

The body is a run of row groups. A row group holds one column chunk per column; a column chunk is all the pages of one column within that group. A page is a 32-byte uncompressed header followed by the page payload.

Because every page header is uncompressed and fixed size, a reader can stride over pages, reading offsets and counts, without decoding any payload. A page header records the page kind, the value count, the encoding and codec, the payload length, a flags byte (inline min and max, nulls present), and the payload CRC.

A row group defaults to a bound of row count or uncompressed size, whichever it hits first, and a page defaults to a bound on its value count or its byte size. These are tunable through `WriterOptions`.

## Logical types

A column has one of sixteen logical types.

| Value | Type | Value | Type |
|-------|------|-------|------|
| 0 | `BOOL` | 8 | `UINT64` |
| 1 | `INT8` | 9 | `FLOAT32` |
| 2 | `INT16` | 10 | `FLOAT64` |
| 3 | `INT32` | 11 | `STRING` |
| 4 | `INT64` | 12 | `BYTES` |
| 5 | `UINT8` | 13 | `TIMESTAMP_MICROS` |
| 6 | `UINT16` | 14 | `LIST` |
| 7 | `UINT32` | 15 | `BLOBREF` |

A `BLOBREF` column stores, per row, an offset and length and codec into the blob region rather than the value inline, the WiscKey idea of separating large values from the scan path.

## Encodings and codecs

Each page is encoded with one scheme, then compressed with one codec. The cascade picks the encoding that produces the smallest pre-codec payload for the values in the page.

| Value | Encoding | Used for |
|-------|----------|----------|
| 0 | `PLAIN` | the floor for any type |
| 1 | `RLE` | runs of repeated values |
| 2 | `DICTIONARY` | low-cardinality values |
| 3 | `BITPACK_FOR` | integers, frame of reference plus bit-packing |
| 4 | `DELTA` | sorted or slowly changing integers |
| 5 | `GROUPVARINT` | variable-width integer runs |
| 6 | `PFORDELTA` | integers with a few outliers |
| 7 | `FSST` | high-cardinality strings (urls, digests) |
| 8 | `BITMAP` | the validity and scatter bitmaps |

| Value | Codec |
|-------|-------|
| 0 | `NONE` |
| 1 | `LZ4` |
| 2 | `ZSTD` (default) |
| 3 | `ZSTD_DICT` (zstd with a shared trained dictionary) |

## Blob region

When a column is blob-separated, its large payloads are packed into runs in the blob region and the column chunk stores only a validity bitmap; the reader computes a global ordinal and looks the run up through a directory in the footer. A run can be compressed plain or against a shared content dictionary, and the writer keeps whichever is smaller including the cost of storing the dictionary. For templated records a content dictionary wins by several times; for large self-similar markdown bodies plain compression usually wins, so the writer trials both rather than always applying the dictionary.

## Dictionary region

The dictionary region holds the trained dictionaries shared across row groups: a per-column value dictionary for `DICTIONARY` and `FSST` columns, and the content dictionaries the blob region references. Sharing one dictionary across the whole file, rather than one per group, is the main on-disk advantage over a per-group format for columns that repeat a small value set across a shard.

## Index region

The index region holds the pruning and search structures, each as a flagged record so a reader takes only what it asks for.

- **Zone maps** are always present per chunk in the footer: the min and max and a present flag, used to skip a chunk a predicate rules out.
- **Bloom filters** are opt-in per column: a double-hashed membership filter at about ten bits per key, used to skip an equality probe.
- **A sparse primary-key index** on a sorted file: coarse per-group key bounds in the footer plus a fine per-page index on the sort column, which turns a lookup into a bounded seek.

A search segment adds an **inverted region** here: the term dictionary, the posting lists, the per-list skip tables, and an optional live-docs bitset. The term dictionary is sorted with a singleton fast path (a term in one document is stored inline rather than as a one-entry list). The posting lists are stored in 128-document blocks with frame-of-reference bit-packing on the gaps, group-varint for the short tail, and PForDelta for the frequency stream. The skip table, one entry per block, carries the first and last document, the max frequency, the byte offset, and the count, which is what lets the cursor skip a block without decoding it. The live-docs bitset is written only when a document has been deleted; absent, every document is live.

## Footer and trailer

The footer carries the full schema, the byte offset and metadata of every column chunk, the zone maps, the descriptors that address the optional regions, and, for a search segment, the inverted descriptor with the offsets and counts of its sub-runs. After the footer come two words, the footer length and its CRC32C, then the end magic. Opening a file is: read the end, find the footer, verify it, decode it, and you have every offset you need.
