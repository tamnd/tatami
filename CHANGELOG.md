# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions track
[semantic versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- M1 encoding cascade. A new `encoding/` subpackage holds the physical per-page
  encoders: BITPACK_FOR, DELTA, RLE, GROUPVARINT, and PFORDELTA for the integer
  family, and BITMAP for bool. A greedy per-page sampler takes PLAIN as the floor
  and trades up to the smallest applicable encoding, recording the choice in the
  page header. Floats, strings, and bytes stay on PLAIN until the dictionary
  region lands in M2. On a crawl-metadata sample the integer and bool columns
  drop to roughly two bytes per row before the block codec runs.
- M0 container. The on-disk format: fixed 64-byte header, row groups of column
  chunks, page framing with uncompressed page headers, a self-describing footer
  written last, and a short trailer carrying the footer length, footer CRC32C,
  and the end magic.
- PLAIN encoding for every logical type (bool, the signed and unsigned integers,
  float32/float64, string, bytes, and timestamp), with per-page null bitmaps so
  only present values hit the page payload.
- zstd block compression, pinned single-threaded at a fixed level so the same
  input produces the same file bytes twice.
- CRC32C (Castagnoli) on every page payload, on the footer, and on the header.
- Streaming writer over an io.WriterAt that buffers one row group at a time and
  patches the header at close, and a column-oriented reader with projection.
- The `tatami` CLI with `inspect` (header and footer summary, footer only, no
  data pages) and `cat` (rows to JSONL with column projection and a row limit).
- Byte-stable round-trip oracle covering all logical types, nulls, multiple row
  groups, multiple pages, and corruption detection.
