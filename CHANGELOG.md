# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions track
[semantic versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-07-02

The scale release. tatami now builds and serves ten million documents on one box
inside the ten millisecond goal, and the build survives an interruption. On the
RTX 4090 box the sharded ten-million-document cluster answers at an overall p50
of 1.08 ms and a p99 of 3.82 ms across a mixed keyword and phrase workload, with
a peak of about 42 GiB inside a 56 GiB machine. Design in Spec 2066, the scale
milestones.

### Added

- A block-tree term dictionary on disk and lazy posting decode. The term
  dictionary is now a block tree read a block at a time instead of a flat table
  held whole, so a segment opens without paging its entire vocabulary into
  memory, and a posting list decodes only when the WAND loop reaches it rather
  than all at once at open. This is what keeps the per-shard working set bounded
  as the shard count climbs into the tens of thousands.
- A streaming external-merge search writer. The scale tiers build a segment by
  spilling sorted runs to disk and merging them, so a shard larger than memory
  builds with a bounded footprint. The spill files are zstd-compressed, which
  cuts the build's scratch space on the large tiers.
- Content clustering into size-balanced topical shards. A build groups documents
  by content into shards that are both balanced in size and coherent in topic, so
  a phrase query touches a small candidate set of shards rather than fanning out
  to all of them. Shards coarsen by a target shard count decoupled from the input
  file count, so the same corpus lands on a fixed shard budget regardless of how
  many Parquet files it arrived in.
- Phrase routing on a fleet adjacency summary. Common phrases route to the boxes
  that actually hold the adjacent terms, computed from an adjacency summary built
  at index time, so a two-word phrase over a common word no longer fans out to
  every box. The fleet fan-out is driven to just the candidate boxes, with
  real-data latency gates guarding the routing decision.
- An off-heap, mmap-aliasable routing index. The term-to-shard routing index is
  now flat and pointer-free and persists to `routing.bin`, so it can be memory
  mapped and shared rather than rebuilt on every open. `WriteRoutingFile` writes
  it atomically through a temp file and rename, and the loader validates the magic,
  version, endianness, and length so a torn file is rejected. This is lever three
  of the scale plan: the routing index leaves the heap.
- A resumable sharded build. A build that dies partway through resumes where it
  stopped: a shard is skipped when both its `seg-NNNNN.tatami` segment and its
  `seg-NNNNN.bgr` bigram sidecar are already on disk, and the routing index is
  rebuilt from the segment dictionaries or loaded from a persisted `routing.bin`
  when one is present. This helps the same way in two places, an idle-suspending
  dev VM and a production crash, since either can now restart without redoing the
  finished shards. The routing write is crash-safe and falls back to a rebuild if
  the persisted file is unusable.
- A worker-pool shard walk and a tunable cache cap. The shard walk fans out across
  a worker pool instead of walking serially, and the clustered benchmark build is
  parallelized, so a build uses the cores it is given. The segment cache cap is
  tunable for memory-bound boxes, which is what lets the ten-million-document build
  fit a 56 GiB machine.
- The ten-million-document tiers and a sharded cluster benchmark. The multi-tier
  real-data benchmark streams WET documents through tiers up to ten million
  documents, and a sharded cluster benchmark builds and measures the ten-million
  case end to end, with a persistent build-and-measure split so a built cluster can
  be measured without re-reading the corpus.

### Changed

- Bounded bigram capture to the routed pair set. The build was holding a bigram
  entry for every adjacent pair it saw, which is where the ten-million-document
  build spent its memory. It now keeps only the bigrams that the phrase routing
  can actually use, which is what brought the peak down to about 42 GiB and kept
  the phrase latency inside the budget.

## [0.2.0] - 2026-06-27

### Added

- M10 a concurrent server and a smart cache. The broker now answers thousands of
  queries at once instead of serializing them behind one lock. The M8 `Cluster`
  held a single mutex across the whole query path, which is correct for one query
  and a queue for a thousand, so M10 removed it after an audit confirmed the
  retrieval path is already reentrant: the WAND loop allocates its cursors per
  call, the scorer is a value type, the inverted index is read-only during
  serving, and the M9 search-only segment serves its snippet from a string column
  so it never takes the blob path that mutates a resolver. A new reference-counted
  concurrent segment cache (`segcache.go`) replaces the lock-held LRU: the lock
  guards only the residency bookkeeping and is never held across a file open, a
  column read, or the WAND loop, `acquire` opens a cold shard outside the lock so
  it never blocks a warm hit, and a pin per reader defers `Close` until the last
  reader releases so eviction never races a read. The lazy forward-column caches
  on `SearchSegment` gained an `RWMutex` with the column read done outside the
  write lock (`groupStrings`). A new `Server` (`server.go`, `NewServer`,
  `Handler`) serves `GET /search`, `/healthz`, and `/stats` over the lock-free
  broker with admission control (a counting semaphore that sheds with 503 when
  saturated), a per-request deadline (504 on overrun, the slot always freed), and
  input validation; `tatami serve <dir>` (`cli/serve.go`) globs the segments,
  builds the routing index, and serves with a graceful drain on SIGINT/SIGTERM.
  The smart-cache finding is that sub-10ms needs the working set resident: a cache
  below the visited working set thrashes on cold inverted-index decodes, so the
  server sizes the cache to hold it. On the real shard split into 254 shards, with
  the working set warm, single-keyword serving runs a p99 of about 1.4 ms at over
  31,000 queries per second under one in-flight query per core, comfortably inside the
  ten millisecond goal; multi-term phrases stay well under the budget at the
  median and are bounded at the tail by admission and the deadline rather than
  gated. Resident segments hold at the cache cap of 128 through five thousand
  concurrent queries, and four thousand concurrent requests return the exact
  single-threaded ranking. Design in Spec 2066 `14-serving.md`; implementation
  note 11.
- M9 search-only segments and scale to a hundred thousand shards. A search
  segment can now drop the document body it never serves and keep only what a
  result row shows. `NewSearchBuilderWith(SearchBuilderOptions{Snippet: true})`
  builds a search-only segment whose forward store carries a short precomputed
  lead excerpt (`makeSnippet`, `DefaultSnippetRunes` of 200) in place of the body
  blob; the only column that changes is the variable slot, so a reader tells the
  two shapes apart by its type (`SearchSegment.SnippetOnly`) and the inverted
  index is untouched, which makes retrieval byte-identical to a full-document
  segment. `MergeSegments` derives the output shape from its inputs and refuses to
  mix a snippet segment with a full-document one. On the production ccrawl shard
  the search-only segment is 46.26 MiB against the full-document segment's 80.94
  MiB, 42.8 percent smaller, with retrieval proven identical. An `Aggregator`
  (`OpenAggregator`, `Search`) is the tree-of-brokers tier above the leaf
  `Cluster`: it fans a query out to many leaf brokers concurrently, scores every
  leaf against fleet-wide statistics summed across the leaves (`fleetStats`
  satisfying `search.GlobalStats`, driven through `RouteWith`/`SearchWith`), and
  merges the leaves' partial top-k lists (`mergeLeafResults`) into one fleet-wide
  top-k that dedups a re-crawled page by stable doc_id and is byte-identical to a
  single broker over every shard. The per-shard over-fetch in `SearchWith` is now
  an unconditional `k*2` rather than gated on the shard count, so a single-shard
  leaf surfaces the tie candidates the fleet merge ranks against, which is what
  makes the cross-leaf merge exact. On the real shard split into 254 shards over 8
  leaves, the warm fan-out keyword p99 is 1.63 ms; a single-root merge over a
  hundred thousand shards' worth of leaves costs 12.37 ms, and a tree of
  aggregators with a 64-way fan-out clears it in two tiers at 271 us, projecting a
  fleet p99 of 1.29 ms at a hundred thousand shards, inside the ten millisecond
  goal. Design in Spec 2066 `13-search-only-and-scale.md`; implementation note 10.
- M8 distributed serving at shard scale. A broker now serves a large fan of cold
  shards behind one query, visiting only the shards that can contribute and
  keeping a bounded working set open, with an exact cross-shard top-k. Global
  statistics injection (`search.GlobalStats`, `Inverted.SearchWith`) scores every
  shard against the same corpus-wide document count and per-term document
  frequency, so the merged top-k equals the top-k of a monolith over every shard;
  this closes the best-effort merge M7 left open. A routing index
  (`search.RoutingIndex`, `Route`, `EncodeRouting`/`DecodeRouting`) maps each term
  to the shards that hold it with a per-shard impact bound, so a query walks
  shards in descending bound order and stops the moment the next bound cannot beat
  the current k-th best score; because BM25 here runs with b=0 the bound is a true
  upper bound and the pruned result is byte-identical to a full fan-out. A
  `Cluster` broker (`OpenCluster`, `Query`, `Search`) drives the routed walk over a
  lazy LRU segment cache, so the open-file count is the cache cap and not the
  shard count, and `Search` dedups a re-crawled page by stable doc_id. On a real
  ccrawl shard split into 254 shards with a 128-segment cache, keyword retrieval
  p99 is 1.32 ms, with selective queries pruning 254 candidate shards to nine or
  ten. A detailed compression report (`convert/compress_report_test.go`) accounts
  for every column on a real shard: the tatami file is 26.1 percent smaller than
  its zstd Parquet source, the shared-dictionary blob region carries the markdown
  body at 36.78 MiB against Parquet's 50.38 MiB, and the file holds 105.81 MiB of
  raw bytes in 38.44 MiB, a 2.75x in-file ratio. Design in Spec 2066
  `12-distributed-serving.md`; implementation note 9.

## [0.1.0] - 2026-06-26

The first release. tatami ships as a columnar single-file format and a search
engine in one container, proven on real Common Crawl data: keyword queries
return with a p99 of 237 microseconds on one segment and 465 microseconds served
across twenty, more than twenty times under the ten-millisecond goal. Everything
below landed across milestones M0 through M7.

### Added

- M7 tiered merge and serving at scale. A search index now scales from one file
  to many. Deletions clear a bit in a live-docs bitset, one bit per dense
  document id, so a delete is O(1) and never rewrites a sealed file; the
  retrieval loop honors the bitset through a `keep` predicate (`WANDFilter`) that
  stays exact because a delete never raises a score, so the block-max skip stays
  valid (verified against an exhaustive scan over the live set). A tiered merge
  policy (ten per tier, ten merged at once, two-thousand doc floor,
  thirty-three percent delete threshold) folds small segments into large ones;
  `MergeSegments` reads N segments and writes one, re-deriving dense ids in one
  ascending pass, dropping deleted documents, and rebuilding the postings, skip
  tables, and term dictionary from scratch (a merge of split segments is
  byte-identical in retrieval to a monolith over the same documents). An `Index`
  serves many segments behind one query, fanning out, merging a global top-k, and
  deduplicating a re-crawled page by its stable `doc_id`. The live-docs record is
  an optional index record written only when something is deleted, and the footer
  descriptor reads it only when present, so M6 files decode unchanged. Splitting
  the production ccrawl-cli shard into twenty segments served as one Index, the
  fan-out keyword query p99 is 465 microseconds, more than twenty times under the
  ten millisecond goal.
- M6 search-segment role. A tatami file can now be a search index, not only a
  cold document store. A header role bit turns the same container into a search
  segment: a forward store laid out as columns indexed by a dense document id
  (`doc_id`, `url`, `title`, a blob-separated `body`, and four `norm_*` length
  columns) plus an inverted sub-region in the index region carrying the term
  dictionary, the postings, and the per-list skip tables. Retrieval is block-max
  WAND over a posting codec ported from openindex (128-doc blocks, FOR
  bit-packing, group-varint tails, PForDelta frequencies) with a singleton fast
  path for the long tail of hapax terms, and an exact top-k under a monotonic
  BM25 scorer (verified against an exhaustive scan). A full BM25F scorer with
  field weights is kept for an optional re-rank, which is why the per-field norms
  are stored. The role is flag-gated, so a document-store file from earlier
  milestones decodes unchanged. The retrieval core lives in a self-contained
  `search/` subpackage; `SearchBuilder` and `SearchSegment` in the root package
  tie it to the container. On the production ccrawl-cli markdown shard, 20246
  documents and 1.4 million terms, the keyword query p99 is 237 microseconds,
  more than forty times under the ten millisecond goal.
- M5 fleet adoption. A new `convert/` subpackage reads a producer's zstd Parquet
  shard (ami or ccrawl-cli output) and re-encodes it as tatami, the bridge that
  lets existing crawl output gain blob separation, shared dictionaries, and
  pruning structures without a producer change. The conversion is schema-driven:
  it reads the Parquet leaf schema, maps each column to a tatami logical type,
  and applies overridable heuristics (separate a body column into the blob
  region, hint low-cardinality strings toward a shared dictionary, build a
  membership filter on the identity columns). It streams a batch at a time so
  memory stays bounded regardless of shard size, and round-trips nullable columns
  and every scalar type. The format library stays Parquet-free; only this package
  and the CLI import parquet-go. A `convert` CLI command reports the size both
  ways. On a real Common Crawl markdown shard the tatami file is 26 percent
  smaller than the parquet-go zstd source, with every body verified byte-for-byte.
- M4 collection manifest. A directory of `.tatami` files is now one queryable
  dataset through a `tatami.manifest` catalog: an append-only log of tagged,
  CRC-checked edit records (ADD, REMOVE, SET_TIER) in a new self-contained
  `manifest/` subpackage, replayed on open to rebuild the live member set and
  compacted from that set when the log grows. A torn tail from a crash mid-append
  is detected by its CRC and discarded, so replay keeps the last consistent
  prefix. Each member carries its key range and a per-column zone rollup, so the
  root `Collection` prunes across files in memory before opening any of them,
  reusing M3's three-valued evaluator through a `memberView` adapter: `Lookup`
  over a sorted disjoint collection opens exactly one file, and a predicate
  `Scan` skips every shard the zone rollup rules out. `Merge` decodes a set of
  members, re-encodes them into one fresh shard through the normal writer, and
  swaps the manifest atomically in one batch. A `collection` CLI command (alias
  `col`) adds, lists, and compacts. Members default to a deterministic
  path-and-footer-CRC identity when the writer left the header UUID zero, so the
  file format stays byte-stable.
- M3 indexing. The file now carries the pruning structures a selective read
  needs so its cost tracks the size of the answer, not the size of the data.
  Every column chunk records a min/max zone map, so a predicate scan skips whole
  row groups whose range cannot hold the value. Opt-in columns carry a per-group
  bloom membership filter (a new self-contained `index/` subpackage), so an
  equality probe on an unsorted column skips the groups the filter rules out. A
  sorted file carries a sparse primary-key index: the coarse per-group key bounds
  plus a per-page index on the sort column, so a point lookup descends to one
  page in a bounded number of seeks. The reader gained `Scan(pred, projection)`
  with a three-valued pushdown evaluator and `Lookup(key)` for the bounded-seek
  point read. The footer gained the zone, bloom-reference, page-index, and
  sort-bound fields, all flag-gated so an older chunk decodes unchanged, plus an
  index-region descriptor section; the header gained the index-region flag and
  the format minor version moved to 1.1. `inspect` reports each column's index
  structures and the sort key.
- M2 blob region and shared dictionaries. BLOBREF columns are separated out of
  the row groups into a trailing blob region of packed runs, so a large body
  compresses against the whole file instead of one page at a time. A new `blob/`
  subpackage owns the run layout and the ordinal-to-run directory; the column
  chunk keeps only a validity bitmap, and the reader resolves each present row to
  its run with no per-row offset stored. A per-column raw-dictionary zstd codec
  (ZSTD_DICT) is trained and kept only when it earns back its own stored size, so
  a small-record column takes the dictionary win while a large self-similar body
  stays on plain zstd. On a markdown sample the separated file lands at about two
  thirds the inline size. The footer gained blob and dict descriptor sections,
  the header gained the blob and dict region flags, and `inspect` reports the
  blob bytes and dictionaries.
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
