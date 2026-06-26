# tatami

[![ci](https://github.com/tamnd/tatami/actions/workflows/ci.yml/badge.svg)](https://github.com/tamnd/tatami/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/tamnd/tatami)](https://github.com/tamnd/tatami/releases/latest)
[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/tatami.svg)](https://pkg.go.dev/github.com/tamnd/tatami)
[![Go Report Card](https://goreportcard.com/badge/github.com/tamnd/tatami)](https://goreportcard.com/report/github.com/tamnd/tatami)
[![License](https://img.shields.io/github/license/tamnd/tatami)](./LICENSE)

**tatami** (畳) is a single-file columnar storage format for web-scale crawl and search. One `.tatami` file is a self-describing mat of compressed columns: it stores crawled documents compactly, reads them back fast with column projection, and doubles as a search segment. Think of it as a Parquet cousin tuned for two jobs at once, a cold document store and a hot inverted index, both behind one file layout and one reader.

The format is built for the rest of the fleet. A crawler like [ami](https://github.com/tamnd/ami) or a corpus like [Common Crawl via ccrawl-cli](https://github.com/tamnd/ccrawl-cli) writes billions of pages into many `.tatami` files, a manifest stitches them into one logical collection, and a reader serves point lookups, column scans, and keyword queries off the same bytes.

Full documentation, with guides and the complete reference, is at [tatami.tamnd.com](https://tatami.tamnd.com).

## Status

Complete. The format and the search engine are both implemented and proven on real Common Crawl data. A `.tatami` file is a stable, self-describing container with an encoding cascade, blob separation, shared trained dictionaries, zone maps, bloom filters, and a sparse key index. A manifest stitches many files into one collection, the `convert` command brings existing Parquet shards in, and the search-segment role adds an inverted index with BM25 ranking and block-max WAND retrieval. On a real shard, 20246 documents and 1.4 million terms, keyword queries return with a p99 of 237 microseconds; served across twenty segments at once, fan-out retrieval stays at a p99 of 465 microseconds, more than twenty times under the ten-millisecond goal the format was built to hit. A search-only segment drops the document body it never serves and keeps a short snippet, 42.8 percent smaller on a real shard with byte-identical retrieval, and an aggregator tier fans a query out to many leaf brokers and merges an exact fleet-wide top-k, projecting a p99 of 1.29 milliseconds at a hundred thousand shards.

## Install

```bash
brew install tamnd/tap/tatami       # macOS and Linux
go install github.com/tamnd/tatami/cmd/tatami@latest
```

Prebuilt binaries, `.deb`/`.rpm`/`.apk` packages, a multi-arch GHCR image, and checksums ship on [releases](https://github.com/tamnd/tatami/releases). See [installation](https://tatami.tamnd.com/getting-started/installation/) for every channel.

## Quick start

The CLI reads `.tatami` files. Point it at one to see the layout:

```bash
tatami inspect data.tatami
```

```
file:    data.tatami
version: 1.0
rows:    3
groups:  1
role:    document-store
size:    100 compressed / 85 uncompressed (0.85x)
columns:
  url      string  enc=plain codec=zstd values=3 nulls=0 pages=1
  status   int32   enc=plain codec=zstd values=3 nulls=0 pages=1
  title    string  enc=plain codec=zstd values=3 nulls=1 pages=1
```

Dump rows to JSONL, with optional column projection and a row limit:

```bash
tatami cat data.tatami --columns url,status --limit 10
```

## Writing a file

The Go API takes typed columns a batch at a time and streams row groups to disk:

```go
schema, _ := tatami.NewSchema(
	tatami.Field{Name: "url", Type: tatami.TypeString, SortKey: true},
	tatami.Field{Name: "status", Type: tatami.TypeInt32},
	tatami.Field{Name: "title", Type: tatami.TypeString, Nullable: true},
)

w, f, _ := tatami.Create("data.tatami", schema, tatami.WriterOptions{})
defer f.Close()

w.Append(tatami.Batch{Columns: []tatami.Column{
	{Data: []string{"https://a/1", "https://b/2"}},
	{Data: []int32{200, 404}},
	{Data: []string{"Alpha", ""}, Valid: []bool{true, false}},
})
w.Close()
```

Reading back is column-oriented, so you only pay for the columns you touch:

```go
r, f, _ := tatami.OpenFile("data.tatami")
defer f.Close()

for g := 0; g < r.NumRowGroups(); g++ {
	col, _ := r.ReadColumn(g, 0) // the url column
	urls := col.Data.([]string)
	_ = urls
}
```

## How it works

A `.tatami` file is laid out as a fixed 64-byte header, a run of row groups, an optional blob region for separated large payloads, optional dictionary and index regions, then a footer directory and a short trailer. The footer is written last and carries the full schema and the byte offsets of every column chunk, so opening a file is one read of the tail followed by seeks straight to the columns a query needs. The magic `TAT1` sits at both ends, every page header is uncompressed so a reader can stride over pages without decoding them, and a CRC32C guards each page payload and the footer.

Two roles share the layout. A document-store file holds the crawled columns as written. A search-segment file (a header flag bit) adds an inverted region so the same reader can answer keyword queries. The design notes live in the spec; the implementation notes track each milestone as it lands.

## License

MIT. See [LICENSE](./LICENSE).
