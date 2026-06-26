# tatami

[![ci](https://github.com/tamnd/tatami/actions/workflows/ci.yml/badge.svg)](https://github.com/tamnd/tatami/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/tamnd/tatami)](https://github.com/tamnd/tatami/releases/latest)
[![Go Reference](https://pkg.go.dev/badge/github.com/tamnd/tatami.svg)](https://pkg.go.dev/github.com/tamnd/tatami)
[![Go Report Card](https://goreportcard.com/badge/github.com/tamnd/tatami)](https://goreportcard.com/report/github.com/tamnd/tatami)
[![License](https://img.shields.io/github/license/tamnd/tatami)](./LICENSE)

**tatami** (畳) is a single-file columnar storage format for web-scale crawl and search. One `.tatami` file is a self-describing mat of compressed columns: it stores crawled documents compactly, reads them back fast with column projection, and doubles as a search segment. Think of it as a Parquet cousin tuned for two jobs at once, a cold document store and a hot inverted index, both behind one file layout and one reader.

The format is built for the rest of the fleet. A crawler like [ami](https://github.com/tamnd/ami) or a corpus like [Common Crawl via ccrawl-cli](https://github.com/tamnd/ccrawl-cli) writes billions of pages into many `.tatami` files, a manifest stitches them into one logical collection, and a reader serves point lookups, column scans, and keyword queries off the same bytes.

## Status

Early. The container is in place and stable: a fixed header, row groups of column chunks, a self-describing footer written last so a reader learns the whole layout from one tail read, and CRC32C on every page and on the footer. The first milestone (M0) ships the on-disk container, PLAIN encoding for every logical type, zstd block compression, and a byte-stable round trip. The encoding cascade, blob separation, trained dictionaries, indexing, the collection manifest, and the search-segment role land in the milestones that follow.

## Install

```bash
go install github.com/tamnd/tatami/cmd/tatami@latest
```

Prebuilt binaries, `.deb`/`.rpm`/`.apk` packages, and checksums ship on [releases](https://github.com/tamnd/tatami/releases) once the first tag is cut.

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
