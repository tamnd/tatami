---
title: "Go API"
description: "The types and functions the tatami library exports: writing and reading document-store files, predicates and lookups, collections, and the search-segment API."
weight: 30
---

The library lives at `github.com/tamnd/tatami`. The full, authoritative reference is on [pkg.go.dev](https://pkg.go.dev/github.com/tamnd/tatami); this page is a guided tour of the surface. The search subpackage is at `github.com/tamnd/tatami/search`.

## Schema

```go
type Field struct {
	Name          string
	Type          LogicalType
	Nullable      bool
	SortKey       bool      // the single column the file is sorted on
	BlobSeparated bool      // keep this column's payload in the blob region
	BloomFilter   bool      // build a membership filter on this column
	DictHint      bool      // prefer dictionary encoding for this column
}

func NewSchema(fields ...Field) (*Schema, error)
```

The logical types are the constants `TypeBool`, `TypeInt8` through `TypeInt64`, `TypeUint8` through `TypeUint64`, `TypeFloat32`, `TypeFloat64`, `TypeString`, `TypeBytes`, `TypeTimestampMicros`, `TypeList`, and `TypeBlobRef`.

## Writing

```go
func Create(path string, schema *Schema, opts WriterOptions) (*Writer, *os.File, error)
func NewWriter(w io.WriterAt, schema *Schema, opts WriterOptions) (*Writer, error)

func (w *Writer) Append(b Batch) error
func (w *Writer) Close() error
```

`Create` opens a file; `NewWriter` writes to any `io.WriterAt`. A `Batch` is a set of `Column` values, one per schema field, each holding a typed `Data` slice and an optional `Valid` slice for nullable columns.

```go
type Batch struct {
	Columns []Column
}
type Column struct {
	Data  any       // a typed slice: []string, []int32, [][]byte, ...
	Valid []bool    // optional, for nullable columns
}
```

`WriterOptions` tunes the layout (`RowGroupMaxRows`, `RowGroupMaxBytes`, `PageMaxValues`, `PageSizeHint`, `BlobRunTargetBytes`) and the header metadata (`UUID`, `CreatedMillis`, `CreatorID`). The zero value is a good default.

## Reading

```go
func OpenFile(path string) (*Reader, *os.File, error)
func Open(r io.ReaderAt, size int64) (*Reader, error)

func (r *Reader) NumRowGroups() int
func (r *Reader) ReadColumn(group, col int) (Column, error)
```

`OpenFile` opens a path; `Open` reads from any `io.ReaderAt`. `ReadColumn` reads one column of one row group, decoding only that column.

## Predicates, scans, and lookups

```go
func Eq(col string, val any) *Pred
func Ne(col string, val any) *Pred
func Lt(col string, val any) *Pred
func Le(col string, val any) *Pred
func Gt(col string, val any) *Pred
func Ge(col string, val any) *Pred
func Between(col string, lo, hi any) *Pred
func IsNull(col string) *Pred
func And(kids ...*Pred) *Pred
func Or(kids ...*Pred) *Pred

func (r *Reader) Scan(pred *Pred, projection ...string) (*ScanResult, error)
func (r *Reader) Lookup(key any) (RowRef, bool, error)
```

`Scan` projects the named columns and pushes the predicate down, returning the surviving rows and counters (`GroupsScanned`, `GroupsTotal`) that show the pruning. `Lookup` on a sorted file returns a `RowRef{Group, Row}` with a bounded seek.

## Collections

```go
func OpenCollection(dir string) (*Collection, error)

func (c *Collection) Scan(pred *Pred, projection ...string) (*CollectionScan, error)
func (c *Collection) Lookup(key any) (CollHit, bool, int, error)
func (c *Collection) Merge(inRels []string, outRel string, opts WriterOptions, createdMillis uint64) error
```

A `Collection` is a directory of files cataloged by a manifest. `Scan` prunes files by their rollup before opening them and reports `FilesScanned` against `FilesTotal`. `Lookup` returns a `CollHit` (the member and the `RowRef`) and the fan-out count. `Merge` decodes several members and re-encodes them into one, swapping the manifest atomically.

## Search segments

```go
type SearchDoc struct {
	DocID  string  // stable identity, e.g. sha256 of the url
	URL    string
	Title  string
	Body   string
	Anchor string
}

func NewSearchBuilder() *SearchBuilder
func (b *SearchBuilder) Add(doc SearchDoc)
func (b *SearchBuilder) Write(path string, opts WriterOptions) error

func OpenSearch(path string) (*SearchSegment, error)
func (s *SearchSegment) Search(query string, k int) ([]SearchResult, error)
func (s *SearchSegment) Query(query string, k int) []search.Hit
func (s *SearchSegment) Delete(docID string) (bool, error)
func (s *SearchSegment) NumDocs() int
func (s *SearchSegment) NumTerms() int
func (s *SearchSegment) NumDeleted() int
func (s *SearchSegment) Close() error
```

`Search` returns the top-k resolved to url, title, and score; `Query` is the retrieval-only hot path. `Delete` clears a document by its stable id and is honored at query time.

## Merging and serving

```go
func MergeSegments(segs []*SearchSegment, outPath string, opts WriterOptions) error

func OpenIndex(paths []string) (*Index, error)
func NewIndex(segs []*SearchSegment) *Index
func (ix *Index) Search(query string, k int) ([]SearchResult, error)
func (ix *Index) Query(query string, k int) []IndexHit
func (ix *Index) SelectMerge(p search.MergePolicy) []int
func (ix *Index) Segments() []*SearchSegment
func (ix *Index) NumDocs() int
func (ix *Index) Close() error
```

`MergeSegments` folds segments into one, dropping deletions and re-deriving dense ids. An `Index` serves many segments behind one query with a global top-k and stable-id dedup. `SelectMerge` applies the tiered policy from the search subpackage (`search.DefaultMergePolicy`) and returns the segment indices to merge.
