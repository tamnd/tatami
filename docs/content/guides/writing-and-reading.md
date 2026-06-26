---
title: "Writing and reading files"
description: "Use the Go library to write typed columns to a tatami file, read them back with column projection, and prune rows with predicate pushdown and key lookups."
weight: 10
---

The CLI is a thin wrapper over the Go library. This guide writes a file, reads it back, and uses the pruning structures to avoid touching data a query does not need.

## Define a schema

A schema is an ordered list of fields, each with a name and a logical type. A field can be nullable, can be marked as the file's sort key, can ask for a bloom filter, and can ask to have its payload separated into the blob region.

```go
schema, err := tatami.NewSchema(
	tatami.Field{Name: "url", Type: tatami.TypeString, SortKey: true},
	tatami.Field{Name: "status", Type: tatami.TypeInt32},
	tatami.Field{Name: "title", Type: tatami.TypeString, Nullable: true},
	tatami.Field{Name: "body", Type: tatami.TypeString, BlobSeparated: true},
)
if err != nil {
	log.Fatal(err)
}
```

A column marked `SortKey` makes the file sorted on that column, which turns on the per-group key bounds and the sparse key index that make `Lookup` a bounded seek. `BlobSeparated` keeps a large string or byte column out of the main scan path. `BloomFilter` on a field adds a membership filter used to prune equality probes.

## Write columns a batch at a time

`Create` opens a file and returns a `Writer` and the underlying `*os.File`. Append batches of typed columns, then close the writer. Each column's `Data` is a typed Go slice; a nullable column carries a parallel `Valid` slice.

```go
w, f, err := tatami.Create("pages.tatami", schema, tatami.WriterOptions{})
if err != nil {
	log.Fatal(err)
}
defer f.Close()

err = w.Append(tatami.Batch{Columns: []tatami.Column{
	{Data: []string{"https://a/1", "https://b/2"}},
	{Data: []int32{200, 404}},
	{Data: []string{"Alpha", ""}, Valid: []bool{true, false}},
	{Data: []string{"first body", "second body"}},
}})
if err != nil {
	log.Fatal(err)
}

if err := w.Close(); err != nil {
	log.Fatal(err)
}
```

The writer streams row groups to disk as it goes, slicing an oversized `Append` into multiple groups, so memory stays bounded regardless of how much you append. The footer, with the full schema and every column offset, is written last.

`WriterOptions` tunes the layout: `RowGroupMaxRows` and `RowGroupMaxBytes` bound a group, `PageMaxValues` and `PageSizeHint` bound a page, and `BlobRunTargetBytes` sizes a packed blob run. The zero value is a sensible default for crawl-shaped data.

## Read columns back

Open the file and read one column at a time. Reading a column reads only that column off disk.

```go
r, f, err := tatami.OpenFile("pages.tatami")
if err != nil {
	log.Fatal(err)
}
defer f.Close()

for g := 0; g < r.NumRowGroups(); g++ {
	col, err := r.ReadColumn(g, 0) // the url column
	if err != nil {
		log.Fatal(err)
	}
	urls := col.Data.([]string)
	for _, u := range urls {
		fmt.Println(u)
	}
}
```

A nullable column comes back with its `Valid` slice populated, so a reader can tell a null apart from an empty value.

## Prune rows with a predicate

`Scan` projects a set of columns and pushes a predicate down to the row-group and page level, so a group whose zone map or bloom filter rules out a match is never decoded. Predicates compose from leaves (`Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Between`, `IsNull`) and the connectives `And` and `Or`.

```go
res, err := r.Scan(
	tatami.And(
		tatami.Ge("status", int32(200)),
		tatami.Lt("status", int32(300)),
	),
	"url", "status",
)
if err != nil {
	log.Fatal(err)
}

for _, row := range res.Rows {
	fmt.Println(row[0], row[1]) // url, status
}
fmt.Printf("scanned %d of %d groups\n", res.GroupsScanned, res.GroupsTotal)
```

`GroupsScanned` against `GroupsTotal` shows how much the pruning saved: a selective predicate on a sorted or zoned column scans only the groups that could contain a match.

## Look a row up by key

On a file with a sort key, `Lookup` finds a single row with a bounded seek that reads the footer, one page index, and one data page, independent of file size.

```go
ref, ok, err := r.Lookup("https://b/2")
if err != nil {
	log.Fatal(err)
}
if ok {
	col, _ := r.ReadColumn(ref.Group, 1) // status column
	fmt.Println(col.Data.([]int32)[ref.Row])
}
```

`ref` is a `RowRef{Group, Row}` pointing at the matching row, which you then read out of whatever columns you want.

## Where to go next

- To bring existing Parquet shards into the format, see [converting crawl shards](/guides/converting-crawl-shards/).
- To group many files into one logical dataset, see [managing a collection](/guides/managing-a-collection/).
- For the full type list and function signatures, see the [Go API reference](/reference/go-api/).
