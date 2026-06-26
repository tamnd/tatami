---
title: "Managing a collection"
description: "Stitch many tatami files into one logical dataset with a manifest, prune whole files before opening them, look a key up across the set, and compact the log."
weight: 30
---

A single `.tatami` file is a shard. A crawl is thousands of shards, and you want to treat them as one dataset: scan across all of them, look a key up without knowing which file holds it, and add or retire files over time. A `tatami.manifest` is the catalog that makes that work.

## How the manifest works

The manifest is an append-only log in a directory next to the files. Each record adds, removes, or re-tiers a member, and carries a rollup of that file's key range and per-column zone statistics. Because it is append-only, a write is one fsync of a new record, and a torn write at the tail is discarded on read, keeping the last consistent prefix. The rollup is what lets a query prune at the file level: a scan can rule out a whole file from its zone summary before opening it.

## Add files

```bash
tatami collection add ./corpus shard0.tatami shard1.tatami shard2.tatami
```

This catalogs each file into the collection rooted at `./corpus`. The files can live anywhere the path can reach; the manifest records how to find them and what they can be pruned on.

## List members

```bash
tatami collection list ./corpus
```

```
members: 3 live
  shard0.tatami  key [https://a ... https://m]  zones: status, crawl_date
  shard1.tatami  key [https://m ... https://z]  zones: status, crawl_date
  shard2.tatami  (no sort key)                  zones: status, content_type
```

The listing shows each live member, its key span if it has a sort key, and the columns the manifest can prune on. A converted shard with no sort key still prunes by its zone summary.

## Scan and look up from Go

The Go API opens a collection over a directory and reuses the same predicate evaluator as a single file, lifted to the file level.

```go
c, err := tatami.OpenCollection("./corpus")
if err != nil {
	log.Fatal(err)
}

scan, err := c.Scan(tatami.Eq("status", int32(404)), "url", "status")
if err != nil {
	log.Fatal(err)
}
fmt.Printf("scanned %d of %d files\n", scan.FilesScanned, scan.FilesTotal)

hit, ok, opened, err := c.Lookup("https://example.com/page")
if err != nil {
	log.Fatal(err)
}
if ok {
	fmt.Printf("found in %s, opened %d file(s)\n", hit.Member, opened)
}
```

`Scan` reports how many files it actually had to open against the total, which is the file-level analogue of group pruning. `Lookup` on a set of disjoint sorted files opens exactly the one file whose key range contains the key.

## Merge and compact

Two operations keep a collection healthy as it grows.

`collection compact` rolls the append-only log into a fresh one containing only the live set, dropping the accumulated add-and-remove churn. It writes a new manifest to a temp file and renames it into place, so the swap is atomic.

```bash
tatami collection compact ./corpus
```

A data-level `Merge` (from the Go API) decodes several member files and re-encodes them into one, then swaps the inputs out and the output in as a single manifest batch, so a reader never sees a half-merged state. This is how small files fold into larger ones over time.

## Where to go next

- To turn a corpus into a keyword index, see [searching a corpus](/guides/searching-a-corpus/).
- For the on-disk shape of the manifest and the file it catalogs, see the [file-format reference](/reference/file-format/).
