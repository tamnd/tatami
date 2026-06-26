---
title: "CLI reference"
description: "Every tatami command and flag."
weight: 10
---

```
tatami [command] [flags]
```

The CLI inspects, reads, converts, and catalogs `.tatami` files. Run `tatami <command> --help` for the canonical, up-to-date list.

## tatami inspect

```
tatami inspect <file>
```

Prints a header and footer summary: the format version, row and group counts, the role (document store or search segment), the compressed and uncompressed sizes, and a per-column line with the encoding, codec, value and null counts, and any index structures (bloom filter, blob region, sort key). It reads only the header and footer, so it is instant on a file of any size.

## tatami cat

```
tatami cat <file> [flags]
```

Streams the rows of a file to standard output as JSONL, one object per line.

| Flag | Default | Meaning |
|------|---------|---------|
| `--columns` | all | Comma-separated columns to project |
| `--limit` | `0` | Stop after this many rows (0 = no limit) |

Projecting a subset of columns reads only those columns off disk. A blob-separated body column is read only if you project it.

## tatami convert

```
tatami convert <in.parquet> <out.tatami> [flags]
```

Re-encodes a producer Parquet shard as tatami. Reads the Parquet leaf schema, maps each column to a tatami logical type, applies the layout heuristics below, and streams the rows in bounded memory. Prints the size both ways when it finishes.

| Flag | Default | Meaning |
|------|---------|---------|
| `--blob` | `markdown,body,html` | Comma-separated columns to separate into the blob region |
| `--bloom` | `doc_id,url,digest` | Comma-separated columns to build a membership filter on |
| `--dict` | all other strings | Comma-separated string columns to hint toward the shared dictionary |
| `--batch` | `4096` | Rows to read and append at a time (0 = default) |

Passing an empty list (`--bloom ""`) disables that heuristic; omitting a flag keeps its default.

## tatami collection

```
tatami collection [command]
```

Manages the `tatami.manifest` catalog over a directory of files. See [managing a collection](/guides/managing-a-collection/) for the workflow.

### tatami collection add

```
tatami collection add <dir> <file.tatami>...
```

Catalogs one or more files into the collection rooted at `<dir>`, recording each file's key range and zone-statistic rollup so a query can prune it before opening it.

### tatami collection list

```
tatami collection list <dir>
```

Lists the live members of the collection, each with its key span (if it has a sort key) and the columns the manifest can prune on.

### tatami collection compact

```
tatami collection compact <dir>
```

Rolls the append-only manifest log into a fresh one containing only the live set, dropping accumulated add-and-remove churn. The swap is atomic (write a temp file, rename it into place).

## tatami version

```
tatami --version
```

Prints the version, the commit it was built from, and the build date.
