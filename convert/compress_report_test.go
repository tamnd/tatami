package convert

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/tamnd/tatami"
)

// This file is the compression report: it converts a real ccrawl shard to tatami
// through the production convert.File path and prints a column-by-column accounting
// of where the bytes go and how tatami compares to the zstd Parquet it came from.
// It is a measurement, not a pass/fail unit test, so it logs a detailed table and
// only guards the one invariant the format promises: the converted file is smaller
// than its Parquet source while preserving every value. It is gated on a local
// shard, so CI skips it cleanly (Spec 2066, implementation note 9).

// reportShardPath is the shard the report runs against, overridable with the same
// environment variable the latency tests use.
func reportShardPath() string {
	if p := os.Getenv("TATAMI_BENCH_SHARD"); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, "data", "ccrawl", "markdown", "CC-MAIN-2026-25", "000000.parquet")
}

// parquetColumn is one Parquet column's footer accounting, keyed by name so it
// lines up against the tatami column of the same name.
type parquetColumn struct {
	codec       string
	values      int64
	compressed  int64
	uncompresed int64
}

// readParquetColumns sums each Parquet column's compressed and uncompressed bytes
// across row groups from the file footer, the fair baseline for the side-by-side.
func readParquetColumns(t *testing.T, path string) (map[string]parquetColumn, int64) {
	t.Helper()
	in, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(in, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	cols := map[string]parquetColumn{}
	for _, rg := range pf.Metadata().RowGroups {
		for _, c := range rg.Columns {
			md := c.MetaData
			name := md.PathInSchema[len(md.PathInSchema)-1]
			pc := cols[name]
			pc.codec = md.Codec.String()
			pc.values += md.NumValues
			pc.compressed += md.TotalCompressedSize
			pc.uncompresed += md.TotalUncompressedSize
			cols[name] = pc
		}
	}
	return cols, info.Size()
}

func TestCompressionReport(t *testing.T) {
	src := reportShardPath()
	if _, err := os.Stat(src); err != nil {
		t.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", err)
	}

	dir := t.TempDir()
	out := filepath.Join(dir, "shard.tatami")
	stats, err := File(src, out, Options{})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	pqCols, pqBytes := readParquetColumns(t, src)

	r, f, err := tatami.OpenFile(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	fi := r.Info()

	// Headline file-level comparison.
	t.Logf("shard %s", src)
	t.Logf("rows %d, columns %d", stats.Rows, stats.Columns)
	t.Logf("parquet source : %s", humanBytes(pqBytes))
	t.Logf("tatami output  : %s", humanBytes(stats.OutBytes))
	savings := 100 * (1 - float64(stats.OutBytes)/float64(pqBytes))
	t.Logf("tatami is %.1f%% smaller than the zstd parquet source (ratio %.3f)", savings, stats.Ratio())

	// Per-column side-by-side. Raw is the sum of the column's own uncompressed
	// bytes plus, for a separated blob column, the blob run uncompressed bytes.
	t.Logf("")
	t.Logf("%-14s %-12s %-10s %12s %12s %7s   %-10s %12s %7s", "column", "tat-encoding", "tat-codec", "tat-uncomp", "tat-comp", "ratio", "pq-codec", "pq-comp", "pq/tat")
	var rawTotal, compTotal int64
	for _, c := range fi.Columns {
		uncomp := c.TotalUncompressed + c.BlobUncompressed
		comp := c.TotalCompressed + c.BlobCompressed
		rawTotal += uncomp
		compTotal += comp
		ratio := safeRatio(uncomp, comp)
		pq := pqCols[c.Name]
		pqVsTat := safeRatio(pq.compressed, comp) // how many parquet bytes per tatami byte
		note := ""
		if c.BlobRuns > 0 {
			note = "blob"
			if c.BlobDict {
				note += "+dict"
			}
		}
		t.Logf("%-14s %-12s %-10s %12s %12s %6.2fx   %-10s %12s %6.2fx %s",
			c.Name, c.Encoding.String(), c.Codec.String(),
			humanBytes(uncomp), humanBytes(comp), ratio,
			pq.codec, humanBytes(pq.compressed), pqVsTat, note)
	}

	// Dictionary region: the shared cross-group dictionary is where the format
	// pulls ahead of a per-group layout, so it gets its own line.
	rawTotal += fi.DictUncompressed
	t.Logf("")
	t.Logf("shared dictionaries: %d, %s uncompressed across all row groups", fi.NumDicts, humanBytes(fi.DictUncompressed))
	t.Logf("raw uncompressed total : %s", humanBytes(rawTotal))
	t.Logf("tatami compressed total: %s", humanBytes(compTotal))
	t.Logf("in-file compression    : %.2fx (%.1f%% of raw)", safeRatio(rawTotal, compTotal), 100*float64(compTotal)/float64(rawTotal))

	// The one hard invariant: the format must beat its Parquet source.
	if stats.OutBytes >= pqBytes {
		t.Fatalf("tatami output %d is not smaller than parquet source %d", stats.OutBytes, pqBytes)
	}

	// Sanity: every column's value count matches the source, so nothing was dropped.
	names := make([]string, 0, len(fi.Columns))
	for _, c := range fi.Columns {
		names = append(names, c.Name)
	}
	sort.Strings(names)
	for _, c := range fi.Columns {
		if pq, ok := pqCols[c.Name]; ok && c.NumValues != pq.values {
			t.Fatalf("column %s: tatami %d values, parquet %d", c.Name, c.NumValues, pq.values)
		}
	}
}

// safeRatio returns a/b as a float, or zero when b is zero.
func safeRatio(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

// humanBytes formats a byte count with a binary unit suffix.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
