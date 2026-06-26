// Package convert turns a producer's Parquet shard into a tatami file. It is the
// fleet-adoption bridge: ami and ccrawl-cli already write zstd Parquet through
// parquet-go, and this reads one of those files column by column and re-encodes
// it as tatami, so the same crawl output gains tatami's blob separation, shared
// dictionaries, and pruning structures without a producer change.
//
// The mapping is schema-driven, not hardcoded to one producer. It reads the
// Parquet leaf schema, maps each column to a tatami logical type, and applies a
// small set of overridable heuristics: a large body column (markdown, body,
// html) is separated into the blob region, the low-cardinality string columns
// get a dictionary hint, and the identity columns (doc_id, url, digest) get a
// membership filter. The format library itself stays Parquet-free; only this
// package and the CLI import parquet-go.
package convert

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/tamnd/tatami"
)

// Options tunes a conversion. The zero value is valid: it auto-picks the blob,
// dictionary, and bloom columns by name and streams in the producer's row order
// with no sort key.
type Options struct {
	// Blob names the columns to separate into the blob region as BLOBREF. When
	// nil, columns named markdown, body, or html are separated automatically.
	Blob []string
	// Bloom names the columns to build a per-group membership filter on. When nil,
	// columns named doc_id, url, or digest get one automatically.
	Bloom []string
	// Dict names the string columns to hint toward dictionary encoding. When nil,
	// every string column that is not an identity or body column is hinted.
	Dict []string
	// BatchRows is how many rows to read and append at a time. Zero selects 4096,
	// which bounds memory regardless of shard size.
	BatchRows int
	// Writer is passed through to the tatami writer (row-group size, page size,
	// blob run target).
	Writer tatami.WriterOptions
}

// Stats summarizes one conversion.
type Stats struct {
	Rows     int64
	Columns  int
	InBytes  int64
	OutBytes int64
}

// Ratio is the converted size as a fraction of the source size, so a value below
// one means tatami is smaller. It returns zero when the source is empty.
func (s Stats) Ratio() float64 {
	if s.InBytes == 0 {
		return 0
	}
	return float64(s.OutBytes) / float64(s.InBytes)
}

var defaultBlob = map[string]bool{"markdown": true, "body": true, "html": true}
var defaultBloom = map[string]bool{"doc_id": true, "url": true, "digest": true}

// File converts the Parquet file at inPath into a tatami file at outPath.
func File(inPath, outPath string, opts Options) (Stats, error) {
	in, err := os.Open(inPath)
	if err != nil {
		return Stats{}, err
	}
	defer func() { _ = in.Close() }()
	info, err := in.Stat()
	if err != nil {
		return Stats{}, err
	}
	pf, err := parquet.OpenFile(in, info.Size())
	if err != nil {
		return Stats{}, err
	}

	leaves, schema, err := buildSchema(pf.Schema(), opts)
	if err != nil {
		return Stats{}, err
	}

	w, out, err := tatami.Create(outPath, schema, opts.Writer)
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{Columns: len(leaves), InBytes: info.Size()}

	batch := opts.BatchRows
	if batch <= 0 {
		batch = 4096
	}
	reader := parquet.NewGenericReader[any](pf)
	defer func() { _ = reader.Close() }()
	rows := make([]parquet.Row, batch)
	for {
		n, rerr := reader.ReadRows(rows)
		if n > 0 {
			cols := buildColumns(leaves, rows[:n])
			if err := w.Append(tatami.Batch{Columns: cols}); err != nil {
				_ = w.Close()
				_ = out.Close()
				return stats, err
			}
			stats.Rows += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			_ = w.Close()
			_ = out.Close()
			return stats, rerr
		}
	}
	if err := w.Close(); err != nil {
		_ = out.Close()
		return stats, err
	}
	if err := out.Close(); err != nil {
		return stats, err
	}
	if oi, err := os.Stat(outPath); err == nil {
		stats.OutBytes = oi.Size()
	}
	return stats, nil
}

// leaf records how one Parquet column maps onto a tatami column: its position in
// a row, its tatami type, and whether the converter reads it as raw bytes (for a
// blob or bytes column) rather than the kind's natural Go scalar.
type leaf struct {
	name  string
	col   int
	typ   tatami.LogicalType
	bytes bool // read ByteArray straight to []byte (BLOBREF or BYTES), not string
}

// buildSchema walks the Parquet leaf fields and produces the tatami schema plus
// the per-leaf mapping. It rejects a nested or repeated schema, since the crawl
// producers are flat and the row mapping assumes one value per column per row.
func buildSchema(ps *parquet.Schema, opts Options) ([]leaf, *tatami.Schema, error) {
	blob := nameSet(opts.Blob, defaultBlob)
	bloom := nameSet(opts.Bloom, defaultBloom)
	dict := nameSet(opts.Dict, nil)

	fields := ps.Fields()
	leaves := make([]leaf, 0, len(fields))
	tf := make([]tatami.Field, 0, len(fields))
	for i, f := range fields {
		if !f.Leaf() {
			return nil, nil, fmt.Errorf("convert: column %q is not a leaf; nested schemas are not supported", f.Name())
		}
		name := f.Name()
		typ, isStr, err := mapType(f.Type())
		if err != nil {
			return nil, nil, fmt.Errorf("convert: column %q: %w", name, err)
		}
		field := tatami.Field{Name: name, Type: typ, Nullable: f.Optional()}
		lf := leaf{name: name, col: i, typ: typ, bytes: typ == tatami.TypeBytes}

		switch {
		case blob[name] && (isStr || typ == tatami.TypeBytes):
			// Separate a large body column into the blob region.
			field.Type = tatami.TypeBlobRef
			field.BlobSeparated = true
			lf.typ = tatami.TypeBlobRef
			lf.bytes = true
		case isStr:
			field.Type = tatami.TypeString
			lf.typ = tatami.TypeString
			if bloom[name] {
				field.BloomFilter = true
			} else if dict == nil || dict[name] {
				// Default: hint every non-identity string toward a shared dictionary.
				field.DictHint = true
			}
		}
		if bloom[name] && field.Type != tatami.TypeBlobRef {
			field.BloomFilter = true
		}
		leaves = append(leaves, lf)
		tf = append(tf, field)
	}
	schema, err := tatami.NewSchema(tf...)
	if err != nil {
		return nil, nil, err
	}
	return leaves, schema, nil
}

// mapType maps a Parquet leaf type to a tatami logical type. isStr reports a
// UTF8 byte array, which the heuristics may redirect to a blob or keep as string.
func mapType(t parquet.Type) (typ tatami.LogicalType, isStr bool, err error) {
	switch t.Kind() {
	case parquet.Boolean:
		return tatami.TypeBool, false, nil
	case parquet.Int32:
		if lt := t.LogicalType(); lt != nil && lt.Integer != nil && !lt.Integer.IsSigned {
			return tatami.TypeUint32, false, nil
		}
		return tatami.TypeInt32, false, nil
	case parquet.Int64:
		if lt := t.LogicalType(); lt != nil {
			if lt.Timestamp != nil {
				return tatami.TypeTimestampMicros, false, nil
			}
			if lt.Integer != nil && !lt.Integer.IsSigned {
				return tatami.TypeUint64, false, nil
			}
		}
		return tatami.TypeInt64, false, nil
	case parquet.Float:
		return tatami.TypeFloat32, false, nil
	case parquet.Double:
		return tatami.TypeFloat64, false, nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		if isUTF8(t.LogicalType()) {
			return tatami.TypeString, true, nil
		}
		return tatami.TypeBytes, false, nil
	default:
		return 0, false, fmt.Errorf("unsupported parquet kind %v", t.Kind())
	}
}

func isUTF8(lt *format.LogicalType) bool {
	return lt != nil && lt.UTF8 != nil
}

// buildColumns turns a batch of Parquet rows into tatami columns. A flat row
// holds one value per leaf in column order; a null value contributes the type's
// zero plus a cleared validity bit, and the bitmap is allocated only when the
// batch actually carries a null.
func buildColumns(leaves []leaf, rows []parquet.Row) []tatami.Column {
	n := len(rows)
	cols := make([]tatami.Column, len(leaves))
	for li, lf := range leaves {
		valid, hasNull := validity(rows, lf.col, n)
		cols[li] = column(lf, rows, valid, hasNull, n)
	}
	return cols
}

func validity(rows []parquet.Row, col, n int) ([]bool, bool) {
	hasNull := false
	for _, row := range rows {
		if row[col].IsNull() {
			hasNull = true
			break
		}
	}
	if !hasNull {
		return nil, false
	}
	valid := make([]bool, n)
	for i, row := range rows {
		valid[i] = !row[col].IsNull()
	}
	return valid, true
}

func column(lf leaf, rows []parquet.Row, valid []bool, hasNull bool, n int) tatami.Column {
	switch lf.typ {
	case tatami.TypeBool:
		d := make([]bool, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.Boolean()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeInt32:
		d := make([]int32, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.Int32()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeUint32:
		d := make([]uint32, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = uint32(v.Int32())
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeInt64, tatami.TypeTimestampMicros:
		d := make([]int64, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.Int64()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeUint64:
		d := make([]uint64, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = uint64(v.Int64())
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeFloat32:
		d := make([]float32, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.Float()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeFloat64:
		d := make([]float64, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.Double()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	case tatami.TypeBytes, tatami.TypeBlobRef:
		d := make([][]byte, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = append([]byte(nil), v.ByteArray()...)
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	default: // TypeString
		d := make([]string, n)
		for i, row := range rows {
			if v := row[lf.col]; !v.IsNull() {
				d[i] = v.String()
			}
		}
		return tatami.Column{Data: d, Valid: valid}
	}
}

// nameSet turns an override list into a set, falling back to def when the list is
// nil so an unset option keeps the built-in heuristic. An explicit empty
// (non-nil, zero-length) list disables the heuristic.
func nameSet(list []string, def map[string]bool) map[string]bool {
	if list == nil {
		return def
	}
	s := make(map[string]bool, len(list))
	for _, n := range list {
		s[strings.TrimSpace(n)] = true
	}
	return s
}
