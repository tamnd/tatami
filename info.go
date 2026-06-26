package tatami

// ColumnStat aggregates one column across all row groups, for inspect and stats.
// For a separated BLOBREF column the chunk totals cover only the validity pages;
// the value bytes are reported in the Blob fields, which come from the blob
// region directory.
type ColumnStat struct {
	Name              string
	Type              LogicalType
	Encoding          Encoding
	Codec             Codec
	NumValues         int64
	NullCount         int64
	NumPages          int64
	TotalUncompressed int64
	TotalCompressed   int64
	// Blob fields are non-zero only for separated BLOBREF columns.
	BlobUncompressed int64
	BlobCompressed   int64
	BlobRuns         int
	BlobDict         bool // true when the column kept a shared dictionary
}

// FileInfo is a read-only summary of a file for the CLI.
type FileInfo struct {
	Header            Header
	Schema            *Schema
	NumRowGroups      int
	RowCount          uint64
	UncompressedTotal uint64
	CompressedTotal   uint64
	Columns           []ColumnStat
	KeyValue          []KeyValue
	// NumDicts and DictUncompressed summarize the dict region.
	NumDicts         int
	DictUncompressed int64
}

// KeyValue is one footer key-value metadata pair.
type KeyValue struct {
	Key, Value string
}

// Info builds a summary of the file from the footer, touching no data pages.
func (r *Reader) Info() FileInfo {
	fi := FileInfo{
		Header:            *r.header,
		Schema:            r.meta.schema,
		NumRowGroups:      len(r.meta.groups),
		RowCount:          r.meta.rowCount,
		UncompressedTotal: r.meta.uncompressedTotal,
		CompressedTotal:   r.meta.compressedTotal,
	}
	cols := make([]ColumnStat, len(r.meta.schema.Fields))
	for i, f := range r.meta.schema.Fields {
		cols[i].Name = f.Name
		cols[i].Type = f.Type
	}
	for _, g := range r.meta.groups {
		for _, c := range g.chunks {
			if c.columnID < 0 || c.columnID >= len(cols) {
				continue
			}
			cs := &cols[c.columnID]
			cs.Encoding = c.encoding
			cs.Codec = c.codec
			cs.NumValues += int64(c.numValues)
			cs.NullCount += int64(c.nullCount)
			cs.NumPages += int64(c.numPages)
			cs.TotalUncompressed += c.totalUncompressed
			cs.TotalCompressed += c.totalCompressed
		}
	}
	for _, bc := range r.meta.blobCols {
		if bc.columnID < 0 || bc.columnID >= len(cols) {
			continue
		}
		cs := &cols[bc.columnID]
		cs.BlobRuns = len(bc.runs)
		cs.BlobDict = bc.dictIndex > 0
		for _, run := range bc.runs {
			cs.BlobUncompressed += run.uncompressedSize
			cs.BlobCompressed += run.compressedSize
		}
	}
	fi.Columns = cols
	fi.NumDicts = len(r.meta.dicts)
	for _, d := range r.meta.dicts {
		fi.DictUncompressed += d.length
	}
	for _, p := range r.meta.kv {
		fi.KeyValue = append(fi.KeyValue, KeyValue{p.key, p.value})
	}
	return fi
}
