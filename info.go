package tatami

// ColumnStat aggregates one column across all row groups, for inspect and stats.
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
	fi.Columns = cols
	for _, p := range r.meta.kv {
		fi.KeyValue = append(fi.KeyValue, KeyValue{p.key, p.value})
	}
	return fi
}
