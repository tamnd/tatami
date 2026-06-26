package tatami

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/tatami/codec"
)

// Reader opens a .tatami file and yields its columns. It reads the trailer and
// footer first (one tail read for a remote file), then reads only the column
// chunks a caller asks for.
type Reader struct {
	r      io.ReaderAt
	size   int64
	header *Header
	meta   *fileMeta
}

// Open parses a file accessible through r whose total length is size.
func Open(r io.ReaderAt, size int64) (*Reader, error) {
	if size < HeaderSize+TrailerSize {
		return nil, fmt.Errorf("tatami: file too small: %d bytes", size)
	}
	tr, err := readAt(r, size-TrailerSize, TrailerSize)
	if err != nil {
		return nil, err
	}
	if string(tr[8:12]) != Magic {
		return nil, fmt.Errorf("tatami: bad end magic %q", tr[8:12])
	}
	footerLen := int64(binary.LittleEndian.Uint32(tr[0:4]))
	footerCRC := binary.LittleEndian.Uint32(tr[4:8])
	footerOff := size - TrailerSize - footerLen
	if footerOff < HeaderSize {
		return nil, fmt.Errorf("tatami: footer offset %d before header", footerOff)
	}
	footer, err := readAt(r, footerOff, int(footerLen))
	if err != nil {
		return nil, err
	}
	if got := crc32c(footer); got != footerCRC {
		return nil, fmt.Errorf("tatami: footer checksum mismatch: got %08x want %08x", got, footerCRC)
	}
	meta, err := decodeFooter(footer)
	if err != nil {
		return nil, err
	}
	hb, err := readAt(r, 0, HeaderSize)
	if err != nil {
		return nil, err
	}
	h, err := decodeHeader(hb)
	if err != nil {
		return nil, err
	}
	if h.FooterOffset != uint64(footerOff) {
		return nil, fmt.Errorf("tatami: header footer offset %d disagrees with trailer %d", h.FooterOffset, footerOff)
	}
	if h.RowCount != meta.rowCount {
		return nil, fmt.Errorf("tatami: header row count %d disagrees with stats %d", h.RowCount, meta.rowCount)
	}
	if err := meta.schema.validate(); err != nil {
		return nil, err
	}
	return &Reader{r: r, size: size, header: h, meta: meta}, nil
}

// OpenFile opens path and returns a Reader plus the underlying file. The caller
// closes the file when done.
func OpenFile(path string) (*Reader, *os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	r, err := Open(f, st.Size())
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return r, f, nil
}

func readAt(r io.ReaderAt, off int64, n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := r.ReadAt(b, off); err != nil {
		return nil, fmt.Errorf("tatami: read %d bytes at %d: %w", n, off, err)
	}
	return b, nil
}

// Schema returns the file schema.
func (r *Reader) Schema() *Schema { return r.meta.schema }

// NumRows returns the total row count.
func (r *Reader) NumRows() uint64 { return r.meta.rowCount }

// NumRowGroups returns the number of row groups.
func (r *Reader) NumRowGroups() int { return len(r.meta.groups) }

// RowGroupRows returns the row count of group g.
func (r *Reader) RowGroupRows(g int) int { return r.meta.groups[g].numRows }

// Meta returns the value of a footer key-value pair and whether it was present.
func (r *Reader) Meta(key string) (string, bool) {
	for _, p := range r.meta.kv {
		if p.key == key {
			return p.value, true
		}
	}
	return "", false
}

// Header exposes a copy of the file header for inspection tooling.
func (r *Reader) Header() Header { return *r.header }

// ReadColumn reads column col of row group group and returns its values.
func (r *Reader) ReadColumn(group, col int) (Column, error) {
	if group < 0 || group >= len(r.meta.groups) {
		return Column{}, fmt.Errorf("tatami: row group %d out of range", group)
	}
	if col < 0 || col >= len(r.meta.schema.Fields) {
		return Column{}, fmt.Errorf("tatami: column %d out of range", col)
	}
	g := r.meta.groups[group]
	var cm chunkMeta
	found := false
	for _, c := range g.chunks {
		if c.columnID == col {
			cm = c
			found = true
			break
		}
	}
	if !found {
		return Column{}, fmt.Errorf("tatami: column %d missing in group %d", col, group)
	}
	f := r.meta.schema.Fields[col]
	out := emptyTyped(f.Type)
	anyNull := cm.nullCount > 0
	var valid []bool
	if anyNull {
		valid = make([]bool, 0, cm.numValues)
	}
	off := cm.firstPageOffset
	for p := 0; p < cm.numPages; p++ {
		hb, err := readAt(r.r, off, PageHeaderSize)
		if err != nil {
			return Column{}, err
		}
		ph, err := decodePageHeader(hb)
		if err != nil {
			return Column{}, err
		}
		payloadOff := off + PageHeaderSize
		comp, err := readAt(r.r, payloadOff, int(ph.compressedSize))
		if err != nil {
			return Column{}, err
		}
		if got := crc32c(comp); got != ph.payloadCRC32C {
			return Column{}, fmt.Errorf("tatami: page checksum mismatch in column %d group %d", col, group)
		}
		cdc, err := codec.ByID(codec.ID(ph.codec))
		if err != nil {
			return Column{}, err
		}
		plain, err := cdc.Decompress(nil, comp, int(ph.uncompressedSize))
		if err != nil {
			return Column{}, err
		}
		num := int(ph.numValues)
		var bitmap []byte
		body := plain
		if ph.flags&pageFlagNullsPresent != 0 {
			bl := validityBytes(num)
			if bl > len(plain) {
				return Column{}, fmt.Errorf("tatami: validity bitmap overruns page")
			}
			bitmap = plain[:bl]
			body = plain[bl:]
		}
		vals, err := decodePageValues(f.Type, ph.encoding, body, num, bitmap)
		if err != nil {
			return Column{}, err
		}
		out = appendTyped(f.Type, out, vals)
		if anyNull {
			for i := 0; i < num; i++ {
				valid = append(valid, validAt(bitmap, i))
			}
		}
		off = payloadOff + int64(ph.compressedSize)
	}
	return Column{Data: out, Valid: valid}, nil
}

// ReadRowGroup reads every column of one row group.
func (r *Reader) ReadRowGroup(group int) ([]Column, error) {
	cols := make([]Column, len(r.meta.schema.Fields))
	for c := range cols {
		col, err := r.ReadColumn(group, c)
		if err != nil {
			return nil, err
		}
		cols[c] = col
	}
	return cols, nil
}
