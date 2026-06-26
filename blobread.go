package tatami

import (
	"fmt"

	"github.com/tamnd/tatami/blob"
	"github.com/tamnd/tatami/codec"
)

// blobResolver turns a column's global value ordinal into its bytes. It owns the
// run directory, the codec the runs use (dictionary-bound or plain zstd), and a
// cache of decoded runs so repeated reads inside one run pay the decompression
// once.
type blobResolver struct {
	dir   blob.Directory
	runs  []blobRunDesc
	cdc   codec.Codec
	cache map[int][][]byte
}

// blobResolver builds (or returns the cached) resolver for a separated column.
func (r *Reader) blobResolver(colID int) (*blobResolver, error) {
	if res, ok := r.resolvers[colID]; ok {
		return res, nil
	}
	var bc *blobColDesc
	for i := range r.meta.blobCols {
		if r.meta.blobCols[i].columnID == colID {
			bc = &r.meta.blobCols[i]
			break
		}
	}
	if bc == nil {
		return nil, fmt.Errorf("tatami: column %d has no blob descriptor", colID)
	}
	cdc, err := r.blobCodec(bc)
	if err != nil {
		return nil, err
	}
	counts := make([]int, len(bc.runs))
	for i, run := range bc.runs {
		counts[i] = run.numValues
	}
	res := &blobResolver{
		dir:   blob.NewDirectory(counts),
		runs:  bc.runs,
		cdc:   cdc,
		cache: map[int][][]byte{},
	}
	r.resolvers[colID] = res
	return res, nil
}

// blobCodec returns the codec a column's run records were written with: a
// dictionary-bound zstd when the column trained one, otherwise plain zstd.
func (r *Reader) blobCodec(bc *blobColDesc) (codec.Codec, error) {
	if bc.dictIndex > 0 {
		if bc.dictIndex > len(r.meta.dicts) {
			return nil, fmt.Errorf("tatami: dict index %d out of range", bc.dictIndex)
		}
		d := r.meta.dicts[bc.dictIndex-1]
		ph, comp, err := r.readRecord(d.recordOffset)
		if err != nil {
			return nil, err
		}
		dcdc, err := codec.ByID(codec.ID(ph.codec))
		if err != nil {
			return nil, err
		}
		dictBytes, err := dcdc.Decompress(nil, comp, int(d.length))
		if err != nil {
			return nil, err
		}
		return codec.NewZstdDict(dictBytes, codec.DefaultLevel)
	}
	return codec.ByID(codec.ID(bc.recordCodec))
}

// value returns the bytes for one global ordinal of a column, decoding and
// caching the run that holds it.
func (res *blobResolver) value(r *Reader, ordinal int) ([]byte, error) {
	run, idx, err := res.dir.Locate(ordinal)
	if err != nil {
		return nil, err
	}
	vals, ok := res.cache[run]
	if !ok {
		rd := res.runs[run]
		_, comp, err := r.readRecord(rd.recordOffset)
		if err != nil {
			return nil, err
		}
		payload, err := res.cdc.Decompress(nil, comp, int(rd.uncompressedSize))
		if err != nil {
			return nil, err
		}
		vals, err = blob.UnpackRun(payload)
		if err != nil {
			return nil, err
		}
		res.cache[run] = vals
	}
	if idx >= len(vals) {
		return nil, fmt.Errorf("tatami: blob value index %d overruns run of %d", idx, len(vals))
	}
	return vals[idx], nil
}

// readRecord reads a page-framed record (a row-group page, a blob run, or a
// dictionary): its 32-byte header followed by compressedSize payload bytes, with
// the payload checksum verified.
func (r *Reader) readRecord(off int64) (pageHeader, []byte, error) {
	hb, err := readAt(r.r, off, PageHeaderSize)
	if err != nil {
		return pageHeader{}, nil, err
	}
	ph, err := decodePageHeader(hb)
	if err != nil {
		return pageHeader{}, nil, err
	}
	comp, err := readAt(r.r, off+PageHeaderSize, int(ph.compressedSize))
	if err != nil {
		return pageHeader{}, nil, err
	}
	if got := crc32c(comp); got != ph.payloadCRC32C {
		return pageHeader{}, nil, fmt.Errorf("tatami: record checksum mismatch at offset %d", off)
	}
	return ph, comp, nil
}

// readSeparatedBlobColumn reconstructs a BLOBREF column for one row group. It
// reads the chunk's validity bitmap, maps each present row to its global value
// ordinal for the column, and resolves the ordinal through the blob region.
func (r *Reader) readSeparatedBlobColumn(group, col int, cm chunkMeta) (Column, error) {
	valid, err := r.readBlobChunkValidity(cm, col, group)
	if err != nil {
		return Column{}, err
	}
	res, err := r.blobResolver(col)
	if err != nil {
		return Column{}, err
	}
	base := r.blobOrdinalBase(group, col)
	out := make([][]byte, cm.numValues)
	k := 0
	for i := 0; i < cm.numValues; i++ {
		if valid != nil && !valid[i] {
			continue
		}
		v, err := res.value(r, base+k)
		if err != nil {
			return Column{}, fmt.Errorf("tatami: column %d group %d row %d: %w", col, group, i, err)
		}
		out[i] = v
		k++
	}
	return Column{Data: out, Valid: valid}, nil
}

// blobOrdinalBase returns the number of present values of column col in all row
// groups before group, which is the global ordinal of this group's first present
// value.
func (r *Reader) blobOrdinalBase(group, col int) int {
	base := 0
	for g := 0; g < group; g++ {
		for _, c := range r.meta.groups[g].chunks {
			if c.columnID == col {
				base += c.numValues - c.nullCount
				break
			}
		}
	}
	return base
}

// readBlobChunkValidity walks a separated column chunk's bitmap-only pages and
// returns the per-row validity, or nil when the chunk has no nulls.
func (r *Reader) readBlobChunkValidity(cm chunkMeta, col, group int) ([]bool, error) {
	var valid []bool
	if cm.nullCount > 0 {
		valid = make([]bool, 0, cm.numValues)
	}
	off := cm.firstPageOffset
	for p := 0; p < cm.numPages; p++ {
		ph, comp, err := r.readRecord(off)
		if err != nil {
			return nil, err
		}
		cdc, err := codec.ByID(codec.ID(ph.codec))
		if err != nil {
			return nil, err
		}
		plain, err := cdc.Decompress(nil, comp, int(ph.uncompressedSize))
		if err != nil {
			return nil, err
		}
		num := int(ph.numValues)
		var bitmap []byte
		if ph.flags&pageFlagNullsPresent != 0 {
			bl := validityBytes(num)
			if bl > len(plain) {
				return nil, fmt.Errorf("tatami: validity bitmap overruns blob page in column %d group %d", col, group)
			}
			bitmap = plain[:bl]
		}
		if valid != nil {
			for i := 0; i < num; i++ {
				valid = append(valid, validAt(bitmap, i))
			}
		}
		off += PageHeaderSize + int64(ph.compressedSize)
	}
	return valid, nil
}
