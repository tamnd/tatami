package tatami

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/tamnd/tatami/codec"
)

// Default row-group and page sizes from the format canon. A row group flushes
// at whichever of the two limits it hits first; pages cap at a value count.
const (
	DefaultRowGroupMaxRows  = 128 * 1024
	DefaultRowGroupMaxBytes = 256 << 20
	DefaultPageMaxValues    = 64 * 1024
)

// WriterOptions tune a Writer. The zero value is valid and deterministic: no
// uuid, no creation timestamp, default sizes.
type WriterOptions struct {
	RowGroupMaxRows  int
	RowGroupMaxBytes int
	PageMaxValues    int
	PageSizeHint     int
	// BlobRunTargetBytes caps the raw size of one packed blob run. Larger runs
	// compress better; smaller runs cost less to decode for a single-value read
	// and make a shared dictionary more likely to pay off. Zero takes the default.
	BlobRunTargetBytes int
	UUID               [16]byte
	CreatedMillis      uint64
	CreatorID          uint32
}

func (o *WriterOptions) withDefaults() {
	if o.RowGroupMaxRows <= 0 {
		o.RowGroupMaxRows = DefaultRowGroupMaxRows
	}
	if o.RowGroupMaxBytes <= 0 {
		o.RowGroupMaxBytes = DefaultRowGroupMaxBytes
	}
	if o.PageMaxValues <= 0 {
		o.PageMaxValues = DefaultPageMaxValues
	}
	if o.BlobRunTargetBytes <= 0 {
		o.BlobRunTargetBytes = blobRunTargetBytes
	}
}

// Writer serializes batches of columns into a .tatami file. It streams to an
// io.WriterAt, buffering at most one row group in memory, and patches the fixed
// header at the end once the footer offset and row count are known.
type Writer struct {
	w             io.WriterAt
	pos           int64
	schema        *Schema
	opts          WriterOptions
	blockc        codec.Codec
	builders      []*columnBuilder
	blobCols      []*blobColAccum
	pendingBlooms [][]byte
	inverted      *invertedAttachment
	meta          fileMeta
	rowCount      uint64
	bufRows       int
	bufBytes      int
	kv            []kvPair
	closed        bool
	err           error
}

// NewWriter creates a Writer over an io.WriterAt (an *os.File is the common
// case). It reserves the 64-byte header up front and patches it on Close.
func NewWriter(w io.WriterAt, schema *Schema, opts WriterOptions) (*Writer, error) {
	if err := schema.validate(); err != nil {
		return nil, err
	}
	opts.withDefaults()
	bc, err := codec.Default()
	if err != nil {
		return nil, err
	}
	tw := &Writer{
		w:      w,
		schema: schema,
		opts:   opts,
		blockc: bc,
	}
	tw.builders = make([]*columnBuilder, len(schema.Fields))
	tw.blobCols = make([]*blobColAccum, len(schema.Fields))
	for i, f := range schema.Fields {
		tw.builders[i] = newColumnBuilder(f.Type)
		if separatedBlob(f) {
			tw.blobCols[i] = &blobColAccum{}
		}
	}
	// Reserve the header region so the body starts at a fixed offset.
	if _, err := w.WriteAt(make([]byte, HeaderSize), 0); err != nil {
		return nil, err
	}
	tw.pos = HeaderSize
	return tw, nil
}

// Create opens path for writing and returns a Writer over it.
func Create(path string, schema *Schema, opts WriterOptions) (*Writer, *os.File, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	w, err := NewWriter(f, schema, opts)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return w, f, nil
}

func (w *Writer) write(p []byte) error {
	if w.err != nil {
		return w.err
	}
	n, err := w.w.WriteAt(p, w.pos)
	if err != nil {
		w.err = err
		return err
	}
	w.pos += int64(n)
	return nil
}

// Append adds a batch of rows. All columns must match the schema and have equal
// length. When the buffered row group reaches a size limit it is flushed.
func (w *Writer) Append(batch Batch) error {
	if w.closed {
		return ErrClosed
	}
	if w.err != nil {
		return w.err
	}
	n, err := batch.rows(w.schema)
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	// Cut the batch into pieces that fit the current row group, flushing each
	// time a group fills. A single large Append therefore lands across as many
	// row groups as its size requires.
	for off := 0; off < n; {
		take := n - off
		if room := w.opts.RowGroupMaxRows - w.bufRows; room > 0 && take > room {
			take = room
		}
		for i := range batch.Columns {
			sub := batch.Columns[i].slice(off, take)
			w.builders[i].appendBatch(sub, take)
			w.bufBytes += columnBytes(w.schema.Fields[i].Type, sub, take)
		}
		w.bufRows += take
		off += take
		if w.bufRows >= w.opts.RowGroupMaxRows || w.bufBytes >= w.opts.RowGroupMaxBytes {
			if err := w.flushGroup(); err != nil {
				return err
			}
		}
	}
	return nil
}

// flushGroup writes the buffered rows as one row group and resets the builders.
func (w *Writer) flushGroup() error {
	if w.bufRows == 0 {
		return nil
	}
	g := rowGroupMeta{firstRow: w.rowCount, numRows: w.bufRows}
	groupStart := w.pos
	sortIdx, hasSort := w.schema.sortKeyIndex()
	for i := range w.builders {
		col := w.builders[i].column()
		var cm chunkMeta
		var err error
		if w.blobCols[i] != nil {
			cm, err = w.writeBlobRefChunk(i, col)
		} else {
			cm, err = w.writeChunk(i, col)
		}
		if err != nil {
			return err
		}
		// Build a membership filter for an opted-in, non-blob column. The sort key
		// needs none: the sparse key index answers membership exactly.
		f := w.schema.Fields[i]
		isSortCol := hasSort && sortIdx == i
		if f.BloomFilter && w.blobCols[i] == nil && !isSortCol {
			cm.bloomRef = w.buildGroupBloom(f.Type, col)
		}
		g.chunks = append(g.chunks, cm)
		w.meta.uncompressedTotal += uint64(cm.totalUncompressed)
		w.meta.compressedTotal += uint64(cm.totalCompressed)
	}
	if hasSort {
		for _, cm := range g.chunks {
			if cm.columnID == sortIdx && cm.zone.present {
				g.sortKeyMin = cm.zone.min
				g.sortKeyMax = cm.zone.max
				g.hasSortBounds = true
				break
			}
		}
	}
	g.totalBytes = w.pos - groupStart
	w.meta.groups = append(w.meta.groups, g)
	w.rowCount += uint64(w.bufRows)
	for i := range w.builders {
		w.builders[i].reset()
	}
	w.bufRows = 0
	w.bufBytes = 0
	return nil
}

// writeChunk writes all pages of one column chunk and returns its footer entry.
func (w *Writer) writeChunk(colID int, col Column) (chunkMeta, error) {
	f := w.schema.Fields[colID]
	n, err := col.length()
	if err != nil {
		return chunkMeta{}, err
	}
	cm := chunkMeta{
		columnID:        colID,
		firstPageOffset: w.pos,
		numValues:       n,
		encoding:        EncPlain,
		codec:           Codec(w.blockc.ID()),
	}
	sortCol := false
	if si, ok := w.schema.sortKeyIndex(); ok && si == colID {
		sortCol = true
	}
	var pages []pageEntry
	var chunkZone zoneStat
	step := w.opts.PageMaxValues
	for s := 0; s < n; s += step {
		cnt := step
		if s+cnt > n {
			cnt = n - s
		}
		var present []bool
		if col.Valid != nil {
			present = col.Valid[s : s+cnt]
		}
		bitmap, nullCount := buildValidityMask(present, cnt)
		enc, valbytes := encodePageValues(f.Type, col, s, cnt, present)
		payload := make([]byte, 0, len(bitmap)+len(valbytes))
		payload = append(payload, bitmap...)
		payload = append(payload, valbytes...)
		uncompressed := len(payload)
		compressed := w.blockc.Compress(nil, payload)
		var flags uint8
		if bitmap != nil {
			flags |= pageFlagNullsPresent
		}
		if s == 0 {
			cm.encoding = enc
		}
		pageStart := w.pos
		ph := pageHeader{
			kind:             PageData,
			encoding:         enc,
			codec:            Codec(w.blockc.ID()),
			flags:            flags,
			numValues:        uint32(cnt),
			uncompressedSize: uint32(uncompressed),
			compressedSize:   uint32(len(compressed)),
			nullCount:        uint32(nullCount),
			firstRowIndex:    uint32(s),
			payloadCRC32C:    crc32c(compressed),
		}
		if err := w.write(ph.encode()); err != nil {
			return cm, err
		}
		if err := w.write(compressed); err != nil {
			return cm, err
		}
		pz := columnZone(f.Type, col, s, cnt)
		chunkZone = chunkZone.merge(f.Type, pz)
		pages = append(pages, pageEntry{firstRow: s, firstPageOffset: pageStart, zone: pz})
		cm.totalCompressed += int64(len(compressed))
		cm.totalUncompressed += int64(uncompressed)
		cm.nullCount += nullCount
		cm.numPages++
	}
	cm.zone = chunkZone
	// The sort column carries a per-page index so a point or range lookup binary
	// searches to one page. Other columns rely on the chunk-level zone map for
	// group pruning and do not pay for a page index.
	if sortCol {
		off, err := w.writeIndexRecord(PageIdx, encodePageIndex(pages))
		if err != nil {
			return cm, err
		}
		cm.pageIndexOffset = off
	}
	return cm, nil
}

func (w *Writer) flags() uint16 {
	var fl uint16
	if _, ok := w.schema.sortKeyIndex(); ok {
		fl |= FlagSorted
	}
	if len(w.meta.blobCols) > 0 {
		fl |= FlagHasBlobRegion
	}
	if len(w.meta.dicts) > 0 {
		fl |= FlagHasDictRegion
	}
	if len(w.meta.blooms) > 0 || w.meta.invert.present {
		fl |= FlagHasIndexRegion
	}
	if w.meta.invert.present {
		fl |= FlagRoleSearchSeg
	}
	return fl
}

// invertedAttachment carries the serialized inverted sub-region from a
// SearchBuilder to the writer, to be emitted as index records at Close.
type invertedAttachment struct {
	termDict []byte
	postings []byte
	skips    []byte
	live     []byte
	numTerms uint64
	numDocs  uint64
}

// AttachInverted hands the writer the four serialized runs of an inverted
// sub-region (term dictionary, posting payloads, skip tables, and the live-docs
// bitset), turning the file it produces into a search segment (role bit 4). It
// must be called before Close. The runs are written into the index region and
// addressed by the footer's inverted descriptor. A nil live run records no
// deletions, which a reader treats as all-live.
func (w *Writer) AttachInverted(termDict, postings, skips, live []byte, numTerms, numDocs uint64) {
	w.inverted = &invertedAttachment{
		termDict: termDict,
		postings: postings,
		skips:    skips,
		live:     live,
		numTerms: numTerms,
		numDocs:  numDocs,
	}
}

// Close flushes the last row group, writes the footer and trailer, and patches
// the header. It is safe to call once; later calls are no-ops.
func (w *Writer) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	if err := w.flushGroup(); err != nil {
		return err
	}
	if err := w.writeBlobRegions(); err != nil {
		return err
	}
	if err := w.writeIndexRegion(); err != nil {
		return err
	}

	w.meta.schema = w.schema
	w.meta.rowCount = w.rowCount
	w.meta.kv = w.defaultKV()

	footerOffset := w.pos
	footer := w.meta.encodeFooter()
	if err := w.write(footer); err != nil {
		return err
	}

	var tr [TrailerSize]byte
	binary.LittleEndian.PutUint32(tr[0:4], uint32(len(footer)))
	binary.LittleEndian.PutUint32(tr[4:8], crc32c(footer))
	copy(tr[8:12], Magic)
	if err := w.write(tr[:]); err != nil {
		return err
	}

	h := &Header{
		VersionMajor: VersionMajor,
		VersionMinor: VersionMinor,
		Flags:        w.flags(),
		Checksum:     ChecksumCRC32C,
		DefaultCodec: Codec(w.blockc.ID()),
		PageSizeHint: uint32(w.opts.PageSizeHint),
		FileUUID:     w.opts.UUID,
		RowCount:     w.rowCount,
		FooterOffset: uint64(footerOffset),
		CreatedMphis: w.opts.CreatedMillis,
		CreatorID:    w.opts.CreatorID,
	}
	if _, err := w.w.WriteAt(h.encode(), 0); err != nil {
		w.err = err
		return err
	}
	return nil
}

// defaultKV stamps a deterministic, ordered set of key-value metadata pairs.
func (w *Writer) defaultKV() []kvPair {
	pairs := []kvPair{
		{"tatami.version", fmt.Sprintf("%d.%d", VersionMajor, VersionMinor)},
	}
	if i, ok := w.schema.sortKeyIndex(); ok {
		pairs = append(pairs, kvPair{"tatami.sort_key", w.schema.Fields[i].Name})
	}
	pairs = append(pairs, w.kv...)
	return pairs
}

// SetMeta records a free-form key-value pair in the footer. Pairs are written
// in the order added, after the built-in ones, so output stays deterministic.
func (w *Writer) SetMeta(key, value string) {
	w.kv = append(w.kv, kvPair{key, value})
}
