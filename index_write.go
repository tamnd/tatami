package tatami

import "github.com/tamnd/tatami/index"

// This file is the M3 write path for index structures that do not live inline in
// a column chunk: the per-page index record framing (shared with the inline
// sort-column index) and the membership-filter region.
//
// A page index is written inline right after its chunk's data pages, so its
// offset is known at once. Membership filters are different: the file skeleton
// puts the index region after the row groups and the blob and dict regions, so a
// filter built while a group flushes is buffered and the whole region is written
// at Close, after the blob and dict regions are in place.

// writeIndexRecord frames a payload as a 32-byte page header plus the payload
// and returns the header offset. The payload is stored uncompressed: a page
// index and a bloom bit array are both small and do not compress well, and
// keeping them raw keeps the record byte-stable without a codec round-trip.
func (w *Writer) writeIndexRecord(kind PageKind, payload []byte) (int64, error) {
	off := w.pos
	ph := pageHeader{
		kind:             kind,
		encoding:         EncPlain,
		codec:            CodecNone,
		numValues:        0,
		uncompressedSize: uint32(len(payload)),
		compressedSize:   uint32(len(payload)),
		payloadCRC32C:    crc32c(payload),
	}
	if err := w.write(ph.encode()); err != nil {
		return 0, err
	}
	if err := w.write(payload); err != nil {
		return 0, err
	}
	return off, nil
}

// buildGroupBloom builds a membership filter over the present values of one
// column in the flushing group and buffers it, returning the 1-based bloomRef to
// store in the chunk entry. It returns 0 when the column has no present value,
// so an all-null chunk carries no filter.
func (w *Writer) buildGroupBloom(t LogicalType, col Column) int {
	n, _ := col.length()
	keys := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		if !col.isValid(i) {
			continue
		}
		keys = append(keys, encodeScalar(t, scalarAt(t, col, i)))
	}
	if len(keys) == 0 {
		return 0
	}
	bf := index.BuildBloom(keys, index.BloomBitsPerKey)
	w.pendingBlooms = append(w.pendingBlooms, bf.Encode())
	return len(w.pendingBlooms)
}

// writeIndexRegion writes every buffered bloom filter as an index record and
// records its descriptor, so the footer can address each by offset. It then
// writes the inverted sub-region, when one is attached, as three more index
// records. It runs at Close after the blob and dict regions.
func (w *Writer) writeIndexRegion() error {
	for _, blob := range w.pendingBlooms {
		off, err := w.writeIndexRecord(PageIdx, blob)
		if err != nil {
			return err
		}
		w.meta.blooms = append(w.meta.blooms, bloomDesc{
			recordOffset: off,
			length:       int64(len(blob)),
			kind:         0, // bloom; ribbon reserved for a later slice
		})
	}
	if w.inverted != nil {
		tdOff, err := w.writeIndexRecord(PageIdx, w.inverted.termDict)
		if err != nil {
			return err
		}
		ppOff, err := w.writeIndexRecord(PageIdx, w.inverted.postings)
		if err != nil {
			return err
		}
		skOff, err := w.writeIndexRecord(PageIdx, w.inverted.skips)
		if err != nil {
			return err
		}
		var lvOff int64
		if len(w.inverted.live) > 0 {
			lvOff, err = w.writeIndexRecord(PageIdx, w.inverted.live)
			if err != nil {
				return err
			}
		}
		w.meta.invert = invertDesc{
			present:     true,
			termDictOff: tdOff,
			termDictLen: int64(len(w.inverted.termDict)),
			postingsOff: ppOff,
			postingsLen: int64(len(w.inverted.postings)),
			skipsOff:    skOff,
			skipsLen:    int64(len(w.inverted.skips)),
			numTerms:    w.inverted.numTerms,
			numDocs:     w.inverted.numDocs,
			liveOff:     lvOff,
			liveLen:     int64(len(w.inverted.live)),
		}
	}
	return nil
}
