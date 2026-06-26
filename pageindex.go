package tatami

import "encoding/binary"

// The per-page index is the fine level of pruning inside a surviving chunk and
// the fine level of the sparse primary-key index on the sort column. It is one
// small page, kind PageIdx, written right after the chunk's data pages; the
// chunk entry in the footer points at it through pageIndexOffset. Each entry
// gives a page's first row, its byte offset, and its zone min/max, so a reader
// binary-searches to the one page that can hold a key or skips pages a predicate
// rules out, without decoding any data page.

// pageEntry is one row of the per-page index.
type pageEntry struct {
	firstRow        int
	firstPageOffset int64
	zone            zoneStat
}

// encodePageIndex packs the entries into the uncompressed page payload.
func encodePageIndex(entries []pageEntry) []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(entries)))
	for _, e := range entries {
		b = binary.AppendUvarint(b, uint64(e.firstRow))
		b = binary.AppendUvarint(b, uint64(e.firstPageOffset))
		var present byte
		if e.zone.present {
			present = 1
		}
		b = append(b, present)
		if e.zone.present {
			b = appendBytes(b, e.zone.min)
			b = appendBytes(b, e.zone.max)
		}
	}
	return b
}

// decodePageIndex reverses encodePageIndex.
func decodePageIndex(payload []byte) ([]pageEntry, error) {
	c := &cursor{b: payload}
	n := c.uvarint()
	out := make([]pageEntry, 0, n)
	for i := uint64(0); i < n && c.err == nil; i++ {
		e := pageEntry{}
		e.firstRow = int(c.uvarint())
		e.firstPageOffset = int64(c.uvarint())
		if c.byte1() != 0 {
			e.zone.min = c.lenBytes()
			e.zone.max = c.lenBytes()
			e.zone.present = true
		}
		out = append(out, e)
	}
	if c.err != nil {
		return nil, c.err
	}
	return out, nil
}

// lowerBoundPages returns the first page index whose zone max is >= key, the
// standard binary search for a sorted-column lookup. It assumes the page zones
// are non-overlapping and ascending, which holds for the sort column.
func lowerBoundPages(t LogicalType, pages []pageEntry, key []byte) int {
	lo, hi := 0, len(pages)
	for lo < hi {
		mid := (lo + hi) / 2
		if !pages[mid].zone.present || cmpEncoded(t, pages[mid].zone.max, key) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}
