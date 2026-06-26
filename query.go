package tatami

import (
	"fmt"
	"sort"

	"github.com/tamnd/tatami/index"
)

// This file is the reader's pushdown surface: a predicate scan that prunes whole
// row groups before decoding, and a point lookup that walks the sparse key index
// in a bounded number of seeks. Both compose the structures the writer laid
// down: chunk zone maps, the per-group membership filters, and the sort-column
// page index.

// RowRef locates a row by its group and its index within that group.
type RowRef struct {
	Group int
	Row   int
}

// ScanResult is what Scan returns: the projected schema, one row per surviving
// row in row order, and counters that show how much the pruning saved.
type ScanResult struct {
	Schema        *Schema
	Columns       []string
	Rows          [][]any
	GroupsTotal   int
	GroupsScanned int // groups the predicate could not prune
}

// groupView adapts a row group's footer metadata to the regionView the
// predicate evaluator consumes. It lazily loads a column's membership filter the
// first time an equality leaf probes it.
type groupView struct {
	r      *Reader
	g      *rowGroupMeta
	byCol  map[int]*chunkMeta
	blooms map[int]*index.Bloom // colID -> loaded filter, nil when absent
}

func (r *Reader) newGroupView(g *rowGroupMeta) *groupView {
	byCol := make(map[int]*chunkMeta, len(g.chunks))
	for i := range g.chunks {
		byCol[g.chunks[i].columnID] = &g.chunks[i]
	}
	return &groupView{r: r, g: g, byCol: byCol, blooms: map[int]*index.Bloom{}}
}

func (v *groupView) zone(colID int) (zoneStat, bool) {
	c, ok := v.byCol[colID]
	if !ok {
		return zoneStat{}, false
	}
	return c.zone, c.zone.present
}

func (v *groupView) nullCount(colID int) int {
	if c, ok := v.byCol[colID]; ok {
		return c.nullCount
	}
	return 0
}

func (v *groupView) rowCount() int { return v.g.numRows }

func (v *groupView) rejectsEq(colID int, enc []byte) bool {
	c, ok := v.byCol[colID]
	if !ok || c.bloomRef == 0 {
		return false
	}
	bf, loaded := v.blooms[colID]
	if !loaded {
		bf = v.r.loadBloom(c.bloomRef)
		v.blooms[colID] = bf
	}
	if bf == nil {
		return false
	}
	return !bf.MayContain(enc)
}

// loadBloom reads and parses the membership filter at a 1-based bloomRef, or
// returns nil when the ref is out of range or the record cannot be read; a nil
// filter degrades to no pruning, never to a wrong answer.
func (r *Reader) loadBloom(ref int) *index.Bloom {
	if ref <= 0 || ref > len(r.meta.blooms) {
		return nil
	}
	d := r.meta.blooms[ref-1]
	payload, err := r.readIndexRecord(d.recordOffset)
	if err != nil {
		return nil
	}
	bf, err := index.LoadBloom(payload)
	if err != nil {
		return nil
	}
	return bf
}

// readIndexRecord reads an index record (page index or bloom) written by
// writeIndexRecord: a 32-byte header plus an uncompressed, CRC-checked payload.
func (r *Reader) readIndexRecord(off int64) ([]byte, error) {
	hb, err := readAt(r.r, off, PageHeaderSize)
	if err != nil {
		return nil, err
	}
	ph, err := decodePageHeader(hb)
	if err != nil {
		return nil, err
	}
	payload, err := readAt(r.r, off+PageHeaderSize, int(ph.compressedSize))
	if err != nil {
		return nil, err
	}
	if got := crc32c(payload); got != ph.payloadCRC32C {
		return nil, fmt.Errorf("tatami: index record checksum mismatch at offset %d", off)
	}
	return payload, nil
}

// Scan evaluates pred against the file and returns the projected columns of
// every surviving row. An empty projection returns every column. A nil pred
// matches all rows (a pure projection scan).
func (r *Reader) Scan(pred *Pred, projection ...string) (*ScanResult, error) {
	proj, err := r.resolveProjection(projection)
	if err != nil {
		return nil, err
	}
	if pred != nil {
		if err := pred.bind(r.meta.schema); err != nil {
			return nil, err
		}
	}
	// The reader must decode the predicate's columns to apply the row-level
	// filter, plus the projected columns to return. Union the two sets.
	need := map[int]struct{}{}
	for _, id := range proj {
		need[id] = struct{}{}
	}
	if pred != nil {
		pred.columns(need)
	}
	res := &ScanResult{
		Schema:      r.meta.schema,
		Columns:     r.columnNames(proj),
		GroupsTotal: len(r.meta.groups),
	}
	for gi := range r.meta.groups {
		g := &r.meta.groups[gi]
		verdict := triYes
		if pred != nil {
			verdict = pred.eval(r.meta.schema, r.newGroupView(g))
			if verdict == triNo {
				continue // group pruned, no data page touched
			}
		}
		res.GroupsScanned++
		if err := r.scanGroup(gi, pred, verdict == triYes, proj, need, res); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// scanGroup decodes the needed columns of one surviving group, applies the
// row-level predicate (unless allMatch lets it take every row), and appends the
// projected rows to res.
func (r *Reader) scanGroup(gi int, pred *Pred, allMatch bool, proj []int, need map[int]struct{}, res *ScanResult) error {
	cols := make([]Column, len(r.meta.schema.Fields))
	for id := range need {
		c, err := r.ReadColumn(gi, id)
		if err != nil {
			return err
		}
		cols[id] = c
	}
	n := r.meta.groups[gi].numRows
	for row := 0; row < n; row++ {
		if pred != nil && !allMatch && !pred.matchRow(r.meta.schema, cols, row) {
			continue
		}
		out := make([]any, len(proj))
		for j, id := range proj {
			out[j] = cols[id].At(row)
		}
		res.Rows = append(res.Rows, out)
	}
	return nil
}

// Lookup finds the row whose sort key equals key, using the sparse primary-key
// index for a bounded-seek descent. It requires a sorted file; on an unsorted
// file use Scan with an Eq predicate instead. The bool is false when no row has
// the key.
func (r *Reader) Lookup(key any) (RowRef, bool, error) {
	si, ok := r.meta.schema.sortKeyIndex()
	if !ok || r.header.Flags&FlagSorted == 0 {
		return RowRef{}, false, fmt.Errorf("tatami: Lookup needs a sorted file; use Scan(Eq(...)) instead")
	}
	t := r.meta.schema.Fields[si].Type
	if !scalarMatches(t, key) {
		return RowRef{}, false, fmt.Errorf("tatami: Lookup key type %T does not match sort column %s", key, t)
	}
	enc := encodeScalar(t, key)

	// Level 1: binary-search the coarse row-group array on the key range.
	groups := r.meta.groups
	g := sort.Search(len(groups), func(i int) bool {
		return groups[i].hasSortBounds && cmpEncoded(t, groups[i].sortKeyMax, enc) >= 0
	})
	if g == len(groups) || !groups[g].hasSortBounds || cmpEncoded(t, groups[g].sortKeyMin, enc) > 0 {
		return RowRef{}, false, nil
	}

	// Level 2: the sort-column page index inside that group.
	cm := r.chunkOf(g, si)
	if cm == nil || cm.pageIndexOffset == 0 {
		return RowRef{}, false, fmt.Errorf("tatami: sorted file missing sort-column page index in group %d", g)
	}
	payload, err := r.readIndexRecord(cm.pageIndexOffset)
	if err != nil {
		return RowRef{}, false, err
	}
	pages, err := decodePageIndex(payload)
	if err != nil {
		return RowRef{}, false, err
	}
	p := lowerBoundPages(t, pages, enc)
	if p == len(pages) || !pages[p].zone.present || cmpEncoded(t, pages[p].zone.min, enc) > 0 {
		return RowRef{}, false, nil
	}

	// Level 3: decode exactly one data page and binary-search it.
	col, err := r.readDataPage(pages[p].firstPageOffset, t)
	if err != nil {
		return RowRef{}, false, err
	}
	idx, found := binarySearchColumn(t, col, key)
	if !found {
		return RowRef{}, false, nil
	}
	return RowRef{Group: g, Row: pages[p].firstRow + idx}, true, nil
}

// binarySearchColumn finds key in a sorted column page, returning the first
// matching index. Null slots sort as absent; the page is the sort column so it
// has no nulls in practice, but the comparison treats a null as not-equal.
func binarySearchColumn(t LogicalType, col Column, key any) (int, bool) {
	n, _ := col.length()
	lo := sort.Search(n, func(i int) bool {
		if !col.isValid(i) {
			return true
		}
		return cmpScalar(t, scalarAt(t, col, i), key) >= 0
	})
	if lo < n && col.isValid(lo) && cmpScalar(t, scalarAt(t, col, lo), key) == 0 {
		return lo, true
	}
	return 0, false
}

// chunkOf returns the chunk entry for a column in a group, or nil.
func (r *Reader) chunkOf(group, col int) *chunkMeta {
	g := &r.meta.groups[group]
	for i := range g.chunks {
		if g.chunks[i].columnID == col {
			return &g.chunks[i]
		}
	}
	return nil
}

// resolveProjection turns column names into ids; an empty list means every
// column in schema order.
func (r *Reader) resolveProjection(names []string) ([]int, error) {
	if len(names) == 0 {
		ids := make([]int, len(r.meta.schema.Fields))
		for i := range ids {
			ids[i] = i
		}
		return ids, nil
	}
	ids := make([]int, len(names))
	for i, name := range names {
		id := -1
		for j, f := range r.meta.schema.Fields {
			if f.Name == name {
				id = j
				break
			}
		}
		if id < 0 {
			return nil, fmt.Errorf("tatami: projection names unknown column %q", name)
		}
		ids[i] = id
	}
	return ids, nil
}

func (r *Reader) columnNames(ids []int) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = r.meta.schema.Fields[id].Name
	}
	return out
}
