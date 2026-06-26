package tatami

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"path/filepath"
	"sort"

	"github.com/tamnd/tatami/manifest"
)

// This file turns a directory of .tatami files into one queryable dataset. A
// Collection wraps a manifest.Manifest (the append-only catalog) and adds the
// type-aware pruning the manifest deliberately leaves out: it reads each
// member's key range and zone rollup and uses the same three-valued evaluator
// the in-file Scan uses to drop whole members before opening them. Querying a
// collection is the manifest prune (in memory, no file opened) followed by an
// in-file Scan or Lookup on the few members that survive.

// ManifestName is the catalog file at a collection root.
const ManifestName = "tatami.manifest"

// Collection is a dataset of many tatami files addressed through one manifest.
type Collection struct {
	root   string
	man    *manifest.Manifest
	schema *Schema // cached from the first member, the homogeneous dataset schema
}

// OpenCollection opens (or starts) the collection rooted at dir. A missing
// manifest is an empty collection, not an error.
func OpenCollection(dir string) (*Collection, error) {
	man, err := manifest.Open(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, err
	}
	return &Collection{root: dir, man: man}, nil
}

// Members returns the live members in catalog order.
func (c *Collection) Members() []manifest.Member { return c.man.Live() }

// AddFile catalogs the tatami file at rel (relative to the collection root): it
// opens the file, summarizes its footer into a member entry, and appends one
// ADD_FILE edit. The file itself is untouched.
func (c *Collection) AddFile(rel string) error {
	r, f, err := OpenFile(filepath.Join(c.root, rel))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	mem := r.memberEntry(rel)
	return c.man.AppendAdd(mem)
}

// Compact rolls the manifest log into a fresh one from the live set, bounding
// replay time as removes accumulate.
func (c *Collection) Compact() error { return c.man.Compact() }

// memberEntry summarizes a reader's footer into a manifest member: its identity
// and size, its sort-key range (the coarse level of cross-file pruning), and a
// per-column zone rollup (the file-level zone summary). The bounds are the same
// encoded bytes the in-file zone maps use, so cross-file and in-file pruning
// share one comparison.
func (r *Reader) memberEntry(rel string) manifest.Member {
	mem := manifest.Member{
		FilePath:  rel,
		FileUUID:  r.collectionID(rel),
		RowCount:  r.meta.rowCount,
		ByteSize:  uint64(r.size),
		FooterCRC: r.footerCRC,
	}
	// Sort-key range: widen over every group's bounds, so an unsorted file
	// (no bounds) simply leaves the range empty.
	if si, ok := r.meta.schema.sortKeyIndex(); ok {
		t := r.meta.schema.Fields[si].Type
		var z zoneStat
		for gi := range r.meta.groups {
			g := &r.meta.groups[gi]
			if g.hasSortBounds {
				z = z.merge(t, zoneStat{min: g.sortKeyMin, max: g.sortKeyMax, present: true})
			}
		}
		if z.present {
			mem.SortColumn = r.meta.schema.Fields[si].Name
			mem.SortType = uint8(t)
			mem.SortKeyMin = z.min
			mem.SortKeyMax = z.max
		}
	}
	// Zone rollup: fold each column's chunk zones across all groups into one
	// file-level (min, max), for the columns that carry a zone map.
	rollup := make([]zoneStat, len(r.meta.schema.Fields))
	for gi := range r.meta.groups {
		for _, c := range r.meta.groups[gi].chunks {
			if c.columnID < 0 || c.columnID >= len(rollup) || !c.zone.present {
				continue
			}
			t := r.meta.schema.Fields[c.columnID].Type
			rollup[c.columnID] = rollup[c.columnID].merge(t, c.zone)
		}
	}
	for id, z := range rollup {
		if !z.present {
			continue
		}
		f := r.meta.schema.Fields[id]
		mem.Zones = append(mem.Zones, manifest.ZoneBound{
			Column: f.Name,
			Type:   uint8(f.Type),
			Min:    z.min,
			Max:    z.max,
		})
	}
	return mem
}

// collectionID is the member identity. It uses the file's header file_uuid when
// the writer stamped one; otherwise (the byte-stable default leaves it zero) it
// derives a deterministic 16-byte id from the relative path and the footer CRC,
// so two distinct files never collide in the manifest and re-adding the same
// file yields the same id.
func (r *Reader) collectionID(rel string) [16]byte {
	if r.header.FileUUID != ([16]byte{}) {
		return r.header.FileUUID
	}
	var id [16]byte
	h := fnv.New64a()
	_, _ = h.Write([]byte(rel))
	binary.LittleEndian.PutUint64(id[0:8], h.Sum64())
	h.Reset()
	_, _ = h.Write([]byte(rel))
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], r.footerCRC)
	_, _ = h.Write(crc[:])
	binary.LittleEndian.PutUint64(id[8:16], h.Sum64())
	return id
}

// schemaOf loads and caches the dataset schema by opening the first live member.
// A collection is homogeneous, so any member's schema describes all of them.
func (c *Collection) schemaOf() (*Schema, error) {
	if c.schema != nil {
		return c.schema, nil
	}
	live := c.man.Live()
	if len(live) == 0 {
		return nil, fmt.Errorf("tatami: empty collection has no schema")
	}
	r, f, err := OpenFile(filepath.Join(c.root, live[0].FilePath))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	c.schema = r.meta.schema
	return c.schema, nil
}

// memberView adapts a manifest member's zone rollup to the regionView the
// predicate evaluator consumes, so file-level pruning reuses the in-file
// three-valued logic. It has no membership filter, so an equality leaf prunes
// only by the zone bounds; nullCount is unknown at the file level, reported as
// zero so an IS NULL leaf never prunes a member.
type memberView struct {
	zones map[int]zoneStat
	rows  int
}

func (v *memberView) zone(colID int) (zoneStat, bool) {
	z, ok := v.zones[colID]
	return z, ok && z.present
}
func (v *memberView) nullCount(int) int          { return 0 }
func (v *memberView) rowCount() int              { return v.rows }
func (v *memberView) rejectsEq(int, []byte) bool { return false }

// newMemberView maps a member's named zone bounds onto column ids in schema.
func newMemberView(s *Schema, mem manifest.Member) *memberView {
	byName := make(map[string]int, len(s.Fields))
	for i, f := range s.Fields {
		byName[f.Name] = i
	}
	zones := make(map[int]zoneStat, len(mem.Zones))
	for _, zb := range mem.Zones {
		id, ok := byName[zb.Column]
		if !ok {
			continue
		}
		zones[id] = zoneStat{min: zb.Min, max: zb.Max, present: true}
	}
	return &memberView{zones: zones, rows: int(mem.RowCount)}
}

// Prune returns the members whose key range and zone rollup cannot be ruled out
// by pred, the cross-file pruning that lets a query skip most files without a
// single tail read. A nil pred keeps every member.
func (c *Collection) Prune(pred *Pred) ([]manifest.Member, error) {
	live := c.man.Live()
	if pred == nil {
		return live, nil
	}
	s, err := c.schemaOf()
	if err != nil {
		return nil, err
	}
	if err := pred.bind(s); err != nil {
		return nil, err
	}
	var keep []manifest.Member
	for _, mem := range live {
		if pred.eval(s, newMemberView(s, mem)) != triNo {
			keep = append(keep, mem)
		}
	}
	return keep, nil
}

// CollectionScan is the result of a dataset-wide scan: the projected rows from
// every surviving member, plus counters showing how many files the manifest
// pruning let the query skip.
type CollectionScan struct {
	Schema       *Schema
	Columns      []string
	Rows         [][]any
	FilesTotal   int
	FilesScanned int
}

// Scan runs pred across the collection: it prunes members on the manifest, then
// opens only the survivors and runs the in-file Scan on each, concatenating the
// projected rows in member order.
func (c *Collection) Scan(pred *Pred, projection ...string) (*CollectionScan, error) {
	survivors, err := c.Prune(pred)
	if err != nil {
		return nil, err
	}
	out := &CollectionScan{FilesTotal: c.man.Len()}
	for _, mem := range survivors {
		r, f, err := OpenFile(filepath.Join(c.root, mem.FilePath))
		if err != nil {
			return nil, err
		}
		res, err := r.Scan(pred, projection...)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		_ = f.Close()
		out.FilesScanned++
		if out.Schema == nil {
			out.Schema = res.Schema
			out.Columns = res.Columns
		}
		out.Rows = append(out.Rows, res.Rows...)
	}
	return out, nil
}

// CollHit locates a row across the collection: which member holds it and where
// inside that member.
type CollHit struct {
	Member string
	RowRef
}

// Lookup finds a key across a sorted collection. It keeps only the members whose
// key range contains the key (one member for a disjoint, sorted dataset), opens
// them, and runs the in-file bounded-seek Lookup. FilesOpened reports the fan-out
// so a caller sees that a disjoint collection opens exactly one file.
func (c *Collection) Lookup(key any) (CollHit, bool, int, error) {
	live := c.man.Live()
	opened := 0
	for _, mem := range live {
		if mem.SortColumn == "" {
			continue
		}
		t := LogicalType(mem.SortType)
		if !scalarMatches(t, key) {
			return CollHit{}, false, opened, fmt.Errorf("tatami: lookup key type %T does not match sort column %s", key, t)
		}
		enc := encodeScalar(t, key)
		// Skip a member whose key range cannot hold the key.
		if cmpEncoded(t, enc, mem.SortKeyMin) < 0 || cmpEncoded(t, enc, mem.SortKeyMax) > 0 {
			continue
		}
		r, f, err := OpenFile(filepath.Join(c.root, mem.FilePath))
		if err != nil {
			return CollHit{}, false, opened, err
		}
		opened++
		ref, found, err := r.Lookup(key)
		_ = f.Close()
		if err != nil {
			return CollHit{}, false, opened, err
		}
		if found {
			return CollHit{Member: mem.FilePath, RowRef: ref}, true, opened, nil
		}
	}
	return CollHit{}, false, opened, nil
}

// Merge combines a set of members into one new file under outRel and swaps the
// manifest: the inputs leave the live set and the output joins it, all in one
// atomic batch. It is the general decode-and-re-encode merge: it reads every
// input row, orders the combined stream by the sort key when the inputs are
// sorted, and writes one output through the normal writer, so the output gets
// fresh, well-fitted encodings, dictionaries, and clean row-group boundaries.
// The zero-copy concat path for already-disjoint sorted inputs is a later slice.
func (c *Collection) Merge(inRels []string, outRel string, opts WriterOptions, createdMillis uint64) error {
	if len(inRels) == 0 {
		return fmt.Errorf("tatami: merge needs at least one input")
	}
	rows, schema, inputs, err := c.gatherRows(inRels)
	if err != nil {
		return err
	}
	if si, ok := schema.sortKeyIndex(); ok {
		t := schema.Fields[si].Type
		sort.SliceStable(rows, func(a, b int) bool {
			return cmpScalar(t, rows[a][si], rows[b][si]) < 0
		})
	}
	outPath := filepath.Join(c.root, outRel)
	if err := writeRows(outPath, schema, rows, opts, createdMillis); err != nil {
		return err
	}
	r, f, err := OpenFile(outPath)
	if err != nil {
		return err
	}
	mem := r.memberEntry(outRel)
	_ = f.Close()
	return c.man.Swap([]manifest.Member{mem}, inputs)
}

// gatherRows reads every row of the named members into a single row-major slice,
// returning the shared schema and the input uuids for the manifest swap.
func (c *Collection) gatherRows(inRels []string) ([][]any, *Schema, [][16]byte, error) {
	var schema *Schema
	var rows [][]any
	var uuids [][16]byte
	for _, rel := range inRels {
		r, f, err := OpenFile(filepath.Join(c.root, rel))
		if err != nil {
			return nil, nil, nil, err
		}
		if schema == nil {
			schema = r.meta.schema
		}
		uuids = append(uuids, r.collectionID(rel))
		for gi := 0; gi < r.NumRowGroups(); gi++ {
			cols, err := r.ReadRowGroup(gi)
			if err != nil {
				_ = f.Close()
				return nil, nil, nil, err
			}
			n := r.RowGroupRows(gi)
			for row := 0; row < n; row++ {
				rec := make([]any, len(cols))
				for ci := range cols {
					rec[ci] = cols[ci].At(row)
				}
				rows = append(rows, rec)
			}
		}
		_ = f.Close()
	}
	return rows, schema, uuids, nil
}
