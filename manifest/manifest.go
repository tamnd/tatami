// Package manifest is the collection catalog for tatami: an append-only log of
// edits that names the live members of a dataset and carries enough per-member
// summary (key range and a coarse zone rollup) that a reader prunes across
// files before opening any of them.
//
// The design mirrors the kv MANIFEST: a sequence of length-prefixed, tagged,
// CRC-checked edit records, replayed in order on open to rebuild the current
// member set, and compacted into a fresh log from the live set when the dead
// fraction grows. The package is self-contained, it knows nothing about the
// file format it catalogs beyond the opaque key bounds the caller hands it, so
// the comparison semantics (which type a bound is, how it orders) stay with the
// root package that owns them.
package manifest

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// Magic marks a manifest file. Version is bumped only for an incompatible log
// framing change; new edit kinds and new member fields ride a minor reader that
// skips what it does not recognize, the same forward-compatibility the footer
// uses.
const (
	Magic   = "TMAN"
	Version = byte(1)
)

// Edit kinds, the tags in the log.
const (
	EditAdd     byte = 1 // body is an encoded Member
	EditRemove  byte = 2 // body is a 16-byte file_uuid
	EditSetTier byte = 3 // body is a 16-byte file_uuid plus one tier byte
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ZoneBound is one column's coarse (min, max) rollup across a member, lifted
// from its per-row-group zone maps. Type mirrors the column's logical type as a
// byte so the caller can decode and compare the bounds; the manifest itself
// treats them as opaque.
type ZoneBound struct {
	Column string
	Type   uint8
	Min    []byte
	Max    []byte
}

// Member describes one live file in the collection. The fields are chosen so a
// reader prunes on the manifest alone: SortKeyMin/Max bound the member's key
// range, and Zones rolls up the columns a reader commonly filters on.
type Member struct {
	FilePath      string
	FileUUID      [16]byte
	RowCount      uint64
	ByteSize      uint64
	SortColumn    string // empty when the member is unsorted
	SortType      uint8  // logical type of the sort column, when sorted
	SortKeyMin    []byte
	SortKeyMax    []byte
	Zones         []ZoneBound
	Tier          uint8
	Crawls        []string
	CreatedMillis uint64
	FooterCRC     uint32
}

// Manifest is the replayed live state plus a handle to the log it came from.
// It assumes a single appender and tolerates many readers, the same discipline
// kv uses for its MANIFEST.
type Manifest struct {
	path    string
	byUUID  map[[16]byte]*Member
	added   int // ADD records seen, for the dead-fraction estimate
	records int // total records replayed
}

// Open replays the manifest at path. A missing file is an empty collection, not
// an error, so a fresh dataset opens cleanly and gains members by appending.
func Open(path string) (*Manifest, error) {
	m := &Manifest{path: path, byUUID: map[[16]byte]*Member{}}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}
	defer func() { _ = f.Close() }()
	if err := m.replay(f); err != nil {
		return nil, err
	}
	return m, nil
}

// replay reads the header and applies every intact record. A torn tail (a crash
// mid-append) is detected by a short read or a CRC mismatch and discarded, so
// replay recovers the last consistent prefix rather than failing.
func (m *Manifest) replay(r io.Reader) error {
	br := bufio.NewReader(r)
	head := make([]byte, len(Magic)+1)
	if _, err := io.ReadFull(br, head); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil // empty or truncated header: nothing live
		}
		return err
	}
	if string(head[:len(Magic)]) != Magic {
		return fmt.Errorf("manifest: bad magic %q", head[:len(Magic)])
	}
	if head[len(Magic)] != Version {
		return fmt.Errorf("manifest: unsupported version %d", head[len(Magic)])
	}
	for {
		rec, ok, err := readRecord(br)
		if err != nil {
			return err
		}
		if !ok {
			return nil // clean end or torn tail discarded
		}
		m.apply(rec.kind, rec.body)
	}
}

func (m *Manifest) apply(kind byte, body []byte) {
	m.records++
	switch kind {
	case EditAdd:
		mem, err := decodeMember(body)
		if err != nil {
			return
		}
		m.byUUID[mem.FileUUID] = mem
		m.added++
	case EditRemove:
		if len(body) >= 16 {
			var id [16]byte
			copy(id[:], body[:16])
			delete(m.byUUID, id)
		}
	case EditSetTier:
		if len(body) >= 17 {
			var id [16]byte
			copy(id[:], body[:16])
			if mem, ok := m.byUUID[id]; ok {
				mem.Tier = body[16]
			}
		}
	}
}

// Live returns the current members in a deterministic order (by file path) so
// callers and tests see stable output.
func (m *Manifest) Live() []Member {
	out := make([]Member, 0, len(m.byUUID))
	for _, mem := range m.byUUID {
		out = append(out, *mem)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].FilePath < out[j].FilePath })
	return out
}

// Len reports the live member count.
func (m *Manifest) Len() int { return len(m.byUUID) }

// DeadFraction estimates how much of the log is dead (removed or re-tiered
// records) relative to the live set, the signal a compaction policy reads to
// decide when to roll the log.
func (m *Manifest) DeadFraction() float64 {
	if m.records == 0 {
		return 0
	}
	return float64(m.records-len(m.byUUID)) / float64(m.records)
}

// edit is one record to append. Callers build a batch so a set swap (add the
// merge output, remove its inputs) commits atomically from a replaying reader's
// point of view: the reader either sees the whole appended run or, on a torn
// write, none of the records past the tear.
type edit struct {
	kind byte
	body []byte
}

// AppendAdd records a new member.
func (m *Manifest) AppendAdd(mem Member) error {
	return m.append([]edit{{EditAdd, encodeMember(mem)}})
}

// AppendRemove drops a member by uuid.
func (m *Manifest) AppendRemove(id [16]byte) error {
	return m.append([]edit{{EditRemove, append([]byte(nil), id[:]...)}})
}

// AppendSetTier re-tiers a member in place without rewriting its file.
func (m *Manifest) AppendSetTier(id [16]byte, tier uint8) error {
	body := append(append([]byte(nil), id[:]...), tier)
	return m.append([]edit{{EditSetTier, body}})
}

// Swap atomically replaces a set of inputs with a set of outputs, the edit a
// merge or split commits: one ADD per output and one REMOVE per input, in one
// appended batch.
func (m *Manifest) Swap(add []Member, remove [][16]byte) error {
	edits := make([]edit, 0, len(add)+len(remove))
	for _, mem := range add {
		edits = append(edits, edit{EditAdd, encodeMember(mem)})
	}
	for _, id := range remove {
		edits = append(edits, edit{EditRemove, append([]byte(nil), id[:]...)})
	}
	return m.append(edits)
}

// append writes a batch of records to the log, creating it with a header if it
// is new, then applies them to the in-memory state. It fsyncs so a committed
// edit survives a crash.
func (m *Manifest) append(edits []edit) error {
	f, err := os.OpenFile(m.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	var buf []byte
	if st.Size() == 0 {
		buf = append(buf, Magic...)
		buf = append(buf, Version)
	}
	for _, e := range edits {
		buf = appendRecord(buf, e.kind, e.body)
	}
	if _, err := f.Write(buf); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	for _, e := range edits {
		m.apply(e.kind, e.body)
	}
	return nil
}

// Compact rewrites the log from the current live set, so replay time stays
// bounded as removes and re-tiers accumulate. It writes a temp file and renames
// it over the old one, an atomic swap on a POSIX filesystem.
func (m *Manifest) Compact() error {
	live := m.Live()
	var buf []byte
	buf = append(buf, Magic...)
	buf = append(buf, Version)
	for _, mem := range live {
		buf = appendRecord(buf, EditAdd, encodeMember(mem))
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	m.added = len(live)
	m.records = len(live)
	return nil
}

// record framing: kind(1) + len(uvarint) + body + crc32c(u32 LE) over the kind
// byte, the length varint, and the body. The trailing CRC localizes a torn tail.
type record struct {
	kind byte
	body []byte
}

func appendRecord(dst []byte, kind byte, body []byte) []byte {
	start := len(dst)
	dst = append(dst, kind)
	dst = binary.AppendUvarint(dst, uint64(len(body)))
	dst = append(dst, body...)
	sum := crc32.Checksum(dst[start:], castagnoli)
	dst = binary.LittleEndian.AppendUint32(dst, sum)
	return dst
}

// readRecord reads one framed record. It returns ok=false at a clean EOF or on a
// torn or corrupt tail (a partial record or a CRC mismatch), so replay keeps the
// last consistent prefix; it returns an error only on an unexpected read fault.
// The uvarint length is canonical, so the framing CRC is recomputed by
// re-encoding it rather than capturing the raw reader bytes.
func readRecord(br *bufio.Reader) (record, bool, error) {
	kind, err := br.ReadByte()
	if err != nil {
		return record{}, false, nil // clean EOF or unreadable tail
	}
	n, err := binary.ReadUvarint(br)
	if err != nil {
		return record{}, false, nil
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(br, body); err != nil {
		return record{}, false, nil // torn tail
	}
	var crcb [4]byte
	if _, err := io.ReadFull(br, crcb[:]); err != nil {
		return record{}, false, nil
	}
	framed := []byte{kind}
	framed = binary.AppendUvarint(framed, n)
	framed = append(framed, body...)
	if binary.LittleEndian.Uint32(crcb[:]) != crc32.Checksum(framed, castagnoli) {
		return record{}, false, nil // corrupt tail discarded
	}
	return record{kind: kind, body: body}, true, nil
}

func encodeMember(m Member) []byte {
	var b []byte
	b = appendStr(b, m.FilePath)
	b = append(b, m.FileUUID[:]...)
	b = binary.AppendUvarint(b, m.RowCount)
	b = binary.AppendUvarint(b, m.ByteSize)
	b = appendStr(b, m.SortColumn)
	b = append(b, m.SortType)
	b = appendBytes(b, m.SortKeyMin)
	b = appendBytes(b, m.SortKeyMax)
	b = binary.AppendUvarint(b, uint64(len(m.Zones)))
	for _, z := range m.Zones {
		b = appendStr(b, z.Column)
		b = append(b, z.Type)
		b = appendBytes(b, z.Min)
		b = appendBytes(b, z.Max)
	}
	b = append(b, m.Tier)
	b = binary.AppendUvarint(b, uint64(len(m.Crawls)))
	for _, c := range m.Crawls {
		b = appendStr(b, c)
	}
	b = binary.AppendUvarint(b, m.CreatedMillis)
	b = binary.LittleEndian.AppendUint32(b, m.FooterCRC)
	return b
}

func decodeMember(body []byte) (*Member, error) {
	c := &cur{b: body}
	m := &Member{}
	m.FilePath = c.str()
	copy(m.FileUUID[:], c.fixed(16))
	m.RowCount = c.uvarint()
	m.ByteSize = c.uvarint()
	m.SortColumn = c.str()
	m.SortType = c.byte1()
	m.SortKeyMin = c.lenBytes()
	m.SortKeyMax = c.lenBytes()
	nz := c.uvarint()
	for i := uint64(0); i < nz && c.err == nil; i++ {
		z := ZoneBound{}
		z.Column = c.str()
		z.Type = c.byte1()
		z.Min = c.lenBytes()
		z.Max = c.lenBytes()
		m.Zones = append(m.Zones, z)
	}
	m.Tier = c.byte1()
	nc := c.uvarint()
	for i := uint64(0); i < nc && c.err == nil; i++ {
		m.Crawls = append(m.Crawls, c.str())
	}
	m.CreatedMillis = c.uvarint()
	m.FooterCRC = binary.LittleEndian.Uint32(c.fixed(4))
	if c.err != nil {
		return nil, c.err
	}
	return m, nil
}

func appendStr(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// cur is a tiny forward cursor over a member body.
type cur struct {
	b   []byte
	pos int
	err error
}

func (c *cur) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.pos:])
	if n <= 0 {
		c.err = fmt.Errorf("manifest: bad uvarint at offset %d", c.pos)
		return 0
	}
	c.pos += n
	return v
}

func (c *cur) byte1() byte {
	if c.err != nil || c.pos >= len(c.b) {
		if c.err == nil {
			c.err = fmt.Errorf("manifest: truncated member")
		}
		return 0
	}
	v := c.b[c.pos]
	c.pos++
	return v
}

func (c *cur) fixed(n int) []byte {
	if c.err != nil || c.pos+n > len(c.b) {
		if c.err == nil {
			c.err = fmt.Errorf("manifest: member fixed field overruns body")
		}
		return make([]byte, n)
	}
	out := c.b[c.pos : c.pos+n]
	c.pos += n
	return out
}

func (c *cur) str() string {
	n := c.uvarint()
	if c.err != nil || c.pos+int(n) > len(c.b) {
		if c.err == nil {
			c.err = fmt.Errorf("manifest: member string overruns body")
		}
		return ""
	}
	s := string(c.b[c.pos : c.pos+int(n)])
	c.pos += int(n)
	return s
}

func (c *cur) lenBytes() []byte {
	n := c.uvarint()
	if c.err != nil || c.pos+int(n) > len(c.b) {
		if c.err == nil {
			c.err = fmt.Errorf("manifest: member bytes overrun body")
		}
		return nil
	}
	out := make([]byte, n)
	copy(out, c.b[c.pos:c.pos+int(n)])
	c.pos += int(n)
	return out
}
