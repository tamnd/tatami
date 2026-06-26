package tatami

import (
	"encoding/binary"
	"fmt"
)

// Footer section tags. Each section is framed as tag(uvarint), length(uvarint),
// body(bytes) so an old reader skips a tag it does not recognize.
const (
	secSchema    uint64 = 1
	secRowGroups uint64 = 2
	secBlobDesc  uint64 = 3
	secDictDesc  uint64 = 4
	secIndexDesc uint64 = 5
	secKeyValue  uint64 = 6
	secStats     uint64 = 7
)

// footerCodecNone marks an uncompressed footer body. M0 never compresses the
// footer; a later milestone may zstd it for large files.
const footerCodecNone byte = 0

// chunkMeta is the footer entry for one column chunk (03 section 4). M0 fills
// the size and navigation fields; zone maps, blooms, dictionaries, and page
// indexes arrive in later milestones and are gated by chunkFlags.
type chunkMeta struct {
	columnID          int
	firstPageOffset   int64
	totalCompressed   int64
	totalUncompressed int64
	numValues         int
	nullCount         int
	numPages          int
	encoding          Encoding
	codec             Codec
}

// rowGroupMeta is the footer entry for one row group (03 section 5).
type rowGroupMeta struct {
	firstRow   uint64
	numRows    int
	totalBytes int64
	chunks     []chunkMeta
}

// kvPair is one KEY_VALUE_META entry, kept as an ordered slice so the footer
// bytes are deterministic.
type kvPair struct {
	key, value string
}

// blobRunDesc is the footer entry for one packed-and-compressed blob run. The
// reader reads compressedSize bytes at recordOffset+PageHeaderSize, decompresses
// to uncompressedSize, and slices out one of the run's numValues values.
type blobRunDesc struct {
	recordOffset     int64
	compressedSize   int64
	uncompressedSize int64
	numValues        int
}

// blobColDesc is the footer entry for one separated blob column: which column it
// belongs to, the dictionary it shares (1-based into fileMeta.dicts, 0 for
// none), the codec its run records use, and the run directory in order.
type blobColDesc struct {
	columnID    int
	dictIndex   int
	recordCodec Codec
	runs        []blobRunDesc
}

// dictDesc is the footer entry for one dictionary record in the dict region.
type dictDesc struct {
	recordOffset int64
	length       int64
}

// fileMeta is everything the footer records about a file.
type fileMeta struct {
	schema            *Schema
	groups            []rowGroupMeta
	blobCols          []blobColDesc
	dicts             []dictDesc
	rowCount          uint64
	kv                []kvPair
	uncompressedTotal uint64
	compressedTotal   uint64
}

func appendStr(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func (m *fileMeta) encodeSchema() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(m.schema.Fields)))
	for _, f := range m.schema.Fields {
		b = appendStr(b, f.Name)
		b = append(b, byte(f.Type))
		b = append(b, f.flags())
		b = append(b, byte(f.Element))
	}
	return b
}

func (m *fileMeta) encodeRowGroups() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(m.groups)))
	for _, g := range m.groups {
		b = binary.AppendUvarint(b, g.firstRow)
		b = binary.AppendUvarint(b, uint64(g.numRows))
		b = binary.AppendUvarint(b, uint64(g.totalBytes))
		b = binary.AppendUvarint(b, uint64(len(g.chunks)))
		for _, c := range g.chunks {
			b = binary.AppendUvarint(b, uint64(c.columnID))
			b = binary.AppendUvarint(b, uint64(c.firstPageOffset))
			b = binary.AppendUvarint(b, uint64(c.totalCompressed))
			b = binary.AppendUvarint(b, uint64(c.totalUncompressed))
			b = binary.AppendUvarint(b, uint64(c.numValues))
			b = binary.AppendUvarint(b, uint64(c.nullCount))
			b = binary.AppendUvarint(b, uint64(c.numPages))
			b = append(b, byte(c.encoding))
			b = append(b, byte(c.codec))
			b = append(b, 0) // chunk flags, none set in M0
		}
	}
	return b
}

func (m *fileMeta) encodeBlobDesc() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(m.blobCols)))
	for _, c := range m.blobCols {
		b = binary.AppendUvarint(b, uint64(c.columnID))
		b = binary.AppendUvarint(b, uint64(c.dictIndex))
		b = append(b, byte(c.recordCodec))
		b = binary.AppendUvarint(b, uint64(len(c.runs)))
		for _, r := range c.runs {
			b = binary.AppendUvarint(b, uint64(r.recordOffset))
			b = binary.AppendUvarint(b, uint64(r.compressedSize))
			b = binary.AppendUvarint(b, uint64(r.uncompressedSize))
			b = binary.AppendUvarint(b, uint64(r.numValues))
		}
	}
	return b
}

func (m *fileMeta) encodeDictDesc() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(m.dicts)))
	for _, d := range m.dicts {
		b = binary.AppendUvarint(b, uint64(d.recordOffset))
		b = binary.AppendUvarint(b, uint64(d.length))
	}
	return b
}

func (m *fileMeta) encodeKeyValue() []byte {
	var b []byte
	b = binary.AppendUvarint(b, uint64(len(m.kv)))
	for _, p := range m.kv {
		b = appendStr(b, p.key)
		b = appendStr(b, p.value)
	}
	return b
}

func (m *fileMeta) encodeStats() []byte {
	var b []byte
	b = binary.AppendUvarint(b, m.rowCount)
	b = binary.AppendUvarint(b, m.uncompressedTotal)
	b = binary.AppendUvarint(b, m.compressedTotal)
	return b
}

// encodeFooter produces the on-disk footer bytes: the codec marker followed by
// the tagged sections in canonical order.
func (m *fileMeta) encodeFooter() []byte {
	out := []byte{footerCodecNone}
	appendSection := func(tag uint64, body []byte) {
		out = binary.AppendUvarint(out, tag)
		out = binary.AppendUvarint(out, uint64(len(body)))
		out = append(out, body...)
	}
	appendSection(secSchema, m.encodeSchema())
	appendSection(secRowGroups, m.encodeRowGroups())
	if len(m.blobCols) > 0 {
		appendSection(secBlobDesc, m.encodeBlobDesc())
	}
	if len(m.dicts) > 0 {
		appendSection(secDictDesc, m.encodeDictDesc())
	}
	appendSection(secKeyValue, m.encodeKeyValue())
	appendSection(secStats, m.encodeStats())
	return out
}

// cursor is a tiny forward reader over a byte slice.
type cursor struct {
	b   []byte
	pos int
	err error
}

func (c *cursor) uvarint() uint64 {
	if c.err != nil {
		return 0
	}
	v, n := binary.Uvarint(c.b[c.pos:])
	if n <= 0 {
		c.err = fmt.Errorf("tatami: bad uvarint at footer offset %d", c.pos)
		return 0
	}
	c.pos += n
	return v
}

func (c *cursor) byte1() byte {
	if c.err != nil || c.pos >= len(c.b) {
		if c.err == nil {
			c.err = fmt.Errorf("tatami: footer truncated")
		}
		return 0
	}
	v := c.b[c.pos]
	c.pos++
	return v
}

func (c *cursor) str() string {
	n := c.uvarint()
	if c.err != nil {
		return ""
	}
	if c.pos+int(n) > len(c.b) {
		c.err = fmt.Errorf("tatami: footer string overruns body")
		return ""
	}
	s := string(c.b[c.pos : c.pos+int(n)])
	c.pos += int(n)
	return s
}

func (c *cursor) bytes(n int) []byte {
	if c.err != nil {
		return nil
	}
	if c.pos+n > len(c.b) {
		c.err = fmt.Errorf("tatami: footer slice overruns body")
		return nil
	}
	b := c.b[c.pos : c.pos+n]
	c.pos += n
	return b
}

// decodeFooter parses the on-disk footer bytes into a fileMeta.
func decodeFooter(raw []byte) (*fileMeta, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("tatami: empty footer")
	}
	if raw[0] != footerCodecNone {
		return nil, fmt.Errorf("tatami: unsupported footer codec %d", raw[0])
	}
	c := &cursor{b: raw, pos: 1}
	m := &fileMeta{}
	for c.pos < len(c.b) && c.err == nil {
		tag := c.uvarint()
		length := c.uvarint()
		if c.err != nil {
			break
		}
		body := c.bytes(int(length))
		if c.err != nil {
			break
		}
		switch tag {
		case secSchema:
			m.schema = decodeSchema(body, &c.err)
		case secRowGroups:
			m.groups = decodeRowGroups(body, &c.err)
		case secBlobDesc:
			m.blobCols = decodeBlobDesc(body, &c.err)
		case secDictDesc:
			m.dicts = decodeDictDesc(body, &c.err)
		case secKeyValue:
			m.kv = decodeKeyValue(body, &c.err)
		case secStats:
			decodeStats(body, m, &c.err)
		default:
			// Unknown section: skip, already consumed by length.
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	if m.schema == nil {
		return nil, fmt.Errorf("tatami: footer missing schema section")
	}
	return m, nil
}

func decodeSchema(body []byte, errp *error) *Schema {
	c := &cursor{b: body}
	n := c.uvarint()
	fields := make([]Field, 0, n)
	for i := uint64(0); i < n; i++ {
		name := c.str()
		typ := LogicalType(c.byte1())
		f := fieldFromFlags(c.byte1())
		f.Name = name
		f.Type = typ
		f.Element = LogicalType(c.byte1())
		fields = append(fields, f)
	}
	if c.err != nil {
		*errp = c.err
		return nil
	}
	return &Schema{Fields: fields}
}

// decodeRowGroups decodes the row groups. Each group carries its own chunk
// count so the decoder never needs the schema to walk the section.
func decodeRowGroups(body []byte, errp *error) []rowGroupMeta {
	c := &cursor{b: body}
	ng := c.uvarint()
	groups := make([]rowGroupMeta, 0, ng)
	for gi := uint64(0); gi < ng && c.err == nil; gi++ {
		g := rowGroupMeta{}
		g.firstRow = c.uvarint()
		g.numRows = int(c.uvarint())
		g.totalBytes = int64(c.uvarint())
		nc := c.uvarint()
		g.chunks = make([]chunkMeta, 0, nc)
		for ci := uint64(0); ci < nc && c.err == nil; ci++ {
			cm := chunkMeta{}
			cm.columnID = int(c.uvarint())
			cm.firstPageOffset = int64(c.uvarint())
			cm.totalCompressed = int64(c.uvarint())
			cm.totalUncompressed = int64(c.uvarint())
			cm.numValues = int(c.uvarint())
			cm.nullCount = int(c.uvarint())
			cm.numPages = int(c.uvarint())
			cm.encoding = Encoding(c.byte1())
			cm.codec = Codec(c.byte1())
			_ = c.byte1() // chunk flags, ignored in M0
			g.chunks = append(g.chunks, cm)
		}
		groups = append(groups, g)
	}
	if c.err != nil {
		*errp = c.err
		return nil
	}
	return groups
}

func decodeBlobDesc(body []byte, errp *error) []blobColDesc {
	c := &cursor{b: body}
	nc := c.uvarint()
	cols := make([]blobColDesc, 0, nc)
	for ci := uint64(0); ci < nc && c.err == nil; ci++ {
		bc := blobColDesc{}
		bc.columnID = int(c.uvarint())
		bc.dictIndex = int(c.uvarint())
		bc.recordCodec = Codec(c.byte1())
		nr := c.uvarint()
		bc.runs = make([]blobRunDesc, 0, nr)
		for ri := uint64(0); ri < nr && c.err == nil; ri++ {
			r := blobRunDesc{}
			r.recordOffset = int64(c.uvarint())
			r.compressedSize = int64(c.uvarint())
			r.uncompressedSize = int64(c.uvarint())
			r.numValues = int(c.uvarint())
			bc.runs = append(bc.runs, r)
		}
		cols = append(cols, bc)
	}
	if c.err != nil {
		*errp = c.err
		return nil
	}
	return cols
}

func decodeDictDesc(body []byte, errp *error) []dictDesc {
	c := &cursor{b: body}
	n := c.uvarint()
	dicts := make([]dictDesc, 0, n)
	for i := uint64(0); i < n && c.err == nil; i++ {
		d := dictDesc{}
		d.recordOffset = int64(c.uvarint())
		d.length = int64(c.uvarint())
		dicts = append(dicts, d)
	}
	if c.err != nil {
		*errp = c.err
		return nil
	}
	return dicts
}

func decodeKeyValue(body []byte, errp *error) []kvPair {
	c := &cursor{b: body}
	n := c.uvarint()
	pairs := make([]kvPair, 0, n)
	for i := uint64(0); i < n; i++ {
		k := c.str()
		v := c.str()
		pairs = append(pairs, kvPair{k, v})
	}
	if c.err != nil {
		*errp = c.err
		return nil
	}
	return pairs
}

func decodeStats(body []byte, m *fileMeta, errp *error) {
	c := &cursor{b: body}
	m.rowCount = c.uvarint()
	m.uncompressedTotal = c.uvarint()
	m.compressedTotal = c.uvarint()
	if c.err != nil {
		*errp = c.err
	}
}
