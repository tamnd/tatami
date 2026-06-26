// Package tatami implements a compact columnar single-file storage format for
// web-scale crawl and search. A tatami file holds a header, a run of row
// groups (one column chunk per column per group, each a sequence of pages), an
// optional blob region for separated large payloads, optional dictionary and
// index regions, and a footer directory written last so a reader learns the
// whole layout from one tail read.
//
// This file pins the format-wide constants from Spec 2066: the magic, the
// version, the enum spaces for logical types, encodings, block codecs and
// checksums, and the fixed 64-byte header. Everything else in the package is
// built on the values declared here.
package tatami

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// Magic is the 4-byte marker at the start and end of every tatami file.
const Magic = "TAT1"

// Format version. A reader refuses a major it does not implement; a minor bump
// only adds optional footer sections or new enum values, which old readers
// tolerate for the parts they understand.
const (
	VersionMajor uint16 = 1
	VersionMinor uint16 = 1
)

// HeaderSize is the fixed size of the file header in bytes.
const HeaderSize = 64

// PageHeaderSize is the fixed size of a page header in bytes. It is uncompressed
// so a reader can stride over pages without decoding them.
const PageHeaderSize = 32

// TrailerSize is the fixed size of the bytes after the footer: footer length
// (u32), footer CRC32C (u32), and the end magic (4 bytes).
const TrailerSize = 12

// Header flag bits.
const (
	FlagSorted         uint16 = 1 << 0
	FlagHasBlobRegion  uint16 = 1 << 1
	FlagHasDictRegion  uint16 = 1 << 2
	FlagHasIndexRegion uint16 = 1 << 3
	FlagRoleSearchSeg  uint16 = 1 << 4
)

// LogicalType is the type of the values in a column. The enum values are pinned
// by the format canon and must never be renumbered.
type LogicalType uint8

const (
	TypeBool            LogicalType = 0
	TypeInt8            LogicalType = 1
	TypeInt16           LogicalType = 2
	TypeInt32           LogicalType = 3
	TypeInt64           LogicalType = 4
	TypeUint8           LogicalType = 5
	TypeUint16          LogicalType = 6
	TypeUint32          LogicalType = 7
	TypeUint64          LogicalType = 8
	TypeFloat32         LogicalType = 9
	TypeFloat64         LogicalType = 10
	TypeString          LogicalType = 11
	TypeBytes           LogicalType = 12
	TypeTimestampMicros LogicalType = 13
	TypeList            LogicalType = 14
	TypeBlobRef         LogicalType = 15
)

// String renders a logical type for diagnostics.
func (t LogicalType) String() string {
	switch t {
	case TypeBool:
		return "bool"
	case TypeInt8:
		return "int8"
	case TypeInt16:
		return "int16"
	case TypeInt32:
		return "int32"
	case TypeInt64:
		return "int64"
	case TypeUint8:
		return "uint8"
	case TypeUint16:
		return "uint16"
	case TypeUint32:
		return "uint32"
	case TypeUint64:
		return "uint64"
	case TypeFloat32:
		return "float32"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBytes:
		return "bytes"
	case TypeTimestampMicros:
		return "timestamp_micros"
	case TypeList:
		return "list"
	case TypeBlobRef:
		return "blobref"
	default:
		return fmt.Sprintf("type(%d)", uint8(t))
	}
}

// fixedWidth returns the on-disk width of a fixed-width logical type, and false
// for variable-length types (string, bytes, blobref, list).
func (t LogicalType) fixedWidth() (int, bool) {
	switch t {
	case TypeBool:
		return 1, true // PLAIN bool is bit-packed; this is a marker, not a byte width
	case TypeInt8, TypeUint8:
		return 1, true
	case TypeInt16, TypeUint16:
		return 2, true
	case TypeInt32, TypeUint32, TypeFloat32:
		return 4, true
	case TypeInt64, TypeUint64, TypeFloat64, TypeTimestampMicros:
		return 8, true
	default:
		return 0, false
	}
}

// Encoding identifies how values inside a page are encoded before the block
// codec runs. M0 ships PLAIN only; the later milestones fill in the cascade.
type Encoding uint8

const (
	EncPlain      Encoding = 0
	EncRLE        Encoding = 1
	EncDictionary Encoding = 2
	EncBitpackFOR Encoding = 3
	EncDelta      Encoding = 4
	EncGroupVar   Encoding = 5
	EncPForDelta  Encoding = 6
	EncFSST       Encoding = 7
	EncBitmap     Encoding = 8
)

// String renders an encoding for diagnostics.
func (e Encoding) String() string {
	switch e {
	case EncPlain:
		return "plain"
	case EncRLE:
		return "rle"
	case EncDictionary:
		return "dictionary"
	case EncBitpackFOR:
		return "bitpack_for"
	case EncDelta:
		return "delta"
	case EncGroupVar:
		return "groupvarint"
	case EncPForDelta:
		return "pfordelta"
	case EncFSST:
		return "fsst"
	case EncBitmap:
		return "bitmap"
	default:
		return fmt.Sprintf("enc(%d)", uint8(e))
	}
}

// Codec identifies the block compressor applied to an encoded page payload.
type Codec uint8

const (
	CodecNone     Codec = 0
	CodecLZ4      Codec = 1
	CodecZstd     Codec = 2
	CodecZstdDict Codec = 3
)

// String renders a block codec for diagnostics.
func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecLZ4:
		return "lz4"
	case CodecZstd:
		return "zstd"
	case CodecZstdDict:
		return "zstd_dict"
	default:
		return fmt.Sprintf("codec(%d)", uint8(c))
	}
}

// ChecksumAlgo identifies the integrity hash used for pages and the footer.
type ChecksumAlgo uint8

const (
	ChecksumNone   ChecksumAlgo = 0
	ChecksumCRC32C ChecksumAlgo = 1
	ChecksumXXH64  ChecksumAlgo = 2
)

// PageKind tags what a page holds.
type PageKind uint8

const (
	PageData PageKind = 0
	PageDict PageKind = 1
	PageIdx  PageKind = 2
	PageBlob PageKind = 3
)

// Page header flag bits.
const (
	pageFlagInlineMinMax uint8 = 1 << 0
	pageFlagNullsPresent uint8 = 1 << 1
)

// castagnoli is the CRC32C polynomial table, the same Castagnoli choice kv uses
// in format/checksum.go. crc32c hashes any byte span with it.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// crc32c returns the CRC32C (Castagnoli) checksum of b.
func crc32c(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// Header is the fixed 64-byte file header. It lets a reader sanity-check the
// file and learn the global defaults without parsing the footer.
type Header struct {
	VersionMajor uint16
	VersionMinor uint16
	Flags        uint16
	Checksum     ChecksumAlgo
	DefaultCodec Codec
	PageSizeHint uint32
	FileUUID     [16]byte
	RowCount     uint64
	FooterOffset uint64
	CreatedMphis uint64 // created_unix_millis, supplied by the caller
	CreatorID    uint32
}

// encode writes the header into a fresh 64-byte slice, stamping the magic and
// the trailing CRC32C over bytes 0..59.
func (h *Header) encode() []byte {
	b := make([]byte, HeaderSize)
	copy(b[0:4], Magic)
	binary.LittleEndian.PutUint16(b[4:6], h.VersionMajor)
	binary.LittleEndian.PutUint16(b[6:8], h.VersionMinor)
	binary.LittleEndian.PutUint16(b[8:10], h.Flags)
	b[10] = byte(h.Checksum)
	b[11] = byte(h.DefaultCodec)
	binary.LittleEndian.PutUint32(b[12:16], h.PageSizeHint)
	copy(b[16:32], h.FileUUID[:])
	binary.LittleEndian.PutUint64(b[32:40], h.RowCount)
	binary.LittleEndian.PutUint64(b[40:48], h.FooterOffset)
	binary.LittleEndian.PutUint64(b[48:56], h.CreatedMphis)
	binary.LittleEndian.PutUint32(b[56:60], h.CreatorID)
	binary.LittleEndian.PutUint32(b[60:64], crc32c(b[0:60]))
	return b
}

// decodeHeader parses and verifies a 64-byte header.
func decodeHeader(b []byte) (*Header, error) {
	if len(b) < HeaderSize {
		return nil, fmt.Errorf("tatami: short header: %d bytes", len(b))
	}
	if string(b[0:4]) != Magic {
		return nil, fmt.Errorf("tatami: bad magic %q, not a tatami file", b[0:4])
	}
	want := binary.LittleEndian.Uint32(b[60:64])
	if got := crc32c(b[0:60]); got != want {
		return nil, fmt.Errorf("tatami: header checksum mismatch: got %08x want %08x", got, want)
	}
	h := &Header{
		VersionMajor: binary.LittleEndian.Uint16(b[4:6]),
		VersionMinor: binary.LittleEndian.Uint16(b[6:8]),
		Flags:        binary.LittleEndian.Uint16(b[8:10]),
		Checksum:     ChecksumAlgo(b[10]),
		DefaultCodec: Codec(b[11]),
		PageSizeHint: binary.LittleEndian.Uint32(b[12:16]),
		RowCount:     binary.LittleEndian.Uint64(b[32:40]),
		FooterOffset: binary.LittleEndian.Uint64(b[40:48]),
		CreatedMphis: binary.LittleEndian.Uint64(b[48:56]),
		CreatorID:    binary.LittleEndian.Uint32(b[56:60]),
	}
	copy(h.FileUUID[:], b[16:32])
	if h.VersionMajor != VersionMajor {
		return nil, fmt.Errorf("tatami: unsupported major version %d (this build reads %d)", h.VersionMajor, VersionMajor)
	}
	return h, nil
}
