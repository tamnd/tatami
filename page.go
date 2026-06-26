package tatami

import (
	"encoding/binary"
	"fmt"
)

// pageHeader is the in-memory form of the fixed 32-byte page header from
// 03-file-layout.md section 3.
type pageHeader struct {
	kind             PageKind
	encoding         Encoding
	codec            Codec
	flags            uint8
	numValues        uint32
	uncompressedSize uint32
	compressedSize   uint32
	nullCount        uint32
	firstRowIndex    uint32
	payloadCRC32C    uint32
}

// encode writes the page header into a fresh 32-byte slice.
func (p pageHeader) encode() []byte {
	b := make([]byte, PageHeaderSize)
	b[0] = byte(p.kind)
	b[1] = byte(p.encoding)
	b[2] = byte(p.codec)
	b[3] = p.flags
	binary.LittleEndian.PutUint32(b[4:8], p.numValues)
	binary.LittleEndian.PutUint32(b[8:12], p.uncompressedSize)
	binary.LittleEndian.PutUint32(b[12:16], p.compressedSize)
	binary.LittleEndian.PutUint32(b[16:20], p.nullCount)
	binary.LittleEndian.PutUint32(b[20:24], p.firstRowIndex)
	binary.LittleEndian.PutUint32(b[24:28], p.payloadCRC32C)
	// bytes 28..32 reserved, left zero
	return b
}

// decodePageHeader parses a 32-byte page header.
func decodePageHeader(b []byte) (pageHeader, error) {
	if len(b) < PageHeaderSize {
		return pageHeader{}, fmt.Errorf("tatami: short page header: %d bytes", len(b))
	}
	return pageHeader{
		kind:             PageKind(b[0]),
		encoding:         Encoding(b[1]),
		codec:            Codec(b[2]),
		flags:            b[3],
		numValues:        binary.LittleEndian.Uint32(b[4:8]),
		uncompressedSize: binary.LittleEndian.Uint32(b[8:12]),
		compressedSize:   binary.LittleEndian.Uint32(b[12:16]),
		nullCount:        binary.LittleEndian.Uint32(b[16:20]),
		firstRowIndex:    binary.LittleEndian.Uint32(b[20:24]),
		payloadCRC32C:    binary.LittleEndian.Uint32(b[24:28]),
	}, nil
}
