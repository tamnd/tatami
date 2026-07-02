package search

// A routing.bin is the off-heap form of a RoutingIndex (scale/11 lever three).
// OpenCluster rebuilds the routing index in memory from every shard's dictionary
// on every open, and that rebuild peaks tens of gigabytes before it compacts,
// which fits a 64GB box at 10M documents and fits nothing at 100M. The flat
// columns of RoutingIndex are already the on-disk form, so this file writes them
// once at build time as fixed-width, 8-byte-aligned sections and loads them at
// serve time by memory-mapping the file and aliasing each section as a typed
// slice over the mapped bytes with no copy.
//
// The serving box then pays the file, not the rebuild: a demand-paged mmap brings
// in only the pages a query's binary search and posting walk touch, so a routing
// index larger than RAM serves from the page cache, and the garbage collector
// traces the handful of slice headers and nothing inside them, exactly as the
// in-heap flat form already arranged. The aliased columns point into the mapping,
// so the index owns the unmap through Close and the columns must not be read after.
//
// The bytes are written in the build machine's native byte order and the header
// carries an endianness marker, so a serving box of the same order (every amd64
// and arm64 target) aliases directly, and a mismatched box is told to fall back to
// the portable uvarint form (EncodeRouting/DecodeRouting). The layout also assumes
// a 64-bit int, which the shardDocs column aliases directly; the guard below fails
// to compile on a 32-bit build rather than mis-aliasing at runtime.

import (
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"
)

// _ fails to compile where int is not 8 bytes, so the shardDocs alias (mapped
// int64 read back as []int) is only ever built on a 64-bit target.
const _ = uint(unsafe.Sizeof(int(0)) - 8)

const (
	routingMagic   = "TATR"
	routingVersion = 1
	routingHdrSize = 128 // fixed header, so the first section starts 8-byte aligned

	endianLittle = 0
	endianBig    = 1
)

// nativeEndian is the byte order the columns are written and aliased in. A file
// written on one and read on the other takes the decode fallback rather than
// aliasing the wrong-order bytes.
var nativeEndian = func() uint8 {
	x := uint16(1)
	if *(*byte)(unsafe.Pointer(&x)) == 1 {
		return endianLittle
	}
	return endianBig
}()

// align rounds up to the next multiple of 8, so every fixed-width section starts
// at an offset aligned for the widest element it holds.
func align8(n uint64) uint64 { return (n + 7) &^ 7 }

// routingLayout is the byte offset of each section, computed from the counts so
// the writer and reader agree without threading offsets between them. The writer
// also stores the offsets in the header, and the reader validates its computed
// layout against them.
type routingLayout struct {
	shardDocs uint64
	termOff   uint64
	globalDF  uint64
	postOff   uint64
	postShard uint64
	postDf    uint64
	postMax   uint64
	termBlob  uint64
	fileSize  uint64
}

// computeLayout lays the sections out in column order starting after the header,
// each padded to an 8-byte boundary. numTerms is the distinct term count, so the
// offset tables termOff and postOff hold numTerms+1 entries. blobLen is the total
// term-blob byte length.
func computeLayout(numShards, numTerms, numPost, blobLen uint64) routingLayout {
	var l routingLayout
	o := uint64(routingHdrSize)
	l.shardDocs = o
	o = align8(o + numShards*8)
	l.termOff = o
	o = align8(o + (numTerms+1)*4)
	l.globalDF = o
	o = align8(o + numTerms*8)
	l.postOff = o
	o = align8(o + (numTerms+1)*8)
	l.postShard = o
	o = align8(o + numPost*4)
	l.postDf = o
	o = align8(o + numPost*4)
	l.postMax = o
	o = align8(o + numPost*4)
	l.termBlob = o
	o += blobLen
	l.fileSize = o
	return l
}

// bytesOf views a fixed-width slice as its raw native-order bytes for writing,
// without copying. An empty slice yields no bytes.
func bytesOf[T any](s []T) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*int(unsafe.Sizeof(s[0])))
}

// WriteRoutingFile serializes a routing index to path as a mmap-aliasable
// routing.bin. It is meant to run once as the last step of the build pipeline,
// on the machine that already holds the corpus, so the serving box never pays the
// rebuild.
// WriteRoutingFile writes ri to path atomically: it writes a sibling temp file,
// fsyncs it, and renames it over path, so a reader (or a resuming build) only ever
// sees a complete file. A crash mid-write leaves the temp file, not a truncated
// routing.bin, so the next start rebuilds the index rather than mmapping a partial
// one. This is what makes the persisted routing safe to resume from in production,
// where the writer can die at any point.
func WriteRoutingFile(path string, ri *RoutingIndex) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := writeRoutingTo(f, ri); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("search: sync routing file: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("search: close routing file: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("search: rename routing file into place: %w", err)
	}
	return nil
}

func writeRoutingTo(f *os.File, ri *RoutingIndex) error {
	numShards := uint64(len(ri.shardDocs))
	numTerms := uint64(ri.numTerms())
	numPost := uint64(len(ri.postShard))
	blobLen := uint64(len(ri.termBlob))
	l := computeLayout(numShards, numTerms, numPost, blobLen)

	hdr := make([]byte, routingHdrSize)
	copy(hdr[0:4], routingMagic)
	binary.LittleEndian.PutUint32(hdr[4:], routingVersion)
	hdr[8] = nativeEndian
	binary.LittleEndian.PutUint64(hdr[16:], uint64(ri.totalDocs))
	binary.LittleEndian.PutUint64(hdr[24:], numShards)
	binary.LittleEndian.PutUint64(hdr[32:], numTerms)
	binary.LittleEndian.PutUint64(hdr[40:], numPost)
	binary.LittleEndian.PutUint64(hdr[48:], l.shardDocs)
	binary.LittleEndian.PutUint64(hdr[56:], l.termOff)
	binary.LittleEndian.PutUint64(hdr[64:], l.globalDF)
	binary.LittleEndian.PutUint64(hdr[72:], l.postOff)
	binary.LittleEndian.PutUint64(hdr[80:], l.postShard)
	binary.LittleEndian.PutUint64(hdr[88:], l.postDf)
	binary.LittleEndian.PutUint64(hdr[96:], l.postMax)
	binary.LittleEndian.PutUint64(hdr[104:], l.termBlob)
	binary.LittleEndian.PutUint64(hdr[112:], l.fileSize)

	// Write the header then each section at its offset, padding the gap between the
	// previous section's end and the next section's aligned start with zeros. pos
	// tracks the bytes written so the pad is exactly the alignment slack.
	pos := uint64(0)
	write := func(at uint64, b []byte) error {
		if at < pos {
			return fmt.Errorf("search: routing layout overlap at %d < %d", at, pos)
		}
		if at > pos {
			if _, err := f.Write(make([]byte, at-pos)); err != nil {
				return err
			}
			pos = at
		}
		if len(b) > 0 {
			if _, err := f.Write(b); err != nil {
				return err
			}
			pos += uint64(len(b))
		}
		return nil
	}

	if err := write(0, hdr); err != nil {
		return err
	}
	if err := write(l.shardDocs, bytesOf(ri.shardDocs)); err != nil {
		return err
	}
	if err := write(l.termOff, bytesOf(ri.termOff)); err != nil {
		return err
	}
	if err := write(l.globalDF, bytesOf(ri.globalDF)); err != nil {
		return err
	}
	if err := write(l.postOff, bytesOf(ri.postOff)); err != nil {
		return err
	}
	if err := write(l.postShard, bytesOf(ri.postShard)); err != nil {
		return err
	}
	if err := write(l.postDf, bytesOf(ri.postDf)); err != nil {
		return err
	}
	if err := write(l.postMax, bytesOf(ri.postMax)); err != nil {
		return err
	}
	if err := write(l.termBlob, ri.termBlob); err != nil {
		return err
	}
	return nil
}

// OpenRoutingFile memory-maps a routing.bin and returns an index whose columns
// alias the mapping with no copy. The returned index owns the mapping; Close
// unmaps it, and the columns must not be read after Close.
func OpenRoutingFile(path string) (*RoutingIndex, error) {
	data, closer, err := openMapped(path)
	if err != nil {
		return nil, err
	}
	ri, err := routingFromBytes(data, closer)
	if err != nil {
		_ = closer()
		return nil, err
	}
	return ri, nil
}

// aliasSlice views a section of data at offset off as n elements of type T. It
// returns nil for an empty section so it never indexes past the mapping. The
// offset is 8-aligned by construction, so the resulting slice is naturally
// aligned for T.
func aliasSlice[T any](data []byte, off uint64, n uint64) []T {
	if n == 0 {
		return nil
	}
	return unsafe.Slice((*T)(unsafe.Pointer(&data[off])), int(n))
}

// routingFromBytes validates the header and aliases each section over data,
// storing closer as the index's Close. data is the whole mapped file; it must
// outlive the returned index, which the caller guarantees by holding the index
// and calling Close.
func routingFromBytes(data []byte, closer func() error) (*RoutingIndex, error) {
	if len(data) < routingHdrSize {
		return nil, fmt.Errorf("search: routing file too small: %d bytes", len(data))
	}
	if string(data[0:4]) != routingMagic {
		return nil, fmt.Errorf("search: routing file bad magic %q", data[0:4])
	}
	if v := binary.LittleEndian.Uint32(data[4:]); v != routingVersion {
		return nil, fmt.Errorf("search: routing file version %d, want %d", v, routingVersion)
	}
	if data[8] != nativeEndian {
		return nil, fmt.Errorf("search: routing file byte order differs from this machine; rebuild it here or load the uvarint form")
	}
	totalDocs := int(binary.LittleEndian.Uint64(data[16:]))
	numShards := binary.LittleEndian.Uint64(data[24:])
	numTerms := binary.LittleEndian.Uint64(data[32:])
	numPost := binary.LittleEndian.Uint64(data[40:])
	fileSize := binary.LittleEndian.Uint64(data[112:])
	if fileSize > uint64(len(data)) {
		return nil, fmt.Errorf("search: routing file truncated: header says %d bytes, have %d", fileSize, len(data))
	}
	// The term blob is the file's tail after the last fixed-width section, so its
	// length is the file size minus the stored blob offset, not a separate field to
	// keep consistent.
	termBlobOff := binary.LittleEndian.Uint64(data[104:])
	if termBlobOff > fileSize {
		return nil, fmt.Errorf("search: routing blob offset %d past file size %d", termBlobOff, fileSize)
	}
	realBlobLen := fileSize - termBlobOff
	l := computeLayout(numShards, numTerms, numPost, realBlobLen)
	// Every computed offset must match the stored one, or the file was written by a
	// layout this build does not understand.
	for _, c := range []struct {
		name          string
		got, storedAt uint64
	}{
		{"shardDocs", l.shardDocs, 48},
		{"termOff", l.termOff, 56},
		{"globalDF", l.globalDF, 64},
		{"postOff", l.postOff, 72},
		{"postShard", l.postShard, 80},
		{"postDf", l.postDf, 88},
		{"postMax", l.postMax, 96},
		{"termBlob", l.termBlob, 104},
		{"fileSize", l.fileSize, 112},
	} {
		if stored := binary.LittleEndian.Uint64(data[c.storedAt:]); stored != c.got {
			return nil, fmt.Errorf("search: routing layout mismatch for %s: header %d, computed %d", c.name, stored, c.got)
		}
	}

	ri := &RoutingIndex{
		termBlob:  data[l.termBlob : l.termBlob+realBlobLen],
		termOff:   aliasSlice[uint32](data, l.termOff, numTerms+1),
		globalDF:  aliasSlice[uint64](data, l.globalDF, numTerms),
		postOff:   aliasSlice[uint64](data, l.postOff, numTerms+1),
		postShard: aliasSlice[uint32](data, l.postShard, numPost),
		postDf:    aliasSlice[uint32](data, l.postDf, numPost),
		postMax:   aliasSlice[uint32](data, l.postMax, numPost),
		shardDocs: aliasSlice[int](data, l.shardDocs, numShards),
		totalDocs: totalDocs,
		closeFn:   closer,
	}
	return ri, nil
}
