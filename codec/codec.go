// Package codec wraps the block compressors a tatami page payload can use
// behind one interface, so the container code never cares whether a block is
// raw, zstd, or dictionary-trained zstd. The enum IDs match the format canon:
// NONE=0, LZ4=1, ZSTD=2, ZSTD_DICT=3.
//
// All codecs here are deterministic: the same input bytes and the same codec
// settings produce the same output bytes, which is what the M0 byte-stable
// round-trip oracle relies on. The zstd codec pins encoder concurrency to one
// and a fixed level so its output does not drift with GOMAXPROCS.
package codec

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ID is the on-disk codec identifier stored in a page header.
type ID uint8

const (
	None     ID = 0
	LZ4      ID = 1
	Zstd     ID = 2
	ZstdDict ID = 3
)

// Codec compresses and decompresses a single page payload.
type Codec interface {
	// ID returns the on-disk identifier.
	ID() ID
	// Compress appends the compressed form of src to dst and returns dst. A
	// codec must be deterministic for a given build.
	Compress(dst, src []byte) []byte
	// Decompress appends the decompressed form of src to dst and returns it.
	// uncompressedSize is the known decoded length from the page header, used to
	// size the destination and to bound the output.
	Decompress(dst, src []byte, uncompressedSize int) ([]byte, error)
}

// none is the identity codec: the payload is stored verbatim.
type none struct{}

func (none) ID() ID { return None }

func (none) Compress(dst, src []byte) []byte { return append(dst, src...) }

func (none) Decompress(dst, src []byte, _ int) ([]byte, error) {
	return append(dst, src...), nil
}

// zstdCodec is deterministic single-threaded zstd at a fixed level.
type zstdCodec struct {
	level zstd.EncoderLevel
	// Encoders and decoders are pooled because constructing them is not free and
	// they are safe to reuse across pages within one goroutine via EncodeAll and
	// DecodeAll, which are themselves concurrency-safe.
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func (z *zstdCodec) ID() ID { return Zstd }

func (z *zstdCodec) Compress(dst, src []byte) []byte {
	return z.enc.EncodeAll(src, dst)
}

func (z *zstdCodec) Decompress(dst, src []byte, uncompressedSize int) ([]byte, error) {
	if cap(dst) < uncompressedSize {
		grown := make([]byte, len(dst), len(dst)+uncompressedSize)
		copy(grown, dst)
		dst = grown
	}
	return z.dec.DecodeAll(src, dst)
}

var (
	zstdOnce sync.Once
	zstdInst *zstdCodec
	zstdErr  error
)

// Default is the package default block codec for M0: deterministic zstd at a
// level that balances ratio against write CPU. Later milestones tune the level
// per region; for now one level serves every page.
func Default() (Codec, error) {
	return Zstandard(DefaultLevel)
}

// Zstandard returns a shared deterministic zstd codec at the given level. The
// codec is process-wide and safe for concurrent use through EncodeAll and
// DecodeAll.
func Zstandard(level zstd.EncoderLevel) (Codec, error) {
	zstdOnce.Do(func() {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(level),
			zstd.WithEncoderConcurrency(1),
		)
		if err != nil {
			zstdErr = err
			return
		}
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			zstdErr = err
			return
		}
		zstdInst = &zstdCodec{level: level, enc: enc, dec: dec}
	})
	if zstdErr != nil {
		return nil, zstdErr
	}
	return zstdInst, nil
}

// Identity returns the no-op codec that stores payloads verbatim.
func Identity() Codec { return none{} }

// ByID returns the codec for an on-disk identifier. ZstdDict is intentionally
// absent: a dictionary codec needs its dictionary bytes, which the generic page
// path never has. Blob-region records carry the ZstdDict id but are always read
// through a Dict codec the reader builds from the loaded dictionary, never
// through ByID.
func ByID(id ID) (Codec, error) {
	switch id {
	case None:
		return none{}, nil
	case Zstd:
		return Default()
	default:
		return nil, fmt.Errorf("codec: unsupported codec id %d", id)
	}
}

// rawDictID is the dictionary identifier embedded in a raw-dictionary zstd
// frame. Each Dict codec is bound to one dictionary, so a single fixed id serves
// every column; the encoder and decoder of one codec always agree on it.
const rawDictID = 1

// dictCodec is deterministic single-threaded zstd bound to a raw content
// dictionary. The dictionary is shared match history across every record the
// codec compresses, so the repeated boilerplate in separated blob payloads
// (cookie banners, nav chrome, license headers) collapses against it. Unlike a
// trained zstd dictionary it carries no entropy tables, which keeps it robust
// and byte-stable.
type dictCodec struct {
	enc *zstd.Encoder
	dec *zstd.Decoder
}

func (d *dictCodec) ID() ID { return ZstdDict }

func (d *dictCodec) Compress(dst, src []byte) []byte {
	return d.enc.EncodeAll(src, dst)
}

func (d *dictCodec) Decompress(dst, src []byte, uncompressedSize int) ([]byte, error) {
	if cap(dst) < uncompressedSize {
		grown := make([]byte, len(dst), len(dst)+uncompressedSize)
		copy(grown, dst)
		dst = grown
	}
	return d.dec.DecodeAll(src, dst)
}

// NewZstdDict returns a deterministic zstd codec that compresses and
// decompresses against the raw content dictionary dict at the given level. An
// empty dictionary is rejected; the caller should fall back to a plain zstd
// codec when it has no dictionary to train. The returned codec's ID is ZstdDict.
func NewZstdDict(dict []byte, level zstd.EncoderLevel) (Codec, error) {
	if len(dict) == 0 {
		return nil, fmt.Errorf("codec: empty dictionary")
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(1),
		zstd.WithEncoderDictRaw(rawDictID, dict),
	)
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderDictRaw(rawDictID, dict),
	)
	if err != nil {
		return nil, err
	}
	return &dictCodec{enc: enc, dec: dec}, nil
}

// DefaultLevel is the zstd level the writer pins for both page and blob-record
// compression, kept in one place so the block codec and the dictionary codec
// agree.
const DefaultLevel = zstd.SpeedBetterCompression
