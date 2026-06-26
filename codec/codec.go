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
	return Zstandard(zstd.SpeedBetterCompression)
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

// ByID returns the codec for an on-disk identifier.
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
