package tatami

import (
	"github.com/tamnd/tatami/blob"
	"github.com/tamnd/tatami/codec"
)

// Blob-region tuning. These are deterministic so the written file stays
// byte-stable, and they trade compression ratio against random-access cost.
const (
	// blobRunTargetBytes caps the raw size of one packed run. Larger runs amortize
	// the dictionary and zstd framing better; smaller runs cost less to decode when
	// a reader wants a single value. One mebibyte holds hundreds of typical
	// documents while a decode stays around a millisecond.
	blobRunTargetBytes = 1 << 20
	// blobDictMaxBytes caps the trained dictionary. A raw content dictionary this
	// size primes the runs of a small-record column without dominating the file.
	// It is only ever stored when it earns back its own (compressed) size, so the
	// cap bounds the trial cost rather than the file cost.
	blobDictMaxBytes = 64 * 1024
	// blobDictMinBytes is the floor below which a dictionary is not worth its own
	// record; the column falls back to plain zstd for its runs.
	blobDictMinBytes = 256
)

// separatedBlob reports whether a field's values live in the blob region instead
// of inline in its column chunk. M2 separates BLOBREF columns, the WiscKey case
// for large bodies; the BlobSeparated hint for STRING and BYTES is honored in a
// later slice.
func separatedBlob(f Field) bool {
	return f.Type == TypeBlobRef
}

// blobColAccum buffers the present values of one separated column for the whole
// file. The values are packed, dictionary-trained, compressed, and written as
// the blob region at Close. Buffering the raw values keeps M2 simple; a later
// slice spills to disk so a multi-gigabyte file does not have to fit in memory.
type blobColAccum struct {
	values [][]byte
}

// writeBlobRefChunk writes the column chunk for a separated blob column. The
// chunk carries only the per-row validity bitmap; the values themselves go to
// the accumulator and land in the blob region at Close. A reader rebuilds each
// present row's global ordinal from these chunks and resolves it through the
// blob directory, so the chunk never needs to store a value or an offset.
func (w *Writer) writeBlobRefChunk(colID int, col Column) (chunkMeta, error) {
	n, err := col.length()
	if err != nil {
		return chunkMeta{}, err
	}
	vals := col.Data.([][]byte)
	acc := w.blobCols[colID]
	cm := chunkMeta{
		columnID:        colID,
		firstPageOffset: w.pos,
		numValues:       n,
		encoding:        EncPlain,
		codec:           Codec(w.blockc.ID()),
	}
	step := w.opts.PageMaxValues
	for s := 0; s < n; s += step {
		cnt := step
		if s+cnt > n {
			cnt = n - s
		}
		var present []bool
		if col.Valid != nil {
			present = col.Valid[s : s+cnt]
		}
		bitmap, nullCount := buildValidityMask(present, cnt)
		// Copy each present value into the accumulator. The copy decouples the
		// buffered bytes from the caller's batch buffers, which it may reuse after
		// Append returns.
		for j := 0; j < cnt; j++ {
			if isPresent(present, j) {
				v := make([]byte, len(vals[s+j]))
				copy(v, vals[s+j])
				acc.values = append(acc.values, v)
			}
		}
		payload := bitmap // value bytes live in the blob region, not here
		uncompressed := len(payload)
		compressed := w.blockc.Compress(nil, payload)
		var flags uint8
		if bitmap != nil {
			flags |= pageFlagNullsPresent
		}
		ph := pageHeader{
			kind:             PageData,
			encoding:         EncPlain,
			codec:            Codec(w.blockc.ID()),
			flags:            flags,
			numValues:        uint32(cnt),
			uncompressedSize: uint32(uncompressed),
			compressedSize:   uint32(len(compressed)),
			nullCount:        uint32(nullCount),
			firstRowIndex:    uint32(s),
			payloadCRC32C:    crc32c(compressed),
		}
		if err := w.write(ph.encode()); err != nil {
			return cm, err
		}
		if err := w.write(compressed); err != nil {
			return cm, err
		}
		cm.totalCompressed += int64(len(compressed))
		cm.totalUncompressed += int64(uncompressed)
		cm.nullCount += nullCount
		cm.numPages++
	}
	return cm, nil
}

// packedRun holds one run after packing and compression, before it is written.
type packedRun struct {
	comp         []byte
	uncompressed int
	numValues    int
}

// blobColPlan is the in-memory result of preparing one column's blob data: the
// compressed dictionary to store (nil when not worth one), the codec its runs
// use, and the packed runs ready to write.
type blobColPlan struct {
	colID       int
	dictRaw     []byte // raw dictionary length, for the dict record header
	dictComp    []byte // dictionary compressed for storage, nil when no dict
	recordCodec Codec
	runs        []packedRun
}

// writeBlobRegions trains a dictionary per separated column, packs and
// compresses its values into runs, and writes the blob region followed by the
// dict region. It fills w.meta.blobCols and w.meta.dicts so the footer records
// where every run and dictionary lives. Called once at Close after the last row
// group and before the footer.
func (w *Writer) writeBlobRegions() error {
	plans := make([]blobColPlan, 0)
	for colID, acc := range w.blobCols {
		if acc == nil {
			continue
		}
		plan, err := w.planBlobColumn(colID, acc.values)
		if err != nil {
			return err
		}
		plans = append(plans, plan)
	}
	if len(plans) == 0 {
		return nil
	}

	// Blob region: every column's run records, in column order.
	descs := make([]blobColDesc, len(plans))
	for i := range plans {
		p := &plans[i]
		desc := blobColDesc{columnID: p.colID, recordCodec: p.recordCodec}
		for _, run := range p.runs {
			off := w.pos
			ph := pageHeader{
				kind:             PageBlob,
				encoding:         EncPlain,
				codec:            p.recordCodec,
				numValues:        uint32(run.numValues),
				uncompressedSize: uint32(run.uncompressed),
				compressedSize:   uint32(len(run.comp)),
				payloadCRC32C:    crc32c(run.comp),
			}
			if err := w.write(ph.encode()); err != nil {
				return err
			}
			if err := w.write(run.comp); err != nil {
				return err
			}
			desc.runs = append(desc.runs, blobRunDesc{
				recordOffset:     off,
				compressedSize:   int64(len(run.comp)),
				uncompressedSize: int64(run.uncompressed),
				numValues:        run.numValues,
			})
			w.meta.uncompressedTotal += uint64(run.uncompressed)
			w.meta.compressedTotal += uint64(len(run.comp))
		}
		descs[i] = desc
	}

	// Dict region: one record per column whose dictionary earned its keep. The
	// dictionary is stored zstd-compressed so it costs little on disk; the reader
	// decompresses it before binding the run codec to it.
	for i := range plans {
		p := &plans[i]
		if len(p.dictComp) == 0 {
			continue
		}
		off := w.pos
		ph := pageHeader{
			kind:             PageDict,
			encoding:         EncPlain,
			codec:            Codec(w.blockc.ID()),
			numValues:        1,
			uncompressedSize: uint32(len(p.dictRaw)),
			compressedSize:   uint32(len(p.dictComp)),
			payloadCRC32C:    crc32c(p.dictComp),
		}
		if err := w.write(ph.encode()); err != nil {
			return err
		}
		if err := w.write(p.dictComp); err != nil {
			return err
		}
		w.meta.dicts = append(w.meta.dicts, dictDesc{recordOffset: off, length: int64(len(p.dictRaw))})
		descs[i].dictIndex = len(w.meta.dicts) // 1-based
		w.meta.uncompressedTotal += uint64(len(p.dictRaw))
		w.meta.compressedTotal += uint64(len(p.dictComp))
	}

	w.meta.blobCols = descs
	return nil
}

// planBlobColumn packs one column's values into runs and decides whether a
// shared dictionary pays for itself. It compresses every run with plain zstd,
// then, when a dictionary is worth trialing, compresses every run again against
// the dictionary and compares the dictionary path's total (run bytes plus the
// dictionary's own compressed size) against the plain total. It keeps the
// smaller, so the dictionary is stored only when it earns its keep. This is the
// honest form of the trained-dictionary idea: a large self-similar body has all
// the context it needs inside one run and stays on plain, while a column of many
// small records, where each run is too short to learn the boilerplate, takes the
// shared dictionary win.
func (w *Writer) planBlobColumn(colID int, values [][]byte) (blobColPlan, error) {
	plan := blobColPlan{colID: colID, recordCodec: Codec(w.blockc.ID())}
	if len(values) == 0 {
		return plan, nil
	}

	counts := blob.PlanRuns(values, w.opts.BlobRunTargetBytes)
	payloads := make([][]byte, len(counts))
	off := 0
	for i, count := range counts {
		payloads[i] = blob.PackRun(values[off : off+count])
		off += count
	}

	plainRuns := make([]packedRun, len(counts))
	plainTotal := 0
	for i, count := range counts {
		comp := w.blockc.Compress(nil, payloads[i])
		plainRuns[i] = packedRun{comp: comp, uncompressed: len(payloads[i]), numValues: count}
		plainTotal += len(comp)
	}

	dict := blob.SampleDict(values, blobDictMaxBytes)
	if len(dict) >= blobDictMinBytes {
		dc, err := codec.NewZstdDict(dict, codec.DefaultLevel)
		if err != nil {
			return blobColPlan{}, err
		}
		dictRuns := make([]packedRun, len(counts))
		dictTotal := 0
		for i, count := range counts {
			comp := dc.Compress(nil, payloads[i])
			dictRuns[i] = packedRun{comp: comp, uncompressed: len(payloads[i]), numValues: count}
			dictTotal += len(comp)
		}
		dictComp := w.blockc.Compress(nil, dict)
		if dictTotal+len(dictComp) < plainTotal {
			plan.runs = dictRuns
			plan.recordCodec = Codec(dc.ID())
			plan.dictRaw = dict
			plan.dictComp = dictComp
			return plan, nil
		}
	}

	plan.runs = plainRuns
	return plan, nil
}
