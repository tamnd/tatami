// Package blob owns the on-disk shape of tatami's blob region: the place where
// large, separated column payloads (markdown bodies, raw headers, anything a
// schema marks BLOBREF) live apart from the row groups. Keeping them out of the
// column chunks lets the chunk stay a tight stream of fixed-width references and
// lets the big payloads share one trained dictionary across the whole file,
// which is the WiscKey value-separation idea applied to a columnar container.
//
// This package is physical and self-contained the same way encoding/ is: it has
// no tatami import, does no file I/O, and does not compress. It works on plain
// [][]byte values. The caller groups values into runs, asks this package to pack
// each run into a flat payload, compresses that payload with whatever codec it
// likes, and writes the result. On read the caller decompresses a run payload
// and hands it back here to slice out individual values. The footer records, per
// run, how many values it holds; Directory turns a global value ordinal into a
// (run, index-in-run) pair so a reader resolves a reference without scanning.
package blob

import (
	"encoding/binary"
	"fmt"
)

// PackRun serializes the values of one run into a flat payload:
//
//	num_values            uvarint
//	len[0]..len[n-1]      uvarint each
//	value[0]..value[n-1]  raw bytes, concatenated
//
// The length prefix sits up front so a reader learns every value boundary from
// the head of the payload and can slice any one value without decoding the rest.
// The payload is what the caller then compresses; the dictionary makes the
// repeated boilerplate across values collapse.
func PackRun(values [][]byte) []byte {
	total := 0
	for _, v := range values {
		total += len(v)
	}
	dst := make([]byte, 0, total+len(values)*2+binary.MaxVarintLen64)
	dst = binary.AppendUvarint(dst, uint64(len(values)))
	for _, v := range values {
		dst = binary.AppendUvarint(dst, uint64(len(v)))
	}
	for _, v := range values {
		dst = append(dst, v...)
	}
	return dst
}

// UnpackRun reverses PackRun, returning the run's values. Each returned slice is
// a copy so the caller may keep them after the payload buffer is reused.
func UnpackRun(payload []byte) ([][]byte, error) {
	n, off, err := readCount(payload)
	if err != nil {
		return nil, err
	}
	lens := make([]int, n)
	for i := 0; i < n; i++ {
		l, m := binary.Uvarint(payload[off:])
		if m <= 0 {
			return nil, fmt.Errorf("blob: bad length varint at value %d", i)
		}
		off += m
		lens[i] = int(l)
	}
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		if off+lens[i] > len(payload) {
			return nil, fmt.Errorf("blob: value %d overruns run payload", i)
		}
		v := make([]byte, lens[i])
		copy(v, payload[off:off+lens[i]])
		out[i] = v
		off += lens[i]
	}
	return out, nil
}

// readCount reads the leading value count of a run payload.
func readCount(payload []byte) (n, off int, err error) {
	c, m := binary.Uvarint(payload)
	if m <= 0 {
		return 0, 0, fmt.Errorf("blob: bad run value count")
	}
	return int(c), m, nil
}

// PlanRuns groups values into runs by a raw-byte budget, returning the value
// count of each run in order. A run holds as many consecutive values as fit
// under targetBytes, and always at least one so a single oversized value still
// forms its own run. The plan is a pure function of the inputs, which keeps the
// written file byte-stable.
func PlanRuns(values [][]byte, targetBytes int) []int {
	if len(values) == 0 {
		return nil
	}
	if targetBytes <= 0 {
		targetBytes = 1
	}
	var runs []int
	count, bytesIn := 0, 0
	for _, v := range values {
		if count > 0 && bytesIn+len(v) > targetBytes {
			runs = append(runs, count)
			count, bytesIn = 0, 0
		}
		count++
		bytesIn += len(v)
	}
	if count > 0 {
		runs = append(runs, count)
	}
	return runs
}

// SampleDict builds a raw content dictionary from a strided sample of values,
// capped at maxBytes. It walks the values with a stride chosen so the sample
// spans the whole input rather than just its head, which gives the dictionary a
// representative cross-section of the column's boilerplate. The result is the
// raw dictionary content the caller feeds to a raw-dictionary zstd codec; it
// carries no entropy tables, so it never trips the fragile dictionary trainer
// and stays deterministic. An empty input yields a nil dictionary.
func SampleDict(values [][]byte, maxBytes int) []byte {
	if len(values) == 0 || maxBytes <= 0 {
		return nil
	}
	stride := 1
	// Aim for roughly maxBytes worth of samples assuming an average value size,
	// but never skip so far that a small input contributes nothing.
	if total := totalLen(values); total > maxBytes && len(values) > 1 {
		stride = total / maxBytes
		if stride < 1 {
			stride = 1
		}
	}
	dict := make([]byte, 0, maxBytes)
	for i := 0; i < len(values) && len(dict) < maxBytes; i += stride {
		v := values[i]
		room := maxBytes - len(dict)
		if len(v) > room {
			v = v[:room]
		}
		dict = append(dict, v...)
	}
	// Fall back to the head if striding produced nothing usable.
	if len(dict) == 0 {
		for i := 0; i < len(values) && len(dict) < maxBytes; i++ {
			v := values[i]
			room := maxBytes - len(dict)
			if len(v) > room {
				v = v[:room]
			}
			dict = append(dict, v...)
		}
	}
	return dict
}

func totalLen(values [][]byte) int {
	t := 0
	for _, v := range values {
		t += len(v)
	}
	return t
}

// Directory maps a global value ordinal to the run that holds it and the value's
// index within that run. It is built from the per-run value counts the footer
// records, so a reader resolves a reference with one binary search and no page
// scans.
type Directory struct {
	// cum[r] is the number of values in runs before run r; cum[len] is the total.
	cum []int
}

// NewDirectory builds a directory from the per-run value counts in run order.
func NewDirectory(runValueCounts []int) Directory {
	cum := make([]int, len(runValueCounts)+1)
	for i, c := range runValueCounts {
		cum[i+1] = cum[i] + c
	}
	return Directory{cum: cum}
}

// Len returns the total number of values across all runs.
func (d Directory) Len() int {
	if len(d.cum) == 0 {
		return 0
	}
	return d.cum[len(d.cum)-1]
}

// Locate returns the run index and the value's index within that run for a
// global ordinal, or an error when the ordinal is out of range.
func (d Directory) Locate(ordinal int) (run, idx int, err error) {
	if ordinal < 0 || ordinal >= d.Len() {
		return 0, 0, fmt.Errorf("blob: ordinal %d out of range [0,%d)", ordinal, d.Len())
	}
	// Binary search for the last run whose cumulative start is <= ordinal.
	lo, hi := 0, len(d.cum)-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if d.cum[mid] <= ordinal {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	return lo, ordinal - d.cum[lo], nil
}
