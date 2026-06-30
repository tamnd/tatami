package tatami

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// scaledRealDocs loads the real ccrawl shard and, if TATAMI_BENCH_DOCS asks for
// more documents than the shard holds, tiles the shard up to that count with
// rewritten doc ids so the corpus stays unique. This is how the 1M/10M tiers are
// synthesized from real WET-derived text on a box that holds only one shard.
func scaledRealDocs(tb testing.TB) []SearchDoc {
	tb.Helper()
	base, err := readMarkdownShard(shardPath())
	if err != nil {
		tb.Skipf("real shard unavailable (%v); set TATAMI_BENCH_SHARD to run", err)
	}
	target := len(base)
	if v := os.Getenv("TATAMI_BENCH_DOCS"); v != "" {
		n, perr := strconv.Atoi(v)
		if perr != nil {
			tb.Fatalf("TATAMI_BENCH_DOCS=%q: %v", v, perr)
		}
		target = n
	}
	if target <= len(base) {
		return base[:target]
	}
	docs := make([]SearchDoc, 0, target)
	for i := 0; len(docs) < target; i++ {
		src := base[i%len(base)]
		src.DocID = fmt.Sprintf("%s-%d", src.DocID, i)
		docs = append(docs, src)
	}
	return docs
}

// peakHeap runs fn while sampling the heap, returning the build wall time and the
// peak HeapInuse and Sys observed. The sampler polls ReadMemStats so the
// posting-map spike of the in-memory builder is captured, not just the resident
// set at the end (which the GC may already have reclaimed).
func peakHeap(fn func()) (dur time.Duration, peakHeapInuse, peakSys uint64) {
	var stop atomic.Bool
	var inuse, sys uint64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var m runtime.MemStats
		for !stop.Load() {
			runtime.ReadMemStats(&m)
			if m.HeapInuse > inuse {
				inuse = m.HeapInuse
			}
			if m.Sys > sys {
				sys = m.Sys
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	start := time.Now()
	fn()
	dur = time.Since(start)
	stop.Store(true)
	wg.Wait()
	return dur, inuse, sys
}

// TestM3StreamingBuildMemory is the M3 build benchmark: it builds the same real
// corpus in memory and through the streaming external-merge writer, reporting
// build time and peak heap for each. The point it proves is that streaming peak
// heap is bounded by the batch budget while in-memory peak scales with the corpus,
// which is what unlocks the 10M tier. Scale the corpus with TATAMI_BENCH_DOCS.
func TestM3StreamingBuildMemory(t *testing.T) {
	docs := scaledRealDocs(t)
	dir := t.TempDir()

	t.Logf("corpus: %d docs", len(docs))

	memDur, memHeap, memSys := peakHeap(func() {
		b := NewSearchBuilderWith(SearchBuilderOptions{Snippet: true})
		for _, d := range docs {
			b.Add(d)
		}
		if err := b.Write(dir+"/mem.tatami", WriterOptions{}); err != nil {
			t.Fatalf("in-memory write: %v", err)
		}
	})
	runtime.GC()

	streamDur, streamHeap, streamSys := peakHeap(func() {
		sb, err := NewStreamingSearchBuilder(dir+"/stream.tatami", dir, StreamingOptions{Snippet: true})
		if err != nil {
			t.Fatalf("new streaming builder: %v", err)
		}
		for _, d := range docs {
			sb.Add(d)
		}
		if err := sb.Close(); err != nil {
			t.Fatalf("streaming close: %v", err)
		}
	})

	mi, _ := os.Stat(dir + "/mem.tatami")
	si, _ := os.Stat(dir + "/stream.tatami")

	t.Logf("in-memory : build %v  peak heap-inuse %s  peak sys %s  file %s",
		memDur.Round(time.Millisecond), humanByteCount(int(memHeap)), humanByteCount(int(memSys)), humanByteCount(int(mi.Size())))
	t.Logf("streaming : build %v  peak heap-inuse %s  peak sys %s  file %s",
		streamDur.Round(time.Millisecond), humanByteCount(int(streamHeap)), humanByteCount(int(streamSys)), humanByteCount(int(si.Size())))
	if memHeap > 0 {
		t.Logf("streaming peak heap is %.2fx the in-memory peak", float64(streamHeap)/float64(memHeap))
	}

	// The streamed file must equal the in-memory file byte-for-byte at this scale
	// only when it fit a single batch; with spills the inverted runs still match
	// but forward framing differs, so here we assert retrieval identity instead.
	want := queryAll(t, dir+"/mem.tatami")
	got := queryAll(t, dir+"/stream.tatami")
	if !sameStreamHits(want, got) {
		t.Fatal("streamed retrieval differs from in-memory")
	}
}
