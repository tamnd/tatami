package tatami

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

// These tests cover the concurrent segment cache directly, the piece that makes a
// Cluster safe to drive from many goroutines at once. They build tiny real
// segments so acquire opens an actual file and release closes it, then check the
// three invariants the server rests on: the resident set stays within the cap, a
// pinned segment is never closed under a reader, and concurrent acquire and release
// is race-free (14-serving.md).

// tinySegs writes n single-document search segments under dir and returns their
// paths. Each segment is just large enough to open and read a column from, which is
// all the cache machinery needs.
func tinySegs(t *testing.T, n int) []string {
	t.Helper()
	dir := t.TempDir()
	paths := make([]string, n)
	for i := 0; i < n; i++ {
		b := NewSearchBuilder()
		b.Add(SearchDoc{
			DocID: fmt.Sprintf("doc-%04d", i),
			URL:   fmt.Sprintf("https://shard%04d.example/", i),
			Title: fmt.Sprintf("shard %d", i),
			Body:  fmt.Sprintf("alpha beta shard %d content", i),
		})
		p := filepath.Join(dir, fmt.Sprintf("seg-%04d.tatami", i))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatalf("write tiny segment %d: %v", i, err)
		}
		paths[i] = p
	}
	return paths
}

// countingOpener returns an openFn over the given paths and a pointer to the open
// counter, so a test can see how many times a cold open actually ran.
func countingOpener(paths []string) (func(int) (*SearchSegment, error), *atomic.Int64) {
	var opens atomic.Int64
	fn := func(shard int) (*SearchSegment, error) {
		opens.Add(1)
		return OpenSearch(paths[shard])
	}
	return fn, &opens
}

// segReadable reports whether the segment's file is still open, by reading a column
// straight off the reader so the read bypasses the forward-column cache and always
// touches the file. A closed file makes the read error, which is how the tests tell
// a deferred close has fired.
func segReadable(seg *SearchSegment) bool {
	_, err := seg.r.ReadColumn(0, 1) // url column of the first row group
	return err == nil
}

// TestSegCacheHitReuse checks a warm hit returns the same open segment and does not
// open the file a second time.
func TestSegCacheHitReuse(t *testing.T) {
	paths := tinySegs(t, 4)
	openFn, opens := countingOpener(paths)
	c := newSegCache(8, openFn)
	defer c.closeAll()

	h1, err := c.acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := c.acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	if h1.seg != h2.seg {
		t.Fatal("two acquires of the same shard returned different segments")
	}
	if got := opens.Load(); got != 1 {
		t.Fatalf("opened the file %d times, want 1", got)
	}
	if c.len() != 1 {
		t.Fatalf("resident set holds %d, want 1", c.len())
	}
	c.release(h1)
	c.release(h2)
}

// TestSegCacheEvictionBound checks the resident set never exceeds the cap as a
// sequence of distinct shards is acquired and released, and that every segment is
// readable while held.
func TestSegCacheEvictionBound(t *testing.T) {
	const shards, limit = 16, 4
	paths := tinySegs(t, shards)
	openFn, _ := countingOpener(paths)
	c := newSegCache(limit, openFn)
	defer c.closeAll()

	for i := 0; i < shards; i++ {
		h, err := c.acquire(i)
		if err != nil {
			t.Fatal(err)
		}
		if !segReadable(h.seg) {
			t.Fatalf("shard %d not readable while held", i)
		}
		c.release(h)
		if c.len() > limit {
			t.Fatalf("after shard %d resident set is %d, over cap %d", i, c.len(), limit)
		}
	}
}

// TestSegCachePinDefersClose is the core safety property: evicting a segment a
// reader still holds does not close it; the close is deferred until the last
// reader releases. With a cap of one, acquiring a second shard evicts the first,
// but the first stays readable until its handle is released.
func TestSegCachePinDefersClose(t *testing.T) {
	paths := tinySegs(t, 2)
	openFn, _ := countingOpener(paths)
	c := newSegCache(1, openFn)
	defer c.closeAll()

	hA, err := c.acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	// Acquiring shard 1 pushes the resident set over the cap of one, so shard 0 is
	// evicted from the resident set. It is still pinned by hA, so it must not close.
	hB, err := c.acquire(1)
	if err != nil {
		t.Fatal(err)
	}
	if !segReadable(hA.seg) {
		t.Fatal("evicted-but-pinned segment was closed while a reader held it")
	}
	if !segReadable(hB.seg) {
		t.Fatal("resident segment not readable")
	}
	if c.len() != 1 {
		t.Fatalf("resident set holds %d, want 1 (shard 0 evicted)", c.len())
	}

	// Releasing the last reader of the evicted segment closes it now.
	segA := hA.seg
	c.release(hA)
	if segReadable(segA) {
		t.Fatal("evicted segment stayed open after its last reader released")
	}
	c.release(hB)
}

// TestSegCacheConcurrent drives the cache from many goroutines with a cap well
// below the shard count, the server's steady state. Under the race detector this
// catches any unsynchronized access to the resident set or the reference counts. It
// also checks every segment a goroutine holds is readable for the whole hold, so
// no eviction closes a segment out from under a reader.
func TestSegCacheConcurrent(t *testing.T) {
	const shards, limit, workers, iters = 32, 8, 64, 200
	paths := tinySegs(t, shards)
	openFn, _ := countingOpener(paths)
	c := newSegCache(limit, openFn)
	defer c.closeAll()

	var wg sync.WaitGroup
	var bad atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				shard := (w*7 + i*13) % shards
				h, err := c.acquire(shard)
				if err != nil {
					bad.Add(1)
					return
				}
				// The segment must stay open for the whole time the handle is held,
				// even as other goroutines evict and reopen around it.
				if !segReadable(h.seg) {
					bad.Add(1)
				}
				c.release(h)
			}
		}(w)
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d concurrent acquires saw a closed or failed segment", bad.Load())
	}
	if c.len() > limit {
		t.Fatalf("resident set is %d, over cap %d", c.len(), limit)
	}
}

// TestSegCacheConcurrentSameShard checks that many goroutines racing to open the
// same cold shard end with exactly one resident entry; any duplicate opens are
// closed on install. Every handle returned is readable.
func TestSegCacheConcurrentSameShard(t *testing.T) {
	paths := tinySegs(t, 1)
	openFn, _ := countingOpener(paths)
	c := newSegCache(4, openFn)
	defer c.closeAll()

	const workers = 32
	var wg sync.WaitGroup
	handles := make([]*segHandle, workers)
	errs := make([]error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			handles[w], errs[w] = c.acquire(0)
		}(w)
	}
	wg.Wait()

	for w := 0; w < workers; w++ {
		if errs[w] != nil {
			t.Fatalf("worker %d: %v", w, errs[w])
		}
		if !segReadable(handles[w].seg) {
			t.Fatalf("worker %d got a closed segment", w)
		}
	}
	if c.len() != 1 {
		t.Fatalf("resident set holds %d after a same-shard race, want 1", c.len())
	}
	for _, h := range handles {
		c.release(h)
	}
}

// TestSegCacheCloseAll checks shutdown empties the resident set.
func TestSegCacheCloseAll(t *testing.T) {
	paths := tinySegs(t, 4)
	openFn, _ := countingOpener(paths)
	c := newSegCache(8, openFn)

	for i := 0; i < 4; i++ {
		h, err := c.acquire(i)
		if err != nil {
			t.Fatal(err)
		}
		c.release(h)
	}
	if err := c.closeAll(); err != nil {
		t.Fatalf("closeAll: %v", err)
	}
	if c.len() != 0 {
		t.Fatalf("resident set holds %d after closeAll, want 0", c.len())
	}
}
