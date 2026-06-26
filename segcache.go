package tatami

// A broker over a large fan of shards keeps only a small working set of segments
// open, the rest cold on disk. The M8 Cluster did this with one mutex held across
// the whole query, which is correct for one query at a time but serializes a
// server: a thousand concurrent queries would queue behind one lock and the
// tail latency would be the sum, not the max. segCache is the concurrent version.
// It separates two concerns that the single mutex conflated: the bookkeeping that
// decides which segments are resident, and the work of reading a segment. The
// lock guards only the bookkeeping, a map splice and a list move that take
// nanoseconds, and is never held across a file open, a column read, or the WAND
// loop. Those run after the lock is dropped, against a segment the caller has
// pinned (14-serving.md).
//
// Pinning is what makes eviction safe under concurrency. A segment a query is
// reading must not be closed by another query's eviction, or the reader's ReadAt
// races a closed file. Each acquire increments the entry's reference count and
// each release decrements it; eviction removes an entry from the resident set but
// defers the actual Close until the last reader releases it. So the open-file
// count a query sees is always valid, and the resident-set size stays bounded by
// the cap plus the few entries pinned-but-evicted in flight.

import (
	"container/list"
	"sync"
)

// segEntry is one resident segment. refs counts the live pins; evicted marks an
// entry the cache has dropped from the resident set but cannot close yet because
// a reader still holds it.
type segEntry struct {
	shard   int
	seg     *SearchSegment
	elem    *list.Element
	refs    int
	evicted bool
}

// segHandle is a pinned reference to an open segment. Read seg while the handle is
// held, then pass the handle to release. A handle is valid until released and the
// segment behind it will not be closed in that window.
type segHandle struct {
	seg *SearchSegment
	ent *segEntry
}

// segCache is a concurrent reference-counted LRU of open search segments. openFn
// opens the segment for a shard id; the cache calls it on a miss, outside the
// lock, so a cold open never blocks a warm hit on another shard.
type segCache struct {
	mu     sync.Mutex
	open   map[int]*segEntry
	lru    *list.List // front = most recently used; value is *segEntry
	limit  int
	openFn func(shard int) (*SearchSegment, error)
}

// newSegCache returns a cache that keeps at most limit segments resident and opens
// a missing shard through openFn.
func newSegCache(limit int, openFn func(shard int) (*SearchSegment, error)) *segCache {
	if limit <= 0 {
		limit = DefaultCacheSize
	}
	return &segCache{
		open:   make(map[int]*segEntry),
		lru:    list.New(),
		limit:  limit,
		openFn: openFn,
	}
}

// acquire returns a pinned handle to the shard's segment, opening it on a miss.
// The caller must release the handle when done. A hit is a map lookup and a list
// splice under the lock; a miss opens the file outside the lock so concurrent
// misses on different shards proceed in parallel, and a concurrent miss on the
// same shard is resolved on install by keeping one open segment and closing the
// duplicate.
func (c *segCache) acquire(shard int) (*segHandle, error) {
	c.mu.Lock()
	if e, ok := c.open[shard]; ok {
		c.lru.MoveToFront(e.elem)
		e.refs++
		c.mu.Unlock()
		return &segHandle{seg: e.seg, ent: e}, nil
	}
	c.mu.Unlock()

	seg, err := c.openFn(shard)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if e, ok := c.open[shard]; ok {
		// Another goroutine opened this shard while we were opening ours. Adopt
		// theirs and discard the duplicate, so the resident set holds one entry
		// per shard.
		c.lru.MoveToFront(e.elem)
		e.refs++
		c.mu.Unlock()
		_ = seg.Close()
		return &segHandle{seg: e.seg, ent: e}, nil
	}
	e := &segEntry{shard: shard, seg: seg, refs: 1}
	e.elem = c.lru.PushFront(e)
	c.open[shard] = e
	c.evictLocked()
	c.mu.Unlock()
	return &segHandle{seg: seg, ent: e}, nil
}

// release drops a pin. If the entry was evicted while pinned and this was its last
// reader, the segment is closed now, the deferred close that keeps eviction from
// racing an in-flight read.
func (c *segCache) release(h *segHandle) {
	if h == nil {
		return
	}
	c.mu.Lock()
	e := h.ent
	e.refs--
	closeNow := e.evicted && e.refs == 0
	c.mu.Unlock()
	if closeNow {
		_ = e.seg.Close()
	}
}

// evictLocked drops least-recently-used entries until the resident set is within
// the cap. An unpinned victim is closed immediately; a pinned victim is removed
// from the resident set and marked so its last release closes it. The caller
// holds c.mu.
func (c *segCache) evictLocked() {
	for c.lru.Len() > c.limit {
		back := c.lru.Back()
		if back == nil {
			return
		}
		e := back.Value.(*segEntry)
		c.lru.Remove(back)
		delete(c.open, e.shard)
		e.evicted = true
		if e.refs == 0 {
			_ = e.seg.Close()
		}
	}
}

// len reports how many segments are resident, the open-file count a single query
// observes. Pinned-but-evicted entries are not resident and are not counted; they
// close as their readers drain.
func (c *segCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// closeAll closes every resident segment. Callers must ensure no queries are in
// flight; it is the shutdown path.
func (c *segCache) closeAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for e := c.lru.Front(); e != nil; e = e.Next() {
		if err := e.Value.(*segEntry).seg.Close(); err != nil && first == nil {
			first = err
		}
	}
	c.open = make(map[int]*segEntry)
	c.lru.Init()
	return first
}
