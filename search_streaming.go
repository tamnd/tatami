package tatami

// search_streaming.go is the bounded-RSS search-segment writer (Spec 2066,
// scale/06, M3). The in-memory SearchBuilder (search.go) holds the whole corpus
// resident: every SearchDoc in b.docs and every posting in the InvertedBuilder's
// map[string][]Posting, which is ~16 bytes per posting with slice and map
// overhead. At 1M docs that map is ~8 GB of live heap and at 10M it is an
// unconditional OOM on a 23 GB machine (scale/06, section 1).
//
// StreamingSearchBuilder removes that wall with an external merge sort. Phase 1
// accumulates postings into a byte-bounded batch and spills it to a sorted run
// file when the budget is hit, while the forward columns stream to the output
// through the writer's own row-group flush. Phase 2 k-way merges the runs into
// the three inverted runs via search.StreamEncoder, which holds the index in its
// compact encoded form (~2 bytes per posting), never the raw map. Peak RSS
// becomes a function of the batch budget and the merge fan-in, not the corpus
// size.
//
// The output is byte-identical to the in-memory builder's for the same documents
// and options (the M3 differential test asserts it), because both assign dense
// docids in add order, sort terms ascending, and serialize each non-singleton
// term's payload at the same dense index.

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/tamnd/tatami/search"
)

// DefaultBatchBudgetBytes is the phase-1 batch budget: the accumulated posting
// heap that triggers a spill. 256 MiB holds a couple of WET files' worth of
// postings and keeps phase-1 RSS bounded well under a GB (scale/06, section 2.3).
const DefaultBatchBudgetBytes = 256 << 20

// StreamingOptions configures a streaming build. Snippet and SnippetRunes select
// the search-only forward store exactly as SearchBuilderOptions does. Writer is
// passed straight to Create so the streamed and in-memory builds share container
// framing, which is what makes byte-identity achievable. BatchBudgetBytes tunes
// the spill threshold; zero selects DefaultBatchBudgetBytes.
type StreamingOptions struct {
	Snippet          bool
	SnippetRunes     int
	BatchBudgetBytes int
	Writer           WriterOptions
}

// StreamingSearchBuilder builds a search segment from a document stream without
// holding the corpus resident. It is created against an output path and a scratch
// directory, fed documents with Add, and sealed with Close.
type StreamingSearchBuilder struct {
	path     string // final output path
	building string // path + ".building", renamed to path on success
	scratch  string // per-build scratch directory for spill runs

	w *Writer
	f *os.File

	snippet      bool
	snippetRunes int
	budget       int

	numDocs int

	// Current batch: a per-batch posting map (bounded by budget) plus the forward
	// columns for the batch's documents, flushed together on spill.
	batch      map[string][]search.Posting
	batchPosts int
	fwd        forwardBatch

	runs []string // spill run file paths, in docid-range order
	err  error
}

// forwardBatch holds one batch's forward columns in docid order, sized to the
// batch, not the corpus.
type forwardBatch struct {
	docID, url, title []string
	mid               []string // snippet text (snippet mode)
	body              [][]byte // body bytes (full-document mode)
	nb, nt, na, nu    []uint16
	snippet           bool
}

func (fb *forwardBatch) len() int { return len(fb.docID) }

func (fb *forwardBatch) reset() {
	fb.docID = fb.docID[:0]
	fb.url = fb.url[:0]
	fb.title = fb.title[:0]
	fb.mid = fb.mid[:0]
	fb.body = fb.body[:0]
	fb.nb = fb.nb[:0]
	fb.nt = fb.nt[:0]
	fb.na = fb.na[:0]
	fb.nu = fb.nu[:0]
}

// NewStreamingSearchBuilder opens a streaming build at path, spilling runs under
// a fresh subdirectory of scratchDir. It creates the output writer immediately so
// forward columns can stream during Add. Close finalizes or, on error, removes
// the partial output and the scratch.
func NewStreamingSearchBuilder(path, scratchDir string, opts StreamingOptions) (*StreamingSearchBuilder, error) {
	budget := opts.BatchBudgetBytes
	if budget <= 0 {
		budget = DefaultBatchBudgetBytes
	}
	runes := opts.SnippetRunes
	if opts.Snippet && runes <= 0 {
		runes = DefaultSnippetRunes
	}
	scratch, err := os.MkdirTemp(scratchDir, "tatami-stream-")
	if err != nil {
		return nil, err
	}
	building := path + ".building"
	w, f, err := Create(building, searchSchema(opts.Snippet), opts.Writer)
	if err != nil {
		_ = os.RemoveAll(scratch)
		return nil, err
	}
	return &StreamingSearchBuilder{
		path:         path,
		building:     building,
		scratch:      scratch,
		w:            w,
		f:            f,
		snippet:      opts.Snippet,
		snippetRunes: runes,
		budget:       budget,
		batch:        make(map[string][]search.Posting),
		fwd:          forwardBatch{snippet: opts.Snippet},
	}, nil
}

// Add tokenizes a document, folds its per-field term frequencies, assigns it the
// next dense docid, and records its postings and forward fields into the current
// batch. The same tokenization, frequency fold, and snippet/norm handling as the
// in-memory SearchBuilder.Add (search.go), so the two paths produce identical
// postings and forward values.
func (b *StreamingSearchBuilder) Add(doc SearchDoc) {
	if b.err != nil {
		return
	}
	var norm [search.NumFields]uint16
	freqs := map[string]uint32{}
	index := func(text string, f search.Field) {
		n := 0
		for _, tok := range tokenize(text) {
			freqs[tok]++
			n++
		}
		norm[f] = capU16(n)
	}
	index(doc.Body, search.FieldBody)
	index(doc.Title, search.FieldTitle)
	index(doc.Anchor, search.FieldAnchor)
	index(doc.URL, search.FieldURL)

	id := search.DocID(b.numDocs)
	for t, fr := range freqs {
		if fr == 0 {
			continue
		}
		b.batch[t] = append(b.batch[t], search.Posting{Doc: id, Frequency: fr})
		b.batchPosts++
	}

	// Forward fields for this document, in docid order.
	fb := &b.fwd
	fb.docID = append(fb.docID, doc.DocID)
	fb.url = append(fb.url, doc.URL)
	fb.title = append(fb.title, doc.Title)
	if b.snippet {
		fb.mid = append(fb.mid, makeSnippet(doc.Body, b.snippetRunes))
	} else {
		fb.body = append(fb.body, []byte(doc.Body))
	}
	fb.nb = append(fb.nb, norm[search.FieldBody])
	fb.nt = append(fb.nt, norm[search.FieldTitle])
	fb.na = append(fb.na, norm[search.FieldAnchor])
	fb.nu = append(fb.nu, norm[search.FieldURL])

	b.numDocs++

	// Spill when the batch's posting heap crosses the budget. The estimate is the
	// dominant cost: ~16 bytes per posting in the map's []Posting slices.
	if b.batchPosts*16 >= b.budget {
		b.flushBatch()
	}
}

// NumDocs reports how many documents have been added.
func (b *StreamingSearchBuilder) NumDocs() int { return b.numDocs }

// flushBatch sorts the current batch by term, writes it as a sorted spill run,
// appends the batch's forward columns to the writer, and resets both buffers. The
// forward append streams the columns to disk through the writer's row-group flush,
// so the forward store never accumulates beyond one batch.
func (b *StreamingSearchBuilder) flushBatch() {
	if b.err != nil {
		return
	}
	if b.fwd.len() > 0 {
		if err := b.w.Append(b.forwardBatchColumns()); err != nil {
			b.err = err
			return
		}
		b.fwd.reset()
	}
	if len(b.batch) == 0 {
		return
	}
	if err := b.spill(); err != nil {
		b.err = err
		return
	}
	b.batch = make(map[string][]search.Posting)
	b.batchPosts = 0
}

// forwardBatchColumns turns the current forward batch into writer columns in the
// searchSchema order, the same column shape forwardColumns produces in-memory.
func (b *StreamingSearchBuilder) forwardBatchColumns() Batch {
	fb := &b.fwd
	var mid Column
	if b.snippet {
		mid = Column{Data: append([]string(nil), fb.mid...)}
	} else {
		mid = Column{Data: append([][]byte(nil), fb.body...)}
	}
	return Batch{Columns: []Column{
		{Data: append([]string(nil), fb.docID...)},
		{Data: append([]string(nil), fb.url...)},
		{Data: append([]string(nil), fb.title...)},
		mid,
		{Data: append([]uint16(nil), fb.nb...)},
		{Data: append([]uint16(nil), fb.nt...)},
		{Data: append([]uint16(nil), fb.na...)},
		{Data: append([]uint16(nil), fb.nu...)},
	}}
}

// spill writes the current batch to a sorted run file: term groups in ascending
// term order, each a term then its postings as docDelta/freq pairs (scale/06,
// section 3.2). Docids within a term are already ascending because they were
// appended in add order.
func (b *StreamingSearchBuilder) spill() error {
	keys := make([]string, 0, len(b.batch))
	for t := range b.batch {
		keys = append(keys, t)
	}
	sort.Strings(keys)

	p := filepath.Join(b.scratch, fmt.Sprintf("run-%05d", len(b.runs)))
	f, err := os.Create(p)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 1<<20)
	var tmp [binary.MaxVarintLen64]byte
	putUvarint := func(v uint64) {
		n := binary.PutUvarint(tmp[:], v)
		_, _ = bw.Write(tmp[:n])
	}
	for _, t := range keys {
		ps := b.batch[t]
		putUvarint(uint64(len(t)))
		_, _ = bw.WriteString(t)
		putUvarint(uint64(len(ps)))
		var prev uint64
		for i, pp := range ps {
			doc := uint64(pp.Doc)
			if i == 0 {
				putUvarint(doc)
			} else {
				putUvarint(doc - prev)
			}
			putUvarint(uint64(pp.Frequency))
			prev = doc
		}
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	b.runs = append(b.runs, p)
	return nil
}

// Close runs phase 2 (the k-way merge into the three inverted runs), attaches
// them, closes the writer, and renames the output into place. On any error it
// removes the partial output. The scratch directory is always removed.
func (b *StreamingSearchBuilder) Close() (err error) {
	defer func() {
		_ = os.RemoveAll(b.scratch)
		if err != nil {
			_ = b.f.Close()
			_ = os.Remove(b.building)
		}
	}()
	if b.err != nil {
		return b.err
	}
	// Flush the final partial batch: its forward columns and its spill run.
	b.flushBatch()
	if b.err != nil {
		return b.err
	}

	enc := search.NewStreamEncoder()
	if err := b.merge(enc); err != nil {
		return err
	}
	td, pp, sk, err := enc.Finish()
	if err != nil {
		return err
	}
	b.w.AttachInverted(td, pp, sk, nil, uint64(enc.NumTerms()), uint64(b.numDocs))
	if err := b.w.Close(); err != nil {
		return err
	}
	if err := b.f.Close(); err != nil {
		return err
	}
	return os.Rename(b.building, b.path)
}

// merge opens every spill run and k-way merges them by (term, docid) into the
// stream encoder. For each term it gathers the matching fragment from every run
// that carries it, in run order (which is docid order by the non-overlap
// invariant, scale/06 section 3.3), concatenates them into the term's complete
// ascending posting list, and feeds it to the encoder.
func (b *StreamingSearchBuilder) merge(enc *search.StreamEncoder) error {
	h := &runHeap{}
	for i, p := range b.runs {
		rr, err := openRunReader(p, i)
		if err != nil {
			return err
		}
		if rr.done {
			_ = rr.close()
			continue
		}
		*h = append(*h, rr)
	}
	heap.Init(h)

	for h.Len() > 0 {
		term := (*h)[0].term
		var popped []*runReader
		for h.Len() > 0 && (*h)[0].term == term {
			popped = append(popped, heap.Pop(h).(*runReader))
		}
		// Fragments in run-index order give globally ascending docids.
		sort.Slice(popped, func(i, j int) bool { return popped[i].idx < popped[j].idx })

		var ps []search.Posting
		for _, rr := range popped {
			ps = append(ps, rr.postings...)
		}
		if err := enc.Add(term, ps); err != nil {
			return err
		}
		for _, rr := range popped {
			advanced, err := rr.advance()
			if err != nil {
				return err
			}
			if advanced {
				heap.Push(h, rr)
			} else {
				if err := rr.close(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// runReader is a forward iterator over one spill run. It decodes one term group
// at a time, exposing the current term and its decoded postings.
type runReader struct {
	f        *os.File
	r        *bufio.Reader
	idx      int // run index, the docid-range order
	term     string
	postings []search.Posting
	done     bool
}

func openRunReader(path string, idx int) (*runReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	rr := &runReader{f: f, r: bufio.NewReaderSize(f, 1<<20), idx: idx}
	if _, err := rr.advance(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return rr, nil
}

// advance reads the next term group, returning false at clean EOF. The decoded
// postings reconstruct absolute docids by running sum over the deltas.
func (rr *runReader) advance() (bool, error) {
	termLen, err := binary.ReadUvarint(rr.r)
	if err != nil {
		// EOF before any byte of a new group is a clean end of run.
		rr.done = true
		rr.postings = nil
		return false, nil
	}
	term := make([]byte, termLen)
	if _, err := readFull(rr.r, term); err != nil {
		return false, err
	}
	n, err := binary.ReadUvarint(rr.r)
	if err != nil {
		return false, err
	}
	ps := make([]search.Posting, n)
	var prev uint64
	for i := range ps {
		d, err := binary.ReadUvarint(rr.r)
		if err != nil {
			return false, err
		}
		fr, err := binary.ReadUvarint(rr.r)
		if err != nil {
			return false, err
		}
		var doc uint64
		if i == 0 {
			doc = d
		} else {
			doc = prev + d
		}
		ps[i] = search.Posting{Doc: search.DocID(doc), Frequency: uint32(fr)}
		prev = doc
	}
	rr.term = string(term)
	rr.postings = ps
	return true, nil
}

func (rr *runReader) close() error { return rr.f.Close() }

// readFull fills buf from r, the bufio analogue of io.ReadFull used so the run
// reader does not pull in io for one call.
func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// runHeap is a min-heap of run readers keyed by current term, the merge frontier.
type runHeap []*runReader

func (h runHeap) Len() int           { return len(h) }
func (h runHeap) Less(i, j int) bool { return h[i].term < h[j].term }
func (h runHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *runHeap) Push(x any)        { *h = append(*h, x.(*runReader)) }
func (h *runHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}
