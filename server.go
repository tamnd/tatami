package tatami

// The Cluster broker answers a query without taking a single process-wide lock:
// it routes, prunes, and scores against a concurrent reference-counted segment
// cache, so two goroutines that touch different shards never wait on each other
// (14-serving.md). server.go is the layer that turns that property into a running
// service. It is deliberately thin: net/http already gives one goroutine per
// request, and the Cluster is already safe to call from all of them, so the server
// adds only the two things a raw handler over a shared broker still needs.
//
// The first is admission control. Goroutine-per-request is unbounded by default:
// a flood of slow clients spawns a goroutine and an in-flight query each, and the
// resident memory grows with the flood rather than with the working set. A
// counting semaphore caps the number of queries running at once; arrivals past the
// cap get a 503 immediately instead of queuing without limit, so the memory and
// CPU a burst can consume are bounded by the cap, not by the arrival rate. The
// segment cache already bounds the open-file and decoded-index memory to its own
// cap independently of how many queries run, so the two caps together fix a memory
// ceiling that does not move with load.
//
// The second is a deadline. A query that routes to a cold shard can stall on a
// disk read; without a bound, one slow shard ties up an admission slot and a
// goroutine indefinitely. Each request carries a timeout: the search runs on a
// worker goroutine and the handler waits on whichever finishes first, the search
// or the deadline, returning 504 on the deadline so the client is not left
// hanging. The worker is left to finish and release its own resources rather than
// being killed mid-read, which the immutable served segment makes safe. Because
// the worker, not the handler, owns the search, it is also the worker that frees
// the admission slot: the slot stays held until the search actually finishes, so
// the cap bounds concurrent work and not just concurrent connections, even when a
// flood of requests all trip the deadline. A WaitGroup tracks the live workers so
// a graceful shutdown can wait for them to drain before the cluster is closed,
// which keeps a timed-out worker's in-flight segment read from racing the close.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultMaxInFlight is the admission cap when ServerOptions leaves it zero. It is
// the number of queries allowed to run at once, the knob that trades throughput
// against the memory and CPU a burst can claim.
const DefaultMaxInFlight = 256

// DefaultQueryTimeout is the per-request deadline when ServerOptions leaves it
// zero. A query that does not finish inside it returns 504, freeing the admission
// slot. It is generous next to the sub-10ms warm path so it fires only on a real
// stall, not on normal cold-shard variance.
const DefaultQueryTimeout = 2 * time.Second

// DefaultMaxK caps the k a request may ask for, so a single query cannot force an
// unbounded result set and the per-query memory stays bounded along with the
// admission cap.
const DefaultMaxK = 100

// ServerOptions tunes the serving layer. The zero value is usable: every field
// falls back to its package default.
type ServerOptions struct {
	// MaxInFlight caps concurrent queries; arrivals past it get 503. Zero uses
	// DefaultMaxInFlight.
	MaxInFlight int
	// Timeout bounds a single query; on expiry the handler returns 504. Zero uses
	// DefaultQueryTimeout.
	Timeout time.Duration
	// MaxK caps the requested result count. Zero uses DefaultMaxK.
	MaxK int
	// DefaultK is the k used when a request omits it. Zero means 10.
	DefaultK int
}

// Server serves a Cluster over HTTP. It is safe for thousands of concurrent
// requests: the Cluster answers each query without a shared lock, and the server
// adds only an admission semaphore and a per-request deadline on top. One Server
// drives one Cluster; build several behind a load balancer to scale past one
// process, or front several with an Aggregator to scale past one broker's shard
// reach.
type Server struct {
	cluster  *Cluster
	sem      chan struct{}
	timeout  time.Duration
	maxK     int
	defaultK int

	// workers tracks the search goroutines still running, so Drain can wait for
	// them before the caller closes the cluster.
	workers sync.WaitGroup

	// Counters, read by /stats. Atomic so the stats handler never contends with
	// the query handlers.
	total    atomic.Uint64 // requests admitted to the search path
	rejected atomic.Uint64 // requests turned away by admission (503)
	timedOut atomic.Uint64 // requests that hit the deadline (504)
	canceled atomic.Uint64 // requests the client disconnected before completion (408)
	failed   atomic.Uint64 // requests whose search returned an error (500)
}

// NewServer wraps a Cluster in a serving layer with the given options. The Cluster
// must outlive the Server; closing the Cluster is the caller's job at shutdown.
func NewServer(c *Cluster, opts ServerOptions) *Server {
	maxInFlight := opts.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = DefaultMaxInFlight
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultQueryTimeout
	}
	maxK := opts.MaxK
	if maxK <= 0 {
		maxK = DefaultMaxK
	}
	defaultK := opts.DefaultK
	if defaultK <= 0 {
		defaultK = 10
	}
	return &Server{
		cluster:  c,
		sem:      make(chan struct{}, maxInFlight),
		timeout:  timeout,
		maxK:     maxK,
		defaultK: defaultK,
	}
}

// Handler returns the http.Handler that routes the server's endpoints: GET /search
// for queries, GET /healthz for liveness, and GET /stats for the broker and
// serving counters. Mount it under a path prefix or serve it at the root.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/search", s.handleSearch)
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/stats", s.handleStats)
	return mux
}

// searchResponse is the JSON body of a successful /search. Took is the wall time
// the broker spent, in milliseconds, the number the <10ms target is read against.
// Stats mirrors the broker's routing and pruning so a caller can see how few
// shards the answer actually touched.
type searchResponse struct {
	Query   string         `json:"query"`
	K       int            `json:"k"`
	Total   int            `json:"total"`
	TookMS  float64        `json:"took_ms"`
	Stats   responseStats  `json:"stats"`
	Results []resultRecord `json:"results"`
}

// responseStats is the per-query routing and pruning summary, the QueryStats the
// broker returns rendered for the wire.
type responseStats struct {
	Candidates int     `json:"candidates"`
	Visited    int     `json:"visited"`
	Threshold  float32 `json:"threshold"`
}

// resultRecord is one ranked hit on the wire.
type resultRecord struct {
	DocID   string  `json:"doc_id"`
	URL     string  `json:"url"`
	Title   string  `json:"title"`
	Snippet string  `json:"snippet,omitempty"`
	Score   float32 `json:"score"`
}

// errorResponse is the JSON body of any non-200.
type errorResponse struct {
	Error string `json:"error"`
}

// handleSearch answers GET /search?q=<query>&k=<n>. It validates the request, takes
// an admission slot or returns 503, runs the search under the per-request deadline,
// and writes the ranked results as JSON. The slot is held only for the duration of
// the search, so the cap measures concurrent work, not concurrent connections.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}
	q := r.URL.Query().Get("q")
	if q == "" {
		writeError(w, http.StatusBadRequest, "missing query parameter q")
		return
	}
	k, err := s.parseK(r.URL.Query().Get("k"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Admission: take a slot or shed the request. A non-blocking send keeps the
	// reject path instant rather than letting the arrival queue without bound. The
	// slot is freed by the worker below when the search finishes, not when this
	// handler returns, so a request that trips the deadline keeps occupying the cap
	// until its search actually completes.
	select {
	case s.sem <- struct{}{}:
	default:
		s.rejected.Add(1)
		writeError(w, http.StatusServiceUnavailable, "server at capacity, retry shortly")
		return
	}
	s.total.Add(1)

	type result struct {
		res   []SearchResult
		stats QueryStats
		err   error
		took  time.Duration
	}
	done := make(chan result, 1)
	s.workers.Add(1)
	go func() {
		// The worker owns the slot and the WaitGroup token: it releases both when
		// the search returns, whether or not the handler is still waiting. The done
		// channel is buffered, so the send never blocks even after a deadline or a
		// client disconnect moved the handler on.
		defer s.workers.Done()
		defer func() { <-s.sem }()
		start := time.Now()
		res, st, err := s.cluster.Search(q, k)
		done <- result{res: res, stats: st, err: err, took: time.Since(start)}
	}()

	// The request context cancels if the client disconnects; the timer bounds a
	// stalled search. Whichever fires first ends the wait; the worker frees the slot.
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case out := <-done:
		if out.err != nil {
			s.failed.Add(1)
			writeError(w, http.StatusInternalServerError, out.err.Error())
			return
		}
		writeJSON(w, http.StatusOK, s.render(q, k, out.res, out.stats, out.took))
	case <-timer.C:
		s.timedOut.Add(1)
		writeError(w, http.StatusGatewayTimeout, "query exceeded the deadline")
	case <-r.Context().Done():
		s.canceled.Add(1)
		writeError(w, http.StatusRequestTimeout, "client closed the request")
	}
}

// Drain waits for every in-flight search worker to finish. A server's caller
// invokes it after the HTTP server has stopped accepting requests and before it
// closes the cluster, so a worker still reading a segment never races the close.
func (s *Server) Drain() {
	s.workers.Wait()
}

// render turns the broker's results and stats into the wire response.
func (s *Server) render(q string, k int, res []SearchResult, st QueryStats, took time.Duration) searchResponse {
	records := make([]resultRecord, len(res))
	for i, r := range res {
		records[i] = resultRecord{DocID: r.DocID, URL: r.URL, Title: r.Title, Snippet: r.Snippet, Score: r.Score}
	}
	return searchResponse{
		Query:   q,
		K:       k,
		Total:   len(res),
		TookMS:  float64(took.Microseconds()) / 1000.0,
		Stats:   responseStats{Candidates: st.Candidates, Visited: st.Visited, Threshold: st.Threshold},
		Results: records,
	}
}

// parseK reads the k parameter, applying the default when absent and the cap when
// present. It rejects a non-numeric or non-positive k so a bad request fails fast
// rather than reaching the broker.
func (s *Server) parseK(raw string) (int, error) {
	if raw == "" {
		return s.defaultK, nil
	}
	k, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("k must be an integer")
	}
	if k <= 0 {
		return 0, fmt.Errorf("k must be positive")
	}
	if k > s.maxK {
		return 0, fmt.Errorf("k must be at most %d", s.maxK)
	}
	return k, nil
}

// handleHealth answers GET /healthz with a plain 200 once the cluster is open, the
// liveness probe a load balancer polls.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// statsResponse is the JSON body of /stats: the static shape of the served cluster
// and the running counters, enough to watch load and shedding without a metrics
// backend.
type statsResponse struct {
	Shards      int    `json:"shards"`
	Docs        int    `json:"docs"`
	CacheLen    int    `json:"cache_len"`
	MaxInFlight int    `json:"max_in_flight"`
	InFlight    int    `json:"in_flight"`
	Total       uint64 `json:"total"`
	Rejected    uint64 `json:"rejected"`
	TimedOut    uint64 `json:"timed_out"`
	Canceled    uint64 `json:"canceled"`
	Failed      uint64 `json:"failed"`
}

// handleStats answers GET /stats with the cluster shape and the serving counters.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, statsResponse{
		Shards:      s.cluster.NumShards(),
		Docs:        s.cluster.NumDocs(),
		CacheLen:    s.cluster.CacheLen(),
		MaxInFlight: cap(s.sem),
		InFlight:    len(s.sem),
		Total:       s.total.Load(),
		Rejected:    s.rejected.Load(),
		TimedOut:    s.timedOut.Load(),
		Canceled:    s.canceled.Load(),
		Failed:      s.failed.Load(),
	})
}

// writeJSON serializes v as the body of an HTTP response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeError writes a JSON error body with the given status.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
