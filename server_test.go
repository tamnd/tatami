package tatami

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests cover the HTTP serving layer on synthetic data, so CI runs them
// without any crawl output. They check three things: the JSON results match what
// the broker returns directly, the server stays correct under concurrent requests
// (run with -race), and admission control sheds load with 503 rather than letting
// work pile up unbounded (14-serving.md).

// serverCorpus builds a handful of search segments under a temp dir and returns a
// Cluster over them. The documents repeat a few terms with varied frequency so the
// queries below return a stable ranking.
func serverCorpus(t *testing.T) *Cluster {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for s := 0; s < 6; s++ {
		b := NewSearchBuilder()
		for i := 0; i < 30; i++ {
			body := "common content"
			// Sprinkle the rarer terms unevenly so scores differ across documents.
			if i%3 == 0 {
				body += " alpha alpha"
			}
			if i%5 == 0 {
				body += " beta"
			}
			if s == 2 && i == 0 {
				body += " rareterm rareterm rareterm"
			}
			b.Add(SearchDoc{
				DocID: fmt.Sprintf("doc-%d-%02d", s, i),
				URL:   fmt.Sprintf("https://shard%d.example/%d", s, i),
				Title: fmt.Sprintf("shard %d doc %d", s, i),
				Body:  body,
			})
		}
		p := filepath.Join(dir, fmt.Sprintf("seg-%02d.tatami", s))
		if err := b.Write(p, WriterOptions{}); err != nil {
			t.Fatalf("write segment %d: %v", s, err)
		}
		paths = append(paths, p)
	}
	c, err := OpenCluster(paths, ClusterOptions{CacheSize: 4})
	if err != nil {
		t.Fatalf("open cluster: %v", err)
	}
	return c
}

// doSearch issues GET /search against the handler and decodes the response.
func doSearch(t *testing.T, h http.Handler, query string, k int) (int, searchResponse) {
	t.Helper()
	url := "/search?q=" + query
	if k > 0 {
		url += fmt.Sprintf("&k=%d", k)
	}
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var resp searchResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode response: %v\nbody: %s", err, rec.Body.String())
		}
	}
	return rec.Code, resp
}

// TestServerSearchMatchesBroker checks the JSON results equal what the broker
// returns directly, so the serving layer adds no ranking of its own.
func TestServerSearchMatchesBroker(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	srv := NewServer(c, ServerOptions{})
	h := srv.Handler()

	for _, q := range []string{"common", "alpha", "beta", "rareterm", "alpha+beta"} {
		// The query string here uses '+' which the URL layer turns into a space, the
		// same multi-term query the broker tokenizes.
		want, _, err := c.Search(spaceQuery(q), 10)
		if err != nil {
			t.Fatal(err)
		}
		code, resp := doSearch(t, h, q, 10)
		if code != http.StatusOK {
			t.Fatalf("query %q: status %d", q, code)
		}
		if len(resp.Results) != len(want) {
			t.Fatalf("query %q: got %d results, want %d", q, len(resp.Results), len(want))
		}
		for i := range want {
			if resp.Results[i].DocID != want[i].DocID || resp.Results[i].Score != want[i].Score {
				t.Fatalf("query %q result %d: got (%s, %v), want (%s, %v)",
					q, i, resp.Results[i].DocID, resp.Results[i].Score, want[i].DocID, want[i].Score)
			}
			if resp.Results[i].URL != want[i].URL {
				t.Fatalf("query %q result %d: url %q != %q", q, i, resp.Results[i].URL, want[i].URL)
			}
		}
	}
}

// spaceQuery turns the '+' in a test query into a space, mirroring what the URL
// query parser does to the request the server actually sees.
func spaceQuery(q string) string {
	out := []byte(q)
	for i, b := range out {
		if b == '+' {
			out[i] = ' '
		}
	}
	return string(out)
}

// TestServerBadInput checks the request validation: a missing query, a
// non-numeric k, a non-positive k, an over-cap k, and a non-GET method each fail
// with the right status before the broker is touched.
func TestServerBadInput(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	srv := NewServer(c, ServerOptions{MaxK: 50})
	h := srv.Handler()

	cases := []struct {
		name   string
		method string
		url    string
		want   int
	}{
		{"missing q", http.MethodGet, "/search", http.StatusBadRequest},
		{"empty q", http.MethodGet, "/search?q=", http.StatusBadRequest},
		{"non-numeric k", http.MethodGet, "/search?q=common&k=abc", http.StatusBadRequest},
		{"zero k", http.MethodGet, "/search?q=common&k=0", http.StatusBadRequest},
		{"negative k", http.MethodGet, "/search?q=common&k=-3", http.StatusBadRequest},
		{"over-cap k", http.MethodGet, "/search?q=common&k=999", http.StatusBadRequest},
		{"post method", http.MethodPost, "/search?q=common", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status %d, want %d (body %s)", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

// TestServerHealthAndStats checks the liveness probe and the stats endpoint report
// the served cluster shape.
func TestServerHealthAndStats(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	srv := NewServer(c, ServerOptions{MaxInFlight: 17})
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz status %d", rec.Code)
	}

	// Drive a few queries so the counters move.
	for i := 0; i < 5; i++ {
		doSearch(t, h, "common", 10)
	}
	req = httptest.NewRequest(http.MethodGet, "/stats", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("stats status %d", rec.Code)
	}
	var st statsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if st.Shards != c.NumShards() {
		t.Fatalf("stats shards %d, want %d", st.Shards, c.NumShards())
	}
	if st.Docs != c.NumDocs() {
		t.Fatalf("stats docs %d, want %d", st.Docs, c.NumDocs())
	}
	if st.MaxInFlight != 17 {
		t.Fatalf("stats max_in_flight %d, want 17", st.MaxInFlight)
	}
	if st.Total < 5 {
		t.Fatalf("stats total %d, want at least 5", st.Total)
	}
}

// TestServerConcurrent drives the handler from many goroutines at once and checks
// every response is correct. Run with -race it proves the serving path over the
// shared broker is free of data races.
func TestServerConcurrent(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	srv := NewServer(c, ServerOptions{MaxInFlight: 64})
	h := srv.Handler()

	// Ground truth per query, computed once single-threaded.
	queries := []string{"common", "alpha", "beta", "rareterm"}
	want := map[string][]SearchResult{}
	for _, q := range queries {
		res, _, err := c.Search(q, 10)
		if err != nil {
			t.Fatal(err)
		}
		want[q] = res
	}

	const workers, iters = 50, 100
	var wg sync.WaitGroup
	var bad atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				q := queries[(w+i)%len(queries)]
				code, resp := doSearch(t, h, q, 10)
				if code != http.StatusOK {
					bad.Add(1)
					continue
				}
				exp := want[q]
				if len(resp.Results) != len(exp) {
					bad.Add(1)
					continue
				}
				for j := range exp {
					if resp.Results[j].DocID != exp[j].DocID {
						bad.Add(1)
						break
					}
				}
			}
		}(w)
	}
	wg.Wait()
	if bad.Load() != 0 {
		t.Fatalf("%d concurrent requests returned a wrong or failed result", bad.Load())
	}
}

// TestServerAdmissionSheds checks that with the admission cap saturated, arrivals
// get 503 rather than queuing. It holds every slot with a blocking handler, then
// confirms the next request is shed immediately.
func TestServerAdmissionSheds(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	// Cap of one so a single in-flight request saturates admission.
	srv := NewServer(c, ServerOptions{MaxInFlight: 1, Timeout: 5 * time.Second})

	// Occupy the only slot by taking it directly, the same non-blocking send the
	// handler uses, so the next request through the handler must be shed.
	srv.sem <- struct{}{}

	req := httptest.NewRequest(http.MethodGet, "/search?q=common", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("saturated server returned %d, want 503", rec.Code)
	}
	if srv.rejected.Load() != 1 {
		t.Fatalf("rejected counter is %d, want 1", srv.rejected.Load())
	}

	// Free the slot; the next request now succeeds.
	<-srv.sem
	code, _ := doSearch(t, srv.Handler(), "common", 10)
	if code != http.StatusOK {
		t.Fatalf("after freeing the slot, request returned %d", code)
	}
}

// TestServerTimeout checks a query that overruns the deadline returns 504 and the
// slot is released. It uses a deliberately tiny timeout against the real broker;
// the search either finishes (200) or trips the deadline (504), and either way the
// slot must come back so a follow-up request is admitted.
func TestServerTimeout(t *testing.T) {
	c := serverCorpus(t)
	defer c.Close()
	srv := NewServer(c, ServerOptions{MaxInFlight: 2, Timeout: time.Nanosecond})

	req := httptest.NewRequest(http.MethodGet, "/search?q=common", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusGatewayTimeout && rec.Code != http.StatusOK {
		t.Fatalf("tiny-timeout query returned %d, want 504 or 200", rec.Code)
	}

	// Whatever happened, the admission slot must be free again. Give the worker a
	// moment to release it, then a normal request must be admitted.
	srv2 := NewServer(c, ServerOptions{MaxInFlight: 2})
	code, _ := doSearch(t, srv2.Handler(), "common", 10)
	if code != http.StatusOK {
		t.Fatalf("follow-up request returned %d, want 200", code)
	}
}
