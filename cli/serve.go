package cli

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/tamnd/tatami"
)

// newServeCmd starts the HTTP search server over a directory of search segments.
// It builds a routing index across the shards, opens a Cluster broker with a
// bounded segment cache, and serves /search, /healthz, and /stats. The broker
// answers queries without a shared lock, so one process handles many concurrent
// requests; the flags cap the open-segment memory and the work a burst can claim.
func newServeCmd() *cobra.Command {
	var (
		addr        string
		cacheSize   int
		maxInFlight int
		timeout     time.Duration
		maxK        int
		defaultK    int
	)
	cmd := &cobra.Command{
		Use:   "serve <dir>",
		Short: "Serve search over a directory of .tatami search segments",
		Long: "serve builds a routing index over every .tatami search segment in <dir>,\n" +
			"opens a broker that keeps only a bounded working set of segments resident,\n" +
			"and answers queries over HTTP. GET /search?q=<query>&k=<n> returns ranked\n" +
			"JSON results, /healthz is a liveness probe, and /stats reports the broker\n" +
			"shape and the serving counters.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			paths, err := segmentPaths(args[0])
			if err != nil {
				return err
			}
			if len(paths) == 0 {
				return fmt.Errorf("no .tatami files found under %s", args[0])
			}

			fmt.Fprintf(cmd.OutOrStdout(), "opening %d shards under %s\n", len(paths), args[0])
			cluster, err := tatami.OpenCluster(paths, tatami.ClusterOptions{CacheSize: cacheSize})
			if err != nil {
				return fmt.Errorf("open cluster: %w", err)
			}
			defer cluster.Close()

			srv := tatami.NewServer(cluster, tatami.ServerOptions{
				MaxInFlight: maxInFlight,
				Timeout:     timeout,
				MaxK:        maxK,
				DefaultK:    defaultK,
			})
			fmt.Fprintf(cmd.OutOrStdout(), "serving %d docs across %d shards on %s (cache=%d, max-in-flight=%d)\n",
				cluster.NumDocs(), cluster.NumShards(), addr, cacheSize, maxInFlight)

			return runServer(cmd.Context(), cmd, addr, srv)
		},
	}
	cmd.Flags().StringVar(&addr, "addr", ":8080", "address to listen on")
	cmd.Flags().IntVar(&cacheSize, "cache", tatami.DefaultCacheSize, "max segments kept open at once")
	cmd.Flags().IntVar(&maxInFlight, "max-in-flight", tatami.DefaultMaxInFlight, "max concurrent queries before shedding with 503")
	cmd.Flags().DurationVar(&timeout, "timeout", tatami.DefaultQueryTimeout, "per-query deadline before 504")
	cmd.Flags().IntVar(&maxK, "max-k", tatami.DefaultMaxK, "largest result count a request may ask for")
	cmd.Flags().IntVar(&defaultK, "default-k", 10, "result count when a request omits k")
	return cmd
}

// segmentPaths returns the .tatami files to serve. A directory is globbed for its
// top-level *.tatami files in sorted order so the shard ids the routing index
// assigns are stable across restarts; a single file path serves that one file.
func segmentPaths(root string) ([]string, error) {
	info, err := os.Stat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []string{root}, nil
	}
	matches, err := filepath.Glob(filepath.Join(root, "*.tatami"))
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	return matches, nil
}

// runServer starts the HTTP server and blocks until the context is canceled or an
// interrupt arrives, then drains in-flight requests with a bounded grace period.
// The graceful shutdown stops accepting requests, waits for the handlers to return,
// then drains the search workers those handlers spawned before returning to the
// caller, whose deferred cluster.Close runs only after the drain. That ordering
// keeps a worker's in-flight segment read from racing the file close, even for a
// query that already tripped its deadline.
func runServer(ctx context.Context, cmd *cobra.Command, addr string, srv *tatami.Server) error {
	hs := &http.Server{Addr: addr, Handler: srv.Handler()}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 1)
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
			return
		}
		errc <- nil
	}()

	select {
	case err := <-errc:
		// The listener stopped on its own (a bind failure or an unexpected close).
		// Drain any workers a request started before handing control back to the
		// caller's cluster.Close.
		srv.Drain()
		return err
	case <-ctx.Done():
		fmt.Fprintln(cmd.OutOrStdout(), "shutting down")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := hs.Shutdown(shutCtx)
		srv.Drain()
		return err
	}
}
