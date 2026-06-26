// Command tatami inspects and dumps .tatami files: the compact columnar
// single-file format for web-scale crawl and search.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/tamnd/tatami/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(cli.Execute(ctx))
}
