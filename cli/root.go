// Package cli wires tatami's command surface: the cobra tree and the
// fang-rendered help and errors. The format work lives in the tatami package
// and its subpackages; this layer parses flags and prints results.
package cli

import (
	"context"
	"fmt"

	"github.com/charmbracelet/fang"
	"github.com/spf13/cobra"
)

// Execute builds the root command and runs it through fang. It returns the
// process exit code.
func Execute(ctx context.Context) int {
	root := newRoot()
	opts := []fang.Option{fang.WithVersion(Version)}
	if err := fang.Execute(ctx, root, opts...); err != nil {
		return 1
	}
	return 0
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "tatami",
		Short: "A compact columnar single-file format for web-scale crawl and search",
		Long: "tatami is a single-file, columnar, compressed storage format. It stores\n" +
			"crawled documents compactly, reads them back fast with projection and\n" +
			"predicate pushdown, and doubles as a search segment. This CLI inspects\n" +
			"and dumps .tatami files.",
		Version:       fmt.Sprintf("%s (commit %s, built %s)", Version, Commit, Date),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newInspectCmd())
	root.AddCommand(newCatCmd())
	root.AddCommand(newCollectionCmd())
	return root
}
