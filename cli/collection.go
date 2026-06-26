package cli

import (
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tamnd/tatami"
)

// newCollectionCmd groups the catalog commands: turn a directory of .tatami
// files into one queryable dataset and report what the manifest knows.
func newCollectionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "collection",
		Aliases: []string{"col"},
		Short:   "Manage the tatami.manifest catalog over a directory of files",
	}
	cmd.AddCommand(newCollectionAddCmd())
	cmd.AddCommand(newCollectionListCmd())
	cmd.AddCommand(newCollectionCompactCmd())
	return cmd
}

func newCollectionAddCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "add <dir> <file.tatami>...",
		Short: "Catalog one or more files into the collection at <dir>",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			c, err := tatami.OpenCollection(dir)
			if err != nil {
				return err
			}
			for _, p := range args[1:] {
				rel, err := relTo(dir, p)
				if err != nil {
					return err
				}
				if err := c.AddFile(rel); err != nil {
					return fmt.Errorf("add %s: %w", rel, err)
				}
				if _, err := fmt.Fprintf(cmd.OutOrStdout(), "added %s\n", rel); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

func newCollectionListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list <dir>",
		Short: "List the live members and what the manifest can prune on",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := tatami.OpenCollection(args[0])
			if err != nil {
				return err
			}
			members := c.Members()
			var b strings.Builder
			fmt.Fprintf(&b, "collection: %s\n", args[0])
			fmt.Fprintf(&b, "members:    %d\n", len(members))
			var rows, bytes uint64
			for _, m := range members {
				rows += m.RowCount
				bytes += m.ByteSize
			}
			fmt.Fprintf(&b, "rows:       %d\n", rows)
			fmt.Fprintf(&b, "bytes:      %d\n", bytes)
			for _, m := range members {
				fmt.Fprintf(&b, "  %-24s rows=%d bytes=%d tier=%d", m.FilePath, m.RowCount, m.ByteSize, m.Tier)
				if m.SortColumn != "" {
					fmt.Fprintf(&b, " key[%s]=%q..%q", m.SortColumn, keyShow(m.SortKeyMin), keyShow(m.SortKeyMax))
				}
				b.WriteString("\n")
				if len(m.Zones) > 0 {
					cols := make([]string, 0, len(m.Zones))
					for _, z := range m.Zones {
						cols = append(cols, z.Column)
					}
					sort.Strings(cols)
					fmt.Fprintf(&b, "  %-24s   zone summary on: %s\n", "", strings.Join(cols, ", "))
				}
			}
			_, err = io.WriteString(cmd.OutOrStdout(), b.String())
			return err
		},
	}
}

func newCollectionCompactCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "compact <dir>",
		Short: "Roll the manifest log into a fresh one from the live set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := tatami.OpenCollection(args[0])
			if err != nil {
				return err
			}
			if err := c.Compact(); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "compacted manifest at %s (%d live members)\n", args[0], len(c.Members()))
			return err
		},
	}
}

// relTo returns p expressed relative to dir, so a member path stays a key under
// the collection root rather than an absolute path.
func relTo(dir, p string) (string, error) {
	rel, err := filepath.Rel(dir, p)
	if err != nil {
		// Not under dir: fall back to the base name.
		return filepath.Base(p), nil
	}
	return rel, nil
}

// keyShow renders a key bound for display. Most crawl sort keys (id, surt, url)
// are strings, so the raw bytes print readably; a binary key shows as bytes.
func keyShow(b []byte) string {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return fmt.Sprintf("%x", b)
		}
	}
	return string(b)
}
