package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tamnd/tatami"
)

func newInspectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect <file>",
		Short: "Print a header and footer summary for a .tatami file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, f, err := tatami.OpenFile(args[0])
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			info := r.Info()

			var b strings.Builder
			fmt.Fprintf(&b, "file:    %s\n", args[0])
			fmt.Fprintf(&b, "version: %d.%d\n", info.Header.VersionMajor, info.Header.VersionMinor)
			fmt.Fprintf(&b, "rows:    %d\n", info.RowCount)
			fmt.Fprintf(&b, "groups:  %d\n", info.NumRowGroups)
			searchSeg := info.Header.Flags&tatami.FlagRoleSearchSeg != 0
			fmt.Fprintf(&b, "role:    %s\n", roleName(searchSeg))
			ratio := 1.0
			if info.CompressedTotal > 0 {
				ratio = float64(info.UncompressedTotal) / float64(info.CompressedTotal)
			}
			fmt.Fprintf(&b, "size:    %d compressed / %d uncompressed (%.2fx)\n",
				info.CompressedTotal, info.UncompressedTotal, ratio)
			b.WriteString("columns:\n")
			for _, c := range info.Columns {
				fmt.Fprintf(&b, "  %-20s %-16s enc=%s codec=%s values=%d nulls=%d pages=%d %d->%d bytes\n",
					c.Name, c.Type, c.Encoding, c.Codec, c.NumValues, c.NullCount, c.NumPages,
					c.TotalUncompressed, c.TotalCompressed)
			}
			if len(info.KeyValue) > 0 {
				b.WriteString("metadata:\n")
				for _, kv := range info.KeyValue {
					fmt.Fprintf(&b, "  %s = %s\n", kv.Key, kv.Value)
				}
			}
			_, err = io.WriteString(cmd.OutOrStdout(), b.String())
			return err
		},
	}
}

func roleName(searchSeg bool) string {
	if searchSeg {
		return "search-segment"
	}
	return "document-store"
}
