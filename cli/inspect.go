package cli

import (
	"fmt"

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
			defer f.Close()
			info := r.Info()
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "file:    %s\n", args[0])
			fmt.Fprintf(out, "version: %d.%d\n", info.Header.VersionMajor, info.Header.VersionMinor)
			fmt.Fprintf(out, "rows:    %d\n", info.RowCount)
			fmt.Fprintf(out, "groups:  %d\n", info.NumRowGroups)
			searchSeg := info.Header.Flags&tatami.FlagRoleSearchSeg != 0
			fmt.Fprintf(out, "role:    %s\n", roleName(searchSeg))
			ratio := 1.0
			if info.CompressedTotal > 0 {
				ratio = float64(info.UncompressedTotal) / float64(info.CompressedTotal)
			}
			fmt.Fprintf(out, "size:    %d compressed / %d uncompressed (%.2fx)\n",
				info.CompressedTotal, info.UncompressedTotal, ratio)
			fmt.Fprintln(out, "columns:")
			for _, c := range info.Columns {
				fmt.Fprintf(out, "  %-20s %-16s enc=%s codec=%s values=%d nulls=%d pages=%d %d->%d bytes\n",
					c.Name, c.Type, c.Encoding, c.Codec, c.NumValues, c.NullCount, c.NumPages,
					c.TotalUncompressed, c.TotalCompressed)
			}
			if len(info.KeyValue) > 0 {
				fmt.Fprintln(out, "metadata:")
				for _, kv := range info.KeyValue {
					fmt.Fprintf(out, "  %s = %s\n", kv.Key, kv.Value)
				}
			}
			return nil
		},
	}
}

func roleName(searchSeg bool) string {
	if searchSeg {
		return "search-segment"
	}
	return "document-store"
}
