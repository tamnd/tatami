package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tamnd/tatami/convert"
)

// newConvertCmd reads a producer Parquet shard (ami or ccrawl-cli output) and
// writes the same rows as a tatami file, reporting the size both ways so an
// operator sees the win on their own data.
func newConvertCmd() *cobra.Command {
	var blob, bloom, dict string
	var batch int
	cmd := &cobra.Command{
		Use:   "convert <in.parquet> <out.tatami>",
		Short: "Convert a producer Parquet shard into a tatami file",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			opts := convert.Options{
				Blob:      splitList(blob),
				Bloom:     splitList(bloom),
				Dict:      splitList(dict),
				BatchRows: batch,
			}
			st, err := convert.File(args[0], args[1], opts)
			if err != nil {
				return err
			}
			pct := (1 - st.Ratio()) * 100
			rel := "smaller"
			if st.Ratio() > 1 {
				rel = "larger"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(),
				"converted %d rows, %d columns\n  parquet: %d bytes\n  tatami:  %d bytes (%.1f%% %s)\n",
				st.Rows, st.Columns, st.InBytes, st.OutBytes, pct, rel)
			return err
		},
	}
	cmd.Flags().StringVar(&blob, "blob", "", "comma-separated columns to separate into the blob region (default: markdown,body,html)")
	cmd.Flags().StringVar(&bloom, "bloom", "", "comma-separated columns to build a membership filter on (default: doc_id,url,digest)")
	cmd.Flags().StringVar(&dict, "dict", "", "comma-separated string columns to hint toward a shared dictionary (default: all non-identity strings)")
	cmd.Flags().IntVar(&batch, "batch", 0, "rows to read and append at a time (0 = 4096)")
	return cmd
}

// splitList parses a comma list flag into the option slice, returning nil for an
// unset flag (keep the heuristic) and a non-nil slice once the flag is given.
func splitList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
