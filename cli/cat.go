package cli

import (
	"bufio"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/tamnd/tatami"
)

func newCatCmd() *cobra.Command {
	var columns string
	var limit int
	cmd := &cobra.Command{
		Use:   "cat <file>",
		Short: "Stream rows of a .tatami file to jsonl on stdout",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, f, err := tatami.OpenFile(args[0])
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			schema := r.Schema()

			sel := make([]int, 0, len(schema.Fields))
			if columns == "" {
				for i := range schema.Fields {
					sel = append(sel, i)
				}
			} else {
				for _, name := range strings.Split(columns, ",") {
					name = strings.TrimSpace(name)
					idx := -1
					for i, fld := range schema.Fields {
						if fld.Name == name {
							idx = i
							break
						}
					}
					if idx < 0 {
						return fmt.Errorf("unknown column %q", name)
					}
					sel = append(sel, idx)
				}
			}

			w := bufio.NewWriter(cmd.OutOrStdout())
			emitted := 0
			for g := 0; g < r.NumRowGroups(); g++ {
				cols := make([]tatami.Column, len(sel))
				for j, idx := range sel {
					c, err := r.ReadColumn(g, idx)
					if err != nil {
						return err
					}
					cols[j] = c
				}
				rows := r.RowGroupRows(g)
				for i := 0; i < rows; i++ {
					line, err := encodeRow(schema, sel, cols, i)
					if err != nil {
						return err
					}
					if _, err := w.WriteString(line); err != nil {
						return err
					}
					emitted++
					if limit > 0 && emitted >= limit {
						return w.Flush()
					}
				}
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&columns, "columns", "", "comma-separated columns to project (default all)")
	cmd.Flags().IntVar(&limit, "limit", 0, "stop after this many rows (0 = no limit)")
	return cmd
}

// encodeRow renders one jsonl object, keeping the schema column order.
func encodeRow(schema *tatami.Schema, sel []int, cols []tatami.Column, i int) (string, error) {
	var b strings.Builder
	b.WriteByte('{')
	for j, idx := range sel {
		if j > 0 {
			b.WriteByte(',')
		}
		key, _ := json.Marshal(schema.Fields[idx].Name)
		b.Write(key)
		b.WriteByte(':')
		val, err := json.Marshal(cols[j].At(i))
		if err != nil {
			return "", err
		}
		b.Write(val)
	}
	b.WriteString("}\n")
	return b.String(), nil
}
