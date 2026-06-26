package tatami

// writeRows materializes a row-major slice (one []any per row, a nil cell for a
// null) into columnar batches and writes them through the normal writer, so a
// merge output gets the same encoding cascade, dictionaries, and index
// structures any freshly written file gets. A nil cell marks a null; the column
// gets a validity bitmap only when some row is null.

func writeRows(path string, schema *Schema, rows [][]any, opts WriterOptions, createdMillis uint64) error {
	w, f, err := Create(path, schema, opts)
	if err != nil {
		return err
	}
	cols := make([]Column, len(schema.Fields))
	for ci, field := range schema.Fields {
		cols[ci] = buildColumn(field.Type, rows, ci)
	}
	if err := w.Append(Batch{Columns: cols}); err != nil {
		_ = w.Close()
		_ = f.Close()
		return err
	}
	if err := w.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// buildColumn assembles one typed column from column ci of the rows. A row whose
// cell is nil contributes the type's zero value and a false validity bit.
func buildColumn(t LogicalType, rows [][]any, ci int) Column {
	n := len(rows)
	var valid []bool
	hasNull := false
	for _, row := range rows {
		if row[ci] == nil {
			hasNull = true
			break
		}
	}
	if hasNull {
		valid = make([]bool, n)
		for i := range rows {
			valid[i] = rows[i][ci] != nil
		}
	}
	switch t {
	case TypeBool:
		d := make([]bool, n)
		for i, row := range rows {
			if v, ok := row[ci].(bool); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeInt8:
		d := make([]int8, n)
		for i, row := range rows {
			if v, ok := row[ci].(int8); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeInt16:
		d := make([]int16, n)
		for i, row := range rows {
			if v, ok := row[ci].(int16); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeInt32:
		d := make([]int32, n)
		for i, row := range rows {
			if v, ok := row[ci].(int32); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeInt64, TypeTimestampMicros:
		d := make([]int64, n)
		for i, row := range rows {
			if v, ok := row[ci].(int64); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeUint8:
		d := make([]uint8, n)
		for i, row := range rows {
			if v, ok := row[ci].(uint8); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeUint16:
		d := make([]uint16, n)
		for i, row := range rows {
			if v, ok := row[ci].(uint16); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeUint32:
		d := make([]uint32, n)
		for i, row := range rows {
			if v, ok := row[ci].(uint32); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeUint64:
		d := make([]uint64, n)
		for i, row := range rows {
			if v, ok := row[ci].(uint64); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeFloat32:
		d := make([]float32, n)
		for i, row := range rows {
			if v, ok := row[ci].(float32); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeFloat64:
		d := make([]float64, n)
		for i, row := range rows {
			if v, ok := row[ci].(float64); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeString:
		d := make([]string, n)
		for i, row := range rows {
			if v, ok := row[ci].(string); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	case TypeBytes, TypeBlobRef:
		d := make([][]byte, n)
		for i, row := range rows {
			if v, ok := row[ci].([]byte); ok {
				d[i] = v
			}
		}
		return Column{Data: d, Valid: valid}
	default:
		return Column{Data: emptyTyped(t), Valid: valid}
	}
}
