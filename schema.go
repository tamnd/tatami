package tatami

import "fmt"

// Field flag bits, stored in the SCHEMA footer section.
const (
	fieldFlagNullable uint8 = 1 << 0
	fieldFlagSortKey  uint8 = 1 << 1
	fieldFlagBlobSep  uint8 = 1 << 2
	fieldFlagDictHint uint8 = 1 << 3
	fieldFlagBloom    uint8 = 1 << 4
)

// Field describes one column: its name, logical type, and how it is treated.
// The name matches the producer struct tag so the same schema round-trips
// between the Go record and the file.
type Field struct {
	Name     string
	Type     LogicalType
	Nullable bool
	// SortKey marks the single column the file is sorted on, if any. A file with
	// a sort key sets the header's sorted flag and carries the per-group key
	// bounds plus, from M3 on, a sparse key index.
	SortKey bool
	// BlobSeparated asks the writer to keep this column's payload in the blob
	// region (M2 on). For STRING and BYTES columns it is a hint; for BLOBREF it
	// is implied. M0 stores everything inline and ignores it.
	BlobSeparated bool
	// DictHint asks the sampler to prefer dictionary encoding for this column
	// (M1 on). M0 ignores it.
	DictHint bool
	// BloomFilter asks the writer to build a membership filter over this column
	// per row group (M3 on), so an equality probe skips groups that cannot hold
	// the value. It is the opt-in for the point-lookup columns (doc_id, url,
	// digest in the document-store role). A sort-key column needs none, since the
	// sparse key index answers membership exactly.
	BloomFilter bool
	// Element is the element type for a LIST column, otherwise zero.
	Element LogicalType
}

func (f Field) flags() uint8 {
	var fl uint8
	if f.Nullable {
		fl |= fieldFlagNullable
	}
	if f.SortKey {
		fl |= fieldFlagSortKey
	}
	if f.BlobSeparated {
		fl |= fieldFlagBlobSep
	}
	if f.DictHint {
		fl |= fieldFlagDictHint
	}
	if f.BloomFilter {
		fl |= fieldFlagBloom
	}
	return fl
}

func fieldFromFlags(fl uint8) Field {
	return Field{
		Nullable:      fl&fieldFlagNullable != 0,
		SortKey:       fl&fieldFlagSortKey != 0,
		BlobSeparated: fl&fieldFlagBlobSep != 0,
		DictHint:      fl&fieldFlagDictHint != 0,
		BloomFilter:   fl&fieldFlagBloom != 0,
	}
}

// Schema is the ordered list of columns in a file.
type Schema struct {
	Fields []Field
}

// NewSchema builds a schema from fields and validates it.
func NewSchema(fields ...Field) (*Schema, error) {
	s := &Schema{Fields: fields}
	if err := s.validate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Schema) validate() error {
	if len(s.Fields) == 0 {
		return fmt.Errorf("tatami: schema has no fields")
	}
	seen := make(map[string]bool, len(s.Fields))
	sortKeys := 0
	for i, f := range s.Fields {
		if f.Name == "" {
			return fmt.Errorf("tatami: field %d has no name", i)
		}
		if seen[f.Name] {
			return fmt.Errorf("tatami: duplicate field name %q", f.Name)
		}
		seen[f.Name] = true
		if f.Type > TypeBlobRef {
			return fmt.Errorf("tatami: field %q has unknown type %d", f.Name, f.Type)
		}
		if f.SortKey {
			sortKeys++
		}
	}
	if sortKeys > 1 {
		return fmt.Errorf("tatami: schema declares %d sort keys, at most one is allowed", sortKeys)
	}
	return nil
}

// sortKeyIndex returns the index of the sort-key column and true, or -1 and
// false when the schema has no sort key.
func (s *Schema) sortKeyIndex() (int, bool) {
	for i, f := range s.Fields {
		if f.SortKey {
			return i, true
		}
	}
	return -1, false
}
