package tatami

// Segment merge is how a tatami search index stays servable as it grows: many
// small segments fold into one larger segment, deleted documents are dropped,
// and dense doc ids are re-derived in one ascending pass so the rebuilt posting
// lists stay sorted (09-search-scale.md, section 7). The merge reads N
// search-segment files and writes one new search-segment file; nothing is
// mutated in place, which is what keeps segments object-storage friendly.
//
// The tiered policy in the search/ subpackage decides which segments to merge;
// this file performs the merge it selects.

import (
	"fmt"

	"github.com/tamnd/tatami/search"
)

// forwardRow is one document's stored fields plus its per-field length norms,
// read out of a segment's forward columns so a merge can rewrite them under fresh
// dense ids. The norms are carried verbatim because they cannot be re-derived
// from the combined-frequency posting lists.
type forwardRow struct {
	docID   string
	url     string
	title   string
	body    []byte // set on a full-document segment
	snippet string // set on a search-only segment
	norm    [search.NumFields]uint16
}

// readForwardAll reads every document's forward fields across all row groups, in
// dense-id order. The slice index is the dense docID.
func (s *SearchSegment) readForwardAll() ([]forwardRow, error) {
	rows := make([]forwardRow, 0, s.inv.NumDocs())
	for g := 0; g < s.r.NumRowGroups(); g++ {
		docID, err := s.readStringColumn(g, 0)
		if err != nil {
			return nil, err
		}
		url, err := s.readStringColumn(g, 1)
		if err != nil {
			return nil, err
		}
		title, err := s.readStringColumn(g, 2)
		if err != nil {
			return nil, err
		}
		var body [][]byte
		var snip []string
		if s.snippet {
			snip, err = s.readStringColumn(g, colVariable)
			if err != nil {
				return nil, err
			}
		} else {
			bodyCol, err := s.r.ReadColumn(g, colVariable)
			if err != nil {
				return nil, err
			}
			body = bodyCol.Data.([][]byte)
		}
		nb, err := s.readU16Column(g, 4)
		if err != nil {
			return nil, err
		}
		nt, err := s.readU16Column(g, 5)
		if err != nil {
			return nil, err
		}
		na, err := s.readU16Column(g, 6)
		if err != nil {
			return nil, err
		}
		nu, err := s.readU16Column(g, 7)
		if err != nil {
			return nil, err
		}
		for i := range docID {
			r := forwardRow{
				docID: docID[i],
				url:   url[i],
				title: title[i],
				norm: [search.NumFields]uint16{
					search.FieldBody:   nb[i],
					search.FieldTitle:  nt[i],
					search.FieldAnchor: na[i],
					search.FieldURL:    nu[i],
				},
			}
			if s.snippet {
				r.snippet = snip[i]
			} else {
				r.body = body[i]
			}
			rows = append(rows, r)
		}
	}
	return rows, nil
}

func (s *SearchSegment) readStringColumn(group, col int) ([]string, error) {
	c, err := s.r.ReadColumn(group, col)
	if err != nil {
		return nil, err
	}
	return c.Data.([]string), nil
}

func (s *SearchSegment) readU16Column(group, col int) ([]uint16, error) {
	c, err := s.r.ReadColumn(group, col)
	if err != nil {
		return nil, err
	}
	return c.Data.([]uint16), nil
}

// MergeSegments merges the live documents of the given open search segments into
// one new search-segment file at outPath, honoring each segment's in-memory
// deletions. Dense doc ids are reassigned in a single ascending pass over the
// inputs in order, so each term's concatenated postings stay sorted; the merged
// segment's posting lists, skip tables, and term dictionary are rebuilt from
// scratch. The inputs are left untouched; the caller retires them after a
// successful merge.
func MergeSegments(segs []*SearchSegment, outPath string, opts WriterOptions) error {
	if len(segs) == 0 {
		return fmt.Errorf("tatami: MergeSegments needs at least one segment")
	}
	snippet := segs[0].snippet
	inv := search.NewInvertedBuilder()
	var (
		docID []string
		url   []string
		title []string
		body  [][]byte
		snip  []string
		nb    []uint16
		nt    []uint16
		na    []uint16
		nu    []uint16
	)
	for _, seg := range segs {
		if seg.snippet != snippet {
			return fmt.Errorf("tatami: MergeSegments cannot mix snippet and full-document segments")
		}
		perDoc := seg.inv.PerDocFreqs()
		rows, err := seg.readForwardAll()
		if err != nil {
			return err
		}
		for old := 0; old < seg.inv.NumDocs(); old++ {
			if !seg.inv.IsLive(search.DocID(old)) {
				continue
			}
			var freqs map[string]uint32
			if old < len(perDoc) {
				freqs = perDoc[old]
			}
			inv.AddDocument(freqs)
			r := rows[old]
			docID = append(docID, r.docID)
			url = append(url, r.url)
			title = append(title, r.title)
			if snippet {
				snip = append(snip, r.snippet)
			} else {
				body = append(body, r.body)
			}
			nb = append(nb, r.norm[search.FieldBody])
			nt = append(nt, r.norm[search.FieldTitle])
			na = append(na, r.norm[search.FieldAnchor])
			nu = append(nu, r.norm[search.FieldURL])
		}
	}

	built, err := inv.Build()
	if err != nil {
		return err
	}
	w, f, err := Create(outPath, searchSchema(snippet), opts)
	if err != nil {
		return err
	}
	var mid Column
	if snippet {
		mid = Column{Data: snip}
	} else {
		mid = Column{Data: body}
	}
	cols := []Column{
		{Data: docID}, {Data: url}, {Data: title}, mid,
		{Data: nb}, {Data: nt}, {Data: na}, {Data: nu},
	}
	if err := w.Append(Batch{Columns: cols}); err != nil {
		_ = w.Close()
		_ = f.Close()
		return err
	}
	td, pp, sk := search.EncodeInverted(built)
	w.AttachInverted(td, pp, sk, nil, uint64(built.NumTerms()), uint64(built.NumDocs()))
	if err := w.Close(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
