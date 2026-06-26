package search

// BM25F, the fielded variant of BM25, is the lexical relevance scorer. It scores
// a query term against a document from the term's per-field frequencies and the
// document's per-field lengths, using a single weighted pseudo-frequency across
// fields rather than a sum of independent per-field BM25 scores.
//
// The distinction matters. Summing per-field BM25 lets a term saturate
// separately in each field, so a term stuffed into the title saturates the title
// contribution while still earning full body credit, which breaks the
// non-linearity BM25 exists to provide. The Robertson BM25F formulation folds
// per-field length normalization into one combined frequency and applies the
// saturation once:
//
//	B_f(D)      = (1 - b_f) + b_f * len_f(D) / avgdl_f
//	tf_eff(t,D) = Sum_f  w_f * tf(t,f) / B_f(D)
//	score(t,D)  = IDF(t) * tf_eff * (k1 + 1) / (k1 + tf_eff)
//
// with the Lucene/Elasticsearch IDF form. Per-field b_f and avgdl_f are tracked
// separately; k1 is shared. Anchor text and title carry high field weights. The
// exact variant is pinned here and recorded with the index version so a score is
// reproducible (09-search-scale.md, section 5).

import "math"

// Default parameters. k1 = 1.2 and b = 0.75 are the Lucene defaults; the field
// weights lift title and anchor text, which are short and high-signal, above the
// body.
const (
	DefaultK1 = 1.2
	DefaultB  = 0.75
)

// Params is the pinned scorer configuration.
type Params struct {
	// K1 is the term-frequency saturation point, shared across fields.
	K1 float32
	// B is the per-field length-normalization strength in [0,1]. b=0 disables
	// length normalization for that field; b=1 fully normalizes by length.
	B [NumFields]float32
	// Weights is the per-field boost w_f folded into the pseudo-frequency.
	Weights [NumFields]float32
}

// DefaultParams returns the pinned defaults: k1=1.2, b=0.75 on every field, and
// field weights that put anchor and title above the body and url.
func DefaultParams() Params {
	p := Params{K1: DefaultK1}
	for f := range p.B {
		p.B[f] = DefaultB
	}
	p.Weights[FieldBody] = 1.0
	p.Weights[FieldTitle] = 3.0
	p.Weights[FieldAnchor] = 2.0
	p.Weights[FieldURL] = 1.5
	return p
}

// Collection holds the corpus statistics a score depends on: the document count
// and the average length of each field. These come from the index at build time
// and are stable for the life of a segment.
type Collection struct {
	// N is the number of documents in the collection.
	N int
	// AvgFieldLen is the mean length (in terms) of each field across the
	// collection. A zero average means the field is absent and contributes no
	// length normalization.
	AvgFieldLen [NumFields]float32
}

// IDF is the Lucene/Elasticsearch inverse document frequency for a term that
// appears in df documents:
//
//	IDF = log(1 + (N - df + 0.5) / (df + 0.5))
//
// The +0.5 smoothing keeps it finite at df=0 and df=N, and the form is
// non-negative, which is the precondition block-max WAND places on the scorer. A
// df above N is clamped to N.
func (c Collection) IDF(df int) float32 {
	if df > c.N {
		df = c.N
	}
	if df < 0 {
		df = 0
	}
	num := float64(c.N) - float64(df) + 0.5
	den := float64(df) + 0.5
	return float32(math.Log(1 + num/den))
}

// Scorer is the fielded document scorer. It is constructed once per query from
// the pinned params and the collection stats, then evaluated per candidate
// document.
type Scorer struct {
	params Params
	col    Collection
}

// NewScorer returns a Scorer for the given params and collection. A zero average
// field length is replaced by 1 so the length ratio is well defined for a field
// that happens to have no observed content.
func NewScorer(p Params, c Collection) *Scorer {
	for f := range c.AvgFieldLen {
		if c.AvgFieldLen[f] <= 0 {
			c.AvgFieldLen[f] = 1
		}
	}
	return &Scorer{params: p, col: c}
}

// pseudoFreq computes tf_eff(t,D): the field-weighted, length-normalized
// frequency that the saturation is applied to once.
func (s *Scorer) pseudoFreq(tf, fieldLen [NumFields]uint32) float32 {
	var eff float32
	for f := range NumFields {
		if tf[f] == 0 || s.params.Weights[f] == 0 {
			continue
		}
		b := s.params.B[f]
		bNorm := (1 - b) + b*float32(fieldLen[f])/s.col.AvgFieldLen[f]
		if bNorm <= 0 {
			bNorm = 1
		}
		eff += s.params.Weights[f] * float32(tf[f]) / bNorm
	}
	return eff
}

// saturate applies the BM25 saturation IDF * eff*(k1+1)/(k1+eff) to a combined
// pseudo-frequency.
func (s *Scorer) saturate(idf, eff float32) Score {
	if eff <= 0 {
		return 0
	}
	k1 := s.params.K1
	return Score(idf * eff * (k1 + 1) / (k1 + eff))
}

// ScoreTerm scores one query term against one document. idf is the term's IDF
// (from Collection.IDF), tf is its per-field frequency in the document, and
// fieldLen is the document's per-field length. The result is non-negative.
func (s *Scorer) ScoreTerm(idf float32, tf, fieldLen [NumFields]uint32) Score {
	return s.saturate(idf, s.pseudoFreq(tf, fieldLen))
}

// TermStats is the per-term input to a full document score: the term's IDF and
// its per-field frequencies in the document.
type TermStats struct {
	IDF float32
	TF  [NumFields]uint32
}

// ScoreDoc sums the per-term BM25F contributions for a document whose per-field
// lengths are fieldLen. This is the document-level relevance score; the terms
// slice is the query's terms with their per-document field frequencies.
func (s *Scorer) ScoreDoc(terms []TermStats, fieldLen [NumFields]uint32) Score {
	var total Score
	for i := range terms {
		total += s.saturate(terms[i].IDF, s.pseudoFreq(terms[i].TF, fieldLen))
	}
	return total
}
