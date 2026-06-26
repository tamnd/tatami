package search

// bm25Scorer is the per-term scorer the WAND loop drives. It is BM25 with length
// normalization disabled (b=0), so the score is a strictly monotonic function of
// the term frequency and MaxScore(maxFreq) is a valid block-max bound: no
// document in a block can score above the block's max-frequency document. That is
// the only precondition WAND places on a scorer, so the retrieval phase returns
// an exact top-k under this scorer. Full BM25F (with per-field length norms read
// from the forward store) re-ranks the survivors when fielded scoring is wanted.
type bm25Scorer struct {
	idf float32
	k1  float32
}

// NewBM25Scorer returns a WAND scorer for a term with the given IDF and the
// shared k1 saturation point.
func NewBM25Scorer(idf, k1 float32) FreqScorer { return bm25Scorer{idf: idf, k1: k1} }

func (s bm25Scorer) Score(freq uint32) Score {
	if freq == 0 {
		return 0
	}
	f := float32(freq)
	return Score(s.idf * f * (s.k1 + 1) / (s.k1 + f))
}

// MaxScore bounds a block from its maximum frequency. Because Score is monotonic
// in freq, the block's max-frequency document is its highest scorer.
func (s bm25Scorer) MaxScore(maxFreq uint32) Score { return s.Score(maxFreq) }
