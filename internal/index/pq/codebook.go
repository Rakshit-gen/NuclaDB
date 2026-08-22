// Package pq implements product quantization: a lossy vector-compression
// technique that trades a bounded amount of recall for a large, fixed
// memory reduction (typically 16-32x), used here as an alternative
// collection type for datasets too large to keep as full float32 vectors
// in memory.
//
// A vector of dimension D is split into M equal subvectors. A k-means
// codebook of K centroids (K=256 by default, so an index fits in a byte)
// is trained independently per subspace. Encoding a vector replaces each
// subvector with the index of its nearest centroid, so a 512-byte
// float32[128] vector becomes an M-byte code — with M=16, a 32x reduction.
//
// Search uses asymmetric distance computation (ADC): the query vector is
// left uncompressed, and a per-subspace distance table (query subvector vs
// every centroid in that subspace) is built once per query. Scoring a
// candidate is then M table lookups and additions instead of a full
// D-wide distance computation — both cheaper and, since it never
// quantizes the query, more accurate than symmetric (code-vs-code)
// distance would be.
package pq

import (
	"errors"
	"math/rand"
)

// ErrDimMismatch is returned when a vector's length doesn't match the
// codebook's configured dimensionality, or Dim isn't evenly divisible by
// NumSubvectors.
var ErrDimMismatch = errors.New("pq: dimension mismatch or not divisible by NumSubvectors")

// ErrTooFewVectors is returned when Train is given fewer training vectors
// than NumCentroids — k-means can't produce more clusters than points.
var ErrTooFewVectors = errors.New("pq: fewer training vectors than NumCentroids")

// Config controls codebook training.
type Config struct {
	// Dim is the full (uncompressed) vector dimensionality.
	Dim int
	// NumSubvectors (M) is how many equal chunks each vector is split
	// into. Must evenly divide Dim. Higher M preserves more recall at the
	// cost of a larger code (M bytes) and more centroids to train.
	NumSubvectors int
	// NumCentroids (K) per subspace. 256 is the conventional default,
	// since it lets each code element fit in a single byte.
	NumCentroids int
	// KMeansIters bounds Lloyd's-algorithm iterations per subspace.
	KMeansIters int
	Seed        int64
}

func (c Config) withDefaults() Config {
	if c.NumCentroids <= 0 {
		c.NumCentroids = 256
	}
	if c.KMeansIters <= 0 {
		c.KMeansIters = 25
	}
	return c
}

// Codebook holds M independently-trained k-means codebooks, one per
// subspace, each with NumCentroids centroids of length Dim/NumSubvectors.
type Codebook struct {
	cfg    Config
	subDim int
	// centroids[s] is a flattened NumCentroids x subDim matrix for
	// subspace s: centroid c occupies centroids[s][c*subDim : (c+1)*subDim].
	centroids [][]float32
}

// Train fits a codebook to vectors, which must all have length cfg.Dim.
// NumCentroids must not exceed the number of training vectors, and Dim
// must be evenly divisible by NumSubvectors.
func Train(cfg Config, vectors [][]float32) (*Codebook, error) {
	cfg = cfg.withDefaults()
	if cfg.NumCentroids > 256 {
		return nil, errors.New("pq: NumCentroids > 256 won't fit in a byte code")
	}
	if cfg.Dim <= 0 || cfg.NumSubvectors <= 0 || cfg.Dim%cfg.NumSubvectors != 0 {
		return nil, ErrDimMismatch
	}
	if len(vectors) < cfg.NumCentroids {
		return nil, ErrTooFewVectors
	}
	for _, v := range vectors {
		if len(v) != cfg.Dim {
			return nil, ErrDimMismatch
		}
	}

	subDim := cfg.Dim / cfg.NumSubvectors
	rng := rand.New(rand.NewSource(cfg.Seed))

	cb := &Codebook{cfg: cfg, subDim: subDim, centroids: make([][]float32, cfg.NumSubvectors)}
	for s := 0; s < cfg.NumSubvectors; s++ {
		sub := extractSubvectors(vectors, s, subDim)
		cb.centroids[s] = kmeans(sub, cfg.NumCentroids, cfg.KMeansIters, rng)
	}
	return cb, nil
}

func extractSubvectors(vectors [][]float32, s, subDim int) [][]float32 {
	out := make([][]float32, len(vectors))
	off := s * subDim
	for i, v := range vectors {
		sv := make([]float32, subDim)
		copy(sv, v[off:off+subDim])
		out[i] = sv
	}
	return out
}

func centroidAt(flat []float32, idx, subDim int) []float32 {
	return flat[idx*subDim : (idx+1)*subDim]
}

func sqDist(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(sum)
}

// Dim returns the codebook's full vector dimensionality.
func (c *Codebook) Dim() int { return c.cfg.Dim }

// NumSubvectors returns M, the length of every encoded code.
func (c *Codebook) NumSubvectors() int { return c.cfg.NumSubvectors }

// Encode quantizes vector into an M-byte code, one nearest-centroid index
// per subspace.
func (c *Codebook) Encode(vector []float32) ([]byte, error) {
	if len(vector) != c.cfg.Dim {
		return nil, ErrDimMismatch
	}
	code := make([]byte, c.cfg.NumSubvectors)
	for s := 0; s < c.cfg.NumSubvectors; s++ {
		off := s * c.subDim
		sub := vector[off : off+c.subDim]
		best, bestDist := 0, sqDist(sub, centroidAt(c.centroids[s], 0, c.subDim))
		for k := 1; k < c.cfg.NumCentroids; k++ {
			d := sqDist(sub, centroidAt(c.centroids[s], k, c.subDim))
			if d < bestDist {
				bestDist = d
				best = k
			}
		}
		code[s] = byte(best)
	}
	return code, nil
}

// Decode approximately reconstructs a vector from its code by
// concatenating the assigned centroids. Used for measuring reconstruction
// error, not on the search hot path.
func (c *Codebook) Decode(code []byte) []float32 {
	out := make([]float32, c.cfg.Dim)
	for s, idx := range code {
		off := s * c.subDim
		copy(out[off:off+c.subDim], centroidAt(c.centroids[s], int(idx), c.subDim))
	}
	return out
}

// DistanceTable is a precomputed set of per-subspace, per-centroid
// distances from one query vector, used to score many candidate codes
// cheaply via NewDistanceTable + Distance.
type DistanceTable struct {
	m, k  int
	table []float32
}

// NewDistanceTable precomputes the squared-L2 distance from query to
// every centroid in every subspace: an m*k table. Building it costs
// O(Dim*NumCentroids) once per query; scoring each candidate afterwards
// is then just NumSubvectors lookups and additions.
func (c *Codebook) NewDistanceTable(query []float32) (*DistanceTable, error) {
	if len(query) != c.cfg.Dim {
		return nil, ErrDimMismatch
	}
	t := &DistanceTable{m: c.cfg.NumSubvectors, k: c.cfg.NumCentroids, table: make([]float32, c.cfg.NumSubvectors*c.cfg.NumCentroids)}
	for s := 0; s < t.m; s++ {
		off := s * c.subDim
		sub := query[off : off+c.subDim]
		for k := 0; k < t.k; k++ {
			t.table[s*t.k+k] = sqDist(sub, centroidAt(c.centroids[s], k, c.subDim))
		}
	}
	return t, nil
}

// Distance returns the approximate squared-L2 distance from the query
// this table was built for to the vector code encodes. Because squared L2
// decomposes additively across orthogonal subspaces, this is exactly
// equal (up to float rounding) to computing squared L2 directly between
// the query and Decode(code) — ADC is just a faster way to get that same
// number without materializing the decoded vector.
func (t *DistanceTable) Distance(code []byte) float32 {
	var sum float32
	for s, idx := range code {
		sum += t.table[s*t.k+int(idx)]
	}
	return sum
}
