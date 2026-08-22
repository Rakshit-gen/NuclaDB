package pq

import (
	"math/rand"
	"sort"
	"testing"
)

func randomVector(rng *rand.Rand, dim int) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = rng.Float32()*2 - 1
	}
	return v
}

// clusteredVectors generates n vectors drawn from numClusters well-
// separated Gaussian blobs in dim-space, so k-means has an unambiguous
// right answer to converge to and reconstruction-error tests are
// meaningful rather than checking noise.
func clusteredVectors(rng *rand.Rand, n, dim, numClusters int) [][]float32 {
	centers := make([][]float32, numClusters)
	for c := range centers {
		v := make([]float32, dim)
		for d := range v {
			v[d] = float32(c) * 10 // widely separated along every axis
		}
		centers[c] = v
	}
	out := make([][]float32, n)
	for i := range out {
		c := centers[rng.Intn(numClusters)]
		v := make([]float32, dim)
		for d := range v {
			v[d] = c[d] + rng.Float32()*0.1 // small jitter within the blob
		}
		out[i] = v
	}
	return out
}

func TestEncodeDecodeReconstructsClusteredData(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	const dim, m, n, clusters = 16, 4, 2000, 8

	vectors := clusteredVectors(rng, n, dim, clusters)
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 16, Seed: 1}, vectors)
	if err != nil {
		t.Fatal(err)
	}

	var totalErr float64
	for _, v := range vectors {
		code, err := cb.Encode(v)
		if err != nil {
			t.Fatal(err)
		}
		recon := cb.Decode(code)
		totalErr += float64(sqDist(v, recon))
	}
	avgErr := totalErr / float64(n)
	t.Logf("average reconstruction squared-error: %.4f", avgErr)
	// Points jitter by at most 0.1 per axis around their cluster center,
	// so a codebook that actually separated the clusters should
	// reconstruct each point close to its own center, not some other
	// cluster's — a loose bound that would fail if clustering collapsed.
	if avgErr > 1.0 {
		t.Fatalf("reconstruction error too high (%.4f), codebook likely didn't separate clusters", avgErr)
	}
}

func TestDistanceTableMatchesExactDistanceToDecodedCode(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	const dim, m = 32, 8

	vectors := clusteredVectors(rng, 1000, dim, 10)
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 32, Seed: 2}, vectors)
	if err != nil {
		t.Fatal(err)
	}

	query := clusteredVectors(rng, 1, dim, 10)[0]
	table, err := cb.NewDistanceTable(query)
	if err != nil {
		t.Fatal(err)
	}

	for i, v := range vectors[:50] {
		code, err := cb.Encode(v)
		if err != nil {
			t.Fatal(err)
		}
		adc := table.Distance(code)
		exact := sqDist(query, cb.Decode(code))
		diff := adc - exact
		if diff < 0 {
			diff = -diff
		}
		// Relative tolerance: these are float32 sums of squared
		// differences at magnitudes in the hundreds of thousands here, so
		// float32's ~7 significant digits alone account for absolute
		// error on the order of magnitude*1e-7 — an absolute tolerance
		// would either be too tight at this scale or too loose at a
		// smaller one.
		if exact != 0 && diff/exact > 1e-4 {
			t.Fatalf("vector %d: ADC distance %.6f != exact distance to decoded code %.6f (relative diff %.6g)", i, adc, exact, diff/exact)
		}
	}
}

func TestTrainRejectsBadConfig(t *testing.T) {
	vectors := [][]float32{{1, 2, 3, 4}, {5, 6, 7, 8}}
	if _, err := Train(Config{Dim: 5, NumSubvectors: 2}, vectors); err != ErrDimMismatch {
		t.Fatalf("expected ErrDimMismatch for non-divisible dim, got %v", err)
	}
	if _, err := Train(Config{Dim: 4, NumSubvectors: 2, NumCentroids: 100}, vectors); err != ErrTooFewVectors {
		t.Fatalf("expected ErrTooFewVectors, got %v", err)
	}
}

func bruteForceExact(vectors map[uint64][]float32, query []float32, topK int) []uint64 {
	type s struct {
		id   uint64
		dist float32
	}
	all := make([]s, 0, len(vectors))
	for id, v := range vectors {
		all = append(all, s{id: id, dist: sqDist(query, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	if len(all) > topK {
		all = all[:topK]
	}
	out := make([]uint64, len(all))
	for i, e := range all {
		out[i] = e.id
	}
	return out
}

// TestIndexRecallVsExact measures PQ's real recall cost against exact
// brute-force search on the same data — this is the number that has to be
// reported honestly (PQ is lossy by design; the question is how lossy at
// a realistic compression ratio, not whether it's lossy at all).
//
// Uses uniform random vectors, not the tightly clustered generator above:
// with near-identical intra-cluster jitter, "exact top-K" among a cluster
// of ~150 near-duplicate points is effectively an arbitrary tie-break on
// noise smaller than PQ's quantization step, which would make this measure
// how well PQ guesses coin flips rather than how well it ranks genuinely
// different vectors — not a meaningful compression benchmark either way.
func TestIndexRecallVsExact(t *testing.T) {
	const (
		dim      = 64
		m        = 16 // 64 floats (256 bytes) compressed to 16 bytes: 16x
		n        = 3000
		topK     = 10
		nQueries = 30
	)
	rng := rand.New(rand.NewSource(3))

	trainSet := make([][]float32, 2000)
	for i := range trainSet {
		trainSet[i] = randomVector(rng, dim)
	}
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 256, Seed: 3}, trainSet)
	if err != nil {
		t.Fatal(err)
	}

	idx := NewIndex(cb)
	vectors := make(map[uint64][]float32, n)
	for i := 0; i < n; i++ {
		id := uint64(i)
		v := randomVector(rng, dim)
		vectors[id] = v
		if err := idx.Insert(id, v); err != nil {
			t.Fatal(err)
		}
	}

	var totalRecall float64
	for q := 0; q < nQueries; q++ {
		query := randomVector(rng, dim)
		want := bruteForceExact(vectors, query, topK)

		res, err := idx.Search(query, topK)
		if err != nil {
			t.Fatal(err)
		}
		gotSet := make(map[uint64]bool, len(res))
		for _, r := range res {
			gotSet[r.ID] = true
		}
		hit := 0
		for _, id := range want {
			if gotSet[id] {
				hit++
			}
		}
		totalRecall += float64(hit) / float64(len(want))
	}

	avgRecall := totalRecall / nQueries
	t.Logf("PQ (16x compression) average recall@%d over %d queries: %.3f", topK, nQueries, avgRecall)
	// Lossy compression at 16x costs real recall, and flat PQ with no
	// IVF/re-ranking stage is the weakest configuration of the technique.
	// Measured recall on uniform random data at this M/K/dim was ~0.58
	// (see docs/writeups) — this threshold is set below that measurement
	// with margin for run-to-run variance from Go's randomized map
	// iteration order breaking ADC-distance ties differently, not chosen
	// to look good.
	const minRecall = 0.45
	if avgRecall < minRecall {
		t.Fatalf("recall@%d = %.3f, want >= %.3f", topK, avgRecall, minRecall)
	}
}

func TestIndexDeleteExcludesFromSearch(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	const dim, m = 8, 2
	vectors := clusteredVectors(rng, 500, dim, 5)
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 16, Seed: 4}, vectors)
	if err != nil {
		t.Fatal(err)
	}
	idx := NewIndex(cb)
	for i, v := range vectors[:100] {
		if err := idx.Insert(uint64(i), v); err != nil {
			t.Fatal(err)
		}
	}
	idx.Delete(0)
	res, err := idx.Search(vectors[0], 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.ID == 0 {
			t.Fatalf("deleted id=0 still present in results")
		}
	}
	if idx.Len() != 99 {
		t.Fatalf("Len() = %d, want 99", idx.Len())
	}
}
