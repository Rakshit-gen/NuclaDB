package pq

import (
	"math/rand"
	"testing"
)

func TestIVFFullProbeMatchesFlatIndex(t *testing.T) {
	const dim, m = 32, 8
	rng := rand.New(rand.NewSource(5))
	vectors := clusteredVectors(rng, 2000, dim, 20)
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 64, Seed: 5}, vectors)
	if err != nil {
		t.Fatal(err)
	}
	ivf, err := TrainIVF(cb, 16, vectors, 5)
	if err != nil {
		t.Fatal(err)
	}
	flat := NewIndex(cb)
	for i, v := range vectors {
		_ = flat.Insert(uint64(i), v)
		_ = ivf.Insert(uint64(i), v)
	}

	for q := 0; q < 10; q++ {
		query := vectors[rng.Intn(len(vectors))]
		want, _ := flat.Search(query, 10)
		got, _ := ivf.Search(query, 10, 16)
		// Ids can differ on ADC ties, distances can't.
		for i := range want {
			if got[i].Distance != want[i].Distance {
				t.Fatalf("query %d rank %d: ivf %v, flat %v", q, i, got[i].Distance, want[i].Distance)
			}
		}
	}
}

func TestIVFProbingFewListsKeepsRecall(t *testing.T) {
	const (
		dim, m, n = 32, 8, 5000
		nlist     = 32
		topK      = 10
		nQueries  = 30
	)
	rng := rand.New(rand.NewSource(6))
	data := overlappingClusters(rng, n+nQueries, dim, 40)
	queries := data[n:]
	data = data[:n]
	cb, err := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 256, Seed: 6}, data[:2000])
	if err != nil {
		t.Fatal(err)
	}
	ivf, err := TrainIVF(cb, nlist, data[:2000], 6)
	if err != nil {
		t.Fatal(err)
	}
	vectors := make(map[uint64][]float32, n)
	for i, v := range data {
		vectors[uint64(i)] = v
		if err := ivf.Insert(uint64(i), v); err != nil {
			t.Fatal(err)
		}
	}
	lookup := func(id uint64) ([]float32, bool) { v, ok := vectors[id]; return v, ok }

	recallAt := func(nprobe int) float64 {
		var total float64
		for _, query := range queries {
			want := bruteForceExact(vectors, query, topK)
			res, err := ivf.SearchRerank(query, topK, nprobe, 100, lookup)
			if err != nil {
				t.Fatal(err)
			}
			got := make(map[uint64]bool, len(res))
			for _, r := range res {
				got[r.ID] = true
			}
			hit := 0
			for _, id := range want {
				if got[id] {
					hit++
				}
			}
			total += float64(hit) / float64(topK)
		}
		return total / nQueries
	}
	for _, p := range []int{1, 4, 8, nlist} {
		t.Logf("nprobe %d of %d lists: recall@%d %.3f with re-rank of top 100", p, nlist, topK, recallAt(p))
	}
	// Measured 0.637 / 0.870 / 0.943 / 0.993 at nprobe 1 / 4 / 8 / 32.
	if r := recallAt(8); r < 0.9 {
		t.Fatalf("recall@%d at nprobe 8 = %.3f, want >= 0.9", topK, r)
	}
}

func TestIVFReinsertMovesListsAndDeleteRemoves(t *testing.T) {
	const dim, m = 8, 2
	rng := rand.New(rand.NewSource(7))
	vectors := clusteredVectors(rng, 200, dim, 4)
	cb, _ := Train(Config{Dim: dim, NumSubvectors: m, NumCentroids: 16, Seed: 7}, vectors)
	ivf, err := TrainIVF(cb, 4, vectors, 7)
	if err != nil {
		t.Fatal(err)
	}
	_ = ivf.Insert(1, vectors[0])
	_ = ivf.Insert(1, vectors[150])
	if ivf.Len() != 1 {
		t.Fatalf("Len = %d after re-insert, want 1", ivf.Len())
	}
	res, _ := ivf.Search(vectors[150], 5, 4)
	if len(res) != 1 || res[0].ID != 1 {
		t.Fatalf("got %+v, want only id 1", res)
	}
	ivf.Delete(1)
	if res, _ := ivf.Search(vectors[150], 5, 4); len(res) != 0 || ivf.Len() != 0 {
		t.Fatalf("after delete: Len %d, results %+v", ivf.Len(), res)
	}
	if _, err := TrainIVF(cb, 0, vectors, 7); err != ErrBadNList {
		t.Fatalf("nlist 0: err %v, want ErrBadNList", err)
	}
}

// overlappingClusters draws n vectors around numClusters random centers with
// noise wide enough that clusters overlap, so a query's true neighbors can
// sit in lists other than its nearest one.
func overlappingClusters(rng *rand.Rand, n, dim, numClusters int) [][]float32 {
	centers := make([][]float32, numClusters)
	for c := range centers {
		centers[c] = randomVector(rng, dim)
	}
	out := make([][]float32, n)
	for i := range out {
		c := centers[rng.Intn(numClusters)]
		v := make([]float32, dim)
		for d := range v {
			v[d] = c[d] + float32(rng.NormFloat64())*0.8
		}
		out[i] = v
	}
	return out
}
