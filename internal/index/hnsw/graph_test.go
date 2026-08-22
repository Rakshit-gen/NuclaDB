package hnsw

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

// bruteForce returns the true topK nearest neighbors of query under metric,
// used as ground truth to measure the graph's recall.
func bruteForce(vectors map[uint64][]float32, query []float32, metric Metric, topK int) []uint64 {
	type scored struct {
		id   uint64
		dist float32
	}
	all := make([]scored, 0, len(vectors))
	for id, v := range vectors {
		all = append(all, scored{id: id, dist: metric.Distance(query, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	if len(all) > topK {
		all = all[:topK]
	}
	out := make([]uint64, len(all))
	for i, s := range all {
		out[i] = s.id
	}
	return out
}

func recallAt(got, want []uint64) float64 {
	wantSet := make(map[uint64]bool, len(want))
	for _, id := range want {
		wantSet[id] = true
	}
	hit := 0
	for _, id := range got {
		if wantSet[id] {
			hit++
		}
	}
	if len(want) == 0 {
		return 1
	}
	return float64(hit) / float64(len(want))
}

func TestInsertAndSearchExactMatch(t *testing.T) {
	g := New(Config{Dim: 4, Metric: L2(), Seed: 1})
	if err := g.Insert(1, []float32{1, 0, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := g.Insert(2, []float32{0, 1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := g.Insert(3, []float32{0, 0, 1, 0}); err != nil {
		t.Fatal(err)
	}

	res, err := g.Search([]float32{1, 0, 0, 0}, 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 1 {
		t.Fatalf("expected exact match id=1, got %+v", res)
	}
}

func TestDimensionMismatch(t *testing.T) {
	g := New(Config{Dim: 4})
	if err := g.Insert(1, []float32{1, 2, 3}); err != ErrDimensionMismatch {
		t.Fatalf("expected ErrDimensionMismatch, got %v", err)
	}
	_ = g.Insert(1, []float32{1, 2, 3, 4})
	if _, err := g.Search([]float32{1, 2}, 1, 10); err != ErrDimensionMismatch {
		t.Fatalf("expected ErrDimensionMismatch, got %v", err)
	}
}

func TestDeleteExcludesFromSearch(t *testing.T) {
	g := New(Config{Dim: 2, Metric: L2(), Seed: 1})
	_ = g.Insert(1, []float32{0, 0})
	_ = g.Insert(2, []float32{100, 100})

	if err := g.Delete(1); err != nil {
		t.Fatal(err)
	}
	res, err := g.Search([]float32{0, 0}, 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.ID == 1 {
			t.Fatalf("deleted id=1 still returned in results: %+v", res)
		}
	}
	if err := g.Delete(1); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on double delete, got %v", err)
	}
}

// TestRecallAgainstBruteForce is the core correctness check for the graph:
// on a random dataset, HNSW search with a generous ef should recover the
// large majority of the true nearest neighbors found by brute force.
func TestRecallAgainstBruteForce(t *testing.T) {
	const (
		dim       = 32
		n         = 2000
		topK      = 10
		ef        = 100
		nQueries  = 50
		minRecall = 0.90
	)

	rng := rand.New(rand.NewSource(42))
	g := New(Config{Dim: dim, M: 16, EfConstruction: 200, Metric: L2(), Seed: 42})

	vectors := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		v := randomVector(rng, dim)
		vectors[i] = v
		if err := g.Insert(i, v); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	var totalRecall float64
	for q := 0; q < nQueries; q++ {
		query := randomVector(rng, dim)
		want := bruteForce(vectors, query, L2(), topK)

		res, err := g.Search(query, topK, ef)
		if err != nil {
			t.Fatal(err)
		}
		got := make([]uint64, len(res))
		for i, r := range res {
			got[i] = r.ID
		}
		totalRecall += recallAt(got, want)
	}

	avgRecall := totalRecall / nQueries
	t.Logf("average recall@%d over %d queries: %.3f", topK, nQueries, avgRecall)
	if avgRecall < minRecall {
		t.Fatalf("recall@%d = %.3f, want >= %.3f", topK, avgRecall, minRecall)
	}
}

func TestConcurrentInsertSearch(t *testing.T) {
	const dim = 8
	g := New(Config{Dim: dim, Metric: Cosine(), Seed: 7})
	rng := rand.New(rand.NewSource(7))

	// seed with a base set so early searches have something to find
	for i := uint64(0); i < 100; i++ {
		_ = g.Insert(i, randomVector(rng, dim))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(99))
		for i := uint64(100); i < 300; i++ {
			_ = g.Insert(i, randomVector(r, dim))
		}
	}()

	r := rand.New(rand.NewSource(123))
	for i := 0; i < 200; i++ {
		if _, err := g.Search(randomVector(r, dim), 5, 20); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}
