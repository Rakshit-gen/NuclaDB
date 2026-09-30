package hnsw

import (
	"math/rand"
	"sort"
	"testing"
)

func recallAt10(t *testing.T, g *Graph, data, queries [][]float32) float64 {
	t.Helper()
	var total float64
	for _, q := range queries {
		ids := make([]int, len(data))
		for i := range ids {
			ids[i] = i
		}
		sort.Slice(ids, func(a, b int) bool { return L2().Distance(q, data[ids[a]]) < L2().Distance(q, data[ids[b]]) })
		want := make(map[uint64]bool, 10)
		for _, id := range ids[:10] {
			want[uint64(id)] = true
		}
		res, err := g.Search(q, 10, 50)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range res {
			if want[r.ID] {
				total++
			}
		}
	}
	return total / float64(10*len(queries))
}

func TestInsertBatchRecallMatchesSequential(t *testing.T) {
	const n, dim = 4000, 32
	rng := rand.New(rand.NewSource(1))
	data := make([][]float32, n+50)
	for i := range data {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		data[i] = v
	}
	queries := data[n:]
	data = data[:n]
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = uint64(i)
	}

	cfg := Config{Dim: dim, M: 16, EfConstruction: 100, Metric: L2(), Seed: 1}
	seq := New(cfg)
	for i, v := range data {
		if err := seq.Insert(ids[i], v); err != nil {
			t.Fatal(err)
		}
	}
	batch := New(cfg)
	if err := batch.InsertBatch(ids, data); err != nil {
		t.Fatal(err)
	}
	if batch.Len() != n {
		t.Fatalf("Len = %d, want %d", batch.Len(), n)
	}

	rs, rb := recallAt10(t, seq, data, queries), recallAt10(t, batch, data, queries)
	t.Logf("recall@10 at ef 50: sequential %.3f, batch %.3f", rs, rb)
	if rb < rs-0.02 {
		t.Fatalf("batch recall %.3f is more than 0.02 below sequential %.3f", rb, rs)
	}
}

func TestInsertBatchHandlesReinsertAndDuplicates(t *testing.T) {
	g := New(Config{Dim: 4, M: 8, EfConstruction: 50, Metric: L2(), Seed: 2})
	var ids []uint64
	var vecs [][]float32
	for i := 0; i < 3000; i++ {
		ids = append(ids, uint64(i%2500)) // ids 0..499 appear twice
		vecs = append(vecs, []float32{float32(i), 0, 0, 0})
	}
	if err := g.InsertBatch(ids, vecs); err != nil {
		t.Fatal(err)
	}
	if g.Len() != 2500 {
		t.Fatalf("Len = %d, want 2500", g.Len())
	}
	// The later write wins: id 7 now holds the vector from position 2507.
	res, err := g.Search([]float32{2507, 0, 0, 0}, 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 7 || res[0].Distance != 0 {
		t.Fatalf("got %+v, want id 7 at distance 0", res)
	}
	if err := g.InsertBatch([]uint64{1}, [][]float32{{1, 2}}); err != ErrDimensionMismatch {
		t.Fatalf("short vector: err %v, want ErrDimensionMismatch", err)
	}
}
