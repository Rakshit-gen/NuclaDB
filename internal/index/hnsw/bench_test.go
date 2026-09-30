package hnsw

import (
	"math/rand"
	"testing"
)

func BenchmarkSearch(b *testing.B) {
	rng := rand.New(rand.NewSource(1))
	g := New(Config{Dim: 64, M: 16, EfConstruction: 100, Metric: L2(), Seed: 1})
	for i := uint64(0); i < 5000; i++ {
		v := make([]float32, 64)
		for j := range v {
			v[j] = rng.Float32()
		}
		if err := g.Insert(i, v); err != nil {
			b.Fatal(err)
		}
	}
	q := make([]float32, 64)
	for j := range q {
		q[j] = rng.Float32()
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.Search(q, 10, 100); err != nil {
				b.Fatal(err)
			}
		}
	})
}
