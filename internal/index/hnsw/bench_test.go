package hnsw

import (
	"encoding/binary"
	"math"
	"math/rand"
	"os"
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

// BenchmarkBuildSIFT builds a graph over the 10K SIFT-small base set, the
// same data and M/ef_construct as bench/results.md. Skipped when the dataset
// hasn't been downloaded (bench/download.sh).
func BenchmarkBuildSIFT(b *testing.B) {
	vecs := loadFvecs(b, "../../../bench/data/siftsmall/siftsmall_base.fvecs")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := New(Config{Dim: len(vecs[0]), M: 16, EfConstruction: 200, Metric: L2(), Seed: 1})
		for id, v := range vecs {
			if err := g.Insert(uint64(id), v); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkBuildSIFTBatch is BenchmarkBuildSIFT through InsertBatch.
func BenchmarkBuildSIFTBatch(b *testing.B) {
	vecs := loadFvecs(b, "../../../bench/data/siftsmall/siftsmall_base.fvecs")
	ids := make([]uint64, len(vecs))
	for i := range ids {
		ids[i] = uint64(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := New(Config{Dim: len(vecs[0]), M: 16, EfConstruction: 200, Metric: L2(), Seed: 1})
		if err := g.InsertBatch(ids, vecs); err != nil {
			b.Fatal(err)
		}
	}
}

func loadFvecs(tb testing.TB, path string) [][]float32 {
	tb.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		tb.Skipf("dataset not available: %v", err)
	}
	var out [][]float32
	for off := 0; off < len(raw); {
		dim := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		v := make([]float32, dim)
		for j := range v {
			v[j] = math.Float32frombits(binary.LittleEndian.Uint32(raw[off:]))
			off += 4
		}
		out = append(out, v)
	}
	return out
}
