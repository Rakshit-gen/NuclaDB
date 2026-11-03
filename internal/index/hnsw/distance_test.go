package hnsw

import (
	"math"
	"math/rand"
	"testing"
)

// The unrolled kernels must agree with a plain float64 reference, including
// lengths that aren't a multiple of 4.
func TestDistanceKernelsMatchReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for _, dim := range []int{1, 3, 4, 7, 128, 131} {
		a, b := make([]float32, dim), make([]float32, dim)
		for i := range a {
			a[i], b[i] = rng.Float32()*2-1, rng.Float32()*2-1
		}
		var l2, dot, na, nb float64
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			l2 += d * d
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		cos := 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
		for _, c := range []struct {
			m    Metric
			want float64
		}{{L2(), l2}, {Dot(), -dot}, {Cosine(), cos}} {
			got := float64(c.m.Distance(a, b))
			if math.Abs(got-c.want) > 1e-4*math.Max(1, math.Abs(c.want)) {
				t.Errorf("%s dim %d: got %v, want %v", c.m.Name(), dim, got, c.want)
			}
		}
	}
}

// Cosine graphs store unit vectors and rank by dot product; the distances
// they report must still equal cosine distance on the raw vectors.
func TestCosineGraphMatchesCosineDistance(t *testing.T) {
	rng := rand.New(rand.NewSource(5))
	g := New(Config{Dim: 16, Metric: Cosine(), Seed: 1})
	raw := make(map[uint64][]float32)
	for i := uint64(0); i < 200; i++ {
		v := make([]float32, 16)
		for j := range v {
			v[j] = (rng.Float32() - 0.5) * float32(i+1) // lengths vary a lot
		}
		raw[i] = v
		if err := g.Insert(i, v); err != nil {
			t.Fatal(err)
		}
	}
	q := make([]float32, 16)
	for j := range q {
		q[j] = rng.Float32() * 40
	}
	qCopy := append([]float32(nil), q...)
	res, err := g.Search(q, 10, 200)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		want := Cosine().Distance(q, raw[r.ID])
		if math.Abs(float64(r.Distance-want)) > 1e-5 {
			t.Fatalf("id %d: distance %v, cosine says %v", r.ID, r.Distance, want)
		}
	}
	for j := range q {
		if q[j] != qCopy[j] {
			t.Fatal("Search modified the caller's query")
		}
	}
}

// TestKernelsMatchPortable checks l2 and dot (NEON on arm64) against the
// portable Go loops for every length up to 300, so each tail size and
// several multiples of 16 are covered.
func TestKernelsMatchPortable(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for n := 0; n <= 300; n++ {
		a, b := make([]float32, n), make([]float32, n)
		for i := range a {
			a[i], b[i] = rng.Float32()*2-1, rng.Float32()*2-1
		}
		for _, k := range []struct {
			name      string
			got, want float32
		}{
			{"l2", l2(a, b), l2Go(a, b)},
			{"dot", dot(a, b), dotGo(a, b)},
		} {
			if diff := math.Abs(float64(k.got - k.want)); diff > 1e-4*max(1, math.Abs(float64(k.want))) {
				t.Fatalf("%s n=%d: got %v, want %v", k.name, n, k.got, k.want)
			}
		}
	}
}

func BenchmarkL2(b *testing.B) {
	x, y := make([]float32, 128), make([]float32, 128)
	for i := range x {
		x[i], y[i] = float32(i), float32(2*i)
	}
	b.Run("kernel", func(b *testing.B) {
		for b.Loop() {
			l2(x, y)
		}
	})
	b.Run("portable", func(b *testing.B) {
		for b.Loop() {
			l2Go(x, y)
		}
	})
}
