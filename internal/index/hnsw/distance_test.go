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
