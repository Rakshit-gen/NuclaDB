package hnsw

import "math"

// Metric computes a distance between two equal-length vectors. Lower is
// closer. Implementations must be safe for concurrent use.
type Metric interface {
	Distance(a, b []float32) float32
	Name() string
}

type cosineMetric struct{}

// Cosine returns a metric based on 1 - cosine similarity, so that smaller
// values mean "more similar", matching the convention of the other metrics.
func Cosine() Metric { return cosineMetric{} }

func (cosineMetric) Name() string { return "cosine" }

func (cosineMetric) Distance(a, b []float32) float32 {
	var dot, normA, normB float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		a0, a1, a2, a3 := a[i], a[i+1], a[i+2], a[i+3]
		b0, b1, b2, b3 := b[i], b[i+1], b[i+2], b[i+3]
		dot += a0*b0 + a1*b1 + a2*b2 + a3*b3
		normA += a0*a0 + a1*a1 + a2*a2 + a3*a3
		normB += b0*b0 + b1*b1 + b2*b2 + b3*b3
	}
	for ; i < len(a); i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}
	if normA == 0 || normB == 0 {
		return 1
	}
	sim := float64(dot) / (math.Sqrt(float64(normA)) * math.Sqrt(float64(normB)))
	return float32(1 - sim)
}

// unitCosine is cosine distance for vectors already scaled to unit length.
func unitCosine(a, b []float32) float32 {
	return 1 + dotMetric{}.Distance(a, b)
}

// normalize scales v to unit length in place. A zero vector stays zero.
func normalize(v []float32) {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
}

type l2Metric struct{}

// L2 returns a metric based on squared Euclidean distance. Squared (rather
// than sqrt'd) because nearest-neighbor ranking is identical either way and
// skipping the sqrt is cheaper per comparison.
func L2() Metric { return l2Metric{} }

func (l2Metric) Name() string { return "l2" }

// Distance is the hot loop of both build and search. Four independent
// float32 accumulators let the CPU keep several multiply-adds in flight
// instead of waiting on one running sum; Go has no stable SIMD, so this is
// the portable version of what hnswlib does with intrinsics.
func (l2Metric) Distance(a, b []float32) float32 {
	b = b[:len(a)] // one bounds check here instead of one per element
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		d0 := a[i] - b[i]
		d1 := a[i+1] - b[i+1]
		d2 := a[i+2] - b[i+2]
		d3 := a[i+3] - b[i+3]
		s0 += d0 * d0
		s1 += d1 * d1
		s2 += d2 * d2
		s3 += d3 * d3
	}
	for ; i < len(a); i++ {
		d := a[i] - b[i]
		s0 += d * d
	}
	return (s0 + s1) + (s2 + s3)
}

type dotMetric struct{}

// Dot returns a metric based on negative dot product, so smaller values
// mean "more similar" (highest raw dot product ranks first).
func Dot() Metric { return dotMetric{} }

func (dotMetric) Name() string { return "dot" }

func (dotMetric) Distance(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3 float32
	i := 0
	for ; i+4 <= len(a); i += 4 {
		s0 += a[i] * b[i]
		s1 += a[i+1] * b[i+1]
		s2 += a[i+2] * b[i+2]
		s3 += a[i+3] * b[i+3]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return -((s0 + s1) + (s2 + s3))
}
