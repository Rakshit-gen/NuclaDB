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
	var dot, normA, normB float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}
	if normA == 0 || normB == 0 {
		return 1
	}
	sim := dot / (math.Sqrt(normA) * math.Sqrt(normB))
	return float32(1 - sim)
}

type l2Metric struct{}

// L2 returns a metric based on squared Euclidean distance. Squared (rather
// than sqrt'd) because nearest-neighbor ranking is identical either way and
// skipping the sqrt is cheaper per comparison.
func L2() Metric { return l2Metric{} }

func (l2Metric) Name() string { return "l2" }

func (l2Metric) Distance(a, b []float32) float32 {
	var sum float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		sum += d * d
	}
	return float32(sum)
}

type dotMetric struct{}

// Dot returns a metric based on negative dot product, so smaller values
// mean "more similar" (highest raw dot product ranks first).
func Dot() Metric { return dotMetric{} }

func (dotMetric) Name() string { return "dot" }

func (dotMetric) Distance(a, b []float32) float32 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return float32(-dot)
}
