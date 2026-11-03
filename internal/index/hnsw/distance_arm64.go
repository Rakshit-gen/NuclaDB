package hnsw

// NEON kernels in distance_arm64.s. Each takes a length that is a positive
// multiple of 16; the wrappers below do the tail in Go.

//go:noescape
func l2NEON(a, b *float32, n int) float32

//go:noescape
func dotNEON(a, b *float32, n int) float32

func l2(a, b []float32) float32 {
	b = b[:len(a)]
	n := len(a) &^ 15
	var s float32
	if n > 0 {
		s = l2NEON(&a[0], &b[0], n)
	}
	for i := n; i < len(a); i++ {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

func dot(a, b []float32) float32 {
	b = b[:len(a)]
	n := len(a) &^ 15
	var s float32
	if n > 0 {
		s = dotNEON(&a[0], &b[0], n)
	}
	for i := n; i < len(a); i++ {
		s += a[i] * b[i]
	}
	return s
}
