//go:build !arm64

package hnsw

func l2(a, b []float32) float32  { return l2Go(a, b) }
func dot(a, b []float32) float32 { return dotGo(a, b) }
