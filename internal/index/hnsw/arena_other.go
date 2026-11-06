//go:build !unix

package hnsw

func mapBlock(n int) (b []byte, mapped bool) { return make([]byte, n), false }

func unmapBlock([]byte) {}
