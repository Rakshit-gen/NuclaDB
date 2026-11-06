//go:build unix

package hnsw

import "syscall"

// mapBlock returns n zeroed bytes from an anonymous mmap, or from the Go
// heap (mapped false) if the mmap fails. Pages are only backed by memory
// once touched, so a mostly empty block costs address space, not RSS.
func mapBlock(n int) (b []byte, mapped bool) {
	b, err := syscall.Mmap(-1, 0, n, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_ANON|syscall.MAP_PRIVATE)
	if err != nil {
		return make([]byte, n), false
	}
	return b, true
}

func unmapBlock(b []byte) { _ = syscall.Munmap(b) }
