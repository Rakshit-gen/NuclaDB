package hnsw

import (
	"sync"
	"unsafe"
)

// arena hands out the graph's vector and layer-0 link blocks. On unix they
// are anonymous mmaps, outside the Go heap: the blocks hold no pointers,
// so the GC has nothing to scan in them, and keeping them off the heap
// stops them from counting toward the heap goal. With them on the heap,
// GOGC=100 let a 1M-vector graph (about 0.65 GB of blocks) grow to 2x
// that before collecting. A cleanup on the Graph unmaps them once nothing
// references it, so nothing may hold a slice into a block (g.vec,
// g.links) past the graph's last use; Compact keeps its source alive for
// that reason.
type arena struct {
	mu     sync.Mutex
	mapped [][]byte
}

func (a *arena) bytes(n int) []byte {
	b, mapped := mapBlock(n)
	if mapped {
		a.mu.Lock()
		a.mapped = append(a.mapped, b)
		a.mu.Unlock()
	}
	return b
}

// float32s returns an empty, zeroed slice with capacity n.
func (a *arena) float32s(n int) []float32 {
	b := a.bytes(4 * n)
	return unsafe.Slice((*float32)(unsafe.Pointer(&b[0])), n)[:0]
}

// uint32s returns an empty, zeroed slice with capacity n.
func (a *arena) uint32s(n int) []uint32 {
	b := a.bytes(4 * n)
	return unsafe.Slice((*uint32)(unsafe.Pointer(&b[0])), n)[:0]
}

func (a *arena) free() {
	for _, b := range a.mapped {
		unmapBlock(b)
	}
	a.mapped = nil
}
