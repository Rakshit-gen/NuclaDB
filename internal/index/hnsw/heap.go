package hnsw

import "container/heap"

// candidate is a node reachable during graph traversal, paired with its
// distance to the current query vector.
type candidate struct {
	id   uint64
	dist float32
}

// minHeap pops the closest candidate first; used for the traversal frontier.
type minHeap []candidate

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// maxHeap pops the farthest candidate first; used to keep a bounded
// best-so-far result set, evicting the worst entry as better ones arrive.
type maxHeap []candidate

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(candidate)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func newMinHeap() *minHeap {
	h := &minHeap{}
	heap.Init(h)
	return h
}

func newMaxHeap() *maxHeap {
	h := &maxHeap{}
	heap.Init(h)
	return h
}
