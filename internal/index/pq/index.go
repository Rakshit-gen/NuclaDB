package pq

import (
	"container/heap"
	"sync"
)

// SearchResult is one match returned by Index.Search.
type SearchResult struct {
	ID       uint64
	Distance float32
}

// Index is a flat (unindexed) collection of PQ-encoded vectors: search is
// a brute-force scan over every code, scored via ADC. Without a graph or
// inverted file on top, this is O(N*M) per query rather than HNSW's
// O(log N * M) — the tradeoff bought is memory, not query complexity: an
// M-byte code per vector instead of Dim*4 bytes, so it's the right choice
// once a collection is too large to keep as full float32 vectors at all,
// even before considering search cost.
type Index struct {
	codebook *Codebook

	mu    sync.RWMutex
	codes map[uint64][]byte
}

// NewIndex creates an empty index that encodes vectors with codebook.
func NewIndex(codebook *Codebook) *Index {
	return &Index{codebook: codebook, codes: make(map[uint64][]byte)}
}

// Insert encodes and stores vector under id, replacing any existing entry.
func (idx *Index) Insert(id uint64, vector []float32) error {
	code, err := idx.codebook.Encode(vector)
	if err != nil {
		return err
	}
	idx.mu.Lock()
	idx.codes[id] = code
	idx.mu.Unlock()
	return nil
}

// Delete removes id from the index. Deleting an absent id is a no-op.
func (idx *Index) Delete(id uint64) {
	idx.mu.Lock()
	delete(idx.codes, id)
	idx.mu.Unlock()
}

// Len returns the number of stored codes.
func (idx *Index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.codes)
}

type scored struct {
	id   uint64
	dist float32
}
type maxHeap []scored

func (h maxHeap) Len() int            { return len(h) }
func (h maxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h maxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *maxHeap) Push(x interface{}) { *h = append(*h, x.(scored)) }
func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// Search returns the topK codes with the smallest ADC distance to query,
// scanning every stored code. A single DistanceTable is built once and
// reused across the whole scan.
func (idx *Index) Search(query []float32, topK int) ([]SearchResult, error) {
	table, err := idx.codebook.NewDistanceTable(query)
	if err != nil {
		return nil, err
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	h := &maxHeap{}
	heap.Init(h)
	for id, code := range idx.codes {
		d := table.Distance(code)
		if h.Len() < topK {
			heap.Push(h, scored{id: id, dist: d})
			continue
		}
		if d < (*h)[0].dist {
			heap.Pop(h)
			heap.Push(h, scored{id: id, dist: d})
		}
	}

	out := make([]SearchResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		s := heap.Pop(h).(scored)
		out[i] = SearchResult{ID: s.id, Distance: s.dist}
	}
	return out, nil
}
