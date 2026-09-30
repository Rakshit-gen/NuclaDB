package pq

import (
	"container/heap"
	"errors"
	"math/rand"
	"sort"
	"sync"
)

// ErrBadNList is returned by TrainIVF when nlist is below 1 or above the
// number of training vectors.
var ErrBadNList = errors.New("pq: nlist must be between 1 and the number of training vectors")

// IVFIndex is Index with an inverted file in front of it: vectors are
// split into nlist lists by their nearest coarse centroid, and a search
// only scans the nprobe lists closest to the query instead of every code.
// Scan cost drops to roughly nprobe/nlist of the flat Index, at some recall
// cost when a true neighbor sits in a list that wasn't probed.
//
// Codes are PQ codes of the raw vector, from the same Codebook a flat Index
// uses. Encoding the residual (vector minus its list centroid) usually buys
// more recall per byte; it needs a codebook trained on residuals.
type IVFIndex struct {
	codebook  *Codebook
	dim       int
	nlist     int
	centroids []float32 // nlist x dim

	mu     sync.RWMutex
	lists  []map[uint64][]byte
	listOf map[uint64]int
}

// TrainIVF fits nlist coarse centroids to sample with k-means and returns
// an empty IVFIndex that encodes vectors with codebook.
func TrainIVF(codebook *Codebook, nlist int, sample [][]float32, seed int64) (*IVFIndex, error) {
	if nlist < 1 || nlist > len(sample) {
		return nil, ErrBadNList
	}
	for _, v := range sample {
		if len(v) != codebook.Dim() {
			return nil, ErrDimMismatch
		}
	}
	lists := make([]map[uint64][]byte, nlist)
	for i := range lists {
		lists[i] = make(map[uint64][]byte)
	}
	return &IVFIndex{
		codebook:  codebook,
		dim:       codebook.Dim(),
		nlist:     nlist,
		centroids: kmeans(sample, nlist, 25, rand.New(rand.NewSource(seed))),
		lists:     lists,
		listOf:    make(map[uint64]int),
	}, nil
}

// nearestLists returns the n list numbers whose centroids are closest to v.
func (idx *IVFIndex) nearestLists(v []float32, n int) []int {
	order := make([]int, idx.nlist)
	dists := make([]float32, idx.nlist)
	for c := range order {
		order[c] = c
		dists[c] = sqDist(v, centroidAt(idx.centroids, c, idx.dim))
	}
	sort.Slice(order, func(i, j int) bool { return dists[order[i]] < dists[order[j]] })
	if n > len(order) {
		n = len(order)
	}
	return order[:n]
}

// Insert encodes vector, files it under its nearest list, and replaces any
// existing entry for id.
func (idx *IVFIndex) Insert(id uint64, vector []float32) error {
	code, err := idx.codebook.Encode(vector)
	if err != nil {
		return err
	}
	list := idx.nearestLists(vector, 1)[0]

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if old, ok := idx.listOf[id]; ok {
		delete(idx.lists[old], id)
	}
	idx.lists[list][id] = code
	idx.listOf[id] = list
	return nil
}

// Delete removes id. Deleting an absent id is a no-op.
func (idx *IVFIndex) Delete(id uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if list, ok := idx.listOf[id]; ok {
		delete(idx.lists[list], id)
		delete(idx.listOf, id)
	}
}

// Len returns the number of stored codes.
func (idx *IVFIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.listOf)
}

// Search returns the topK codes with the smallest ADC distance to query,
// scanning only the nprobe lists nearest to it. nprobe >= nlist scans
// everything and matches the flat Index.
func (idx *IVFIndex) Search(query []float32, topK, nprobe int) ([]SearchResult, error) {
	table, err := idx.codebook.NewDistanceTable(query)
	if err != nil {
		return nil, err
	}
	probe := idx.nearestLists(query, nprobe)

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	h := &maxHeap{}
	for _, list := range probe {
		for id, code := range idx.lists[list] {
			d := table.Distance(code)
			if h.Len() < topK {
				heap.Push(h, scored{id: id, dist: d})
			} else if d < (*h)[0].dist {
				(*h)[0] = scored{id: id, dist: d}
				heap.Fix(h, 0)
			}
		}
	}

	out := make([]SearchResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		s := heap.Pop(h).(scored)
		out[i] = SearchResult{ID: s.id, Distance: s.dist}
	}
	return out, nil
}

// SearchRerank is Search followed by the same exact re-ranking stage as
// Index.SearchRerank.
func (idx *IVFIndex) SearchRerank(query []float32, topK, nprobe, candidates int, vectorOf func(id uint64) ([]float32, bool)) ([]SearchResult, error) {
	if candidates < topK {
		candidates = topK
	}
	approx, err := idx.Search(query, candidates, nprobe)
	if err != nil {
		return nil, err
	}
	return rerank(query, topK, approx, vectorOf), nil
}
