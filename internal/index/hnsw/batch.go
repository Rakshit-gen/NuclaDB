package hnsw

import (
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

// maxBatchChunk bounds how many vectors InsertBatch searches for in
// parallel before linking them. Chunk-mates can't find each other through
// the graph, so each one is compared to the earlier ones directly; a small
// chunk keeps that scan cheap.
const maxBatchChunk = 128

// InsertBatch inserts vectors[i] under ids[i], in order, using every core.
// It gives the same result as calling Insert for each pair in order, up to
// which neighbors get picked.
//
// It works a chunk at a time. First, workers search the graph for every
// new vector's neighbor candidates at once; that is read-only, and nothing
// else can change the graph while this writer holds wmu. Then the chunk is
// linked in order, one vector at a time, each picking its best M from its
// search results plus the chunk-mates linked before it, which the search
// couldn't have seen. Searches only wait on the link step, as with Insert.
//
// Chunks that reinsert an existing id, or repeat an id, fall back to one
// Insert at a time.
func (g *Graph) InsertBatch(ids []uint64, vectors [][]float32) error {
	if len(ids) != len(vectors) {
		return ErrDimensionMismatch
	}
	for _, v := range vectors {
		if len(v) != g.cfg.Dim {
			return ErrDimensionMismatch
		}
	}

	g.wmu.Lock()
	defer g.wmu.Unlock()

	workers := runtime.GOMAXPROCS(0)
	for i := 0; i < len(ids); {
		// A chunk much larger than the graph would mostly be linked by the
		// chunk-mate scan instead of the graph search, so grow chunks with
		// the graph.
		size := min(maxBatchChunk, len(g.nodes)/8, len(ids)-i)
		if workers == 1 || size < 2*workers || !g.freshIDs(ids[i:i+size]) {
			g.insertLocked(ids[i], copyVec(vectors[i]))
			i++
			continue
		}
		g.insertChunk(ids[i:i+size], vectors[i:i+size], workers)
		i += size
	}
	return nil
}

func copyVec(v []float32) []float32 { return append([]float32(nil), v...) }

// freshIDs reports whether no id in ids is already in the graph or appears
// twice.
func (g *Graph) freshIDs(ids []uint64) bool {
	seen := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := g.slots[id]; ok {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

// insertChunk inserts ids that are all new to the graph. Caller holds wmu.
func (g *Graph) insertChunk(ids []uint64, vectors [][]float32, workers int) {
	base := uint32(len(g.nodes))
	nodes := make([]*node, len(ids))
	for k, id := range ids {
		level := g.randomLevel() // in order, so a seeded graph stays deterministic
		nodes[k] = &node{id: id, vector: copyVec(vectors[k]), level: level, neighbors: make([][]uint32, level+1)}
	}

	// Parallel stage. For node k at layer l, candidates[k][l] is its beam
	// search result already thinned by the diversity heuristic (for layers
	// the current graph has), and mates[k][l] its distances to the earlier
	// chunk-mates that reach layer l. Neither depends on the link stage, so
	// doing them here keeps the sequential part short.
	candidates := make([][][]candidate, len(ids))
	mates := make([][][]candidate, len(ids))
	entry0, maxLevel0 := g.entryPoint, g.maxLevel
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < min(workers, len(ids)); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := int(next.Add(1) - 1)
				if k >= len(ids) {
					return
				}
				nd := nodes[k]
				entry := entry0
				dist := g.cfg.Metric.Distance(nd.vector, g.nodes[entry].vector)
				for l := maxLevel0; l > nd.level; l-- {
					entry, dist = g.greedyClosest(entry, dist, nd.vector, l)
				}
				top := min(nd.level, maxLevel0)
				perLayer := make([][]candidate, top+1)
				for l := top; l >= 0; l-- {
					found := g.searchLayer(nd.vector, entry, g.cfg.EfConstruction, l)
					if len(found) > 0 {
						entry = found[0].id
					}
					perLayer[l] = g.diverse(found, g.cfg.M)
				}
				candidates[k] = perLayer

				m := make([][]candidate, nd.level+1)
				for j := 0; j < k; j++ {
					d := g.cfg.Metric.Distance(nd.vector, nodes[j].vector)
					for l := 0; l <= min(nd.level, nodes[j].level); l++ {
						m[l] = append(m[l], candidate{id: base + uint32(j), dist: d})
					}
				}
				mates[k] = m
			}
		}()
	}
	wg.Wait()

	// Link stage, in order. The final pick reruns the heuristic over the
	// thinned search result plus the chunk-mates, now all placed.
	for k, nd := range nodes {
		slot := base + uint32(k)
		for l := 0; l <= nd.level; l++ {
			var found []candidate
			if l < len(candidates[k]) {
				found = candidates[k][l]
			}
			nd.neighbors[l] = g.selectNeighbors(mergeClosest(found, mates[k][l], len(found)+len(mates[k][l])), g.cfg.M)
		}

		g.mu.Lock()
		g.place(slot, nd, false)
		for l := range nd.neighbors {
			mMax := g.cfg.M
			if l == 0 {
				mMax = g.cfg.M * 2
			}
			for _, nbr := range nd.neighbors[l] {
				g.connect(nbr, slot, nd.vector, l, mMax)
			}
		}
		g.live++
		switch {
		case nd.level > g.maxLevel:
			g.maxLevel = nd.level
			g.entryPoint = slot
		case len(nd.neighbors[0]) == 0:
			g.entryPoint = slot // same reason as in insertLocked
		}
		g.mu.Unlock()
	}
}

// mergeClosest returns the m closest of sorted (already closest-first) and
// extra (any order), closest first.
func mergeClosest(sorted, extra []candidate, m int) []candidate {
	sort.Slice(extra, func(i, j int) bool { return extra[i].dist < extra[j].dist })
	out := make([]candidate, 0, m)
	i, j := 0, 0
	for len(out) < m && (i < len(sorted) || j < len(extra)) {
		if j >= len(extra) || (i < len(sorted) && sorted[i].dist <= extra[j].dist) {
			out = append(out, sorted[i])
			i++
		} else {
			out = append(out, extra[j])
			j++
		}
	}
	return out
}
