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
		if err := validateVector(v, g.cfg.Dim); err != nil {
			return err
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
			g.insertLocked(ids[i], g.prep(vectors[i]))
			i++
			continue
		}
		g.insertChunk(ids[i:i+size], vectors[i:i+size], workers)
		i += size
	}
	return nil
}

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
	vecs := make([][]float32, len(ids))
	for k, id := range ids {
		level := g.randomLevel() // in order, so a seeded graph stays deterministic
		nodes[k] = &node{id: id, level: level, neighbors: make([][]uint32, level+1)}
		vecs[k] = g.prep(vectors[k])
	}

	// Parallel stage. For node k at layer l, candidates[k][l] is its beam
	// search result already thinned by the diversity heuristic (for layers
	// the current graph has), and mates[k][l] its distances to the earlier
	// chunk-mates that reach layer l. Neither depends on the link stage, so
	// doing them here keeps that stage short.
	candidates := make([][][]candidate, len(ids))
	mates := make([][][]candidate, len(ids))
	entry0, maxLevel0 := g.entryPoint, g.maxLevel
	parallel(len(ids), workers, func(k int) {
		nd, vec := nodes[k], vecs[k]
		entry := entry0
		dist := g.dist(vec, g.vec(entry))
		for l := maxLevel0; l > nd.level; l-- {
			entry, dist = g.greedyClosest(entry, dist, vec, l)
		}
		top := min(nd.level, maxLevel0)
		perLayer := make([][]candidate, top+1)
		for l := top; l >= 0; l-- {
			found := g.searchLayer(vec, entry, g.cfg.EfConstruction, l)
			if len(found) > 0 {
				entry = found[0].id
			}
			perLayer[l] = g.diverse(found, g.maxLinks(l))
		}
		candidates[k] = perLayer

		m := make([][]candidate, nd.level+1)
		for j := 0; j < k; j++ {
			d := g.dist(vec, vecs[j])
			for l := 0; l <= min(nd.level, nodes[j].level); l++ {
				m[l] = append(m[l], candidate{id: base + uint32(j), dist: d})
			}
		}
		mates[k] = m
	})

	// Link stage. It holds mu, so searches wait for it, and runs in three
	// steps. Place every chunk node first, so their vectors can be read.
	// Then each node picks its own links, which reads only vectors. Then
	// the back-links: grouped by the node they land on, each group is
	// appended and, if over budget, re-pruned once, so no two workers touch
	// the same neighbor list. Doing this one node at a time made the link
	// stage serial, and it was most of the build's distance computations.
	g.mu.Lock()
	defer g.mu.Unlock()
	for k, nd := range nodes {
		g.place(base+uint32(k), nd, vecs[k], false)
	}
	parallel(len(nodes), workers, func(k int) {
		nd := nodes[k]
		for l := 0; l <= nd.level; l++ {
			var found []candidate
			if l < len(candidates[k]) {
				found = candidates[k][l]
			}
			nd.neighbors[l] = g.selectNeighbors(mergeClosest(found, mates[k][l], len(found)+len(mates[k][l])), g.maxLinks(l))
		}
	})

	type linkKey struct {
		to uint32
		l  int
	}
	type backLinks struct {
		linkKey
		from []uint32
	}
	var groups []backLinks
	index := make(map[linkKey]int)
	for k, nd := range nodes {
		slot := base + uint32(k)
		for l, nbrs := range nd.neighbors {
			for _, nbr := range nbrs {
				key := linkKey{nbr, l}
				i, ok := index[key]
				if !ok {
					i = len(groups)
					index[key] = i
					groups = append(groups, backLinks{linkKey: key})
				}
				groups[i].from = append(groups[i].from, slot)
			}
		}
	}
	parallel(len(groups), workers, func(i int) {
		gr := groups[i]
		g.addLinks(gr.to, gr.from, gr.l, g.maxLinks(gr.l))
	})

	for k, nd := range nodes {
		slot := base + uint32(k)
		g.live++
		switch {
		case nd.level > g.maxLevel:
			g.maxLevel = nd.level
			g.entryPoint = slot
		case len(nd.neighbors[0]) == 0:
			g.entryPoint = slot // same reason as in insertLocked
		}
	}
}

// parallel calls f(k) for every k in [0, n) on up to workers goroutines.
func parallel(n, workers int, f func(k int)) {
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < min(workers, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				k := int(next.Add(1) - 1)
				if k >= n {
					return
				}
				f(k)
			}
		}()
	}
	wg.Wait()
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
