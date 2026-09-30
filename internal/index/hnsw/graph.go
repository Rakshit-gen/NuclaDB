// Package hnsw implements a Hierarchical Navigable Small World graph for
// approximate nearest-neighbor search over dense vectors, following Malkov &
// Yashunin, "Efficient and robust approximate nearest neighbor search using
// Hierarchical Navigable Small World graphs" (2016/2018).
package hnsw

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"sync"
)

// ErrDimensionMismatch is returned when a vector's length does not match
// the graph's configured dimensionality.
var ErrDimensionMismatch = errors.New("hnsw: vector dimension mismatch")

// ErrNotFound is returned when an operation references an id that does not
// exist in the graph.
var ErrNotFound = errors.New("hnsw: id not found")

// Config controls the graph's build/search quality-vs-cost tradeoffs.
type Config struct {
	// Dim is the fixed dimensionality of every vector stored in the graph.
	Dim int
	// M is the number of bidirectional links created per node at layers
	// above 0. Higher M improves recall at the cost of memory and build
	// time. 16 is a reasonable default per the original paper.
	M int
	// EfConstruction controls the candidate list size used while building
	// the graph. Higher values improve recall at the cost of insert speed.
	EfConstruction int
	// Metric is the distance function used to rank neighbors.
	Metric Metric
	// Seed makes level assignment deterministic for tests; 0 uses a
	// time-seeded source.
	Seed int64
}

func (c Config) withDefaults() Config {
	if c.M <= 0 {
		c.M = 16
	}
	if c.EfConstruction <= 0 {
		c.EfConstruction = 200
	}
	if c.Metric == nil {
		c.Metric = Cosine()
	}
	return c
}

type node struct {
	id        uint64
	vector    []float32
	level     int
	neighbors [][]uint32 // neighbors[l] = neighbor slots at layer l
	deleted   bool
}

// Graph is a concurrency-safe HNSW index.
//
// Writers (Insert, Delete, Restore) run one at a time under wmu. Since only
// writers change the graph, a writer holding wmu can read it with no other
// lock, so Insert does its neighbor search (almost all of its cost) without
// touching mu, and takes mu's write lock only to link the new node in.
// Searches hold mu's read lock, so they wait for that short linking step
// but not for the search that precedes it.
type Graph struct {
	cfg Config

	wmu sync.Mutex // serializes writers; also guards rng
	mu  sync.RWMutex
	// nodes is indexed by slot, a dense internal number each id keeps for
	// the graph's lifetime (a reinsert reuses it). Edges store slots, so
	// traversal indexes a slice instead of hashing into a map, and the
	// visited set can be a flat array (see visitedSet).
	nodes      []*node
	slots      map[uint64]uint32 // id -> slot
	entryPoint uint32
	hasEntry   bool
	maxLevel   int
	live       int // non-deleted nodes, kept current so Len is O(1)
	levelMult  float64
	rng        *rand.Rand

	// unit is set for cosine: vectors and queries are scaled to length 1
	// on the way in, so dist can be a plain dot product instead of
	// recomputing both norms on every comparison.
	unit bool
	dist func(a, b []float32) float32
}

// New creates an empty graph with the given configuration.
func New(cfg Config) *Graph {
	cfg = cfg.withDefaults()
	src := rand.NewSource(cfg.Seed)
	if cfg.Seed == 0 {
		src = rand.NewSource(rand.Int63())
	}
	g := &Graph{
		cfg:       cfg,
		slots:     make(map[uint64]uint32),
		levelMult: 1 / math.Log(float64(cfg.M)),
		rng:       rand.New(src),
		dist:      cfg.Metric.Distance,
	}
	if cfg.Metric.Name() == "cosine" {
		g.unit, g.dist = true, unitCosine
	}
	return g
}

// prep returns the graph's own copy of v, scaled to unit length for cosine.
func (g *Graph) prep(v []float32) []float32 {
	v = append([]float32(nil), v...)
	if g.unit {
		normalize(v)
	}
	return v
}

// Config returns the graph's configuration with defaults filled in.
func (g *Graph) Config() Config { return g.cfg }

// Len returns the number of live (non-deleted) vectors in the graph.
func (g *Graph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.live
}

// Tombstones returns how many deleted nodes are still in the graph. Deletes
// only mark a node, so it keeps its memory and still gets walked through
// by searches until Compact drops it.
func (g *Graph) Tombstones() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.nodes) - g.live
}

// Compact returns a new graph holding only g's live vectors. g itself is
// left as it was. The caller must stop writes to g until it has swapped
// in the result, or they won't be in it.
func (g *Graph) Compact() (*Graph, error) {
	g.wmu.Lock()
	var ids []uint64
	var vecs [][]float32
	for _, nd := range g.nodes {
		if !nd.deleted {
			ids = append(ids, nd.id)
			vecs = append(vecs, nd.vector)
		}
	}
	g.wmu.Unlock()

	ng := New(g.cfg)
	return ng, ng.InsertBatch(ids, vecs)
}

// Get returns a copy of id's stored vector. Cosine graphs store vectors at
// unit length, so that is what comes back for them.
func (g *Graph) Get(id uint64) ([]float32, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	slot, ok := g.slots[id]
	if !ok || g.nodes[slot].deleted {
		return nil, false
	}
	return append([]float32(nil), g.nodes[slot].vector...), true
}

// IDs returns every live id, in no particular order.
func (g *Graph) IDs() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]uint64, 0, g.live)
	for _, nd := range g.nodes {
		if !nd.deleted {
			out = append(out, nd.id)
		}
	}
	return out
}

// CountNew returns how many distinct ids in ids are not live in the graph,
// i.e. how much Len would grow if they were all inserted now.
func (g *Graph) CountNew(ids []uint64) int {
	seen := make(map[uint64]struct{}, len(ids))
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, id := range ids {
		if slot, ok := g.slots[id]; ok && !g.nodes[slot].deleted {
			continue
		}
		seen[id] = struct{}{}
	}
	return len(seen)
}

func (g *Graph) randomLevel() int {
	// Standard HNSW level assignment: P(level >= l) decays exponentially so
	// higher layers stay sparse, giving log-time greedy descent.
	level := int(math.Floor(-math.Log(g.rng.Float64()) * g.levelMult))
	return level
}

// Insert adds or replaces the vector stored under id. The vector's length
// must equal cfg.Dim.
func (g *Graph) Insert(id uint64, vector []float32) error {
	if len(vector) != g.cfg.Dim {
		return ErrDimensionMismatch
	}
	vec := g.prep(vector)

	g.wmu.Lock()
	defer g.wmu.Unlock()
	g.insertLocked(id, vec)
	return nil
}

// insertLocked is Insert after validation. vec is owned by the graph from
// here on. Caller holds wmu.
func (g *Graph) insertLocked(id uint64, vec []float32) {
	level := g.randomLevel()
	nd := &node{
		id:        id,
		vector:    vec,
		level:     level,
		neighbors: make([][]uint32, level+1),
	}

	// A re-insert of an id already in the graph is deferred into g.nodes
	// until after the traversal below: that traversal reads the graph
	// (starting from, and possibly passing through, this very id) to find
	// nd's neighbors, and it must see the old, fully-linked node rather
	// than nd's still-empty one. Swapping nd in early — even just to mark
	// the old node deleted — would make id resolve to an edge-less stub
	// mid-traversal, collapsing the graph to a self-loop wherever id was
	// entryPoint or an intermediate hop (caught by TestReinsertPreservesConnectivity).
	slot, existed := g.slots[id]
	if !existed {
		slot = uint32(len(g.nodes))
	}

	if !g.hasEntry {
		g.mu.Lock()
		g.live++
		g.place(slot, nd, existed)
		g.entryPoint = slot
		g.hasEntry = true
		g.maxLevel = level
		g.mu.Unlock()
		return
	}

	entry := g.entryPoint
	curDist := g.dist(vec, g.nodes[entry].vector)

	// Phase 1: greedy descent from the top layer down to level+1, keeping
	// only the single closest node found at each layer as the entry point
	// for the next layer down.
	for l := g.maxLevel; l > level; l-- {
		entry, curDist = g.greedyClosest(entry, curDist, vec, l)
	}

	// Phase 2: at each layer from min(level, maxLevel) down to 0, run a
	// beam search of width efConstruction and pick the new node's best M
	// neighbors. The edges back to it are added in phase 3; searching layer
	// l-1 never reads layer l's lists, so deferring them changes nothing.
	for l := min(level, g.maxLevel); l >= 0; l-- {
		candidates := g.searchLayer(vec, entry, g.cfg.EfConstruction, l)

		// On a reinsert, the old node under this same id is still in g.nodes
		// (see the comment above) and gets discovered by its own traversal —
		// at distance 0, since the vector is unchanged, making it look like
		// the best possible neighbor. Left in, it gets selected as nd's own
		// neighbor: a self-loop that, with a small M, can crowd out every
		// real edge and leave the node effectively disconnected.
		if existed {
			filtered := candidates[:0]
			for _, c := range candidates {
				if c.id != slot {
					filtered = append(filtered, c)
				}
			}
			candidates = filtered
		}

		nd.neighbors[l] = g.selectNeighbors(candidates, g.cfg.M)
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}

	// Phase 3: link the node in. This is the only part searches wait for.
	g.mu.Lock()
	defer g.mu.Unlock()

	if !existed || g.nodes[slot].deleted {
		g.live++
	}
	g.place(slot, nd, existed)

	for l := range nd.neighbors {
		mMax := g.cfg.M
		if l == 0 {
			mMax = g.cfg.M * 2
		}
		for _, nbr := range nd.neighbors[l] {
			g.connect(nbr, slot, vec, l, mMax)
		}
	}

	switch {
	case level > g.maxLevel:
		g.maxLevel = level
		g.entryPoint = slot
	case len(nd.neighbors[0]) == 0:
		// No live node was found to connect to at the base layer — every
		// node currently reachable from the entry point is tombstoned
		// (searchLayer's results, which candidates comes from, excludes
		// deleted nodes — see Delete's doc comment). Without this, the
		// entry point would stay pinned to dead nodes with no live path to
		// nd, leaving it permanently unreachable from Search despite being
		// live.
		g.entryPoint = slot
	}
}

// place stores nd at slot: a new slot is appended, a reinsert replaces the
// old node so every edge into the slot now reaches nd. Caller holds mu.
func (g *Graph) place(slot uint32, nd *node, existed bool) {
	if existed {
		g.nodes[slot] = nd
		return
	}
	g.nodes = append(g.nodes, nd)
	g.slots[nd.id] = slot
}

// connect adds a bidirectional edge nbrID -> newID at layer l, pruning
// nbrID's neighbor list back down to mMax by keeping its closest links if
// the new edge pushed it over the limit. newVec is passed explicitly
// (rather than looked up via g.nodes[newID]) because during Insert the new
// node isn't registered in g.nodes yet — that swap is deferred until the
// caller's traversal finishes, so it can still see the old node under a
// reused id.
func (g *Graph) connect(nbrID, newID uint32, newVec []float32, l, mMax int) {
	nbr := g.nodes[nbrID]
	if len(nbr.neighbors) <= l {
		grown := make([][]uint32, l+1)
		copy(grown, nbr.neighbors)
		nbr.neighbors = grown
	}
	// A repeated reinsert of newID (unchanged vector) re-selects nbrID as a
	// neighbor on every pass; without this check each pass would append
	// another copy of the same edge, and since every copy sits at the same
	// (shortest) distance, the pruning heap below keeps them all — eventually
	// crowding out nbrID's real, more distant neighbors entirely.
	for _, existing := range nbr.neighbors[l] {
		if existing == newID {
			return
		}
	}
	nbr.neighbors[l] = append(nbr.neighbors[l], newID)
	if len(nbr.neighbors[l]) <= mMax {
		return
	}

	// Over budget: re-pick nbr's links with the same heuristic Insert uses.
	cands := make([]candidate, len(nbr.neighbors[l]))
	for i, id := range nbr.neighbors[l] {
		v := newVec
		if id != newID {
			v = g.nodes[id].vector
		}
		cands[i] = candidate{id: id, dist: g.dist(nbr.vector, v)}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].dist < cands[j].dist })
	nbr.neighbors[l] = g.selectNeighbors(cands, mMax)
}

// greedyClosest walks from entry towards the single closest node to query
// at layer l, used for the coarse descent through the upper, sparse layers.
func (g *Graph) greedyClosest(entry uint32, entryDist float32, query []float32, l int) (uint32, float32) {
	best, bestDist := entry, entryDist
	improved := true
	for improved {
		improved = false
		nd := g.nodes[best]
		if l >= len(nd.neighbors) {
			break
		}
		for _, nbrID := range nd.neighbors[l] {
			d := g.dist(query, g.nodes[nbrID].vector)
			if d < bestDist {
				bestDist = d
				best = nbrID
				improved = true
			}
		}
	}
	return best, bestDist
}

// visitedSet marks slots seen during one traversal. A slot is visited when
// marks[slot] == gen, so starting a new traversal is gen++ rather than
// clearing anything; this is hnswlib's VisitedList.
type visitedSet struct {
	marks []uint32
	gen   uint32
}

func (v *visitedSet) reset(n int) {
	if len(v.marks) < n {
		v.marks = make([]uint32, n+n/4)
		v.gen = 0
	}
	v.gen++
	if v.gen == 0 { // wrapped: old marks could collide with the new gen
		clear(v.marks)
		v.gen = 1
	}
}

// visit marks slot and reports whether it was already marked.
func (v *visitedSet) visit(slot uint32) bool {
	if v.marks[slot] == v.gen {
		return true
	}
	v.marks[slot] = v.gen
	return false
}

var visitedPool = sync.Pool{New: func() any { return new(visitedSet) }}

// searchLayer runs a best-first beam search of width ef starting from
// entry, exploring layer l, and returns up to ef candidates sorted closest
// first. This is the workhorse used by both Insert (efConstruction) and
// Search (efSearch).
func (g *Graph) searchLayer(query []float32, entry uint32, ef, l int) []candidate {
	visited := visitedPool.Get().(*visitedSet)
	defer visitedPool.Put(visited)
	visited.reset(len(g.nodes))
	visited.visit(entry)
	entryDist := g.dist(query, g.nodes[entry].vector)

	frontier := newMinHeap()
	frontier.push(candidate{id: entry, dist: entryDist})

	results := newMaxHeap()
	if !g.nodes[entry].deleted {
		results.push(candidate{id: entry, dist: entryDist})
	}

	for frontier.Len() > 0 {
		c := frontier.pop()
		if results.Len() >= ef && c.dist > (*results)[0].dist {
			break
		}

		nd := g.nodes[c.id]
		if l >= len(nd.neighbors) {
			continue
		}
		for _, nbrID := range nd.neighbors[l] {
			if visited.visit(nbrID) {
				continue
			}
			nbr := g.nodes[nbrID]
			d := g.dist(query, nbr.vector)

			if results.Len() < ef || d < (*results)[0].dist {
				frontier.push(candidate{id: nbrID, dist: d})
				if !nbr.deleted {
					results.push(candidate{id: nbrID, dist: d})
					if results.Len() > ef {
						results.pop()
					}
				}
			}
		}
	}

	out := make([]candidate, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = results.pop()
	}
	return out
}

// selectNeighbors picks up to m links from candidates, which must be
// sorted closest-first and all already placed in g.nodes. It is the
// heuristic from the HNSW paper (Algorithm 4), as in hnswlib: a candidate
// is kept only if it is closer to the base node than to every link already
// kept. Plain "closest m" tends to spend every link on one tight cluster;
// the heuristic spreads links out, so a greedy search can leave a cluster
// through them, which is what lifts recall at low ef.
func (g *Graph) selectNeighbors(candidates []candidate, m int) []uint32 {
	kept := g.diverse(candidates, m)
	out := make([]uint32, len(kept))
	for i, c := range kept {
		out[i] = c.id
	}
	return out
}

// diverse is selectNeighbors keeping each pick's distance.
func (g *Graph) diverse(candidates []candidate, m int) []candidate {
	out := make([]candidate, 0, m)
	for _, c := range candidates {
		if len(out) >= m {
			break
		}
		v := g.nodes[c.id].vector
		keep := true
		for _, kept := range out {
			if g.dist(v, g.nodes[kept.id].vector) < c.dist {
				keep = false
				break
			}
		}
		if keep {
			out = append(out, c)
		}
	}
	return out
}

// NodeState is the exported, serializable state of one graph node, used by
// the snapshot package to persist and restore the graph without paying the
// cost of re-running graph construction (level assignment + neighbor
// selection) for every node on every restart.
type NodeState struct {
	ID        uint64
	Vector    []float32
	Level     int
	Neighbors [][]uint64
	Deleted   bool
}

// Snapshot returns the exported state of every node currently in the
// graph, in an unspecified order, along with the entry point and max
// level needed to resume search without rebuilding.
func (g *Graph) Snapshot() (nodes []NodeState, entryPoint uint64, maxLevel int, hasEntry bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes = make([]NodeState, 0, len(g.nodes))
	for _, nd := range g.nodes {
		neighbors := make([][]uint64, len(nd.neighbors))
		for l, ns := range nd.neighbors {
			ids := make([]uint64, len(ns))
			for i, slot := range ns {
				ids[i] = g.nodes[slot].id
			}
			neighbors[l] = ids
		}
		nodes = append(nodes, NodeState{
			ID:        nd.id,
			Vector:    append([]float32(nil), nd.vector...),
			Level:     nd.level,
			Neighbors: neighbors,
			Deleted:   nd.deleted,
		})
	}
	if g.hasEntry {
		entryPoint = g.nodes[g.entryPoint].id
	}
	return nodes, entryPoint, g.maxLevel, g.hasEntry
}

// Restore rebuilds the graph directly from previously exported NodeState
// records, bypassing Insert's graph-construction logic entirely. This is
// only valid for loading a snapshot taken from an equivalently-configured
// graph: it trusts the neighbor lists as-is rather than recomputing them.
// Restore must be called on a freshly created, empty Graph.
func (g *Graph) Restore(nodes []NodeState, entryPoint uint64, maxLevel int, hasEntry bool) {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, ns := range nodes {
		if g.unit {
			normalize(ns.Vector) // no-op for snapshots written since cosine vectors were stored unit length
		}
		g.slots[ns.ID] = uint32(len(g.nodes))
		g.nodes = append(g.nodes, &node{id: ns.ID, vector: ns.Vector, level: ns.Level, deleted: ns.Deleted})
		if !ns.Deleted {
			g.live++
		}
	}
	for i, ns := range nodes {
		neighbors := make([][]uint32, len(ns.Neighbors))
		for l, ids := range ns.Neighbors {
			neighbors[l] = make([]uint32, 0, len(ids))
			for _, id := range ids {
				if slot, ok := g.slots[id]; ok {
					neighbors[l] = append(neighbors[l], slot)
				}
			}
		}
		g.nodes[i].neighbors = neighbors
	}
	g.entryPoint = g.slots[entryPoint]
	g.maxLevel = maxLevel
	g.hasEntry = hasEntry
}

// Delete soft-deletes id: it is excluded from future Search results and
// from being selected as a neighbor of newly inserted nodes, but its graph
// edges are left intact so traversal through it still works. This mirrors
// hnswlib's mark_deleted and avoids the cost of a full graph repair.
func (g *Graph) Delete(id uint64) error {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	slot, ok := g.slots[id]
	if !ok || g.nodes[slot].deleted {
		return ErrNotFound
	}
	g.nodes[slot].deleted = true
	g.live--
	return nil
}

// SearchResult is one match returned by Search.
type SearchResult struct {
	ID       uint64
	Distance float32
}

// Search returns up to topK nearest neighbors of query. ef controls the
// candidate beam width at layer 0; it must be >= topK for useful recall
// and is typically swept between topK and a few hundred to trade latency
// for accuracy.
func (g *Graph) Search(query []float32, topK, ef int) ([]SearchResult, error) {
	if len(query) != g.cfg.Dim {
		return nil, ErrDimensionMismatch
	}
	if topK <= 0 {
		return nil, nil
	}
	if ef < topK {
		ef = topK
	}
	if g.unit {
		query = g.prep(query)
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.hasEntry {
		return nil, nil
	}

	entry := g.entryPoint
	curDist := g.dist(query, g.nodes[entry].vector)
	for l := g.maxLevel; l > 0; l-- {
		entry, curDist = g.greedyClosest(entry, curDist, query, l)
	}

	candidates := g.searchLayer(query, entry, ef, 0)
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	out := make([]SearchResult, len(candidates))
	for i, c := range candidates {
		out[i] = SearchResult{ID: g.nodes[c.id].id, Distance: c.dist}
	}
	return out, nil
}

// ExactSearch scores query against each id in ids by brute force and
// returns the topK closest live ones. It is for small candidate sets, such
// as the vectors matching a selective metadata filter, where scanning is
// both exact and cheaper than walking the graph. Unknown and deleted ids
// are skipped.
func (g *Graph) ExactSearch(query []float32, ids []uint64, topK int) ([]SearchResult, error) {
	if len(query) != g.cfg.Dim {
		return nil, ErrDimensionMismatch
	}
	if topK <= 0 {
		return nil, nil
	}
	if g.unit {
		query = g.prep(query)
	}
	g.mu.RLock()
	defer g.mu.RUnlock()

	h := newMaxHeap()
	for _, id := range ids {
		slot, ok := g.slots[id]
		if !ok || g.nodes[slot].deleted {
			continue
		}
		d := g.dist(query, g.nodes[slot].vector)
		if h.Len() < topK {
			h.push(candidate{id: slot, dist: d})
		} else if d < (*h)[0].dist {
			h.pop()
			h.push(candidate{id: slot, dist: d})
		}
	}
	out := make([]SearchResult, h.Len())
	for i := len(out) - 1; i >= 0; i-- {
		c := h.pop()
		out[i] = SearchResult{ID: g.nodes[c.id].id, Distance: c.dist}
	}
	return out, nil
}
