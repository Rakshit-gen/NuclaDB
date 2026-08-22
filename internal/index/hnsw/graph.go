// Package hnsw implements a Hierarchical Navigable Small World graph for
// approximate nearest-neighbor search over dense vectors, following Malkov &
// Yashunin, "Efficient and robust approximate nearest neighbor search using
// Hierarchical Navigable Small World graphs" (2016/2018).
package hnsw

import (
	"container/heap"
	"errors"
	"math"
	"math/rand"
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
	neighbors [][]uint64 // neighbors[l] = neighbor ids at layer l
	deleted   bool
}

// Graph is a concurrency-safe HNSW index. Reads (Search) take a read lock;
// writes (Insert, Delete) take a write lock. This is a coarse-grained but
// correct concurrency model — see README for the finer-grained sharding
// approach used once the single-lock graph is proven correct under
// go test -race.
type Graph struct {
	cfg Config

	mu         sync.RWMutex
	nodes      map[uint64]*node
	entryPoint uint64
	hasEntry   bool
	maxLevel   int
	levelMult  float64
	rng        *rand.Rand
}

// New creates an empty graph with the given configuration.
func New(cfg Config) *Graph {
	cfg = cfg.withDefaults()
	src := rand.NewSource(cfg.Seed)
	if cfg.Seed == 0 {
		src = rand.NewSource(rand.Int63())
	}
	return &Graph{
		cfg:       cfg,
		nodes:     make(map[uint64]*node),
		levelMult: 1 / math.Log(float64(cfg.M)),
		rng:       rand.New(src),
	}
}

// Len returns the number of live (non-deleted) vectors in the graph.
func (g *Graph) Len() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n := 0
	for _, nd := range g.nodes {
		if !nd.deleted {
			n++
		}
	}
	return n
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
	vec := make([]float32, len(vector))
	copy(vec, vector)

	g.mu.Lock()
	defer g.mu.Unlock()

	if existing, ok := g.nodes[id]; ok {
		existing.deleted = true
	}

	level := g.randomLevel()
	nd := &node{
		id:        id,
		vector:    vec,
		level:     level,
		neighbors: make([][]uint64, level+1),
	}
	g.nodes[id] = nd

	if !g.hasEntry {
		g.entryPoint = id
		g.hasEntry = true
		g.maxLevel = level
		return nil
	}

	entry := g.entryPoint
	curDist := g.cfg.Metric.Distance(vec, g.nodes[entry].vector)

	// Phase 1: greedy descent from the top layer down to level+1, keeping
	// only the single closest node found at each layer as the entry point
	// for the next layer down.
	for l := g.maxLevel; l > level; l-- {
		entry, curDist = g.greedyClosest(entry, curDist, vec, l)
	}

	// Phase 2: at each layer from min(level, maxLevel) down to 0, run a
	// beam search of width efConstruction and connect the new node to its
	// best M neighbors, pruning any neighbor whose degree now exceeds the
	// layer's max.
	for l := min(level, g.maxLevel); l >= 0; l-- {
		candidates := g.searchLayer(vec, entry, g.cfg.EfConstruction, l)
		neighbors := g.selectNeighbors(candidates, g.cfg.M)
		nd.neighbors[l] = neighbors

		mMax := g.cfg.M
		if l == 0 {
			mMax = g.cfg.M * 2
		}
		for _, nbrID := range neighbors {
			g.connect(nbrID, id, l, mMax)
		}
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}

	if level > g.maxLevel {
		g.maxLevel = level
		g.entryPoint = id
	}
	return nil
}

// connect adds a bidirectional edge nbrID -> newID at layer l, pruning
// nbrID's neighbor list back down to mMax by keeping its closest links if
// the new edge pushed it over the limit.
func (g *Graph) connect(nbrID, newID uint64, l, mMax int) {
	nbr := g.nodes[nbrID]
	if len(nbr.neighbors) <= l {
		grown := make([][]uint64, l+1)
		copy(grown, nbr.neighbors)
		nbr.neighbors = grown
	}
	nbr.neighbors[l] = append(nbr.neighbors[l], newID)
	if len(nbr.neighbors[l]) <= mMax {
		return
	}

	// Over budget: keep the mMax closest neighbors by distance to nbr.
	h := newMaxHeap()
	for _, id := range nbr.neighbors[l] {
		heap.Push(h, candidate{id: id, dist: g.cfg.Metric.Distance(nbr.vector, g.nodes[id].vector)})
	}
	for h.Len() > mMax {
		heap.Pop(h)
	}
	pruned := make([]uint64, 0, mMax)
	for _, c := range *h {
		pruned = append(pruned, c.id)
	}
	nbr.neighbors[l] = pruned
}

// greedyClosest walks from entry towards the single closest node to query
// at layer l, used for the coarse descent through the upper, sparse layers.
func (g *Graph) greedyClosest(entry uint64, entryDist float32, query []float32, l int) (uint64, float32) {
	best, bestDist := entry, entryDist
	improved := true
	for improved {
		improved = false
		nd := g.nodes[best]
		if l >= len(nd.neighbors) {
			break
		}
		for _, nbrID := range nd.neighbors[l] {
			d := g.cfg.Metric.Distance(query, g.nodes[nbrID].vector)
			if d < bestDist {
				bestDist = d
				best = nbrID
				improved = true
			}
		}
	}
	return best, bestDist
}

// searchLayer runs a best-first beam search of width ef starting from
// entry, exploring layer l, and returns up to ef candidates sorted closest
// first. This is the workhorse used by both Insert (efConstruction) and
// Search (efSearch).
func (g *Graph) searchLayer(query []float32, entry uint64, ef, l int) []candidate {
	visited := map[uint64]bool{entry: true}
	entryDist := g.cfg.Metric.Distance(query, g.nodes[entry].vector)

	frontier := newMinHeap()
	heap.Push(frontier, candidate{id: entry, dist: entryDist})

	results := newMaxHeap()
	if !g.nodes[entry].deleted {
		heap.Push(results, candidate{id: entry, dist: entryDist})
	}

	for frontier.Len() > 0 {
		c := heap.Pop(frontier).(candidate)
		if results.Len() >= ef && c.dist > (*results)[0].dist {
			break
		}

		nd := g.nodes[c.id]
		if l >= len(nd.neighbors) {
			continue
		}
		for _, nbrID := range nd.neighbors[l] {
			if visited[nbrID] {
				continue
			}
			visited[nbrID] = true
			nbr := g.nodes[nbrID]
			d := g.cfg.Metric.Distance(query, nbr.vector)

			if results.Len() < ef || d < (*results)[0].dist {
				heap.Push(frontier, candidate{id: nbrID, dist: d})
				if !nbr.deleted {
					heap.Push(results, candidate{id: nbrID, dist: d})
					if results.Len() > ef {
						heap.Pop(results)
					}
				}
			}
		}
	}

	out := make([]candidate, results.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(results).(candidate)
	}
	return out
}

// selectNeighbors picks the m closest candidates. Candidates are assumed
// pre-sorted closest-first by searchLayer.
func (g *Graph) selectNeighbors(candidates []candidate, m int) []uint64 {
	if len(candidates) > m {
		candidates = candidates[:m]
	}
	out := make([]uint64, len(candidates))
	for i, c := range candidates {
		out[i] = c.id
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
			neighbors[l] = append([]uint64(nil), ns...)
		}
		nodes = append(nodes, NodeState{
			ID:        nd.id,
			Vector:    append([]float32(nil), nd.vector...),
			Level:     nd.level,
			Neighbors: neighbors,
			Deleted:   nd.deleted,
		})
	}
	return nodes, g.entryPoint, g.maxLevel, g.hasEntry
}

// Restore rebuilds the graph directly from previously exported NodeState
// records, bypassing Insert's graph-construction logic entirely. This is
// only valid for loading a snapshot taken from an equivalently-configured
// graph: it trusts the neighbor lists as-is rather than recomputing them.
// Restore must be called on a freshly created, empty Graph.
func (g *Graph) Restore(nodes []NodeState, entryPoint uint64, maxLevel int, hasEntry bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, ns := range nodes {
		g.nodes[ns.ID] = &node{
			id:        ns.ID,
			vector:    ns.Vector,
			level:     ns.Level,
			neighbors: ns.Neighbors,
			deleted:   ns.Deleted,
		}
	}
	g.entryPoint = entryPoint
	g.maxLevel = maxLevel
	g.hasEntry = hasEntry
}

// Delete soft-deletes id: it is excluded from future Search results and
// from being selected as a neighbor of newly inserted nodes, but its graph
// edges are left intact so traversal through it still works. This mirrors
// hnswlib's mark_deleted and avoids the cost of a full graph repair.
func (g *Graph) Delete(id uint64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	nd, ok := g.nodes[id]
	if !ok || nd.deleted {
		return ErrNotFound
	}
	nd.deleted = true
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
	if ef < topK {
		ef = topK
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	if !g.hasEntry {
		return nil, nil
	}

	entry := g.entryPoint
	curDist := g.cfg.Metric.Distance(query, g.nodes[entry].vector)
	for l := g.maxLevel; l > 0; l-- {
		entry, curDist = g.greedyClosest(entry, curDist, query, l)
	}

	candidates := g.searchLayer(query, entry, ef, 0)
	if len(candidates) > topK {
		candidates = candidates[:topK]
	}
	out := make([]SearchResult, len(candidates))
	for i, c := range candidates {
		out[i] = SearchResult{ID: c.id, Distance: c.dist}
	}
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
