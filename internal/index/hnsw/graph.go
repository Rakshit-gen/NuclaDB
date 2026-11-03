// Package hnsw implements a Hierarchical Navigable Small World graph for
// approximate nearest-neighbor search over dense vectors, following Malkov &
// Yashunin, "Efficient and robust approximate nearest neighbor search using
// Hierarchical Navigable Small World graphs" (2016/2018).
package hnsw

import (
	"context"
	"errors"
	"math"
	"math/bits"
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"sync"
)

// ErrDimensionMismatch is returned when a vector's length does not match
// the graph's configured dimensionality.
var ErrDimensionMismatch = errors.New("hnsw: vector dimension mismatch")

// ErrNonFiniteVector is returned for NaN or infinite components, which do
// not define a stable distance ordering and would poison graph traversal.
var ErrNonFiniteVector = errors.New("hnsw: vector contains NaN or infinity")

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
	if c.M <= 1 {
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

func validateVector(v []float32, dim int) error {
	if len(v) != dim {
		return ErrDimensionMismatch
	}
	for _, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return ErrNonFiniteVector
		}
	}
	return nil
}

type node struct {
	id        uint64
	level     int
	neighbors [][]uint32 // neighbors[l] = neighbor slots at layer l
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
	nodes []*node
	// data holds every slot's vector back to back, in blocks of
	// 1<<blockShift vectors (about 8 MB each), and dead marks tombstoned
	// slots. Kept out of node so the search loop scores a neighbor with a
	// read into a flat array instead of following a pointer to its node and
	// another to its vector. Blocks, rather than one slice, because a full
	// block is never copied again: growing one 512 MB slice by append
	// copied it over and over under the write lock and left the old copies
	// for the GC, which took a 1M-vector server from 1.7 to 2.8 GB RSS.
	data       [][]float32
	blockShift uint
	dead       []bool
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

	visitedFree chan *visitedSet
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

		visitedFree: make(chan *visitedSet, 4*runtime.GOMAXPROCS(0)),
		// The largest power-of-two vector count that fits in 8 MB.
		blockShift: uint(bits.Len(uint(max(8<<20/(4*max(cfg.Dim, 1)), 1))) - 1),
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

// vec returns slot's stored vector. It aliases g.data, so callers must not
// keep it past a write that could grow the graph.
func (g *Graph) vec(slot uint32) []float32 {
	b := g.data[slot>>g.blockShift]
	i := int(slot&(1<<g.blockShift-1)) * g.cfg.Dim
	return b[i : i+g.cfg.Dim : i+g.cfg.Dim]
}

// appendVec stores the vector for the next new slot, len(g.nodes) before
// that node is appended. A block grows by append while it is small, so a
// graph of a few vectors stays small.
func (g *Graph) appendVec(vec []float32) {
	bi := len(g.nodes) >> g.blockShift
	if bi == len(g.data) {
		g.data = append(g.data, nil)
	}
	g.data[bi] = append(g.data[bi], vec...)
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
	for slot, nd := range g.nodes {
		if !g.dead[slot] {
			ids = append(ids, nd.id)
			vecs = append(vecs, g.vec(uint32(slot)))
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
	if !ok || g.dead[slot] {
		return nil, false
	}
	return append([]float32(nil), g.vec(slot)...), true
}

// IDs returns every live id, in no particular order.
func (g *Graph) IDs() []uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]uint64, 0, g.live)
	for slot, nd := range g.nodes {
		if !g.dead[slot] {
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
		if slot, ok := g.slots[id]; ok && !g.dead[slot] {
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
	return min(level, 64)
}

// Insert adds or replaces the vector stored under id. The vector's length
// must equal cfg.Dim.
func (g *Graph) Insert(id uint64, vector []float32) error {
	if err := validateVector(vector, g.cfg.Dim); err != nil {
		return err
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
		g.place(slot, nd, vec, existed)
		g.entryPoint = slot
		g.hasEntry = true
		g.maxLevel = level
		g.mu.Unlock()
		return
	}

	entry := g.entryPoint
	curDist := g.dist(vec, g.vec(entry))

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

		nd.neighbors[l] = g.selectNeighbors(candidates, g.maxLinks(l))
		if len(candidates) > 0 {
			entry = candidates[0].id
		}
	}

	// Phase 3: link the node in. This is the only part searches wait for.
	g.mu.Lock()
	defer g.mu.Unlock()

	if !existed || g.dead[slot] {
		g.live++
	}
	g.place(slot, nd, vec, existed)

	for l := range nd.neighbors {
		for _, nbr := range nd.neighbors[l] {
			g.addLinks(nbr, []uint32{slot}, l, g.maxLinks(l))
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

// maxLinks is the most links a node keeps at layer l: M above the base
// layer, 2M at it. A new node picks up to this many too, as Qdrant does.
// hnswlib picks only M at layer 0 and leaves the rest to back-links, which
// on SIFT-1M left the median node with 20 of its 32 slots used and cost
// about 0.1 recall@10 at ef=10.
func (g *Graph) maxLinks(l int) int {
	if l == 0 {
		return 2 * g.cfg.M
	}
	return g.cfg.M
}

// place stores nd and its vector at slot: a new slot is appended, a
// reinsert replaces the old node so every edge into the slot now reaches
// nd. Caller holds mu.
func (g *Graph) place(slot uint32, nd *node, vec []float32, existed bool) {
	if existed {
		g.nodes[slot] = nd
		copy(g.vec(slot), vec)
		g.dead[slot] = false
		return
	}
	g.appendVec(vec)
	g.nodes = append(g.nodes, nd)
	g.dead = append(g.dead, false)
	g.slots[nd.id] = slot
}

// addLinks adds edges nbrID -> from[i] at layer l, then prunes nbrID's
// neighbor list back down to mMax with the diversity heuristic if they
// pushed it over. Every node in from must already be placed.
func (g *Graph) addLinks(nbrID uint32, from []uint32, l, mMax int) {
	nbr := g.nodes[nbrID]
	if len(nbr.neighbors) <= l {
		grown := make([][]uint32, l+1)
		copy(grown, nbr.neighbors)
		nbr.neighbors = grown
	}
	for _, newID := range from {
		// A repeated reinsert of newID (unchanged vector) re-selects nbrID
		// as a neighbor on every pass; without this check each pass would
		// append another copy of the same edge, and since every copy sits
		// at the same (shortest) distance, the pruning below keeps them all,
		// eventually crowding out nbrID's real, more distant neighbors.
		if !slices.Contains(nbr.neighbors[l], newID) {
			nbr.neighbors[l] = append(nbr.neighbors[l], newID)
		}
	}
	if len(nbr.neighbors[l]) <= mMax {
		return
	}

	// Over budget: re-pick nbr's links with the same heuristic Insert uses.
	cands := make([]candidate, len(nbr.neighbors[l]))
	nbrVec := g.vec(nbrID)
	for i, id := range nbr.neighbors[l] {
		cands[i] = candidate{id: id, dist: g.dist(nbrVec, g.vec(id))}
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
			d := g.dist(query, g.vec(nbrID))
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
	fresh []uint32 // scratch for searchLayer's per-node neighbor batch
	// searchLayer's heaps, kept so their backing arrays are reused instead
	// of regrown from empty on every call.
	frontier minHeap
	results  maxHeap
	sink     float32 // keeps the prefetch reads from being optimized away
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

// getVisited and putVisited recycle visited sets through a per-graph free
// list. A sync.Pool is emptied by every GC, and refilling it meant
// allocating and zeroing a 4-byte-per-node array (5 MB at 1M vectors),
// which was over a fifth of search CPU.
func (g *Graph) getVisited() *visitedSet {
	select {
	case v := <-g.visitedFree:
		return v
	default:
		return new(visitedSet)
	}
}

func (g *Graph) putVisited(v *visitedSet) {
	select {
	case g.visitedFree <- v:
	default: // more concurrent searches than slots; let this one go
	}
}

// searchLayer runs a best-first beam search of width ef starting from
// entry, exploring layer l, and returns up to ef candidates sorted closest
// first. This is the workhorse used by both Insert (efConstruction) and
// Search (efSearch).
func (g *Graph) searchLayer(query []float32, entry uint32, ef, l int) []candidate {
	out, _ := g.searchLayerContext(context.Background(), query, entry, ef, l)
	return out
}

func (g *Graph) searchLayerContext(ctx context.Context, query []float32, entry uint32, ef, l int) ([]candidate, error) {
	visited := g.getVisited()
	defer g.putVisited(visited)
	visited.reset(len(g.nodes))
	visited.visit(entry)
	entryDist := g.dist(query, g.vec(entry))

	frontier := &visited.frontier
	*frontier = (*frontier)[:0]
	frontier.push(candidate{id: entry, dist: entryDist})

	results := &visited.results
	*results = (*results)[:0]
	if !g.dead[entry] {
		results.push(candidate{id: entry, dist: entryDist})
	}

	for frontier.Len() > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c := frontier.pop()
		if results.Len() >= ef && c.dist > (*results)[0].dist {
			break
		}

		nd := g.nodes[c.id]
		if l >= len(nd.neighbors) {
			continue
		}
		// Gather the unvisited neighbors and read one value from each
		// vector before scoring any of them. Those reads miss cache
		// independently, so the CPU fetches them in parallel instead of
		// stalling once per neighbor inside the distance loop; it is the
		// software prefetch hnswlib does, which Go has no instruction for.
		fresh := visited.fresh[:0]
		var touch float32
		for _, nbrID := range nd.neighbors[l] {
			if !visited.visit(nbrID) {
				fresh = append(fresh, nbrID)
				touch += g.vec(nbrID)[0]
			}
		}
		visited.fresh = fresh
		visited.sink = touch
		for _, nbrID := range fresh {
			d := g.dist(query, g.vec(nbrID))

			if results.Len() < ef || d < (*results)[0].dist {
				frontier.push(candidate{id: nbrID, dist: d})
				if !g.dead[nbrID] {
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
	return out, nil
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
		v := g.vec(c.id)
		keep := true
		for _, kept := range out {
			if g.dist(v, g.vec(kept.id)) < c.dist {
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
	for slot, nd := range g.nodes {
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
			Vector:    append([]float32(nil), g.vec(uint32(slot))...),
			Level:     nd.level,
			Neighbors: neighbors,
			Deleted:   g.dead[slot],
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
		g.appendVec(ns.Vector)
		g.nodes = append(g.nodes, &node{id: ns.ID, level: ns.Level})
		g.dead = append(g.dead, ns.Deleted)
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
	if !ok || g.dead[slot] {
		return ErrNotFound
	}
	g.dead[slot] = true
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
	return g.SearchContext(context.Background(), query, topK, ef)
}

// SearchContext is Search with cooperative cancellation during traversal.
func (g *Graph) SearchContext(ctx context.Context, query []float32, topK, ef int) ([]SearchResult, error) {
	if err := validateVector(query, g.cfg.Dim); err != nil {
		return nil, err
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
	curDist := g.dist(query, g.vec(entry))
	for l := g.maxLevel; l > 0; l-- {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, curDist = g.greedyClosest(entry, curDist, query, l)
	}

	candidates, err := g.searchLayerContext(ctx, query, entry, ef, 0)
	if err != nil {
		return nil, err
	}
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
	return g.ExactSearchContext(context.Background(), query, ids, topK)
}

// ExactSearchContext is ExactSearch with cooperative cancellation.
func (g *Graph) ExactSearchContext(ctx context.Context, query []float32, ids []uint64, topK int) ([]SearchResult, error) {
	if err := validateVector(query, g.cfg.Dim); err != nil {
		return nil, err
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
	for i, id := range ids {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		slot, ok := g.slots[id]
		if !ok || g.dead[slot] {
			continue
		}
		d := g.dist(query, g.vec(slot))
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
