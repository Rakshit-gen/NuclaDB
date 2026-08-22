// Package ring implements consistent hashing over a fixed number of
// shards, used to decide which cluster node owns each shard as nodes join
// and leave.
//
// A fixed shard count (chosen once, at cluster creation) rather than
// hashing vector ids directly onto nodes is the deliberate design: each
// shard is a stable unit that gets its own Raft-coordinated leader and
// replica set (see internal/cluster/raft), so "which node owns this
// data" and "which node is currently the leader for this data" are
// separate questions. Rehashing individual vector ids on every node
// change would make shard identity — and therefore replication — a
// moving target.
package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"sync"
)

// ErrNoNodes is returned by OwnerOf when the ring has no nodes to assign
// shards to.
var ErrNoNodes = errors.New("ring: no nodes available")

// Ring assigns each of a fixed set of shards to one of a changing set of
// nodes via consistent hashing: each node occupies VirtualNodes points on
// a hash circle, and a shard is owned by whichever node's point comes
// next going clockwise from the shard's own hash. Virtual nodes exist so
// that adding or removing one real node redistributes a proportional
// share of shards rather than an unpredictable chunk — with only one
// point per node, an unlucky hash placement could give one node most of
// the keyspace.
type Ring struct {
	mu           sync.RWMutex
	virtualNodes int
	numShards    int
	points       []point // sorted by hash
	nodeSet      map[string]bool
}

type point struct {
	hash   uint64
	nodeID string
}

// New creates a ring over numShards fixed shards (ids 0..numShards-1),
// with virtualNodes hash-circle points per real node. 100-200 virtual
// nodes is a reasonable default for even load distribution at small
// cluster sizes; it's exposed rather than hardcoded so it can be tuned
// and measured, not guessed.
func New(numShards, virtualNodes int) *Ring {
	return &Ring{
		virtualNodes: virtualNodes,
		numShards:    numShards,
		nodeSet:      make(map[string]bool),
	}
}

// hashKey was originally FNV-1a, which turned out to have a real,
// measured distribution problem for this package's specific key shape
// (short, sequentially-suffixed strings like "shard#0".."shard#1023"): a
// 16-bucket histogram over the full uint64 range came back
// [0 100 200 200 200 0 0 90 10 0 0 0 100 124 0 0] — entire buckets empty,
// which is exactly why TestLoadDistribution caught 2 of 8 nodes getting
// zero shards instead of a merely-uneven-but-present split. SHA-256
// (truncated to 8 bytes) doesn't show that structure on the same keys —
// verified with the same histogram before switching, not assumed.
func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

// AddNode adds nodeID's virtual points to the ring. Adding a node that's
// already present is a no-op.
func (r *Ring) AddNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodeSet[nodeID] {
		return
	}
	r.nodeSet[nodeID] = true
	for i := 0; i < r.virtualNodes; i++ {
		r.points = append(r.points, point{
			hash:   hashKey(nodeID + "#" + strconv.Itoa(i)),
			nodeID: nodeID,
		})
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i].hash < r.points[j].hash })
}

// RemoveNode removes nodeID's virtual points from the ring. Removing a
// node that isn't present is a no-op.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodeSet[nodeID] {
		return
	}
	delete(r.nodeSet, nodeID)
	filtered := r.points[:0]
	for _, p := range r.points {
		if p.nodeID != nodeID {
			filtered = append(filtered, p)
		}
	}
	r.points = filtered
}

// Nodes returns every node currently on the ring, in no particular order.
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodeSet))
	for id := range r.nodeSet {
		out = append(out, id)
	}
	return out
}

// NumShards returns the fixed shard count this ring was created with.
func (r *Ring) NumShards() int { return r.numShards }

// OwnerOf returns which node currently owns shardID (0..NumShards()-1).
func (r *Ring) OwnerOf(shardID int) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", ErrNoNodes
	}
	h := hashKey("shard#" + strconv.Itoa(shardID))
	i := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if i == len(r.points) {
		i = 0 // wrap around the circle
	}
	return r.points[i].nodeID, nil
}

// OwnersOf returns up to n distinct nodes responsible for shardID, walk
// order: the shard's primary owner first (the same node OwnerOf would
// return), then the next distinct nodes going clockwise around the
// circle. This is the natural leader+replica placement for a shard —
// finding replica candidates means walking exactly as far around the
// ring as a client already would to route around a dead primary. Returns
// fewer than n entries if the ring has fewer than n distinct nodes.
func (r *Ring) OwnersOf(shardID int, n int) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return nil, ErrNoNodes
	}
	if n > len(r.nodeSet) {
		n = len(r.nodeSet)
	}
	h := hashKey("shard#" + strconv.Itoa(shardID))
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	if start == len(r.points) {
		start = 0
	}
	seen := make(map[string]bool, n)
	owners := make([]string, 0, n)
	for i := 0; len(owners) < n && i < len(r.points); i++ {
		p := r.points[(start+i)%len(r.points)]
		if seen[p.nodeID] {
			continue
		}
		seen[p.nodeID] = true
		owners = append(owners, p.nodeID)
	}
	return owners, nil
}

// Assignment maps every shard to its current owning node — the full
// shard-to-node table, e.g. for a control plane to publish or a test to
// inspect distribution with.
func (r *Ring) Assignment() (map[int]string, error) {
	out := make(map[int]string, r.numShards)
	for shard := 0; shard < r.numShards; shard++ {
		owner, err := r.OwnerOf(shard)
		if err != nil {
			return nil, err
		}
		out[shard] = owner
	}
	return out, nil
}
