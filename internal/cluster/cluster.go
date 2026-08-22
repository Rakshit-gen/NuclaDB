// Package cluster ties the two Phase 2 building blocks together: the Raft
// control plane (internal/cluster/raft), which is the source of truth for
// which nodes exist, and the consistent-hash ring (internal/cluster/ring),
// which turns that node set into a deterministic shard assignment. Every
// node in the cluster runs its own Cluster instance; because it's rebuilt
// from the same replicated Raft state on every node, every node computes
// the same shard assignment independently, without a separate
// gossip/broadcast step for the assignment itself.
package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/cluster/raft"
	"github.com/Rakshit-gen/nucladb/internal/cluster/ring"
)

// Cluster maintains a consistent-hash ring kept in sync with a Raft node's
// replicated topology state.
type Cluster struct {
	node         *raft.Node
	numShards    int
	virtualNodes int

	mu    sync.RWMutex
	ring  *ring.Ring
	addrs map[string]string // node id -> API address, snapshotted alongside ring
}

// New creates a Cluster over node, with a ring of numShards fixed shards
// and virtualNodes hash-circle points per real node (see ring.New for what
// that trades off). It performs one synchronous Refresh so the ring
// reflects whatever topology the node already knows about before
// returning.
func New(node *raft.Node, numShards, virtualNodes int) *Cluster {
	c := &Cluster{
		node:         node,
		numShards:    numShards,
		virtualNodes: virtualNodes,
	}
	c.Refresh()
	return c
}

// Refresh rebuilds the ring from the Raft node's current FSM state. Rings
// don't support incremental node removal cheaply enough to matter here
// (this is a handful of AddNode calls over at most dozens of nodes), so a
// fresh ring is simplest and correct by construction rather than requiring
// the ring and the FSM's node set to be diffed and kept in lockstep by
// hand.
func (c *Cluster) Refresh() {
	state := c.node.State()
	r := ring.New(c.numShards, c.virtualNodes)
	addrs := make(map[string]string, len(state.Nodes))
	for id, addr := range state.Nodes {
		r.AddNode(id)
		addrs[id] = addr
	}

	c.mu.Lock()
	c.ring = r
	c.addrs = addrs
	c.mu.Unlock()
}

// Watch runs Refresh every time the underlying Raft node reports a
// topology change (see raft.Node.Subscribe), until ctx is cancelled. Run
// it in a background goroutine on every node so each one's local ring
// stays current with the replicated node set without polling.
func (c *Cluster) Watch(ctx context.Context) {
	changes := c.node.Subscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case <-changes:
			c.Refresh()
		}
	}
}

// ShardOwner returns the node id and API address currently responsible
// for shardID, per the local ring snapshot. Two nodes' Cluster instances
// can disagree briefly while a topology change is still propagating
// through Raft — by design, since Raft only guarantees the *order* logs
// apply in, not that every node has applied the latest one at any given
// instant. A caller on the wrong node needs to handle a redirect/retry,
// the same as any consistent-hashing client.
func (c *Cluster) ShardOwner(shardID int) (nodeID, addr string, err error) {
	c.mu.RLock()
	r, addrs := c.ring, c.addrs
	c.mu.RUnlock()

	id, err := r.OwnerOf(shardID)
	if err != nil {
		return "", "", err
	}
	addr, ok := addrs[id]
	if !ok {
		return "", "", fmt.Errorf("cluster: shard %d owner %q has no known API address", shardID, id)
	}
	return id, addr, nil
}

// Assignment returns every shard's current owner node id (not address) —
// e.g. for a status endpoint or a test to inspect distribution with.
func (c *Cluster) Assignment() (map[int]string, error) {
	c.mu.RLock()
	r := c.ring
	c.mu.RUnlock()
	return r.Assignment()
}

// NumShards returns the fixed shard count this cluster was created with.
func (c *Cluster) NumShards() int { return c.numShards }

// Join adds a new node to the running cluster: first as a Raft voter (so
// it participates in consensus and log replication), then as tracked
// metadata (its API address, for query routing) via the FSM. Both are
// needed and in this order — a node that's in the FSM's node set but not
// yet a Raft voter would be assigned shards nothing will ever tell it
// about, since it isn't actually receiving the replicated log yet. Only
// the leader can call this; propose calls below return raft's own
// not-leader error otherwise.
func (c *Cluster) Join(nodeID, raftAddr, apiAddr string, timeout time.Duration) error {
	if err := c.node.AddVoter(nodeID, raftAddr, timeout); err != nil {
		return fmt.Errorf("cluster: add raft voter: %w", err)
	}
	if err := c.node.AddNode(nodeID, apiAddr); err != nil {
		return fmt.Errorf("cluster: propose node metadata: %w", err)
	}
	c.Refresh()
	return nil
}

// Leave removes nodeID from Raft voting membership and from tracked node
// metadata. Only the leader can call this.
func (c *Cluster) Leave(nodeID string, timeout time.Duration) error {
	if err := c.node.RemoveServer(nodeID, timeout); err != nil {
		return fmt.Errorf("cluster: remove raft server: %w", err)
	}
	if err := c.node.RemoveNode(nodeID); err != nil {
		return fmt.Errorf("cluster: propose node removal: %w", err)
	}
	c.Refresh()
	return nil
}
