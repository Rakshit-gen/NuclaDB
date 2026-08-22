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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/cluster/raft"
	"github.com/Rakshit-gen/nucladb/internal/cluster/ring"
)

// Cluster maintains a consistent-hash ring kept in sync with a Raft node's
// replicated topology state.
type Cluster struct {
	node              *raft.Node
	numShards         int
	virtualNodes      int
	replicationFactor int

	mu    sync.RWMutex
	ring  *ring.Ring
	addrs map[string]string // node id -> API address, snapshotted alongside ring
}

// New creates a Cluster over node, with a ring of numShards fixed shards
// and virtualNodes hash-circle points per real node (see ring.New for what
// that trades off). replicationFactor is how many nodes (leader + replicas)
// Rebalance assigns to each shard; it performs one synchronous Refresh so
// the ring reflects whatever topology the node already knows about before
// returning.
func New(node *raft.Node, numShards, virtualNodes, replicationFactor int) *Cluster {
	c := &Cluster{
		node:              node,
		numShards:         numShards,
		virtualNodes:      virtualNodes,
		replicationFactor: replicationFactor,
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
// for shardID: the Raft-replicated ShardLeader assignment if Rebalance has
// set one, falling back to the raw ring computation for a shard that
// hasn't been through a Rebalance yet (e.g. immediately after this
// cluster's very first Join, before that Join's own Rebalance call
// commits). Two nodes' Cluster instances can disagree briefly while a
// topology change is still propagating through Raft — by design, since
// Raft only guarantees the *order* logs apply in, not that every node has
// applied the latest one at any given instant. A caller on the wrong node
// needs to handle a redirect/retry, the same as any consistent-hashing
// client.
func (c *Cluster) ShardOwner(shardID int) (nodeID, addr string, err error) {
	c.mu.RLock()
	r, addrs := c.ring, c.addrs
	c.mu.RUnlock()

	id := c.node.State().ShardLeader[shardID]
	if id == "" {
		id, err = r.OwnerOf(shardID)
		if err != nil {
			return "", "", err
		}
	}
	addr, ok := addrs[id]
	if !ok {
		return "", "", fmt.Errorf("cluster: shard %d owner %q has no known API address", shardID, id)
	}
	return id, addr, nil
}

// Assignment returns every shard's current owner node id (not address),
// sourced the same way ShardOwner is — e.g. for a status endpoint or a
// test to inspect distribution with.
func (c *Cluster) Assignment() (map[int]string, error) {
	c.mu.RLock()
	r := c.ring
	c.mu.RUnlock()

	state := c.node.State()
	out := make(map[int]string, c.numShards)
	for shard := 0; shard < c.numShards; shard++ {
		if id := state.ShardLeader[shard]; id != "" {
			out[shard] = id
			continue
		}
		id, err := r.OwnerOf(shard)
		if err != nil {
			return nil, err
		}
		out[shard] = id
	}
	return out, nil
}

// NumShards returns the fixed shard count this cluster was created with.
func (c *Cluster) NumShards() int { return c.numShards }

// IsLeader reports whether this node currently believes it's the Raft
// leader — see raft.Node.IsLeader's own caveat that this is a snapshot,
// not a guarantee.
func (c *Cluster) IsLeader() bool { return c.node.IsLeader() }

// Nodes returns a snapshot of every known node id -> API address, e.g. for
// a health checker deciding who to probe.
func (c *Cluster) Nodes() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[string]string, len(c.addrs))
	for id, addr := range c.addrs {
		out[id] = addr
	}
	return out
}

// Rebalance recomputes every shard's desired leader and replica set from
// the current ring (ring.OwnersOf, replicationFactor nodes per shard) and
// proposes only the differences against the Raft-replicated FSM state —
// calling it against an already-balanced cluster proposes nothing at all.
// Only the Raft leader can propose (the underlying propose calls return
// raft's own not-leader error otherwise); called after every Join/Leave
// (both already leader-only operations) and by internal/cluster/health
// after a failure-triggered eviction.
func (c *Cluster) Rebalance() error {
	c.mu.RLock()
	r := c.ring
	c.mu.RUnlock()

	current := c.node.State()
	for shard := 0; shard < c.numShards; shard++ {
		desired, err := r.OwnersOf(shard, c.replicationFactor)
		if err != nil {
			if errors.Is(err, ring.ErrNoNodes) {
				continue // no nodes yet to assign to
			}
			return fmt.Errorf("cluster: rebalance shard %d: %w", shard, err)
		}
		if len(desired) == 0 {
			continue
		}

		if desiredLeader := desired[0]; current.ShardLeader[shard] != desiredLeader {
			if err := c.node.SetShardLeader(shard, desiredLeader); err != nil {
				return fmt.Errorf("cluster: set shard %d leader: %w", shard, err)
			}
		}

		desiredReplicas := desired[1:]
		desiredSet := make(map[string]bool, len(desiredReplicas))
		for _, id := range desiredReplicas {
			desiredSet[id] = true
		}
		for _, id := range current.ShardReplicas[shard] {
			if !desiredSet[id] {
				if err := c.node.RemoveShardReplica(shard, id); err != nil {
					return fmt.Errorf("cluster: remove shard %d replica %s: %w", shard, id, err)
				}
			}
		}
		currentSet := make(map[string]bool, len(current.ShardReplicas[shard]))
		for _, id := range current.ShardReplicas[shard] {
			currentSet[id] = true
		}
		for _, id := range desiredReplicas {
			if !currentSet[id] {
				if err := c.node.AddShardReplica(shard, id); err != nil {
					return fmt.Errorf("cluster: add shard %d replica %s: %w", shard, id, err)
				}
			}
		}
	}
	return nil
}

// FailoverShardLeader promotes a healthy replica to lead shardID in place
// of deadNodeID, without touching Raft voting membership — the cheap,
// fast-path response to a shard leader going unreachable, well short of
// evicting it from the cluster entirely. A no-op if shardID's leader isn't
// actually deadNodeID (a stale failover request racing a previous one, or
// a shard deadNodeID never led). Returns an error if no healthy replica is
// known for shardID.
func (c *Cluster) FailoverShardLeader(shardID int, deadNodeID string) error {
	state := c.node.State()
	if state.ShardLeader[shardID] != deadNodeID {
		return nil
	}
	for _, replica := range state.ShardReplicas[shardID] {
		if replica == deadNodeID {
			continue
		}
		if err := c.node.SetShardLeader(shardID, replica); err != nil {
			return fmt.Errorf("cluster: failover shard %d to %s: %w", shardID, replica, err)
		}
		return nil
	}
	return fmt.Errorf("cluster: shard %d has no healthy replica to fail over to", shardID)
}

// FailoverNode promotes a healthy replica to lead every shard deadNodeID
// currently leads (see FailoverShardLeader) — the bulk operation a health
// checker calls once it's decided a node is down, before deciding whether
// to go further and evict it entirely via Leave.
func (c *Cluster) FailoverNode(deadNodeID string) error {
	state := c.node.State()
	for shard, leader := range state.ShardLeader {
		if leader != deadNodeID {
			continue
		}
		if err := c.FailoverShardLeader(shard, deadNodeID); err != nil {
			return err
		}
	}
	return nil
}

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
	if err := c.Rebalance(); err != nil {
		return fmt.Errorf("cluster: rebalance after join: %w", err)
	}
	return nil
}

// Leave removes nodeID from Raft voting membership and from tracked node
// metadata, then rebalances shards away from it. Only the leader can call
// this.
func (c *Cluster) Leave(nodeID string, timeout time.Duration) error {
	if err := c.node.RemoveServer(nodeID, timeout); err != nil {
		return fmt.Errorf("cluster: remove raft server: %w", err)
	}
	if err := c.node.RemoveNode(nodeID); err != nil {
		return fmt.Errorf("cluster: propose node removal: %w", err)
	}
	c.Refresh()
	if err := c.Rebalance(); err != nil {
		return fmt.Errorf("cluster: rebalance after leave: %w", err)
	}
	return nil
}
