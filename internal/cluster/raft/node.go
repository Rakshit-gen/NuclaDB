package raft

import (
	"encoding/json"
	"time"

	"github.com/hashicorp/raft"
)

// Node wraps a *raft.Raft instance together with the FSM it drives,
// exposing the small, typed surface the rest of NuclaDB needs — propose a
// topology command, read current state, check leadership — instead of the
// full hashicorp/raft API.
type Node struct {
	id   string
	raft *raft.Raft
	fsm  *FSM
}

// Deps bundles the raft.NewRaft dependencies so callers can supply either
// the in-memory implementations (tests) or real disk-backed/networked ones
// (production) through the same constructor.
type Deps struct {
	Logs      raft.LogStore
	Stable    raft.StableStore
	Snapshots raft.SnapshotStore
	Transport raft.Transport
}

// New constructs a Node around a fresh FSM. It does not form a cluster by
// itself — call Bootstrap once, on exactly one node, to form a new cluster.
func New(id string, deps Deps) (*Node, error) {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	// Fast timeouts so in-memory test clusters converge and fail over in
	// well under a second; production deployments running over a real
	// network would want the library defaults (measured in seconds), so
	// this becomes a Deps field if/when this ever runs outside tests.
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond

	fsm := newFSM()
	r, err := raft.NewRaft(cfg, fsm, deps.Logs, deps.Stable, deps.Snapshots, deps.Transport)
	if err != nil {
		return nil, err
	}
	return &Node{id: id, raft: r, fsm: fsm}, nil
}

// Bootstrap forms a new cluster with the given voter servers. Call this
// exactly once, on exactly one of the nodes, listing every node that
// should start as a voter. hashicorp/raft itself safely no-ops repeat or
// late calls (see BootstrapCluster's doc), but that's a safety net, not a
// reason to call it from more than one place.
func (n *Node) Bootstrap(servers []raft.Server) error {
	return n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
}

// ID returns this node's Raft server ID.
func (n *Node) ID() string { return n.id }

// IsLeader reports whether this node currently believes it is the Raft
// leader. Like any such check in a distributed system, it's a snapshot,
// not a guarantee — leadership can change between this check and a
// subsequent propose, which is why propose surfaces its own error from
// Raft rather than relying on callers to check IsLeader first.
func (n *Node) IsLeader() bool {
	return n.raft.State() == raft.Leader
}

// State returns the FSM's current topology. Safe to call on any node.
func (n *Node) State() State {
	return n.fsm.State()
}

// LeaderCh notifies on this node's own leadership transitions: true when
// it becomes leader, false when it steps down.
func (n *Node) LeaderCh() <-chan bool {
	return n.raft.LeaderCh()
}

// Shutdown stops the local Raft instance and waits for it to finish.
func (n *Node) Shutdown() error {
	return n.raft.Shutdown().Error()
}

const defaultProposeTimeout = 2 * time.Second

// propose serializes cmd and replicates it through Raft consensus,
// returning once it's committed and applied to this node's own FSM. Only
// the leader can do this; hashicorp/raft's Apply rejects the call on a
// follower with raft.ErrNotLeader, which this method surfaces directly.
func (n *Node) propose(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	future := n.raft.Apply(data, defaultProposeTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if resp := future.Response(); resp != nil {
		if applyErr, ok := resp.(error); ok {
			return applyErr
		}
	}
	return nil
}

// AddNode proposes adding nodeID (at addr) to the known-nodes set.
func (n *Node) AddNode(nodeID, addr string) error {
	return n.propose(Command{Op: OpAddNode, NodeID: nodeID, Addr: addr})
}

// RemoveNode proposes removing nodeID from the known-nodes set.
func (n *Node) RemoveNode(nodeID string) error {
	return n.propose(Command{Op: OpRemoveNode, NodeID: nodeID})
}

// SetShardLeader proposes nodeID as the leader of shardID.
func (n *Node) SetShardLeader(shardID int, nodeID string) error {
	return n.propose(Command{Op: OpSetShardLeader, ShardID: shardID, NodeID: nodeID})
}

// AddShardReplica proposes nodeID as a replica of shardID.
func (n *Node) AddShardReplica(shardID int, nodeID string) error {
	return n.propose(Command{Op: OpAddShardReplica, ShardID: shardID, NodeID: nodeID})
}

// RemoveShardReplica proposes removing nodeID as a replica of shardID.
func (n *Node) RemoveShardReplica(shardID int, nodeID string) error {
	return n.propose(Command{Op: OpRemoveShardReplica, ShardID: shardID, NodeID: nodeID})
}
