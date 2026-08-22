// Package raft implements NuclaDB's cluster control plane: which nodes
// exist and which node currently leads each shard, replicated via
// hashicorp/raft so every node agrees on cluster topology even across
// network partitions and node failures.
//
// This governs *metadata* — "who's the leader for shard 7" — not the
// actual vector writes, which stay on the fast path documented in
// internal/engine (WAL + snapshot per shard). Running every vector
// insert through Raft consensus would be correct but far too slow; Raft
// exists here specifically to make topology decisions durable and
// agreed-upon, then the shard leader replicates its own WAL stream to
// followers directly (see internal/cluster/replication, once built).
package raft

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// Op identifies a cluster topology command.
type Op string

const (
	OpAddNode            Op = "add_node"
	OpRemoveNode         Op = "remove_node"
	OpSetShardLeader     Op = "set_shard_leader"
	OpAddShardReplica    Op = "add_shard_replica"
	OpRemoveShardReplica Op = "remove_shard_replica"
)

// Command is one topology change, serialized as the payload of a Raft
// log entry.
type Command struct {
	Op      Op     `json:"op"`
	NodeID  string `json:"node_id,omitempty"`
	Addr    string `json:"addr,omitempty"`
	ShardID int    `json:"shard_id,omitempty"`
}

// State is the fully replicated cluster topology: every known node's
// address, and per-shard leader/replica assignment.
type State struct {
	Nodes         map[string]string `json:"nodes"`          // node id -> address
	ShardLeader   map[int]string    `json:"shard_leader"`   // shard id -> leader node id
	ShardReplicas map[int][]string  `json:"shard_replicas"` // shard id -> replica node ids
}

func newState() State {
	return State{
		Nodes:         make(map[string]string),
		ShardLeader:   make(map[int]string),
		ShardReplicas: make(map[int][]string),
	}
}

func (s State) clone() State {
	out := newState()
	for k, v := range s.Nodes {
		out.Nodes[k] = v
	}
	for k, v := range s.ShardLeader {
		out.ShardLeader[k] = v
	}
	for k, v := range s.ShardReplicas {
		out.ShardReplicas[k] = append([]string(nil), v...)
	}
	return out
}

// FSM applies replicated Commands to an in-memory State. Every node in
// the Raft cluster runs its own FSM; Raft guarantees every node applies
// the same commands in the same order, so every FSM converges to
// identical state without the nodes needing to coordinate directly.
type FSM struct {
	mu          sync.RWMutex
	state       State
	subscribers []chan struct{}
}

func newFSM() *FSM {
	return &FSM{state: newState()}
}

// Subscribe returns a channel that receives a (coalesced, non-blocking)
// notification after every committed Apply. It's how Cluster learns to
// recompute shard ownership on topology changes without polling: a
// buffered size-1 channel with a non-blocking send means a burst of
// commands collapses to a single pending notification rather than
// backing up, since the only thing that matters to a receiver is "state
// changed, go re-read it" — not how many times.
func (f *FSM) Subscribe() <-chan struct{} {
	ch := make(chan struct{}, 1)
	f.mu.Lock()
	f.subscribers = append(f.subscribers, ch)
	f.mu.Unlock()
	return ch
}

// notifySubscribersLocked must be called with f.mu held.
func (f *FSM) notifySubscribersLocked() {
	for _, ch := range f.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// State returns a snapshot copy of the current topology. Safe to call on
// any node, leader or follower — followers' state reflects whatever
// they've applied so far, which may lag the leader by a bounded amount
// under normal operation and further during a partition.
func (f *FSM) State() State {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state.clone()
}

// Apply implements raft.FSM. It's only ever invoked by the raft library
// itself, once per committed log entry, in log order.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	defer f.notifySubscribersLocked() // runs before Unlock (LIFO defers), while the lock's still held

	switch cmd.Op {
	case OpAddNode:
		f.state.Nodes[cmd.NodeID] = cmd.Addr
	case OpRemoveNode:
		delete(f.state.Nodes, cmd.NodeID)
	case OpSetShardLeader:
		f.state.ShardLeader[cmd.ShardID] = cmd.NodeID
	case OpAddShardReplica:
		replicas := f.state.ShardReplicas[cmd.ShardID]
		for _, r := range replicas {
			if r == cmd.NodeID {
				return nil // already present
			}
		}
		f.state.ShardReplicas[cmd.ShardID] = append(replicas, cmd.NodeID)
	case OpRemoveShardReplica:
		replicas := f.state.ShardReplicas[cmd.ShardID]
		filtered := replicas[:0]
		for _, r := range replicas {
			if r != cmd.NodeID {
				filtered = append(filtered, r)
			}
		}
		f.state.ShardReplicas[cmd.ShardID] = filtered
	}
	return nil
}

// Snapshot implements raft.FSM: a point-in-time copy of the state for
// Raft's own log-compaction snapshotting (distinct from, and much
// smaller than, internal/storage/segment's vector-graph snapshots — this
// one is just cluster topology, at most a few KB even at scale).
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{state: f.State()}, nil
}

// Restore implements raft.FSM, rebuilding state from a prior Snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()
	var s State
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	if s.Nodes == nil {
		s.Nodes = make(map[string]string)
	}
	if s.ShardLeader == nil {
		s.ShardLeader = make(map[int]string)
	}
	if s.ShardReplicas == nil {
		s.ShardReplicas = make(map[int][]string)
	}
	f.mu.Lock()
	f.state = s
	f.notifySubscribersLocked()
	f.mu.Unlock()
	return nil
}

type fsmSnapshot struct {
	state State
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
