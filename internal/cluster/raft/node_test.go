package raft

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// testCluster wires up a fixed set of in-memory Raft nodes, all connected
// to each other directly via raft.InmemTransport (no real networking), so
// leader election and replication can be tested deterministically within a
// single process.
type testCluster struct {
	t          *testing.T
	nodes      map[string]*Node
	transports map[string]*raft.InmemTransport
}

func newTestCluster(t *testing.T, ids ...string) *testCluster {
	t.Helper()
	c := &testCluster{
		t:          t,
		nodes:      make(map[string]*Node),
		transports: make(map[string]*raft.InmemTransport),
	}

	for _, id := range ids {
		_, transport := raft.NewInmemTransport(raft.ServerAddress(id))
		c.transports[id] = transport
		n, err := New(id, Deps{
			Logs:      raft.NewInmemStore(),
			Stable:    raft.NewInmemStore(),
			Snapshots: raft.NewInmemSnapshotStore(),
			Transport: transport,
		})
		if err != nil {
			t.Fatalf("New(%q): %v", id, err)
		}
		c.nodes[id] = n
	}

	// Fully connect every pair of transports — each InmemTransport needs
	// to know how to reach every peer directly, there's no discovery.
	for _, from := range c.transports {
		for id, to := range c.transports {
			from.Connect(raft.ServerAddress(id), to)
		}
	}

	t.Cleanup(func() {
		for _, n := range c.nodes {
			_ = n.Shutdown()
		}
	})

	return c
}

func (c *testCluster) bootstrap() {
	c.t.Helper()
	servers := make([]raft.Server, 0, len(c.nodes))
	for id := range c.nodes {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(id)})
	}
	// Only one node needs to call Bootstrap; hashicorp/raft replicates the
	// initial configuration to the rest once a leader emerges.
	for _, n := range c.nodes {
		if err := n.Bootstrap(servers); err != nil {
			c.t.Fatalf("Bootstrap: %v", err)
		}
		break
	}
}

func (c *testCluster) awaitLeader(timeout time.Duration) *Node {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader elected within %s", timeout)
	return nil
}

func (c *testCluster) followers(leader *Node) []*Node {
	var out []*Node
	for id, n := range c.nodes {
		if id != leader.ID() {
			out = append(out, n)
		}
	}
	return out
}

// TestThreeNodeClusterConverges verifies the core Raft guarantee this
// package exists for: a command applied through the leader ends up
// identically reflected in every node's own FSM, not just the leader's.
func TestThreeNodeClusterConverges(t *testing.T) {
	c := newTestCluster(t, "node-1", "node-2", "node-3")
	c.bootstrap()
	leader := c.awaitLeader(2 * time.Second)

	if err := leader.AddNode("node-1", "10.0.0.1:9090"); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if err := leader.SetShardLeader(7, "node-1"); err != nil {
		t.Fatalf("SetShardLeader: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allConverged := true
		for id, n := range c.nodes {
			s := n.State()
			if s.Nodes["node-1"] != "10.0.0.1:9090" || s.ShardLeader[7] != "node-1" {
				allConverged = false
				if time.Now().After(deadline) {
					t.Fatalf("node %q did not converge: %+v", id, s)
				}
			}
		}
		if allConverged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestFollowerRejectsWrite verifies that proposing a command directly on a
// follower fails rather than silently succeeding or being forwarded — that
// distinction matters because a caller silently talking to the wrong node
// would otherwise believe an unreplicated write had succeeded.
func TestFollowerRejectsWrite(t *testing.T) {
	c := newTestCluster(t, "node-1", "node-2", "node-3")
	c.bootstrap()
	leader := c.awaitLeader(2 * time.Second)
	followers := c.followers(leader)
	if len(followers) == 0 {
		t.Fatal("expected at least one follower")
	}

	if err := followers[0].AddNode("node-x", "10.0.0.9:9090"); err == nil {
		t.Fatal("expected follower AddNode to fail, got nil error")
	}
}

// TestLeaderFailover kills the current leader and verifies the remaining
// two nodes elect a new one and can still accept and replicate writes —
// the actual point of running Raft at all, not just "it elects a leader
// once at startup."
func TestLeaderFailover(t *testing.T) {
	c := newTestCluster(t, "node-1", "node-2", "node-3")
	c.bootstrap()
	firstLeader := c.awaitLeader(2 * time.Second)

	if err := firstLeader.AddNode("node-1", "10.0.0.1:9090"); err != nil {
		t.Fatalf("AddNode before failover: %v", err)
	}

	firstLeaderID := firstLeader.ID()
	if err := firstLeader.Shutdown(); err != nil {
		t.Fatalf("Shutdown leader: %v", err)
	}
	delete(c.nodes, firstLeaderID)

	var newLeader *Node
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			if n.IsLeader() {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatalf("no new leader elected among survivors within timeout")
	}
	if newLeader.ID() == firstLeaderID {
		t.Fatalf("new leader is the same node that was shut down")
	}

	if err := newLeader.SetShardLeader(3, "node-2"); err != nil {
		t.Fatalf("SetShardLeader on new leader: %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		ok := true
		for id, n := range c.nodes {
			if n.State().ShardLeader[3] != "node-2" {
				ok = false
				if time.Now().After(deadline) {
					t.Fatalf("survivor %q did not apply post-failover write: %+v", id, n.State())
				}
			}
		}
		if ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
