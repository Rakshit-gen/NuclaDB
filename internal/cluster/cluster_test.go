package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	crraft "github.com/Rakshit-gen/nucladb/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
)

// freeTCPAddr grabs an ephemeral localhost port by briefly binding to it,
// then releases it for the real raft.NetworkTransport to bind. There's an
// unavoidable, standard-in-tests race if something else grabs the port in
// between; in practice on a local test run that never happens.
func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPAddr: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("freeTCPAddr: close: %v", err)
	}
	return addr
}

// newTCPNode builds a real disk-backed, TCP-transport Raft node — the same
// construction path a production nucladbd process would use — rooted at a
// fresh temp dir so BoltDB state doesn't leak between tests.
func newTCPNode(t *testing.T, id string) (*crraft.Node, string) {
	t.Helper()
	raftAddr := freeTCPAddr(t)
	transport, err := crraft.NewTCPTransport(raftAddr)
	if err != nil {
		t.Fatalf("NewTCPTransport(%q): %v", raftAddr, err)
	}
	deps, err := crraft.DiskDeps(t.TempDir(), transport)
	if err != nil {
		t.Fatalf("DiskDeps: %v", err)
	}
	node, err := crraft.New(id, deps)
	if err != nil {
		t.Fatalf("New(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })
	return node, raftAddr
}

func awaitLeader(t *testing.T, n *crraft.Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("node %q never became leader within %s", n.ID(), timeout)
}

// TestClusterJoinOverRealTCPConverges builds a 3-node cluster over actual
// TCP sockets on localhost (not the in-memory transport used to test the
// raft package in isolation), joining node-2 and node-3 dynamically via
// the real production join path (Cluster.Join: AddVoter then propose node
// metadata), and verifies every node's independently-computed ring
// converges to the same shard assignment once its Watch goroutine picks
// up the change.
func TestClusterJoinOverRealTCPConverges(t *testing.T) {
	const numShards = 32
	const virtualNodes = 50

	node1, addr1 := newTCPNode(t, "node-1")
	node2, addr2 := newTCPNode(t, "node-2")
	node3, addr3 := newTCPNode(t, "node-3")

	if err := node1.Bootstrap([]hraft.Server{
		{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(addr1)},
	}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	awaitLeader(t, node1, 2*time.Second)

	c1 := New(node1, numShards, virtualNodes)
	c2 := New(node2, numShards, virtualNodes)
	c3 := New(node3, numShards, virtualNodes)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c1.Watch(ctx)
	go c2.Watch(ctx)
	go c3.Watch(ctx)

	if err := c1.Join("node-1", addr1, "api-1:9090", 2*time.Second); err != nil {
		t.Fatalf("Join(node-1 self): %v", err)
	}
	if err := c1.Join("node-2", addr2, "api-2:9090", 2*time.Second); err != nil {
		t.Fatalf("Join(node-2): %v", err)
	}
	if err := c1.Join("node-3", addr3, "api-3:9090", 2*time.Second); err != nil {
		t.Fatalf("Join(node-3): %v", err)
	}

	clusters := map[string]*Cluster{"node-1": c1, "node-2": c2, "node-3": c3}
	wantAddrs := map[string]string{"node-1": "api-1:9090", "node-2": "api-2:9090", "node-3": "api-3:9090"}

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for {
		lastErr = nil
		var reference map[int]string
		for id, c := range clusters {
			assignment, err := c.Assignment()
			if err != nil {
				lastErr = fmt.Errorf("%s: Assignment: %w", id, err)
				break
			}
			if len(assignment) != numShards {
				lastErr = fmt.Errorf("%s: got %d shard assignments, want %d", id, len(assignment), numShards)
				break
			}
			if reference == nil {
				reference = assignment
				continue
			}
			for shard, owner := range reference {
				if assignment[shard] != owner {
					lastErr = fmt.Errorf("%s disagrees with reference on shard %d: %q vs %q", id, shard, assignment[shard], owner)
					break
				}
			}
		}
		if lastErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("clusters never converged on shard assignment: %v", lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Every node's ShardOwner should also resolve to the right API address,
	// not just the right node id — this is the actual thing a query router
	// would use to dial the owning node.
	for shard := 0; shard < numShards; shard++ {
		ownerID, addr, err := c1.ShardOwner(shard)
		if err != nil {
			t.Fatalf("ShardOwner(%d): %v", shard, err)
		}
		if addr != wantAddrs[ownerID] {
			t.Fatalf("ShardOwner(%d) = (%q, %q), want addr %q", shard, ownerID, addr, wantAddrs[ownerID])
		}
	}
}

// TestClusterLeaveRemovesNodeFromAssignment verifies that removing a node
// actually moves its shards elsewhere in every node's independently
// recomputed ring, not just in the leader's — the property a rebalancer
// or failure-handler would depend on.
func TestClusterLeaveRemovesNodeFromAssignment(t *testing.T) {
	const numShards = 32
	const virtualNodes = 50

	node1, addr1 := newTCPNode(t, "node-1")
	node2, addr2 := newTCPNode(t, "node-2")

	if err := node1.Bootstrap([]hraft.Server{
		{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(addr1)},
	}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	awaitLeader(t, node1, 2*time.Second)

	c1 := New(node1, numShards, virtualNodes)
	c2 := New(node2, numShards, virtualNodes)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c1.Watch(ctx)
	go c2.Watch(ctx)

	if err := c1.Join("node-1", addr1, "api-1:9090", 2*time.Second); err != nil {
		t.Fatalf("Join(node-1 self): %v", err)
	}
	if err := c1.Join("node-2", addr2, "api-2:9090", 2*time.Second); err != nil {
		t.Fatalf("Join(node-2): %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		nodes := c2.node.State().Nodes
		if len(nodes) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-2 never saw both nodes join: %+v", nodes)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := c1.Leave("node-2", 2*time.Second); err != nil {
		t.Fatalf("Leave(node-2): %v", err)
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		a1, err := c1.Assignment()
		if err != nil {
			t.Fatalf("Assignment: %v", err)
		}
		allOnNode1 := true
		for _, owner := range a1 {
			if owner != "node-1" {
				allOnNode1 = false
				break
			}
		}
		if allOnNode1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shards still assigned away from node-1 after node-2 left: %+v", a1)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
