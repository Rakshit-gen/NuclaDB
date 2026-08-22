package health

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/Rakshit-gen/nucladb/internal/cluster"
	crraft "github.com/Rakshit-gen/nucladb/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
)

// freeTCPAddr grabs an ephemeral localhost port by briefly binding to it,
// then releases it for the real caller to bind. There's an unavoidable,
// standard-in-tests race if something else grabs the port in between; in
// practice on a local test run that never happens.
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

// newTCPNode builds a real disk-backed, TCP-transport Raft node, mirroring
// internal/cluster's own test helper (duplicated rather than shared, since
// it's test-only and unexported on both sides of the package boundary).
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

// startHealthServer runs a real gRPC server with the standard health
// service registered (exactly what cmd/nucladbd wires up) on a real TCP
// port, reporting SERVING immediately.
func startHealthServer(t *testing.T) (addr string, srv *grpc.Server) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv = grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), srv
}

// TestGRPCProberDetectsServingAndDown verifies the prober against a real
// server: SERVING while it's up, an error once it's stopped (connection
// refused) — the two states the failure detector's whole tick loop
// depends on being distinguishable.
func TestGRPCProberDetectsServingAndDown(t *testing.T) {
	addr, srv := startHealthServer(t)

	p := GRPCProber{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Probe(ctx, addr); err != nil {
		t.Fatalf("Probe() on a live server: %v", err)
	}

	srv.Stop()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel2()
	if err := p.Probe(ctx2, addr); err == nil {
		t.Fatal("Probe() on a stopped server: got nil error, want one")
	}
}

// TestCheckerFailsOverThenEvictsDeadNode is the end-to-end case: a
// 2-node cluster with replicationFactor=2 (so every shard has the other
// node as a replica), node-2's API server dies, and the Checker running
// on leader node-1 should first fail every shard node-2 led over to
// node-1 (fast path, no Raft membership change), then — once node-2 stays
// down past the eviction threshold — remove it from the cluster entirely
// via Cluster.Leave, at which point Rebalance also drops it from the
// replica set for good.
func TestCheckerFailsOverThenEvictsDeadNode(t *testing.T) {
	const numShards = 8
	const virtualNodes = 50
	const replicationFactor = 2

	node1, addr1 := newTCPNode(t, "node-1")
	node2, addr2 := newTCPNode(t, "node-2")

	if err := node1.Bootstrap([]hraft.Server{
		{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(addr1)},
	}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	awaitLeader(t, node1, 2*time.Second)

	c1 := cluster.New(node1, numShards, virtualNodes, replicationFactor)
	c2 := cluster.New(node2, numShards, virtualNodes, replicationFactor)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go c1.Watch(ctx)
	go c2.Watch(ctx)

	node2APIAddr, node2Srv := startHealthServer(t)
	node1APIAddr, _ := startHealthServer(t)

	if err := c1.Join("node-1", addr1, node1APIAddr, 2*time.Second); err != nil {
		t.Fatalf("Join(node-1 self): %v", err)
	}
	if err := c1.Join("node-2", addr2, node2APIAddr, 2*time.Second); err != nil {
		t.Fatalf("Join(node-2): %v", err)
	}

	// Sanity: with 2 nodes and replicationFactor=2, node-2 must lead at
	// least one shard, or this test would trivially pass without ever
	// exercising failover.
	deadline := time.Now().Add(2 * time.Second)
	var node2LedAny bool
	for {
		assignment, err := c1.Assignment()
		if err != nil {
			t.Fatalf("Assignment: %v", err)
		}
		if len(assignment) == numShards {
			for _, owner := range assignment {
				if owner == "node-2" {
					node2LedAny = true
				}
			}
			if node2LedAny {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-2 never led any shard after join+rebalance: %+v", assignment)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Kill node-2's API server to simulate the node going down.
	node2Srv.Stop()

	checker := New(c1, GRPCProber{}, "node-1", 20*time.Millisecond, 150*time.Millisecond, 2, 6)
	checkerCtx, checkerCancel := context.WithCancel(context.Background())
	t.Cleanup(checkerCancel)
	go checker.Run(checkerCtx)

	// Stage 1: fast failover — every shard should end up led by node-1,
	// its only replica, well before eviction kicks in.
	deadline = time.Now().Add(5 * time.Second)
	for {
		assignment, err := c1.Assignment()
		if err != nil {
			t.Fatalf("Assignment: %v", err)
		}
		allNode1 := len(assignment) == numShards
		for _, owner := range assignment {
			if owner != "node-1" {
				allNode1 = false
			}
		}
		if allNode1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shards never failed over to node-1: %+v", assignment)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stage 2: full eviction — node-2 should eventually be removed from
	// the tracked node set entirely.
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, ok := c1.Nodes()["node-2"]; !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node-2 was never evicted from the cluster")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// And node-1's own Cluster instance should now report itself as the
	// sole node for every shard, with no replicas dangling on the dead
	// node-2.
	assignment, err := c1.Assignment()
	if err != nil {
		t.Fatalf("Assignment: %v", err)
	}
	for shard, owner := range assignment {
		if owner != "node-1" {
			t.Fatalf("shard %d owner = %q after eviction, want node-1", shard, owner)
		}
	}
}
