// Package network chaos-tests the Raft transport layer against a real
// network partition, injected with Shopify/toxiproxy rather than a process
// kill (test/chaos/kill_test.go already covers that). Each node's raft
// transport advertises a toxiproxy proxy address instead of its real bind
// address, so peers dial through the proxy without the transport itself
// knowing; a "timeout" toxic with timeout=0 then silently black-holes
// traffic in both directions — a partition, not a refused/closed
// connection, which is what Proxy.Disable() would produce and which
// already looks like the kill tests' failure signature.
package network

import (
	"net"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2"
	toxiproxyclient "github.com/Shopify/toxiproxy/v2/client"
	hraft "github.com/hashicorp/raft"
	"github.com/rs/zerolog"

	crraft "github.com/Rakshit-gen/nucladb/internal/cluster/raft"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("freeAddr: close: %v", err)
	}
	return addr
}

// startToxiproxy runs an in-process toxiproxy API server (no external
// toxiproxy-server binary needed) and returns a client pointed at it.
func startToxiproxy(t *testing.T) *toxiproxyclient.Client {
	t.Helper()
	apiAddr := freeAddr(t)
	srv := toxiproxy.NewServer(toxiproxy.NewMetricsContainer(nil), zerolog.Nop())
	go func() { _ = srv.Listen(apiAddr) }()
	t.Cleanup(func() { _ = srv.Shutdown() })

	client := toxiproxyclient.NewClient("http://" + apiAddr)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.Proxies(); err == nil {
			return client
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("toxiproxy API server never came up on %s", apiAddr)
	return nil
}

// partitionableNode is a raft node whose transport advertises a toxiproxy
// proxy address rather than its real bind address, so its raft traffic can
// be black-holed without touching the process itself.
type partitionableNode struct {
	node      *crraft.Node
	advertise string // proxy address peers dial
	proxy     *toxiproxyclient.Proxy
}

func startPartitionableNode(t *testing.T, id string, tox *toxiproxyclient.Client) *partitionableNode {
	t.Helper()

	bindAddr := freeAddr(t)
	proxyAddr := freeAddr(t)
	proxy, err := tox.CreateProxy(id, proxyAddr, bindAddr)
	if err != nil {
		t.Fatalf("CreateProxy(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = proxy.Delete() })

	transport, err := crraft.NewTCPTransportAdvertise(bindAddr, proxyAddr)
	if err != nil {
		t.Fatalf("NewTCPTransportAdvertise(%s): %v", id, err)
	}
	deps, err := crraft.DiskDeps(t.TempDir(), transport)
	if err != nil {
		t.Fatalf("DiskDeps(%s): %v", id, err)
	}
	node, err := crraft.New(id, deps)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() { _ = node.Shutdown() })

	return &partitionableNode{node: node, advertise: proxyAddr, proxy: proxy}
}

// blackhole adds a never-timing-out "timeout" toxic to both directions of
// p's proxy: data stops flowing but the connection is never closed, the
// signature of a real network partition rather than a dead/refused peer.
func blackhole(t *testing.T, p *toxiproxyclient.Proxy) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		if _, err := p.AddToxic("partition_"+stream, "timeout", stream, 1.0,
			toxiproxyclient.Attributes{"timeout": 0}); err != nil {
			t.Fatalf("AddToxic(%s, %s): %v", p.Name, stream, err)
		}
	}
}

func heal(t *testing.T, p *toxiproxyclient.Proxy) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		if err := p.RemoveToxic("partition_" + stream); err != nil {
			t.Fatalf("RemoveToxic(%s, %s): %v", p.Name, stream, err)
		}
	}
}

func awaitLeader(t *testing.T, nodes ...*crraft.Node) *crraft.Node {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no node became leader within timeout")
	return nil
}

// TestFullPartitionLosesQuorumThenHeals partitions both directions between
// the two members of a 2-node raft cluster — majority of 2 requires both,
// so this must sever writes entirely, not just fail over to a survivor —
// then heals the partition and verifies a leader re-emerges and writes
// succeed again. This is the case a process-kill chaos test can't cover:
// both nodes stay alive and healthy throughout, only the network between
// them is broken.
func TestFullPartitionLosesQuorumThenHeals(t *testing.T) {
	tox := startToxiproxy(t)
	n1 := startPartitionableNode(t, "node-1", tox)
	n2 := startPartitionableNode(t, "node-2", tox)

	if err := n1.node.Bootstrap([]hraft.Server{
		{ID: hraft.ServerID("node-1"), Address: hraft.ServerAddress(n1.advertise)},
		{ID: hraft.ServerID("node-2"), Address: hraft.ServerAddress(n2.advertise)},
	}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	leader := awaitLeader(t, n1.node, n2.node)

	if err := leader.AddNode("pre-partition", "127.0.0.1:1"); err != nil {
		t.Fatalf("propose before partition: %v", err)
	}

	blackhole(t, n1.proxy)
	blackhole(t, n2.proxy)

	// The leader can't replicate to a quorum anymore (it needs both nodes
	// out of two), so a propose must fail rather than silently succeed.
	if err := leader.AddNode("during-partition", "127.0.0.1:2"); err == nil {
		t.Fatalf("propose succeeded during a full partition — should have lost quorum")
	}

	heal(t, n1.proxy)
	heal(t, n2.proxy)

	newLeader := awaitLeader(t, n1.node, n2.node)
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if lastErr = newLeader.AddNode("post-heal", "127.0.0.1:3"); lastErr == nil {
			return
		}
		// leadership may still be settling right after the partition heals
		newLeader = awaitLeader(t, n1.node, n2.node)
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cluster never recovered write availability after healing the partition: %v", lastErr)
}
