// Package jepsen runs Jepsen-style linearizability checks against a real,
// in-process 2-node NuclaDB cluster (real TCP Raft, real gRPC servers,
// real WAL-stream replication, real scatter-gather router) using the
// actual github.com/anishathalye/porcupine checker — not a hand-rolled
// one. Concurrent clients hammer a small keyspace through the router
// while recording a linearizability history; one test runs with no
// faults (the steady-state single-leader data path, which must be
// linearizable), the other kills the current shard leader mid-workload
// and fails over to its replica (the interesting case: replication is
// asynchronous, so a failover can lose an already-acknowledged write —
// see docs/writeups for what that costs).
package jepsen

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc"

	grpcapi "github.com/Rakshit-gen/nucladb/internal/api/grpc"
	"github.com/Rakshit-gen/nucladb/internal/cluster"
	crraft "github.com/Rakshit-gen/nucladb/internal/cluster/raft"
	"github.com/Rakshit-gen/nucladb/internal/cluster/replication"
	"github.com/Rakshit-gen/nucladb/internal/cluster/router"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
	hraft "github.com/hashicorp/raft"
)

const numKeys = 6 // also doubles as the vector dimension — see vectorFor.

// --- linearizability model: each key is a boolean present/absent register ---

type opKind int

const (
	opPut opKind = iota
	opDel
	opGet
)

type opInput struct {
	kind opKind
	key  int
}

type opOutput struct {
	err   bool // the client observed an error (ambiguous: may or may not have applied)
	found bool // for opGet only
}

type regState [numKeys]bool

// jepsenModel is nondeterministic only where it has to be: a Put/Del that
// returned an error might still have reached the shard leader before the
// connection died, so both "applied" and "not applied" are valid next
// states. A Put/Del that returned success, and every Get, are
// deterministic.
var jepsenNondeterministicModel = porcupine.NondeterministicModel{
	Init: func() []interface{} { return []interface{}{regState{}} },
	Step: func(st, in, out interface{}) []interface{} {
		state := st.(regState)
		input := in.(opInput)
		output := out.(opOutput)
		switch input.kind {
		case opPut:
			next := state
			next[input.key] = true
			if output.err {
				return []interface{}{state, next}
			}
			return []interface{}{next}
		case opDel:
			next := state
			next[input.key] = false
			if output.err {
				return []interface{}{state, next}
			}
			return []interface{}{next}
		case opGet:
			if state[input.key] == output.found {
				return []interface{}{state}
			}
			return nil
		default:
			return nil
		}
	},
}

var jepsenModel = jepsenNondeterministicModel.ToModel()

func vectorFor(key int) []float32 {
	v := make([]float32, numKeys)
	v[key] = 100
	return v
}

// --- cluster harness: two real nodes, real Raft, real replication ---

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

// member is one running cluster node: a real Raft voter plus a real
// gRPC-served Store, wired together by *cluster.Cluster.
type member struct {
	id       string
	raftNode *crraft.Node
	raftAddr string
	cluster  *cluster.Cluster
	store    *engine.Store
	engine   *engine.Engine
	apiAddr  string
	grpcSrv  *grpc.Server
}

func startMember(t *testing.T, id string) *member {
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
	raftNode, err := crraft.New(id, deps)
	if err != nil {
		t.Fatalf("New(%q): %v", id, err)
	}
	t.Cleanup(func() { _ = raftNode.Shutdown() })

	store, err := engine.OpenStore(t.TempDir(), hnsw.Config{
		Dim: numKeys, M: 8, EfConstruction: 50, Metric: hnsw.L2(),
	})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	eng, err := store.Engine(engine.DefaultTenant)
	if err != nil {
		t.Fatalf("store.Engine: %v", err)
	}

	svc := grpcapi.New(store, pb.DistanceMetric_DISTANCE_METRIC_L2)
	grpcSrv := grpc.NewServer()
	pb.RegisterNuclaDBServer(grpcSrv, svc)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(ln) }()
	t.Cleanup(grpcSrv.Stop)

	return &member{
		id: id, raftNode: raftNode, raftAddr: raftAddr,
		store: store, engine: eng, apiAddr: ln.Addr().String(), grpcSrv: grpcSrv,
	}
}

// newCluster wires two members into one Raft-and-ring cluster with a
// single shard replicated to both nodes, and starts the leader->replica
// WAL stream between whichever node ends up owning shard 0 and the other.
// Returns the two members (m1.cluster is used as the router's resolver;
// either converges to the same view) and the id of the current shard-0
// leader.
func newCluster(t *testing.T) (m1, m2 *member, shardLeaderID string) {
	t.Helper()
	const numShards = 1
	const virtualNodes = 50
	const replicationFactor = 2

	m1 = startMember(t, "node-1")
	m2 = startMember(t, "node-2")

	if err := m1.raftNode.Bootstrap([]hraft.Server{
		{ID: hraft.ServerID(m1.id), Address: hraft.ServerAddress(m1.raftAddr)},
	}); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	awaitLeader(t, m1.raftNode, 2*time.Second)

	m1.cluster = cluster.New(m1.raftNode, numShards, virtualNodes, replicationFactor)
	m2.cluster = cluster.New(m2.raftNode, numShards, virtualNodes, replicationFactor)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go m1.cluster.Watch(ctx)
	go m2.cluster.Watch(ctx)

	if err := m1.cluster.Join(m1.id, m1.raftAddr, m1.apiAddr, 2*time.Second); err != nil {
		t.Fatalf("Join(%s): %v", m1.id, err)
	}
	if err := m1.cluster.Join(m2.id, m2.raftAddr, m2.apiAddr, 2*time.Second); err != nil {
		t.Fatalf("Join(%s): %v", m2.id, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		owner, _, err := m1.cluster.ShardOwner(0)
		if err == nil && owner != "" {
			shardLeaderID = owner
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard 0 never got an owner: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	leader, replica := m1, m2
	if shardLeaderID == m2.id {
		leader, replica = m2, m1
	}

	replLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	replCtx, replCancel := context.WithCancel(context.Background())
	t.Cleanup(replCancel)
	go func() { _ = replication.Serve(replCtx, replLn, leader.engine) }()
	go func() { _ = replication.Follow(replCtx, replLn.Addr().String(), replica.engine) }()

	return m1, m2, shardLeaderID
}

// --- workload ---

// runWorkload drives numClients goroutines issuing random Put/Del/Get
// operations against rtr for duration, recording a porcupine history. If
// fault is non-nil, it fires once from a separate goroutine partway
// through the run.
func runWorkload(rtr *router.Router, numClients int, duration time.Duration, fault func()) []porcupine.Operation {
	var mu sync.Mutex
	var history []porcupine.Operation

	// A shared time.After channel only ever delivers once, so with multiple
	// client goroutines selecting on it concurrently, only one would ever
	// observe the stop signal and the rest would spin forever. Each client
	// checks its own deadline instead.
	deadline := time.Now().Add(duration)
	if fault != nil {
		go func() {
			time.Sleep(duration / 3)
			fault()
		}()
	}

	var wg sync.WaitGroup
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(clientID) + 1))
			for {
				if time.Now().After(deadline) {
					return
				}

				key := rng.Intn(numKeys)
				kind := opKind(rng.Intn(3))
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				call := time.Now().UnixNano()

				var output opOutput
				record := true
				switch kind {
				case opPut:
					err := rtr.Insert(ctx, uint64(key), vectorFor(key), nil)
					output = opOutput{err: err != nil}
				case opDel:
					err := rtr.Delete(ctx, uint64(key))
					output = opOutput{err: err != nil}
				case opGet:
					matches, err := rtr.Search(ctx, vectorFor(key), 1, 200, nil)
					if err != nil {
						record = false // a read that never returned constrains nothing
					} else {
						output = opOutput{found: len(matches) > 0 && matches[0].ID == uint64(key)}
					}
				}
				ret := time.Now().UnixNano()
				cancel()

				if record {
					mu.Lock()
					history = append(history, porcupine.Operation{
						ClientId: clientID,
						Input:    opInput{kind: kind, key: key},
						Call:     call,
						Output:   output,
						Return:   ret,
					})
					mu.Unlock()
				}
			}
		}(c)
	}
	wg.Wait()
	return history
}

// TestLinearizableUnderNormalOperation verifies the steady-state data
// path — every op routed to the single current shard leader, no faults —
// is linearizable, checked by the real porcupine model checker rather
// than assumed from the code.
func TestLinearizableUnderNormalOperation(t *testing.T) {
	m1, _, _ := newCluster(t)
	rtr := router.New(m1.cluster, "")
	defer rtr.Close()

	history := runWorkload(rtr, 6, 1500*time.Millisecond, nil)
	if len(history) < 20 {
		t.Fatalf("only recorded %d ops, workload didn't run enough to be a meaningful check", len(history))
	}

	res, info := porcupine.CheckOperationsVerbose(jepsenModel, history, 10*time.Second)
	if res == porcupine.Ok {
		return
	}
	t0 := history[0].Call
	for _, op := range history {
		in := op.Input.(opInput)
		out := op.Output.(opOutput)
		t.Logf("client=%d kind=%d key=%d call=%dus ret=%dus err=%v found=%v",
			op.ClientId, in.kind, in.key,
			(op.Call-t0)/1000, (op.Return-t0)/1000, out.err, out.found)
	}
	for pi, part := range info.PartialLinearizationsOperations() {
		for li, lin := range part {
			t.Logf("partition %d linearization %d: %d ops linearized before getting stuck", pi, li, len(lin))
			for _, op := range lin {
				in := op.Input.(opInput)
				out := op.Output.(opOutput)
				t.Logf("  linearized: client=%d kind=%d key=%d err=%v found=%v", op.ClientId, in.kind, in.key, out.err, out.found)
			}
		}
	}
	t.Fatalf("history of %d ops with no faults injected was not linearizable (%v) — a real bug in the single-leader data path", len(history), res)
}

// TestFailoverLinearizability kills the current shard leader mid-workload
// and fails over to its replica (the same FailoverNode path
// internal/cluster/health's Checker calls automatically in production),
// then checks the resulting history. Replication is asynchronous
// (internal/cluster/replication streams the WAL, it isn't a per-write
// Raft-committed quorum), so a write the old leader acknowledged just
// before dying can be lost on failover — a real, documented tradeoff
// (see docs/writeups), not necessarily a bug to fix. This test's job is
// to run the check and report what actually happened, not to assert a
// specific outcome that would make it flaky.
func TestFailoverLinearizability(t *testing.T) {
	m1, m2, leaderID := newCluster(t)
	rtr := router.New(m1.cluster, "")
	defer rtr.Close()

	fault := func() {
		dead := m1
		if leaderID == m2.id {
			dead = m2
		}
		dead.grpcSrv.Stop() // simulate the shard leader's API dying

		rc := m1.cluster
		if !rc.IsLeader() {
			rc = m2.cluster
		}
		if err := rc.FailoverNode(leaderID); err != nil {
			t.Errorf("FailoverNode(%s): %v", leaderID, err)
		}
	}

	history := runWorkload(rtr, 6, 1500*time.Millisecond, fault)
	if len(history) < 20 {
		t.Fatalf("only recorded %d ops, workload didn't run enough to be a meaningful check", len(history))
	}

	res := porcupine.CheckOperationsTimeout(jepsenModel, history, 10*time.Second)
	if res == porcupine.Unknown {
		t.Fatalf("porcupine checker timed out on %d ops instead of reaching a verdict", len(history))
	}
	t.Logf("failover history: %d ops, linearizability result = %v", len(history), res)
	if res == porcupine.Illegal {
		t.Log("found a linearizability violation across shard-leader failover — expected given async WAL-stream replication; see docs/writeups for the tradeoff writeup")
	}
}
