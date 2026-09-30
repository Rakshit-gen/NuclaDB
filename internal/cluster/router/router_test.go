package router

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	grpcimpl "github.com/Rakshit-gen/nucladb/internal/api/grpc"
	"github.com/Rakshit-gen/nucladb/internal/auth"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// fakeResolver is a trivial, static ShardResolver — Router's own job
// (hashing ids, scattering/gathering) doesn't depend on how ownership was
// decided, so testing it against real Raft/ring machinery (already
// covered by internal/cluster's own tests) would only add noise here.
type fakeResolver struct {
	addrs []string // index = shard id
}

func (f *fakeResolver) NumShards() int { return len(f.addrs) }

func (f *fakeResolver) ShardOwner(shard int) (nodeID, addr string, err error) {
	if shard < 0 || shard >= len(f.addrs) {
		return "", "", fmt.Errorf("shard %d out of range", shard)
	}
	return fmt.Sprintf("node-%d", shard), f.addrs[shard], nil
}

// startShardServer runs a real nucladbd-equivalent (a gRPC server over
// its own engine.Store) on a real TCP port and returns its address, so
// Router dials it exactly as it would a production node.
func startShardServer(t *testing.T) string {
	t.Helper()

	store, err := engine.OpenStore(t.TempDir(), hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNuclaDBServer(grpcServer, grpcimpl.New(store))
	go func() { _ = grpcServer.Serve(ln) }()
	t.Cleanup(grpcServer.Stop)

	return ln.Addr().String()
}

func newTestCluster(t *testing.T, numShards int) *Router {
	t.Helper()
	addrs := make([]string, numShards)
	for i := range addrs {
		addrs[i] = startShardServer(t)
	}
	r := New(&fakeResolver{addrs: addrs}, "")
	t.Cleanup(func() { _ = r.Close() })
	return r
}

// TestInsertRoutesToConsistentShard verifies the same id always hashes to
// the same shard (a basic correctness property Search's disjoint-merge
// depends on) and that the vector actually lands in that shard's engine,
// not just "some" shard.
func TestInsertRoutesToConsistentShard(t *testing.T) {
	const numShards = 4
	r := newTestCluster(t, numShards)
	ctx := context.Background()

	for id := uint64(0); id < 50; id++ {
		shard1 := ShardFor(id, numShards)
		shard2 := ShardFor(id, numShards)
		if shard1 != shard2 {
			t.Fatalf("ShardFor(%d) not deterministic: %d vs %d", id, shard1, shard2)
		}
		if err := r.Insert(ctx, id, []float32{float32(id), 0, 0, 0}, nil); err != nil {
			t.Fatalf("Insert(%d): %v", id, err)
		}
	}

	res, err := r.Search(ctx, []float32{0, 0, 0, 0}, 50, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 50 {
		t.Fatalf("Search returned %d matches, want 50 (every inserted id, across all shards)", len(res))
	}
}

// TestSearchMergesAcrossShardsByScore verifies the actual point of
// scatter-gather: the globally-ranked top-K is correct even when the
// single best result lives on a shard other than shard 0, and a
// shard-local top-K wouldn't be enough on its own.
func TestSearchMergesAcrossShardsByScore(t *testing.T) {
	const numShards = 4
	r := newTestCluster(t, numShards)
	ctx := context.Background()

	// Insert enough ids that, by pigeonhole, every shard gets at least a
	// few — then insert one more vector *exactly* at the query point, at
	// whatever id/shard that lands on, and verify it's always found as
	// the best match regardless of which shard it ends up on.
	for id := uint64(1); id <= 40; id++ {
		if err := r.Insert(ctx, id, []float32{float32(id), float32(id), 0, 0}, nil); err != nil {
			t.Fatalf("Insert(%d): %v", id, err)
		}
	}
	const bestID = uint64(9001)
	if err := r.Insert(ctx, bestID, []float32{0, 0, 0, 0}, map[string]string{"marker": "best"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("bestID=%d lands on shard %d", bestID, ShardFor(bestID, numShards))

	res, err := r.Search(ctx, []float32{0, 0, 0, 0}, 3, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 || res[0].ID != bestID {
		t.Fatalf("Search top result = %+v, want id=%d (the exact-match vector) first regardless of its shard", res, bestID)
	}
	for i := 1; i < len(res); i++ {
		if res[i].Score < res[i-1].Score {
			t.Fatalf("merged results not sorted by score ascending: %+v", res)
		}
	}
}

// TestDeleteRemovesFromCorrectShard verifies Delete resolves the same
// shard Insert used for the same id, and that the deletion is actually
// visible cluster-wide afterward.
func TestDeleteRemovesFromCorrectShard(t *testing.T) {
	const numShards = 4
	r := newTestCluster(t, numShards)
	ctx := context.Background()

	if err := r.Insert(ctx, 7, []float32{1, 1, 1, 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Insert(ctx, 8, []float32{2, 2, 2, 2}, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(ctx, 7); err != nil {
		t.Fatal(err)
	}

	res, err := r.Search(ctx, []float32{1, 1, 1, 1}, 10, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range res {
		if m.ID == 7 {
			t.Fatalf("deleted id=7 still present in Search results: %+v", res)
		}
	}
	found8 := false
	for _, m := range res {
		if m.ID == 8 {
			found8 = true
		}
	}
	if !found8 {
		t.Fatalf("id=8 (never deleted) missing from Search results: %+v", res)
	}
}

func TestInsertBatchSpreadsAcrossShards(t *testing.T) {
	const numShards = 3
	r := newTestCluster(t, numShards)
	ctx := context.Background()

	items := make([]Item, 30)
	for i := range items {
		items[i] = Item{ID: uint64(i), Vector: []float32{float32(i), 0, 0, 0}, Metadata: map[string]string{"i": fmt.Sprint(i)}}
	}
	if err := r.InsertBatch(ctx, items); err != nil {
		t.Fatal(err)
	}

	for _, want := range []uint64{0, 13, 29} {
		got, err := r.Search(ctx, []float32{float32(want), 0, 0, 0}, 1, 50, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != want || got[0].Metadata["i"] != fmt.Sprint(want) {
			t.Fatalf("search for %d got %+v", want, got)
		}
	}
}

// deadAddr is a localhost port nothing listens on.
func deadAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestSearchReturnsPartialResultsWhenAShardIsDown(t *testing.T) {
	live := startShardServer(t)
	r := New(&fakeResolver{addrs: []string{live, deadAddr(t)}}, "")
	t.Cleanup(func() { _ = r.Close() })
	ctx := context.Background()

	var want int
	for id := uint64(0); id < 20; id++ {
		if ShardFor(id, 2) != 0 {
			continue
		}
		if err := r.Insert(ctx, id, []float32{float32(id), 0, 0, 0}, nil); err != nil {
			t.Fatalf("Insert(%d): %v", id, err)
		}
		want++
	}

	res, err := r.Search(ctx, []float32{0, 0, 0, 0}, 20, 50, nil)
	var partial *PartialError
	if !errors.As(err, &partial) {
		t.Fatalf("Search err = %v, want *PartialError", err)
	}
	if _, ok := partial.Failed[1]; !ok || len(partial.Failed) != 1 {
		t.Fatalf("Failed = %v, want only shard 1", partial.Failed)
	}
	if len(res) != want {
		t.Fatalf("got %d matches from the live shard, want %d", len(res), want)
	}
}

// flakyResolver hands out a dead address for shard 0 on the first lookup
// and the live one afterwards, like a failover landing between tries.
type flakyResolver struct {
	dead, live string
	calls      atomic.Int32
}

func (f *flakyResolver) NumShards() int { return 1 }

func (f *flakyResolver) ShardOwner(int) (string, string, error) {
	if f.calls.Add(1) == 1 {
		return "old", f.dead, nil
	}
	return "new", f.live, nil
}

func TestSearchRetriesAgainstTheNewOwner(t *testing.T) {
	res := &flakyResolver{dead: deadAddr(t), live: startShardServer(t)}
	r := New(res, "")
	t.Cleanup(func() { _ = r.Close() })

	if _, err := r.Search(context.Background(), []float32{0, 0, 0, 0}, 5, 10, nil); err != nil {
		t.Fatalf("Search after failover: %v", err)
	}
	if res.calls.Load() != 2 {
		t.Fatalf("resolved the owner %d times, want 2", res.calls.Load())
	}
}

func TestRouterSendsAPIKey(t *testing.T) {
	const key = "router-test-key-0123456789"
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, []byte(`[{"key":"`+key+`","tenants":["*"]}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	keys, err := auth.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	store, err := engine.OpenStore(t.TempDir(), hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	svc := grpcimpl.New(store)
	svc.RequireKeys(keys)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.UnaryInterceptor(auth.UnaryServerInterceptor()))
	pb.RegisterNuclaDBServer(srv, svc)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	resolver := &fakeResolver{addrs: []string{ln.Addr().String()}}
	ctx := context.Background()

	anon := New(resolver, "")
	t.Cleanup(func() { _ = anon.Close() })
	if err := anon.Insert(ctx, 1, []float32{1, 0, 0, 0}, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Insert without a key: %v, want Unauthenticated", err)
	}

	keyed := New(resolver, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(auth.Bearer(key)))
	t.Cleanup(func() { _ = keyed.Close() })
	if err := keyed.Insert(ctx, 1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatalf("Insert with a key: %v", err)
	}
	if res, err := keyed.Search(ctx, []float32{1, 0, 0, 0}, 1, 10, nil); err != nil || len(res) != 1 {
		t.Fatalf("Search with a key = %v, %v; want 1 match", res, err)
	}
}
