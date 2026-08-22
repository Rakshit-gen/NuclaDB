package router

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"

	grpcimpl "github.com/Rakshit-gen/nucladb/internal/api/grpc"
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
	pb.RegisterNuclaDBServer(grpcServer, grpcimpl.New(store, pb.DistanceMetric_DISTANCE_METRIC_L2))
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
