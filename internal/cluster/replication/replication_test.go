package replication

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

func testConfig() hnsw.Config {
	return hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1}
}

func startServer(t *testing.T, ctx context.Context, e *engine.Engine) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	go func() { _ = Serve(ctx, ln, e) }()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

func awaitLen(t *testing.T, e *engine.Engine, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if e.Len() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Len()=%d after timeout, want %d", e.Len(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestFollowStreamsLiveWrites verifies the common case: a follower joins
// a fresh leader (no snapshot yet needed) and sees both the leader's
// pre-existing writes and writes made after it connects.
func TestFollowStreamsLiveWrites(t *testing.T) {
	leader, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.Insert(1, []float32{1, 0, 0, 0}, map[string]string{"team": "search"}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := startServer(t, ctx, leader)

	follower, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followCtx, followCancel := context.WithCancel(context.Background())
	defer followCancel()
	go func() { _ = Follow(followCtx, addr, follower) }()

	awaitLen(t, follower, 1, 2*time.Second)

	if err := leader.Insert(2, []float32{0, 1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	awaitLen(t, follower, 2, 2*time.Second)

	res, err := follower.Search([]float32{1, 0, 0, 0}, 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 1 || res[0].Metadata["team"] != "search" {
		t.Fatalf("follower.Search() = %+v, want id=1 team=search", res)
	}
}

// TestFollowBootstrapsLateJoiningReplica is the case wal.Follow alone
// can't handle: a follower joins *after* the leader has already
// snapshotted (and rotated its WAL past the point the follower would need
// to start from), so replication must transfer a snapshot before it can
// catch up live — the same role a base backup plays before Postgres-style
// streaming replication.
func TestFollowBootstrapsLateJoiningReplica(t *testing.T) {
	leader, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.Insert(1, []float32{1, 0, 0, 0}, map[string]string{"team": "search"}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Insert(2, []float32{0, 1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := leader.Snapshot(); err != nil {
		t.Fatal(err) // rotates the WAL — a fresh follower's fromSeq=0 no longer covers these writes
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := startServer(t, ctx, leader)

	follower, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followCtx, followCancel := context.WithCancel(context.Background())
	defer followCancel()
	go func() { _ = Follow(followCtx, addr, follower) }()

	awaitLen(t, follower, 2, 2*time.Second)

	// And a write made after the bootstrap should still replicate live.
	if err := leader.Insert(3, []float32{0, 0, 1, 0}, nil); err != nil {
		t.Fatal(err)
	}
	awaitLen(t, follower, 3, 2*time.Second)
}

// TestFollowSurvivesLeaderSnapshotMidStream verifies a follower already
// attached and caught up doesn't stall or diverge when the leader
// snapshots (rotating its WAL) while the follower is live-streaming.
func TestFollowSurvivesLeaderSnapshotMidStream(t *testing.T) {
	leader, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.Insert(1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	addr := startServer(t, ctx, leader)

	follower, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	followCtx, followCancel := context.WithCancel(context.Background())
	defer followCancel()
	go func() { _ = Follow(followCtx, addr, follower) }()

	awaitLen(t, follower, 1, 2*time.Second)

	if err := leader.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := leader.Insert(2, []float32{0, 1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	awaitLen(t, follower, 2, 2*time.Second)
}
