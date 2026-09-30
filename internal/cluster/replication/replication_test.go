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

// TestFollowResyncsDivergedReplica covers a failover leftover: a replica
// whose log disagrees with the new leader's, either at the same length or
// running ahead of it, must end up with the leader's data, not keep its own.
func TestFollowResyncsDivergedReplica(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ownWrite int // records the replica wrote on its own
	}{
		{"same length, different records", 3},
		{"replica ahead of the leader", 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader, err := engine.Open(t.TempDir(), testConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer leader.Close()
			for id := uint64(1); id <= 3; id++ {
				if err := leader.Insert(id, []float32{float32(id), 0, 0, 0}, nil); err != nil {
					t.Fatal(err)
				}
			}

			replica, err := engine.Open(t.TempDir(), testConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer replica.Close()
			for i := 0; i < tc.ownWrite; i++ {
				if err := replica.Insert(uint64(100+i), []float32{0, float32(i), 0, 0}, nil); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() { _ = Follow(ctx, startServer(t, ctx, leader), replica) }()

			deadline := time.Now().Add(3 * time.Second)
			for {
				_, _, stale := replica.Get(100)
				_, _, have := replica.Get(3)
				if have && !stale && replica.Len() == 3 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("replica never resynced: Len=%d, has leader id 3=%v, still has own id 100=%v", replica.Len(), have, stale)
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

// TestFollowWakesOnWrite checks a write reaches the follower from the
// leader's WAL notification, well before the fallback poll would find it.
func TestFollowWakesOnWrite(t *testing.T) {
	leader, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()
	follower, err := engine.Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = Follow(ctx, startServer(t, ctx, leader), follower) }()

	// Let the stream settle into waiting, then time a handful of writes.
	time.Sleep(100 * time.Millisecond)
	for id := uint64(1); id <= 5; id++ {
		start := time.Now()
		if err := leader.Insert(id, []float32{float32(id), 0, 0, 0}, nil); err != nil {
			t.Fatal(err)
		}
		awaitLen(t, follower, int(id), 2*time.Second)
		if took := time.Since(start); took > pollInterval/4 {
			t.Fatalf("write %d took %s to replicate, want well under the %s poll", id, took, pollInterval)
		}
	}
}
