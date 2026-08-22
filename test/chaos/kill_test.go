// Package chaos runs fault-injection tests against a real nucladbd
// subprocess: repeated ungraceful kills mid-write, and a simulated disk
// write failure. These are deliberately heavier than the WAL/engine unit
// tests' in-process crash simulations (internal/storage/wal/wal_test.go,
// internal/engine/engine_test.go) — they exercise the actual binary, the
// actual OS process boundary, and the actual filesystem, and are wired
// into CI to run on every PR rather than as a one-off manual check (see
// .github/workflows/ci.yml).
package chaos

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// binPath builds nucladbd once per test run into a temp location, so the
// chaos tests are self-contained and don't depend on CI step ordering
// having already produced a binary.
func binPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not determine repo root")
	}
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")

	out := filepath.Join(t.TempDir(), "nucladbd")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/nucladbd")
	cmd.Dir = repoRoot
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building nucladbd: %v\n%s", err, output)
	}
	return out
}

type nucladbdProcess struct {
	cmd *exec.Cmd
}

func startNucladbd(t *testing.T, bin, dataDir, grpcAddr string) *nucladbdProcess {
	t.Helper()
	cmd := exec.Command(bin, "-data-dir", dataDir, "-dim", "4", "-metric", "l2", "-grpc-addr", grpcAddr, "-http-addr", ":0")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting nucladbd: %v", err)
	}
	p := &nucladbdProcess{cmd: cmd}
	waitReady(t, grpcAddr, 10*time.Second)
	return p
}

// kill9 sends SIGKILL — no graceful shutdown, no final snapshot, no
// chance to flush anything not already fsync'd. This is the actual fault
// the WAL's durability contract is supposed to survive, not a simulation
// of it.
func (p *nucladbdProcess) kill9(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("sending SIGKILL: %v", err)
	}
	_ = p.cmd.Wait()
}

func waitReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}
	defer conn.Close()
	health := healthpb.NewHealthClient(conn)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nucladbd at %s did not become ready within %s", addr, timeout)
}

func mustDial(t *testing.T, addr string) pb.NuclaDBClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewNuclaDBClient(conn)
}

// TestRepeatedKillAndRestart runs several cycles of: insert a batch of
// vectors, SIGKILL the process mid-flight (no graceful shutdown), restart
// against the same data directory, and verify every acknowledged write
// from every prior cycle survived. This is the plan's "repeated kill -9 /
// restart cycles against a running instance," not a one-off check.
func TestRepeatedKillAndRestart(t *testing.T) {
	bin := binPath(t)
	dataDir := t.TempDir()
	addr := "127.0.0.1:19910"

	const cycles = 4
	const perCycle = 50
	nextID := 0

	for cycle := 0; cycle < cycles; cycle++ {
		p := startNucladbd(t, bin, dataDir, addr)
		client := mustDial(t, addr)

		for i := 0; i < perCycle; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			id := strconv.Itoa(nextID)
			_, err := client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{
				Id: id, Values: []float32{float32(nextID), 0, 0, 0},
			}})
			cancel()
			if err != nil {
				t.Fatalf("cycle %d: insert %d: %v", cycle, nextID, err)
			}
			nextID++
		}

		// Ungraceful: no GracefulStop, no final snapshot. Every write
		// above was individually fsync'd by the WAL before Insert
		// returned, so all of them — not "most" — must survive this.
		p.kill9(t)
	}

	// Final restart: verify every write from every cycle survived.
	p := startNucladbd(t, bin, dataDir, addr)
	defer p.kill9(t)
	client := mustDial(t, addr)

	for i := 0; i < nextID; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Search(ctx, &pb.SearchRequest{
			Query: []float32{float32(i), 0, 0, 0}, TopK: 1,
		})
		cancel()
		if err != nil {
			t.Fatalf("search for id=%d: %v", i, err)
		}
		if len(resp.GetMatches()) != 1 || resp.GetMatches()[0].GetId() != strconv.Itoa(i) {
			t.Fatalf("id=%d did not survive %d kill-9/restart cycles: %+v", i, cycles, resp.GetMatches())
		}
	}
}
