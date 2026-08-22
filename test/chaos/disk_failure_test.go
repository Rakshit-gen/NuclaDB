package chaos

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

// TestOpenFailsCleanlyOnUnwritableDataDir simulates the disk/permission
// failure that's actually reachable and portable to fault-inject: a data
// volume that's read-only at startup (a misconfigured mount, a disk that
// filled up before restart, a permissions error in a deploy).
//
// An earlier version of this test tried to simulate a failure mid-flight
// by chmod'ing the data directory to read-only while the server was
// already running and writing. That doesn't work: POSIX permission
// checks happen at open(2) time, not on every write(2) to an
// already-open file descriptor, so the WAL's already-open fd kept
// accepting writes right through the simulated "outage" — the test
// caught its own flawed assumption rather than a real bug. Testing
// against a directory that's unwritable *before* Engine.Open is called
// exercises the same underlying failure path (open(O_CREATE) genuinely
// respects directory permissions) without that false premise.
func TestOpenFailsCleanlyOnUnwritableDataDir(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root: permission bits don't restrict root, so this simulation has no effect")
	}

	parent := t.TempDir()
	unwritable := filepath.Join(parent, "readonly")
	if err := os.Mkdir(unwritable, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(unwritable, 0o755) }) // let TempDir's own cleanup remove it

	dataDir := filepath.Join(unwritable, "default")
	_, err := engine.Open(dataDir, hnsw.Config{Dim: 4, Metric: hnsw.L2()})
	if err == nil {
		t.Fatal("Open against an unwritable parent directory should fail cleanly, got nil error")
	}
	t.Logf("Open correctly failed cleanly: %v", err)

	// Recovery: once the directory is writable, Open must succeed and
	// behave completely normally — the earlier failed attempt must not
	// have left any partial state behind that trips up a later Open.
	if err := os.Chmod(unwritable, 0o755); err != nil {
		t.Fatal(err)
	}
	eng, err := engine.Open(dataDir, hnsw.Config{Dim: 4, Metric: hnsw.L2()})
	if err != nil {
		t.Fatalf("Open should succeed once the directory is writable: %v", err)
	}
	defer eng.Close()

	if err := eng.Insert(1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatalf("insert after recovery should succeed: %v", err)
	}
	res, err := eng.Search([]float32{1, 0, 0, 0}, 1, 10, nil)
	if err != nil || len(res) != 1 {
		t.Fatalf("search after recovery should succeed: res=%+v err=%v", res, err)
	}
}
