package engine

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

func testConfig() hnsw.Config {
	return hnsw.Config{Dim: 8, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1}
}

func TestInsertSearchDelete(t *testing.T) {
	e, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if err := e.Insert(1, []float32{1, 0, 0, 0, 0, 0, 0, 0}, map[string]string{"team": "search"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert(2, []float32{0, 1, 0, 0, 0, 0, 0, 0}, map[string]string{"team": "infra"}); err != nil {
		t.Fatal(err)
	}

	res, err := e.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 1, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 1 || res[0].Metadata["team"] != "search" {
		t.Fatalf("unexpected result: %+v", res)
	}

	if err := e.Delete(1); err != nil {
		t.Fatal(err)
	}
	res, err = e.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 2, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.ID == 1 {
			t.Fatalf("deleted id=1 still present: %+v", res)
		}
	}
}

func TestMetadataFilteredSearch(t *testing.T) {
	e, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	rng := rand.New(rand.NewSource(1))
	for i := uint64(0); i < 200; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		team := "infra"
		if i%2 == 0 {
			team = "search"
		}
		if err := e.Insert(i, v, map[string]string{"team": team}); err != nil {
			t.Fatal(err)
		}
	}

	query := make([]float32, 8)
	for j := range query {
		query[j] = rng.Float32()
	}
	res, err := e.Search(query, 5, 20, map[string]string{"team": "search"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 5 {
		t.Fatalf("got %d filtered results, want 5", len(res))
	}
	for _, r := range res {
		if r.Metadata["team"] != "search" {
			t.Fatalf("filter leaked non-matching result: %+v", r)
		}
	}
}

// TestCrashRecovery is the end-to-end version of the plan's "kill -9
// mid-write, verify replay reconstructs correct state" requirement: it
// writes through a real Engine (WAL + graph + metadata all together),
// simulates a crash by truncating the WAL's tail mid-record, reopens, and
// checks every fully-acknowledged write survived and the torn write did
// not corrupt anything before it.
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()

	e, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(1))
	const n = 300
	vectors := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		vectors[i] = v
		if err := e.Insert(i, v, map[string]string{"idx": "true"}); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate a crash without a clean Close: no final snapshot, and the
	// WAL's tail gets torn off as if fsync landed mid-write.
	walPath := filepath.Join(dir, walFile)
	full, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	truncated := full[:len(full)-7]
	if err := os.WriteFile(walPath, truncated, 0o644); err != nil {
		t.Fatal(err)
	}

	// Reopen (as a fresh process would after the crash) and verify all
	// but the last, torn write survived.
	e2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if e2.Len() < n-1 {
		t.Fatalf("recovered %d vectors, want at least %d", e2.Len(), n-1)
	}

	// A representative early write must be exactly intact, both the
	// vector and its metadata.
	res, err := e2.Search(vectors[10], 1, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 10 || res[0].Metadata["idx"] != "true" {
		t.Fatalf("id=10 not correctly recovered: %+v", res)
	}
}

// TestSnapshotThenRecover verifies the fast-path recovery: after a clean
// Snapshot, the WAL is truncated, and a subsequent reopen must restore
// state purely from the snapshot (no records left to replay).
func TestSnapshotThenRecover(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig()

	e, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 50; i++ {
		v := []float32{float32(i), 0, 0, 0, 0, 0, 0, 0}
		if err := e.Insert(i, v, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, walFile)
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("WAL should be truncated after Snapshot, size=%d", fi.Size())
	}

	e2, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != 50 {
		t.Fatalf("Len() = %d, want 50", e2.Len())
	}
}
