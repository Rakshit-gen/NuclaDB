package segment

import (
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	const dim = 16
	cfg := hnsw.Config{Dim: dim, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1}
	g := hnsw.New(cfg)

	rng := rand.New(rand.NewSource(1))
	vectors := make(map[uint64][]float32)
	for i := uint64(0); i < 500; i++ {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		vectors[i] = v
		if err := g.Insert(i, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Delete(3); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := Save(path, g, 12345); err != nil {
		t.Fatal(err)
	}

	restored, walSeq, err := Load(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if walSeq != 12345 {
		t.Fatalf("walSeq = %d, want 12345", walSeq)
	}
	if restored.Len() != g.Len() {
		t.Fatalf("restored.Len() = %d, want %d", restored.Len(), g.Len())
	}

	// The deleted node must stay excluded after restore.
	res, err := restored.Search(vectors[3], 500, 500)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.ID == 3 {
			t.Fatalf("deleted id=3 reappeared after restore")
		}
	}

	// Search quality should be preserved: querying with a stored vector's
	// exact values should return that id as the top match.
	res, err = restored.Search(vectors[42], 1, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 42 {
		t.Fatalf("expected exact match id=42 after restore, got %+v", res)
	}
}

func TestLoadMissingFileReturnsEmptyGraph(t *testing.T) {
	cfg := hnsw.Config{Dim: 4}
	g, walSeq, err := Load(filepath.Join(t.TempDir(), "missing.bin"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if walSeq != 0 {
		t.Fatalf("walSeq = %d, want 0", walSeq)
	}
	if g.Len() != 0 {
		t.Fatalf("expected empty graph, got Len()=%d", g.Len())
	}
}

func TestLoadRejectsCorruptHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.bin")
	if err := os.WriteFile(path, []byte("not a snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path, hnsw.Config{Dim: 4}); err != ErrInvalidFormat {
		t.Fatalf("expected ErrInvalidFormat, got %v", err)
	}
}

func savedSnapshot(t *testing.T, cfg hnsw.Config) string {
	t.Helper()
	g := hnsw.New(cfg)
	for i := uint64(0); i < 50; i++ {
		v := make([]float32, cfg.Dim)
		v[int(i)%cfg.Dim] = float32(i + 1)
		if err := g.Insert(i, v); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := Save(path, g, 7); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadRejectsFlippedByte(t *testing.T) {
	cfg := hnsw.Config{Dim: 4, Metric: hnsw.L2(), Seed: 1}
	path := savedSnapshot(t, cfg)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0x01 // inside some vector or neighbor list: still parses
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(path, cfg); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestLoadRejectsDifferentDimOrMetric(t *testing.T) {
	cfg := hnsw.Config{Dim: 4, Metric: hnsw.L2(), Seed: 1}
	path := savedSnapshot(t, cfg)
	for _, other := range []hnsw.Config{
		{Dim: 8, Metric: hnsw.L2()},
		{Dim: 4, Metric: hnsw.Cosine()},
	} {
		if _, _, err := Load(path, other); !errors.Is(err, ErrConfigMismatch) {
			t.Fatalf("dim %d metric %s: err = %v, want ErrConfigMismatch", other.Dim, other.Metric.Name(), err)
		}
	}
	if _, seq, err := Load(path, cfg); err != nil || seq != 7 {
		t.Fatalf("matching config: seq %d, err %v", seq, err)
	}
}

// Rewrites a version 2 file as version 1 (no dim/metric, no trailer) to
// check old snapshots still load, and still get the per-vector dim check.
func TestLoadsVersion1Snapshots(t *testing.T) {
	cfg := hnsw.Config{Dim: 4, Metric: hnsw.L2(), Seed: 1}
	path := savedSnapshot(t, cfg)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const fixed = 37 // magic, seq, hasEntry, entry, maxLevel, count
	nameLen := int(b[fixed+4])
	v1 := append([]byte{}, b[:fixed]...)
	copy(v1, magicV1[:])
	v1 = append(v1, b[fixed+4+1+nameLen:len(b)-4]...)
	if err := os.WriteFile(path, v1, 0o644); err != nil {
		t.Fatal(err)
	}

	g, seq, err := Load(path, cfg)
	if err != nil || seq != 7 || g.Len() != 50 {
		t.Fatalf("v1 load: len %v, seq %d, err %v", g, seq, err)
	}
	if _, _, err := Load(path, hnsw.Config{Dim: 8, Metric: hnsw.L2()}); !errors.Is(err, ErrConfigMismatch) {
		t.Fatalf("v1 with wrong dim: err = %v, want ErrConfigMismatch", err)
	}
}
