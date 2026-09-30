package engine

import (
	"path/filepath"
	"testing"
)

// Snapshot writes metadata before the graph. This test stops right between
// the two steps, as a crash would, and checks the reopened engine still has
// the right metadata for every id.
func TestCrashBetweenMetadataAndGraphSnapshot(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	vec := func(i int) []float32 { v := make([]float32, 8); v[i%8] = float32(i + 1); return v }

	if err := e.Insert(1, vec(1), map[string]string{"v": "old"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert(1, vec(1), map[string]string{"v": "new"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Insert(2, vec(2), map[string]string{"v": "two"}); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(2); err != nil {
		t.Fatal(err)
	}

	// First half of Snapshot only: metadata saved, graph snapshot not.
	e.metaMu.RLock()
	err = saveMetadata(filepath.Join(dir, metadataFile), e.metadata)
	e.metaMu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}

	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if got := e2.metadata[1]["v"]; got != "new" {
		t.Fatalf("id 1 metadata = %q, want new", got)
	}
	if _, ok := e2.metadata[2]; ok {
		t.Fatal("deleted id 2 still has metadata")
	}
	if e2.Len() != 1 {
		t.Fatalf("Len = %d, want 1", e2.Len())
	}
}
