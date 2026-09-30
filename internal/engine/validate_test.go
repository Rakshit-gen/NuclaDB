package engine

import (
	"errors"
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

// A rejected write must not reach the WAL: replay would hit the same error
// and the engine could never be opened again.
func TestWrongDimensionIsRejectedBeforeTheWAL(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Insert(1, []float32{1, 2, 3}, nil); !errors.Is(err, hnsw.ErrDimensionMismatch) {
		t.Fatalf("Insert: got %v, want ErrDimensionMismatch", err)
	}
	bad := []InsertItem{{ID: 2, Vector: make([]float32, 8)}, {ID: 3, Vector: make([]float32, 2)}}
	if err := e.InsertBatch(bad); !errors.Is(err, hnsw.ErrDimensionMismatch) {
		t.Fatalf("InsertBatch: got %v, want ErrDimensionMismatch", err)
	}
	if e.LastSeq() != 0 {
		t.Fatalf("LastSeq = %d after rejected writes, want 0", e.LastSeq())
	}

	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatalf("reopen after rejected writes: %v", err)
	}
	defer e2.Close()
	if e2.Len() != 0 {
		t.Fatalf("Len = %d, want 0", e2.Len())
	}
}
