package engine

import (
	"strconv"
	"testing"
)

func TestInsertBatchSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	items := make([]InsertItem, 50)
	for i := range items {
		v := make([]float32, 8)
		v[i%8] = float32(i + 1)
		items[i] = InsertItem{ID: uint64(i), Vector: v, Metadata: map[string]string{"n": strconv.Itoa(i)}}
	}
	if err := e.InsertBatch(items); err != nil {
		t.Fatal(err)
	}
	if e.LastSeq() != 50 {
		t.Fatalf("LastSeq = %d, want 50", e.LastSeq())
	}

	// No Close: reopen straight from the WAL, as after a crash.
	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != 50 {
		t.Fatalf("recovered %d vectors, want 50", e2.Len())
	}
	res, err := e2.Search(items[7].Vector, 1, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 7 || res[0].Metadata["n"] != "7" {
		t.Fatalf("id 7 not recovered intact: %+v", res)
	}
}

// Replay queues inserts and applies them in batches; deletes and reinserts
// in between must still land in log order.
func TestReplayKeepsInsertDeleteOrder(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	vec := func(x float32) []float32 { return []float32{x, 0, 0, 0, 0, 0, 0, 0} }
	for i := 0; i < 300; i++ {
		if err := e.Insert(uint64(i), vec(float32(i)), nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, step := range []func() error{
		func() error { return e.Delete(5) },
		func() error { return e.Insert(5, vec(1000), nil) },
		func() error { return e.Delete(7) },
		func() error { return e.Insert(9, vec(2000), nil) },
		func() error { return e.Delete(9) },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}

	// No Close: recover from the WAL alone.
	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != 298 {
		t.Fatalf("Len = %d, want 298 (ids 7 and 9 deleted)", e2.Len())
	}
	res, err := e2.Search(vec(1000), 1, 50, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 5 {
		t.Fatalf("reinserted id 5 not at its new vector: %+v", res)
	}
	for _, gone := range []float32{7, 2000} {
		res, err := e2.Search(vec(gone), 1, 50, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(res) == 1 && res[0].Distance == 0 {
			t.Fatalf("deleted vector %v still found: %+v", gone, res)
		}
	}
}
