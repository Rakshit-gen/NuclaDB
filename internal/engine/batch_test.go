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
