package engine

import (
	"strconv"
	"sync"
	"testing"
)

// Concurrent Inserts share fsyncs through the group commit queue. Every
// caller must still get its own record durable and applied, in WAL order.
func TestConcurrentInsertsAreAllDurable(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	const workers, per = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := uint64(w*per + i)
				v := make([]float32, 8)
				v[int(id)%8] = float32(id + 1)
				if err := e.Insert(id, v, map[string]string{"w": strconv.Itoa(w)}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if e.LastSeq() != workers*per {
		t.Fatalf("LastSeq = %d, want %d", e.LastSeq(), workers*per)
	}

	// No Close: recover from the WAL alone.
	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != workers*per {
		t.Fatalf("recovered %d vectors, want %d", e2.Len(), workers*per)
	}
}

// Each worker inserts then deletes its own ids, so every insert and delete
// of one id is ordered by its worker even when they share a group commit.
func TestConcurrentInsertsAndDeletesAreDurable(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	const workers, per = 8, 20
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := uint64(w*per + i)
				v := make([]float32, 8)
				v[int(id)%8] = float32(id + 1)
				if err := e.Insert(id, v, map[string]string{"w": strconv.Itoa(w)}); err != nil {
					t.Error(err)
					return
				}
				if i%2 == 0 {
					if err := e.Delete(id); err != nil {
						t.Error(err)
						return
					}
				}
			}
		}(w)
	}
	wg.Wait()
	if e.Len() != workers*per/2 {
		t.Fatalf("Len = %d, want %d", e.Len(), workers*per/2)
	}

	// Deleting an id that isn't stored logs nothing.
	before := e.LastSeq()
	if err := e.Delete(1 << 40); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(0); err != nil { // already deleted
		t.Fatal(err)
	}
	if e.LastSeq() != before {
		t.Fatalf("deleting missing ids advanced the WAL from %d to %d", before, e.LastSeq())
	}

	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != workers*per/2 {
		t.Fatalf("recovered %d vectors, want %d", e2.Len(), workers*per/2)
	}
}

// Snapshots drop the write lock while they write files, so inserts land in
// the WAL mid-snapshot. Every acknowledged insert must survive a restart.
func TestInsertsDuringSnapshotsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}

	const workers, per = 4, 150
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := uint64(w*per + i)
				v := make([]float32, 8)
				v[int(id)%8] = float32(id + 1)
				if err := e.Insert(id, v, map[string]string{"id": strconv.Itoa(int(id))}); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
loop:
	for {
		select {
		case <-done:
			break loop
		default:
			if err := e.Snapshot(); err != nil {
				t.Fatal(err)
			}
		}
	}

	// No Close: recover from the last snapshot plus the rotated WAL.
	e2, err := Open(dir, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if e2.Len() != workers*per {
		t.Fatalf("recovered %d vectors, want %d", e2.Len(), workers*per)
	}
	for id := uint64(0); id < workers*per; id++ {
		if e2.metadata[id]["id"] != strconv.Itoa(int(id)) {
			t.Fatalf("id %d lost its metadata", id)
		}
	}
}
