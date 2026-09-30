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
