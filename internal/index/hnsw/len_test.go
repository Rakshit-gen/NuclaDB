package hnsw

import "testing"

func TestLenTracksInsertReinsertDelete(t *testing.T) {
	g := New(Config{Dim: 2, Metric: L2(), Seed: 1})
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check := func(want int) {
		t.Helper()
		if got := g.Len(); got != want {
			t.Fatalf("Len = %d, want %d", got, want)
		}
	}
	must(g.Insert(1, []float32{0, 1}))
	must(g.Insert(2, []float32{1, 0}))
	check(2)
	must(g.Insert(1, []float32{0, 2})) // reinsert of a live id
	check(2)
	must(g.Delete(1))
	check(1)
	if g.Delete(1) == nil {
		t.Fatal("second delete should report ErrNotFound")
	}
	check(1)
	must(g.Insert(1, []float32{0, 3})) // reinsert of a deleted id
	check(2)

	nodes, ep, lvl, has := g.Snapshot()
	r := New(Config{Dim: 2, Metric: L2(), Seed: 1})
	r.Restore(nodes, ep, lvl, has)
	if r.Len() != 2 {
		t.Fatalf("restored Len = %d, want 2", r.Len())
	}
}
