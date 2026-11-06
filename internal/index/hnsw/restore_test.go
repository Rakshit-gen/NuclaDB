package hnsw

import "testing"

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	node := func(id uint64, nbrs ...uint64) NodeState {
		return NodeState{ID: id, Vector: []float32{1, 0}, Neighbors: [][]uint64{nbrs}}
	}
	restore := func(entry uint64, nodes ...NodeState) bool {
		g := New(Config{Dim: 2, Metric: L2(), Seed: 1})
		ids := make([]uint64, len(nodes))
		for i, nd := range nodes {
			ids[i] = nd.ID
		}
		if !g.BeginRestore(ids) {
			return false
		}
		for _, nd := range nodes {
			if !g.RestoreNode(nd) {
				return false
			}
		}
		return g.EndRestore(entry, 0, true)
	}

	if !restore(1, node(1, 2), node(2, 1)) {
		t.Fatal("rejected a valid snapshot")
	}
	for name, ok := range map[string]bool{
		"repeated id":       restore(1, node(1), node(1)),
		"self link":         restore(1, node(1, 1), node(2)),
		"unknown neighbor":  restore(1, node(1, 3), node(2)),
		"repeated neighbor": restore(1, node(1, 2, 2), node(2)),
		"unknown entry":     restore(9, node(1), node(2)),
	} {
		if ok {
			t.Errorf("%s: accepted", name)
		}
	}
}
