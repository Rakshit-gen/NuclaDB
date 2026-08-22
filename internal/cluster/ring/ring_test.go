package ring

import (
	"fmt"
	"testing"
)

func TestOwnerOfNoNodes(t *testing.T) {
	r := New(16, 100)
	if _, err := r.OwnerOf(0); err != ErrNoNodes {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestEveryShardGetsAnOwnerAmongAddedNodes(t *testing.T) {
	r := New(64, 100)
	nodes := []string{"node-a", "node-b", "node-c"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	nodeSet := map[string]bool{}
	for _, n := range nodes {
		nodeSet[n] = true
	}

	assignment, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	if len(assignment) != 64 {
		t.Fatalf("got %d shard assignments, want 64", len(assignment))
	}
	for shard, owner := range assignment {
		if !nodeSet[owner] {
			t.Fatalf("shard %d assigned to unknown node %q", shard, owner)
		}
	}
}

func TestAssignmentIsDeterministic(t *testing.T) {
	r1 := New(64, 100)
	r2 := New(64, 100)
	for _, n := range []string{"node-a", "node-b", "node-c"} {
		r1.AddNode(n)
		r2.AddNode(n)
	}
	a1, err := r1.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	a2, err := r2.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	for shard, owner := range a1 {
		if a2[shard] != owner {
			t.Fatalf("shard %d: ring1 says %q, ring2 (same nodes, same config) says %q", shard, owner, a2[shard])
		}
	}
}

// TestLoadDistribution measures how evenly shards spread across nodes —
// consistent hashing with enough virtual nodes should keep every real
// node's share within a reasonable band of numShards/numNodes, not
// perfectly equal but not wildly skewed either. Real measured numbers,
// not an assumption that the algorithm "just works."
func TestLoadDistribution(t *testing.T) {
	const numShards = 1024
	const numNodes = 8
	const virtualNodes = 150

	r := New(numShards, virtualNodes)
	for i := 0; i < numNodes; i++ {
		r.AddNode(fmt.Sprintf("node-%d", i))
	}

	assignment, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, owner := range assignment {
		counts[owner]++
	}

	expected := float64(numShards) / float64(numNodes)
	minCount, maxCount := numShards, 0
	for node, c := range counts {
		t.Logf("%s: %d shards (%.1f%% of expected even share)", node, c, 100*float64(c)/expected)
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}
	if len(counts) != numNodes {
		t.Fatalf("only %d of %d nodes received any shards", len(counts), numNodes)
	}

	// With 150 virtual nodes per real node, skew should be well within
	// 2x of the even share in either direction — a loose bound that
	// would fail if virtual-node placement collapsed to something like
	// one hash function reused verbatim (a real bug this catches).
	if float64(maxCount) > expected*2 || float64(minCount) < expected*0.5 {
		t.Fatalf("uneven distribution: min=%d max=%d expected=%.0f", minCount, maxCount, expected)
	}
}

// TestAddingNodeMovesOnlyAFraction verifies the actual point of
// consistent hashing over plain modulo hashing: adding one node to an
// N-node ring should remap roughly 1/(N+1) of shards, not all of them —
// measured directly by diffing the assignment before and after, not
// assumed from the algorithm's reputation.
func TestAddingNodeMovesOnlyAFraction(t *testing.T) {
	const numShards = 1000
	const virtualNodes = 150

	r := New(numShards, virtualNodes)
	initialNodes := []string{"node-a", "node-b", "node-c", "node-d"}
	for _, n := range initialNodes {
		r.AddNode(n)
	}
	before, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}

	r.AddNode("node-e")
	after, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}

	moved := 0
	for shard, owner := range before {
		if after[shard] != owner {
			moved++
		}
	}
	moveFraction := float64(moved) / float64(numShards)
	t.Logf("adding a 5th node to a 4-node ring moved %d/%d shards (%.1f%%); ideal is ~1/5 = 20%%",
		moved, numShards, 100*moveFraction)

	// Every moved shard should have moved to the new node specifically
	// (that's the point — only the new node "steals" keys, existing
	// nodes never trade with each other) and the total moved should be
	// in a believable range around the ideal 1/(N+1), not close to 100%
	// (which would indicate this had degraded to non-consistent, full
	// rehashing behavior).
	for shard, owner := range before {
		if after[shard] != owner && after[shard] != "node-e" {
			t.Fatalf("shard %d moved from %q to %q — should only ever move to the newly added node", shard, owner, after[shard])
		}
	}
	if moveFraction > 0.40 {
		t.Fatalf("moved %.1f%% of shards on a single node addition — expected roughly 20%%, this looks like full rehashing, not consistent hashing", 100*moveFraction)
	}
}

func TestRemoveNode(t *testing.T) {
	r := New(64, 100)
	r.AddNode("node-a")
	r.AddNode("node-b")
	r.RemoveNode("node-b")

	assignment, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	for shard, owner := range assignment {
		if owner != "node-a" {
			t.Fatalf("shard %d assigned to %q after node-b was removed, only node-a remains", shard, owner)
		}
	}

	if len(r.Nodes()) != 1 || r.Nodes()[0] != "node-a" {
		t.Fatalf("Nodes() = %v, want [node-a]", r.Nodes())
	}
}

func TestAddRemoveNodeIsIdempotent(t *testing.T) {
	r := New(16, 50)
	r.AddNode("node-a")
	r.AddNode("node-a") // duplicate add should not double the node's points
	before, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}

	r.RemoveNode("node-a")
	r.RemoveNode("node-a") // duplicate remove should not error or panic
	if _, err := r.OwnerOf(0); err != ErrNoNodes {
		t.Fatalf("expected ErrNoNodes after removing the only node, got %v", err)
	}

	r.AddNode("node-a")
	after, err := r.Assignment()
	if err != nil {
		t.Fatal(err)
	}
	for shard, owner := range before {
		if after[shard] != owner {
			t.Fatalf("shard %d: re-adding the same single node should reproduce the same assignment, got %q want %q", shard, after[shard], owner)
		}
	}
}

func TestOwnersOfNoNodes(t *testing.T) {
	r := New(16, 100)
	if _, err := r.OwnersOf(0, 2); err != ErrNoNodes {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

// TestOwnersOfAgreesWithOwnerOf verifies OwnersOf's first result is always
// the same node OwnerOf alone would return — replica placement builds on
// top of primary ownership, not a separate computation that could diverge
// from it.
func TestOwnersOfAgreesWithOwnerOf(t *testing.T) {
	r := New(64, 100)
	for _, n := range []string{"node-a", "node-b", "node-c", "node-d"} {
		r.AddNode(n)
	}
	for shard := 0; shard < 64; shard++ {
		primary, err := r.OwnerOf(shard)
		if err != nil {
			t.Fatal(err)
		}
		owners, err := r.OwnersOf(shard, 3)
		if err != nil {
			t.Fatal(err)
		}
		if len(owners) == 0 || owners[0] != primary {
			t.Fatalf("shard %d: OwnersOf(_, 3)[0] = %v, want first entry %q (OwnerOf's own result)", shard, owners, primary)
		}
	}
}

// TestOwnersOfReturnsDistinctNodesCappedAtNodeCount verifies OwnersOf
// never repeats a node in one shard's replica set, and caps out at however
// many distinct nodes actually exist rather than erroring or padding.
func TestOwnersOfReturnsDistinctNodesCappedAtNodeCount(t *testing.T) {
	r := New(32, 100)
	for _, n := range []string{"node-a", "node-b", "node-c"} {
		r.AddNode(n)
	}
	for shard := 0; shard < 32; shard++ {
		owners, err := r.OwnersOf(shard, 5) // more than the 3 nodes that exist
		if err != nil {
			t.Fatal(err)
		}
		if len(owners) != 3 {
			t.Fatalf("shard %d: OwnersOf(_, 5) returned %d owners, want 3 (capped at node count): %v", shard, len(owners), owners)
		}
		seen := map[string]bool{}
		for _, o := range owners {
			if seen[o] {
				t.Fatalf("shard %d: OwnersOf returned duplicate node %q: %v", shard, o, owners)
			}
			seen[o] = true
		}
	}
}
