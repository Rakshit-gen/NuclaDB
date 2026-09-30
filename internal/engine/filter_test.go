package engine

import (
	"errors"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

// A filter that matches a handful of vectors out of thousands used to come
// back short: the post-filter only saw the graph's top few hundred
// candidates, and the rare matches were rarely among them.
func TestSelectiveFilterReturnsExactTopK(t *testing.T) {
	e, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	rng := rand.New(rand.NewSource(7))
	vecs := map[uint64][]float32{}
	var rare []uint64
	var items []InsertItem
	for i := uint64(0); i < 3000; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		md := map[string]string{"kind": "common"}
		if i%500 == 3 {
			md["kind"] = "rare"
			rare = append(rare, i)
		}
		vecs[i] = v
		items = append(items, InsertItem{ID: i, Vector: v, Metadata: md})
	}
	if err := e.InsertBatch(items); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete(rare[0]); err != nil {
		t.Fatal(err)
	}

	query := make([]float32, 8)
	res, err := e.Search(query, 5, 10, []Filter{Eq("kind", "rare")})
	if err != nil {
		t.Fatal(err)
	}

	want := append([]uint64(nil), rare[1:]...)
	l2 := hnsw.L2()
	sort.Slice(want, func(a, b int) bool {
		return l2.Distance(query, vecs[want[a]]) < l2.Distance(query, vecs[want[b]])
	})
	if len(res) != len(want) {
		t.Fatalf("got %d results, want all %d live rare vectors: %+v", len(res), len(want), res)
	}
	for i, r := range res {
		if r.ID != want[i] || r.Metadata["kind"] != "rare" {
			t.Fatalf("result %d = %+v, want id %d", i, r, want[i])
		}
	}

	none, err := e.Search(query, 5, 10, []Filter{Eq("kind", "missing")})
	if err != nil || len(none) != 0 {
		t.Fatalf("filter with no matches: %+v, %v", none, err)
	}
}

// With the up-front exact scan turned off, a rare filter goes through the
// graph path. It must still come back with every match, not a short list.
func TestBroadPathFallsBackToExactScan(t *testing.T) {
	e, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.SetExactFilterLimit(0)

	rng := rand.New(rand.NewSource(3))
	var items []InsertItem
	want := map[uint64]bool{}
	for i := uint64(0); i < 3000; i++ {
		v := make([]float32, 8)
		for j := range v {
			v[j] = rng.Float32()
		}
		md := map[string]string{"kind": "common"}
		if i%500 == 7 {
			md["kind"] = "rare"
			want[i] = true
		}
		items = append(items, InsertItem{ID: i, Vector: v, Metadata: md})
	}
	if err := e.InsertBatch(items); err != nil {
		t.Fatal(err)
	}
	res, err := e.Search(make([]float32, 8), len(want), 10, []Filter{Eq("kind", "rare")})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(want) {
		t.Fatalf("got %d results, want all %d rare vectors", len(res), len(want))
	}
	for _, r := range res {
		if !want[r.ID] {
			t.Fatalf("result %d doesn't match the filter", r.ID)
		}
	}
}

func TestFilterOps(t *testing.T) {
	e, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	// id i has price i and tier a/b/c; id 0 has no metadata at all.
	tiers := []string{"a", "b", "c"}
	for i := uint64(0); i < 30; i++ {
		v := make([]float32, 8)
		v[i%8] = float32(i + 1)
		var md map[string]string
		if i > 0 {
			md = map[string]string{"price": strconv.Itoa(int(i)), "tier": tiers[i%3]}
		}
		if err := e.Insert(i, v, md); err != nil {
			t.Fatal(err)
		}
	}
	count := func(fs ...Filter) int {
		t.Helper()
		res, err := e.Search(make([]float32, 8), 100, 0, fs)
		if err != nil {
			t.Fatal(err)
		}
		return len(res)
	}
	cases := []struct {
		name string
		f    []Filter
		want int
	}{
		{"eq", []Filter{Eq("tier", "a")}, 9},
		{"ne", []Filter{{Key: "tier", Op: OpNe, Values: []string{"a"}}}, 21}, // includes id 0
		{"in", []Filter{{Key: "tier", Op: OpIn, Values: []string{"a", "b"}}}, 19},
		{"not_in", []Filter{{Key: "tier", Op: OpNotIn, Values: []string{"a", "b"}}}, 11},
		{"gt", []Filter{{Key: "price", Op: OpGt, Values: []string{"20"}}}, 9},
		{"gte", []Filter{{Key: "price", Op: OpGte, Values: []string{"20"}}}, 10},
		{"lt", []Filter{{Key: "price", Op: OpLt, Values: []string{"5"}}}, 4},
		{"lte", []Filter{{Key: "price", Op: OpLte, Values: []string{"5"}}}, 5},
		{"exists", []Filter{{Key: "price", Op: OpExists}}, 29},
		{"range and eq", []Filter{
			{Key: "price", Op: OpGte, Values: []string{"10"}},
			{Key: "price", Op: OpLt, Values: []string{"20"}},
			Eq("tier", "b"),
		}, 4}, // prices 10, 13, 16, 19
	}
	for _, c := range cases {
		if got := count(c.f...); got != c.want {
			t.Errorf("%s: %d matches, want %d", c.name, got, c.want)
		}
	}
	if _, err := e.Search(make([]float32, 8), 1, 0, []Filter{{Key: "price", Op: OpGt, Values: []string{"cheap"}}}); !errors.Is(err, ErrBadFilter) {
		t.Fatalf("non-numeric gt: got %v, want ErrBadFilter", err)
	}
}
