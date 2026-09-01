//go:build js && wasm

// Command wasm compiles NuclaDB's real HNSW graph (internal/index/hnsw) to
// WebAssembly, unmodified, so the browser playground on the site runs the
// actual engine instead of simulating it. Build with:
//
//	GOOS=js GOARCH=wasm go build -o nucladb.wasm ./cmd/wasm
package main

import (
	"encoding/json"
	"math/rand"
	"sort"
	"syscall/js"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

var (
	graph         *hnsw.Graph
	currentMetric hnsw.Metric
	dim           int
	nextID        uint64
	vectors       = map[uint64][]float32{}
)

func jsonResult(v any) js.Value {
	b, err := json.Marshal(v)
	if err != nil {
		return js.ValueOf(`{"error":"` + err.Error() + `"}`)
	}
	return js.ValueOf(string(b))
}

func errResult(err error) js.Value {
	return jsonResult(map[string]any{"error": err.Error()})
}

func metricByName(name string) hnsw.Metric {
	switch name {
	case "l2":
		return hnsw.L2()
	case "dot":
		return hnsw.Dot()
	default:
		return hnsw.Cosine()
	}
}

func randomVector(rng *rand.Rand) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = float32(rng.NormFloat64())
	}
	return v
}

// reset(configJSON) creates a fresh graph. configJSON: {dim,m,efConstruction,metric}.
func reset(_ js.Value, args []js.Value) any {
	defer func() { recover() }()
	if len(args) < 1 {
		return errResult(errMissingArg)
	}
	var opts struct {
		Dim            int    `json:"dim"`
		M              int    `json:"m"`
		EfConstruction int    `json:"efConstruction"`
		Metric         string `json:"metric"`
	}
	if err := json.Unmarshal([]byte(args[0].String()), &opts); err != nil {
		return errResult(err)
	}
	dim = opts.Dim
	currentMetric = metricByName(opts.Metric)
	graph = hnsw.New(hnsw.Config{
		Dim:            dim,
		M:              opts.M,
		EfConstruction: opts.EfConstruction,
		Metric:         currentMetric,
	})
	vectors = make(map[uint64][]float32)
	nextID = 0
	return jsonResult(map[string]any{"ok": true})
}

// insertRandom(count) inserts count fresh random vectors, run through the
// real Graph.Insert path, and reports wall-clock build time measured inside
// the compiled engine itself.
func insertRandom(_ js.Value, args []js.Value) any {
	defer func() { recover() }()
	if graph == nil {
		return errResult(errNotInitialized)
	}
	count := args[0].Int()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	start := time.Now()
	for i := 0; i < count; i++ {
		v := randomVector(rng)
		id := nextID
		nextID++
		vectors[id] = v
		if err := graph.Insert(id, v); err != nil {
			return errResult(err)
		}
	}
	elapsed := time.Since(start)

	return jsonResult(map[string]any{
		"insertedCount": count,
		"total":         graph.Len(),
		"elapsedMs":     msFloat(elapsed),
	})
}

// searchRandom(topK, ef) draws a fresh random query vector, runs it through
// the real Graph.Search, and separately computes an exact brute-force
// nearest-neighbor set over every stored vector to report real recall@topK,
// the same way the site's own benchmark pages do.
func searchRandom(_ js.Value, args []js.Value) any {
	defer func() { recover() }()
	if graph == nil {
		return errResult(errNotInitialized)
	}
	topK := args[0].Int()
	ef := args[1].Int()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	query := randomVector(rng)

	start := time.Now()
	results, err := graph.Search(query, topK, ef)
	searchElapsed := time.Since(start)
	if err != nil {
		return errResult(err)
	}

	bfStart := time.Now()
	type scored struct {
		id   uint64
		dist float32
	}
	all := make([]scored, 0, len(vectors))
	for id, v := range vectors {
		all = append(all, scored{id, currentMetric.Distance(query, v)})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].dist < all[j].dist })
	bfElapsed := time.Since(bfStart)

	truth := map[uint64]bool{}
	limit := topK
	if limit > len(all) {
		limit = len(all)
	}
	for i := 0; i < limit; i++ {
		truth[all[i].id] = true
	}
	hits := 0
	for _, r := range results {
		if truth[r.ID] {
			hits++
		}
	}
	recall := 0.0
	if limit > 0 {
		recall = float64(hits) / float64(limit)
	}

	out := make([]map[string]any, len(results))
	for i, r := range results {
		out[i] = map[string]any{"id": r.ID, "distance": r.Distance}
	}

	return jsonResult(map[string]any{
		"results":         out,
		"searchElapsedMs": msFloat(searchElapsed),
		"bruteForceMs":    msFloat(bfElapsed),
		"recall":          recall,
		"topK":            limit,
		"corpusSize":      len(vectors),
	})
}

func msFloat(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

var (
	errMissingArg      = jsonErr("missing argument")
	errNotInitialized  = jsonErr("graph not initialized: call reset first")
)

type jsonErr string

func (e jsonErr) Error() string { return string(e) }

func main() {
	js.Global().Set("nuclaReset", js.FuncOf(reset))
	js.Global().Set("nuclaInsertRandom", js.FuncOf(insertRandom))
	js.Global().Set("nuclaSearchRandom", js.FuncOf(searchRandom))
	js.Global().Set("nuclaReady", js.ValueOf(true))
	select {}
}
