package bench

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Backend is anything that can be bulk-loaded and searched — implemented
// once for NuclaDB (over its own gRPC API) and once for Qdrant (over its
// REST API), so the exact same measurement code drives both and the
// resulting numbers are directly comparable.
type Backend interface {
	Name() string
	Upsert(vectors [][]float32) error
	Search(query []float32, topK, ef int) ([]uint64, error)
	// RSSBytes returns the backend process's current resident set size,
	// for the memory-footprint comparison.
	RSSBytes() (uint64, error)
}

// Point is one measurement at a given ef: recall@topK, QPS, and RSS at
// that point in the run.
type Point struct {
	EF       int
	Recall   float64
	QPS      float64
	RSSBytes uint64
}

// Report is a full run's results for one backend.
type Report struct {
	Backend       string
	NumVectors    int
	NumQueries    int
	Dim           int
	BuildDuration time.Duration
	BuildRSSBytes uint64
	Points        []Point
}

// Run bulk-loads base into backend, then measures recall@topK and QPS at
// every ef in efValues by running every query in queries sequentially
// (single connection — this is a latency/correctness benchmark, not a
// concurrency-throughput one; see docs/writeups for that distinction).
// groundtruth[i] must be query i's true nearest-neighbor ids, closest
// first, as loaded by LoadIvecs.
func Run(backend Backend, base, queries [][]float32, groundtruth [][]int32, efValues []int, topK int) (*Report, error) {
	buildStart := time.Now()
	if err := backend.Upsert(base); err != nil {
		return nil, fmt.Errorf("bench: %s upsert: %w", backend.Name(), err)
	}
	buildDuration := time.Since(buildStart)
	buildRSS, err := backend.RSSBytes()
	if err != nil {
		return nil, fmt.Errorf("bench: %s RSS after build: %w", backend.Name(), err)
	}

	report := &Report{
		Backend:       backend.Name(),
		NumVectors:    len(base),
		NumQueries:    len(queries),
		Dim:           len(base[0]),
		BuildDuration: buildDuration,
		BuildRSSBytes: buildRSS,
	}

	for _, ef := range efValues {
		var totalRecall float64
		start := time.Now()
		for i, q := range queries {
			ids, err := backend.Search(q, topK, ef)
			if err != nil {
				return nil, fmt.Errorf("bench: %s search at ef=%d: %w", backend.Name(), ef, err)
			}
			totalRecall += recallAt(ids, groundtruth[i], topK)
		}
		elapsed := time.Since(start)

		rss, err := backend.RSSBytes()
		if err != nil {
			return nil, fmt.Errorf("bench: %s RSS at ef=%d: %w", backend.Name(), ef, err)
		}

		report.Points = append(report.Points, Point{
			EF:       ef,
			Recall:   totalRecall / float64(len(queries)),
			QPS:      float64(len(queries)) / elapsed.Seconds(),
			RSSBytes: rss,
		})
	}
	return report, nil
}

func recallAt(got []uint64, want []int32, topK int) float64 {
	if len(want) > topK {
		want = want[:topK]
	}
	wantSet := make(map[int32]bool, len(want))
	for _, id := range want {
		wantSet[id] = true
	}
	hit := 0
	for _, id := range got {
		if wantSet[int32(id)] {
			hit++
		}
	}
	if len(want) == 0 {
		return 1
	}
	return float64(hit) / float64(len(want))
}

// rssBytesForPID shells out to `ps` for a process's current resident set
// size — portable across macOS and Linux, unlike /proc (macOS has none).
func rssBytesForPID(pid int) (uint64, error) {
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("ps -o rss= -p %d: %w", pid, err)
	}
	kb, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing ps output %q: %w", out, err)
	}
	return kb * 1024, nil
}
