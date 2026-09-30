package bench

import (
	"fmt"
	"os/exec"
	"sort"
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

// Point is one measurement at a given ef. QPS is the median over the
// run's passes, with the slowest and fastest pass alongside it; P50 and P95
// are per-query latencies pooled across every pass.
type Point struct {
	EF       int
	Recall   float64
	QPS      float64
	QPSMin   float64
	QPSMax   float64
	P50      time.Duration
	P95      time.Duration
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
// every ef in efValues by running every query in queries sequentially,
// iterations times over (one pass is noisy on a shared machine)
// (single connection — this is a latency/correctness benchmark, not a
// concurrency-throughput one; see docs/writeups for that distinction).
// groundtruth[i] must be query i's true nearest-neighbor ids, closest
// first, as loaded by LoadIvecs.
func Run(backend Backend, base, queries [][]float32, groundtruth [][]int32, efValues []int, topK, iterations int) (*Report, error) {
	iterations = max(iterations, 1)
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
		qps := make([]float64, 0, iterations)
		latencies := make([]time.Duration, 0, iterations*len(queries))
		// One unmeasured pass first, so the first measured one doesn't pay
		// for cold caches.
		for _, q := range queries {
			if _, err := backend.Search(q, topK, ef); err != nil {
				return nil, fmt.Errorf("bench: %s warmup at ef=%d: %w", backend.Name(), ef, err)
			}
		}
		for pass := 0; pass < iterations; pass++ {
			start := time.Now()
			for i, q := range queries {
				t0 := time.Now()
				ids, err := backend.Search(q, topK, ef)
				if err != nil {
					return nil, fmt.Errorf("bench: %s search at ef=%d: %w", backend.Name(), ef, err)
				}
				latencies = append(latencies, time.Since(t0))
				if pass == 0 {
					totalRecall += recallAt(ids, groundtruth[i], topK)
				}
			}
			qps = append(qps, float64(len(queries))/time.Since(start).Seconds())
		}
		sort.Float64s(qps)
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		rss, err := backend.RSSBytes()
		if err != nil {
			return nil, fmt.Errorf("bench: %s RSS at ef=%d: %w", backend.Name(), ef, err)
		}

		report.Points = append(report.Points, Point{
			EF:       ef,
			Recall:   totalRecall / float64(len(queries)),
			QPS:      qps[len(qps)/2],
			QPSMin:   qps[0],
			QPSMax:   qps[len(qps)-1],
			P50:      latencies[len(latencies)*50/100],
			P95:      latencies[len(latencies)*95/100],
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

// QPSCell formats the median QPS with the spread across passes, e.g. "5214 (4980-5390)".
func (p Point) QPSCell() string {
	return fmt.Sprintf("%.0f (%.0f-%.0f)", p.QPS, p.QPSMin, p.QPSMax)
}

// LatencyCell formats p50 / p95 latency in milliseconds.
func (p Point) LatencyCell() string {
	return fmt.Sprintf("%.2f / %.2f ms", float64(p.P50)/1e6, float64(p.P95)/1e6)
}
