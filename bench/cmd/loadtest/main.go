// Command loadtest measures NuclaDB's saturated concurrent throughput and
// tail latency over its real gRPC API — the concurrent-client counterpart
// to bench/cmd/compare's single-connection, latency-bound QPS run (see
// bench/README.md "Design notes").
//
// It starts a real nucladbd subprocess, bulk-loads the siftsmall dataset,
// then runs a closed-loop load generator: N goroutines each loop issuing
// Search RPCs (optionally interleaving Insert writes) for a fixed wall
// clock window, and reports req/s and p50/p90/p99/max latency at each
// concurrency level. Nothing here is estimated.
//
// Requires: a built nucladbd binary and the siftsmall dataset. Run from
// bench/: `go run ./cmd/loadtest`. See bench/download.sh and bench/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Rakshit-gen/nucladb/bench"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// grpcAddr is where the load test's nucladbd subprocess listens; the
// worker connection pool dials it directly.
const grpcAddr = "127.0.0.1:19400"

func main() {
	var (
		nucladbdBin = flag.String("nucladbd", "../bin/nucladbd", "path to the nucladbd binary (default assumes running from bench/)")
		dataDir     = flag.String("data", "./data/siftsmall", "path to the extracted siftsmall dataset")
		connsCSV    = flag.String("conns", "1,8,16,32,64,128", "comma-separated concurrency levels to sweep")
		duration    = flag.Duration("duration", 15*time.Second, "measured window per concurrency level")
		warmup      = flag.Duration("warmup", 3*time.Second, "unmeasured warmup before each window")
		sustain     = flag.Duration("sustain", 60*time.Second, "one long read-only run at the top concurrency level (0 to skip)")
		ef          = flag.Int("ef", 100, "HNSW efSearch (100 = recall@10 1.0 on this dataset per bench/results.md)")
		topK        = flag.Int("top-k", 10, "k for the search")
		writeMix    = flag.Float64("write-mix", 0.1, "fraction of ops that are Inserts in the mixed run at the top level (0 to skip)")
		clientConns = flag.Int("client-conns", 16, "gRPC client connections the workers round-robin over (one shared conn serializes on a single HTTP/2 write loop)")
	)
	flag.Parse()

	base, err := bench.LoadFvecs(*dataDir + "/siftsmall_base.fvecs")
	must(err)
	queries, err := bench.LoadFvecs(*dataDir + "/siftsmall_query.fvecs")
	must(err)
	groundtruth, err := bench.LoadIvecs(*dataDir + "/siftsmall_groundtruth.ivecs")
	must(err)
	dim := len(base[0])
	log.Printf("loadtest: loaded %d base vectors, %d queries, dim=%d", len(base), len(queries), dim)

	conns := parseConns(*connsCSV)

	dataDirTmp, err := os.MkdirTemp("", "nucladb-loadtest-*")
	must(err)
	defer os.RemoveAll(dataDirTmp)

	log.Println("loadtest: starting nucladbd...")
	be, err := bench.StartNuclaDB(*nucladbdBin, dataDirTmp, dim, "l2", grpcAddr)
	must(err)
	defer be.Close()

	log.Printf("loadtest: bulk-loading %d vectors (fsync-per-write WAL, expect ~40s)...", len(base))
	loadStart := time.Now()
	must(be.Upsert(base))
	log.Printf("loadtest: loaded in %s", time.Since(loadStart).Round(time.Millisecond))

	recall := measureRecall(be, queries, groundtruth, *topK, *ef)
	rss, _ := be.RSSBytes()

	clients, closePool := dialPool(grpcAddr, *clientConns)
	defer closePool()

	fmt.Printf("\nNuclaDB concurrent load test\n")
	fmt.Printf("%d vectors dim=%d  ef=%d  top-k=%d  recall@%d=%.4f  server RSS %.1f MB\n",
		len(base), dim, *ef, *topK, *topK, recall, float64(rss)/1e6)
	fmt.Printf("closed-loop, %s window (%s warmup), %d gRPC client connections\n",
		*duration, *warmup, *clientConns)
	fmt.Printf("%s  %s\n\n", runtime.Version(), time.Now().Format(time.RFC3339))

	var rows []row
	fmt.Printf("%-8s %10s %10s %10s %10s %10s %8s\n", "conns", "req/s", "p50", "p90", "p99", "max", "errors")
	for _, c := range conns {
		r := runLoad(clients, queries, base, c, *ef, *topK, 0, *warmup, *duration)
		rows = append(rows, r)
		fmt.Printf("%-8d %10.0f %10s %10s %10s %10s %8d\n",
			c, r.rps, d(r.p50), d(r.p90), d(r.p99), d(r.max), r.errs)
	}

	topConns := conns[len(conns)-1]

	var mixed *row
	if *writeMix > 0 {
		fmt.Printf("\nmixed read/write at conns=%d (%.0f%% Insert):\n", topConns, *writeMix*100)
		r := runLoad(clients, queries, base, topConns, *ef, *topK, *writeMix, *warmup, *duration)
		mixed = &r
		fmt.Printf("  reads  %8.0f req/s  p50 %s  p99 %s\n", r.readRPS, d(r.readP50), d(r.readP99))
		fmt.Printf("  writes %8.0f req/s  p50 %s  p99 %s\n", r.writeRPS, d(r.writeP50), d(r.writeP99))
		fmt.Printf("  errors %d\n", r.errs)
	}

	var long *row
	if *sustain > 0 {
		fmt.Printf("\nsustained read-only at conns=%d for %s:\n", topConns, *sustain)
		r := runLoad(clients, queries, base, topConns, *ef, *topK, 0, *warmup, *sustain)
		long = &r
		fmt.Printf("  %8.0f req/s  p50 %s  p90 %s  p99 %s  p99.9 %s  max %s  errors %d\n",
			r.rps, d(r.p50), d(r.p90), d(r.p99), d(r.p999), d(r.max), r.errs)
	}

	must(writeMarkdown(len(base), dim, *ef, *topK, recall, rss, *duration, rows, mixed, long, topConns, *writeMix, *sustain))
}

// row is one concurrency level's measured result.
type row struct {
	conns                    int
	rps                      float64
	p50, p90, p99, p999, max time.Duration
	errs                     int64
	readRPS, writeRPS        float64
	readP50, readP99         time.Duration
	writeP50, writeP99       time.Duration
}

// runLoad runs `conns` closed-loop workers for warmup+window, each pinned
// to one connection from the pool round-robin; only ops that start after
// warmup are recorded. writeMix in (0,1] makes that fraction of ops
// Inserts (new ids past the loaded range) instead of Searches.
func runLoad(clients []pb.NuclaDBClient, queries, base [][]float32, conns, ef, topK int, writeMix float64, warmup, window time.Duration) row {
	var (
		measuring atomic.Bool
		errs      atomic.Int64
		nextID    atomic.Uint64
		wg        sync.WaitGroup
	)
	nextID.Store(uint64(len(base)))
	readLat := make([][]time.Duration, conns)
	writeLat := make([][]time.Duration, conns)

	stop := make(chan struct{})
	for w := 0; w < conns; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			client := clients[w%len(clients)]
			rng := rand.New(rand.NewSource(int64(w) + 1))
			rl := make([]time.Duration, 0, 4096)
			wl := make([]time.Duration, 0, 1024)
			for {
				select {
				case <-stop:
					readLat[w], writeLat[w] = rl, wl
					return
				default:
				}
				isWrite := writeMix > 0 && rng.Float64() < writeMix
				t0 := time.Now()
				var err error
				if isWrite {
					id := nextID.Add(1)
					err = insertRPC(client, id, base[rng.Intn(len(base))])
				} else {
					err = searchRPC(client, queries[rng.Intn(len(queries))], topK, ef)
				}
				elapsed := time.Since(t0)
				if err != nil {
					errs.Add(1)
					continue
				}
				if measuring.Load() {
					if isWrite {
						wl = append(wl, elapsed)
					} else {
						rl = append(rl, elapsed)
					}
				}
			}
		}(w)
	}

	time.Sleep(warmup)
	measuring.Store(true)
	measureStart := time.Now()
	time.Sleep(window)
	measuring.Store(false)
	measured := time.Since(measureStart)
	close(stop)
	wg.Wait()

	var reads, writes []time.Duration
	for w := 0; w < conns; w++ {
		reads = append(reads, readLat[w]...)
		writes = append(writes, writeLat[w]...)
	}
	all := append(append([]time.Duration(nil), reads...), writes...)

	r := row{
		conns: conns,
		rps:   float64(len(all)) / measured.Seconds(),
		errs:  errs.Load(),
		p50:   pct(all, 0.50), p90: pct(all, 0.90),
		p99: pct(all, 0.99), p999: pct(all, 0.999), max: pct(all, 1.0),
	}
	if writeMix > 0 {
		r.readRPS = float64(len(reads)) / measured.Seconds()
		r.writeRPS = float64(len(writes)) / measured.Seconds()
		r.readP50, r.readP99 = pct(reads, 0.50), pct(reads, 0.99)
		r.writeP50, r.writeP99 = pct(writes, 0.50), pct(writes, 0.99)
	}
	return r
}

func pct(xs []time.Duration, q float64) time.Duration {
	if len(xs) == 0 {
		return -1
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	i := int(q * float64(len(xs)-1))
	return xs[i]
}

// measureRecall runs every query once, single-threaded, at ef and reports
// mean recall@topK against the dataset's official groundtruth — so the
// throughput numbers below are anchored to a known accuracy point.
func measureRecall(be *bench.NuclaDBBackend, queries [][]float32, groundtruth [][]int32, topK, ef int) float64 {
	var total float64
	for i, q := range queries {
		got, err := be.Search(q, topK, ef)
		must(err)
		want := groundtruth[i]
		if len(want) > topK {
			want = want[:topK]
		}
		set := make(map[int32]bool, len(want))
		for _, id := range want {
			set[id] = true
		}
		hit := 0
		for _, id := range got {
			if set[int32(id)] {
				hit++
			}
		}
		if len(want) > 0 {
			total += float64(hit) / float64(len(want))
		} else {
			total++
		}
	}
	return total / float64(len(queries))
}

// dialPool opens n gRPC connections to addr and returns a client per
// connection plus a close func. grpc-go drives each ClientConn from a
// single HTTP/2 write-loop goroutine, so one connection caps concurrent
// throughput regardless of how many workers share it — a real high-QPS
// client pools connections, and so does this benchmark.
func dialPool(addr string, n int) ([]pb.NuclaDBClient, func()) {
	if n < 1 {
		n = 1
	}
	conns := make([]*grpc.ClientConn, n)
	clients := make([]pb.NuclaDBClient, n)
	for i := range conns {
		c, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		must(err)
		conns[i] = c
		clients[i] = pb.NewNuclaDBClient(c)
	}
	return clients, func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}
}

func searchRPC(c pb.NuclaDBClient, query []float32, topK, ef int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Search(ctx, &pb.SearchRequest{Query: query, TopK: int32(topK), EfSearch: int32(ef)})
	return err
}

func insertRPC(c pb.NuclaDBClient, id uint64, vec []float32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{Id: strconv.FormatUint(id, 10), Values: vec}})
	return err
}

func parseConns(csv string) []int {
	var out []int
	for _, s := range strings.Split(csv, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			log.Fatalf("loadtest: bad -conns entry %q", s)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out
}

func d(x time.Duration) string {
	if x < 0 {
		return "n/a"
	}
	return x.Round(time.Microsecond).String()
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func writeMarkdown(nVec, dim, ef, topK int, recall float64, rss uint64, window time.Duration, rows []row, mixed, long *row, topConns int, writeMix float64, sustain time.Duration) error {
	path := "results-loadtest.md"
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	p := func(format string, a ...any) { fmt.Fprintf(f, format, a...) }
	p("# NuclaDB concurrent load test\n\n")
	p("Saturated concurrent throughput and tail latency over the real gRPC API — ")
	p("the closed-loop counterpart to `results.md`'s single-connection QPS. Every ")
	p("number is measured by running a real `nucladbd` on this machine, not estimated.\n\n")
	p("- **%d vectors**, dim=%d, metric l2, `ef=%d`, `top-k=%d` — recall@%d = **%.4f** ", nVec, dim, ef, topK, topK, recall)
	p("against the dataset's official groundtruth (so throughput is anchored to a known accuracy point)\n")
	p("- Load generator and server share this machine; `N` closed-loop goroutines round-robin over a pool of gRPC client connections\n")
	p("- Server RSS after load: %.1f MB\n", float64(rss)/1e6)
	p("- %s measured window, 3s warmup, per level\n\n", window)

	p("## Search throughput vs concurrency\n\n")
	p("| conns | req/s | p50 | p90 | p99 | max | errors |\n|---|---|---|---|---|---|---|\n")
	for _, r := range rows {
		p("| %d | %.0f | %s | %s | %s | %s | %d |\n", r.conns, r.rps, d(r.p50), d(r.p90), d(r.p99), d(r.max), r.errs)
	}

	if mixed != nil {
		p("\n## Mixed read/write at conns=%d (%.0f%% Insert)\n\n", topConns, writeMix*100)
		p("Inserts run one at a time. Each one finds its neighbors without the graph write lock and takes it only to link the new node in, so searches wait for that step alone (see the `hnsw.Graph` doc comment).\n\n")
		p("| op | req/s | p50 | p99 |\n|---|---|---|---|\n")
		p("| search | %.0f | %s | %s |\n", mixed.readRPS, d(mixed.readP50), d(mixed.readP99))
		p("| insert | %.0f | %s | %s |\n", mixed.writeRPS, d(mixed.writeP50), d(mixed.writeP99))
		p("\nErrors: %d\n", mixed.errs)
	}

	if long != nil {
		p("\n## Sustained read-only at conns=%d for %s\n\n", topConns, sustain)
		p("| req/s | p50 | p90 | p99 | p99.9 | max | errors |\n|---|---|---|---|---|---|---|\n")
		p("| %.0f | %s | %s | %s | %s | %s | %d |\n", long.rps, d(long.p50), d(long.p90), d(long.p99), d(long.p999), d(long.max), long.errs)
	}

	p("\n## Reproduce\n\nFrom `bench/`:\n\n```sh\ngo build -o ../bin/nucladbd ../cmd/nucladbd\ngo run ./cmd/loadtest -nucladbd=../bin/nucladbd -data=./data/siftsmall\n```\n\nNumbers vary ~10%% run to run — the load generator, server, and mock all\nshare one machine. This file is one representative run; medians of three\nare in the project README.\n")
	log.Printf("loadtest: wrote %s", path)
	return nil
}
