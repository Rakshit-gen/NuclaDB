// Command compare runs the real head-to-head benchmark: build NuclaDB and
// Qdrant against the same dataset over their own network APIs, sweep the
// same ef values, and print recall/QPS/memory side by side. Nothing here
// is estimated — every number is measured by actually running both
// systems on this machine.
//
// Requires: a built nucladbd binary, a downloaded qdrant binary, and the
// siftsmall dataset. See bench/download.sh and bench/README.md.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/Rakshit-gen/nucladb/bench"
)

func main() {
	var (
		nucladbdBin = flag.String("nucladbd", "../../bin/nucladbd", "path to the nucladbd binary")
		qdrantBin   = flag.String("qdrant", "../../bench/.qdrant-bin/qdrant", "path to the qdrant binary")
		dataDir     = flag.String("data", "../../bench/data/siftsmall", "path to the extracted siftsmall dataset")
		topK        = flag.Int("top-k", 10, "k for recall@k")
		m           = flag.Int("m", 16, "HNSW M (bidirectional links per node)")
		efConstruct = flag.Int("ef-construct", 200, "HNSW build-time candidate list size")
	)
	flag.Parse()

	base, err := bench.LoadFvecs(*dataDir + "/siftsmall_base.fvecs")
	must(err)
	queries, err := bench.LoadFvecs(*dataDir + "/siftsmall_query.fvecs")
	must(err)
	groundtruth, err := bench.LoadIvecs(*dataDir + "/siftsmall_groundtruth.ivecs")
	must(err)
	dim := len(base[0])
	log.Printf("compare: loaded %d base vectors, %d queries, dim=%d", len(base), len(queries), dim)

	efValues := []int{10, 20, 50, 100, 200}

	nucladbDataDir, err := os.MkdirTemp("", "nucladb-bench-*")
	must(err)
	defer os.RemoveAll(nucladbDataDir)

	log.Println("compare: starting nucladbd...")
	nuclaBackend, err := bench.StartNuclaDB(*nucladbdBin, nucladbDataDir, dim, "l2", "127.0.0.1:19200")
	must(err)

	log.Println("compare: benchmarking NuclaDB...")
	nuclaReport, err := bench.Run(nuclaBackend, base, queries, groundtruth, efValues, *topK)
	must(err)
	must(nuclaBackend.Close())

	qdrantStorageDir, err := os.MkdirTemp("", "qdrant-bench-*")
	must(err)
	defer os.RemoveAll(qdrantStorageDir)

	log.Println("compare: starting qdrant...")
	qdrantBackend, err := bench.StartQdrant(*qdrantBin, qdrantStorageDir, dim, "Euclid", 19201, 19202, *m, *efConstruct)
	must(err)

	log.Println("compare: benchmarking Qdrant...")
	qdrantReport, err := bench.Run(qdrantBackend, base, queries, groundtruth, efValues, *topK)
	must(err)
	must(qdrantBackend.Close())

	printReport(nuclaReport, *topK)
	printReport(qdrantReport, *topK)
	printComparison(nuclaReport, qdrantReport, *topK)
	must(writeMarkdown(nuclaReport, qdrantReport, *topK))
}

func printReport(r *bench.Report, topK int) {
	fmt.Printf("\n=== %s ===\n", r.Backend)
	fmt.Printf("build: %d vectors, dim=%d, %s, RSS after build: %.1f MB\n",
		r.NumVectors, r.Dim, r.BuildDuration, float64(r.BuildRSSBytes)/1e6)
	fmt.Printf("%-6s %-12s %-12s %-10s\n", "ef", fmt.Sprintf("recall@%d", topK), "QPS", "RSS (MB)")
	for _, p := range r.Points {
		fmt.Printf("%-6d %-12.4f %-12.1f %-10.1f\n", p.EF, p.Recall, p.QPS, float64(p.RSSBytes)/1e6)
	}
}

func printComparison(a, b *bench.Report, topK int) {
	fmt.Printf("\n=== %s vs %s (recall@%d) ===\n", a.Backend, b.Backend, topK)
	fmt.Printf("%-6s %-12s %-12s %-12s %-12s\n", "ef", a.Backend+" recall", b.Backend+" recall", a.Backend+" QPS", b.Backend+" QPS")
	for i := range a.Points {
		pa, pb := a.Points[i], b.Points[i]
		fmt.Printf("%-6d %-12.4f %-12.4f %-12.1f %-12.1f\n", pa.EF, pa.Recall, pb.Recall, pa.QPS, pb.QPS)
	}
}

// errWriter accumulates the first write error across many sequential
// Fprintf calls, so each call site doesn't need its own error check —
// checked once at the end instead of fifteen times inline.
type errWriter struct {
	w   io.Writer
	err error
}

func (ew *errWriter) printf(format string, args ...any) {
	if ew.err != nil {
		return
	}
	_, ew.err = fmt.Fprintf(ew.w, format, args...)
}

func writeMarkdown(a, b *bench.Report, topK int) error {
	path := "results.md"
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer f.Close()

	ew := &errWriter{w: f}
	ew.printf("# NuclaDB vs Qdrant: SIFT-small benchmark\n\n")
	ew.printf("Real measurements from running both systems over their own network APIs ")
	ew.printf("on the same machine, same dataset, same recall@%d target — not estimates.\n\n", topK)
	ew.printf("%d base vectors, %d queries, dim=%d.\n\n", a.NumVectors, a.NumQueries, a.Dim)

	ew.printf("## Build\n\n")
	ew.printf("| Backend | Build time | RSS after build |\n|---|---|---|\n")
	ew.printf("| %s | %s | %.1f MB |\n", a.Backend, a.BuildDuration, float64(a.BuildRSSBytes)/1e6)
	ew.printf("| %s | %s | %.1f MB |\n\n", b.Backend, b.BuildDuration, float64(b.BuildRSSBytes)/1e6)

	ew.printf("## Recall / QPS / memory vs ef\n\n")
	ew.printf("| ef | %s recall@%d | %s recall@%d | %s QPS | %s QPS | %s RSS | %s RSS |\n",
		a.Backend, topK, b.Backend, topK, a.Backend, b.Backend, a.Backend, b.Backend)
	ew.printf("|---|---|---|---|---|---|---|\n")
	for i := range a.Points {
		pa, pb := a.Points[i], b.Points[i]
		ew.printf("| %d | %.4f | %.4f | %.1f | %.1f | %.1f MB | %.1f MB |\n",
			pa.EF, pa.Recall, pb.Recall, pa.QPS, pb.QPS, float64(pa.RSSBytes)/1e6, float64(pb.RSSBytes)/1e6)
	}

	ew.printf("\n## Notes\n\n")
	ew.printf("- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** ")
	ew.printf("Its default (10,000 KB) is comfortably above this dataset's raw size (~%.0f KB), which means ", float64(a.NumVectors*a.Dim*4)/1000)
	ew.printf("an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. ")
	ew.printf("Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.\n")
	ew.printf("- **Build time is the standout gap.** %s's WAL fsyncs on every single write for crash-safety durability; ", a.Backend)
	ew.printf("%s batches durability differently, hence the build-time difference above. This is a genuine, unhidden weakness — see docs/writeups.\n", b.Backend)
	ew.printf("- At only %d vectors, recall for both engines converges close to 1.0 by moderate ef — a real recall/QPS tradeoff separation ", a.NumVectors)
	ew.printf("is more visible at larger scale (SIFT1M); rerunning there is documented future work, not run here due to build-time cost at this fsync-per-write rate.\n")

	if ew.err != nil {
		return fmt.Errorf("writing %s: %w", path, ew.err)
	}
	log.Printf("compare: wrote %s", path)
	return nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
