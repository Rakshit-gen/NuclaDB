// Command compare-cluster re-runs the bench harness against a real
// multi-node NuclaDB cluster (numShards independent nucladbd processes
// behind the real scatter-gather router) and compares it to the same
// single-node run from bench/cmd/compare, on the same dataset. Nothing
// here is estimated — every number is measured by actually running both
// topologies on this machine.
//
// Requires: a built nucladbd binary and the siftsmall dataset. See
// bench/download.sh and bench/README.md.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Rakshit-gen/nucladb/bench"
)

func main() {
	var (
		nucladbdBin = flag.String("nucladbd", "../../bin/nucladbd", "path to the nucladbd binary")
		dataDir     = flag.String("data", "../../bench/data/siftsmall", "path to the extracted siftsmall dataset")
		topK        = flag.Int("top-k", 10, "k for recall@k")
		numShards   = flag.Int("shards", 4, "number of shards in the cluster run")
		basePort    = flag.Int("base-port", 19300, "first gRPC port the cluster's shards bind, one port per shard")
	)
	flag.Parse()

	base, err := bench.LoadFvecs(*dataDir + "/siftsmall_base.fvecs")
	must(err)
	queries, err := bench.LoadFvecs(*dataDir + "/siftsmall_query.fvecs")
	must(err)
	groundtruth, err := bench.LoadIvecs(*dataDir + "/siftsmall_groundtruth.ivecs")
	must(err)
	dim := len(base[0])
	log.Printf("compare-cluster: loaded %d base vectors, %d queries, dim=%d", len(base), len(queries), dim)

	efValues := []int{10, 20, 50, 100, 200}

	singleDataDir, err := os.MkdirTemp("", "nucladb-bench-single-*")
	must(err)
	defer os.RemoveAll(singleDataDir)

	log.Println("compare-cluster: starting single-node nucladbd...")
	singleBackend, err := bench.StartNuclaDB(*nucladbdBin, singleDataDir, dim, "l2", "127.0.0.1:19250")
	must(err)

	log.Println("compare-cluster: benchmarking single-node NuclaDB...")
	singleReport, err := bench.Run(singleBackend, base, queries, groundtruth, efValues, *topK)
	must(err)
	must(singleBackend.Close())

	clusterDataDir, err := os.MkdirTemp("", "nucladb-bench-cluster-*")
	must(err)
	defer os.RemoveAll(clusterDataDir)

	log.Printf("compare-cluster: starting %d-shard nucladbd cluster...", *numShards)
	clusterBackend, err := bench.StartNuclaDBCluster(*nucladbdBin, clusterDataDir, dim, "l2", *numShards, *basePort)
	must(err)

	log.Println("compare-cluster: benchmarking NuclaDB cluster...")
	clusterReport, err := bench.Run(clusterBackend, base, queries, groundtruth, efValues, *topK)
	must(err)
	must(clusterBackend.Close())

	printReport(singleReport, *topK)
	printReport(clusterReport, *topK)
	printComparison(singleReport, clusterReport, *topK)
	must(writeMarkdown(singleReport, clusterReport, *numShards, *topK))
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

func writeMarkdown(single, cluster *bench.Report, numShards, topK int) error {
	path := "results-cluster.md"
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer f.Close()

	w := func(format string, args ...any) {
		if err != nil {
			return
		}
		_, err = fmt.Fprintf(f, format, args...)
	}

	w("# NuclaDB single-node vs %d-shard cluster: SIFT-small benchmark\n\n", numShards)
	w("Real measurements from running a single nucladbd process against a real %d-shard cluster ", numShards)
	w("(independent nucladbd processes behind the real scatter-gather router in internal/cluster/router), ")
	w("same dataset, same recall@%d target — not estimates.\n\n", topK)
	w("%d base vectors, %d queries, dim=%d.\n\n", single.NumVectors, single.NumQueries, single.Dim)

	w("## Build\n\n")
	w("| Topology | Build time | Total RSS after build |\n|---|---|---|\n")
	w("| %s | %s | %.1f MB |\n", single.Backend, single.BuildDuration, float64(single.BuildRSSBytes)/1e6)
	w("| %s | %s | %.1f MB |\n\n", cluster.Backend, cluster.BuildDuration, float64(cluster.BuildRSSBytes)/1e6)

	w("## Recall / QPS / memory vs ef\n\n")
	w("| ef | %s recall@%d | %s recall@%d | %s QPS | %s QPS | %s RSS | %s RSS |\n",
		single.Backend, topK, cluster.Backend, topK, single.Backend, cluster.Backend, single.Backend, cluster.Backend)
	w("|---|---|---|---|---|---|---|\n")
	for i := range single.Points {
		ps, pc := single.Points[i], cluster.Points[i]
		w("| %d | %.4f | %.4f | %.1f | %.1f | %.1f MB | %.1f MB |\n",
			ps.EF, ps.Recall, pc.Recall, ps.QPS, pc.QPS, float64(ps.RSSBytes)/1e6, float64(pc.RSSBytes)/1e6)
	}

	w("\n## Notes\n\n")
	w("- **Build (insert) is single-vector RPCs through the router, not batched.** Unlike the single-node path's ")
	w("`BatchUpsert`, `internal/cluster/router.Router` has no batch-insert API yet — every vector is its own round trip ")
	w("to whichever shard owns it, run over a bounded worker pool (32 concurrent) purely so a %d-vector build finishes ", single.NumVectors)
	w("in reasonable time, not to hide the per-RPC cost. This is a real, unhidden gap versus the single-node path.\n")
	w("- **Search fans out to every shard and merges.** `Router.Search` queries all %d shards concurrently per request ", numShards)
	w("and merges each shard's own top-k into one globally-ranked top-k — recall should track the single-node numbers ")
	w("closely (sharding by id doesn't change which vectors exist, only where), while QPS reflects added network hops ")
	w("(client -> router -> N shards) and per-shard candidate lists shrinking as vectors spread across more processes.\n")
	w("- **RSS is summed across all shard processes**, so it's the cluster's total memory footprint, not comparable ")
	w("1:1 to a single process's number without accounting for %d processes' worth of fixed overhead (goroutine stacks, ", numShards)
	w("gRPC server state, OS-level per-process baseline) on top of the actual vector data.\n")

	if err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	log.Printf("compare-cluster: wrote %s", path)
	return nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
