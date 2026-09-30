# Benchmarks

`results.md` in this directory is a real, committed benchmark run against
Qdrant; `results-cluster.md` is a real, committed run of NuclaDB
single-node vs. a real multi-node NuclaDB cluster. This file explains how
to reproduce both.

## Reproducing: NuclaDB vs Qdrant

```sh
./download.sh                              # fetches siftsmall dataset + a native Qdrant binary
go build -o ../bin/nucladbd ../cmd/nucladbd  # from bench/
cd cmd/compare
go run . -nucladbd=../../../bin/nucladbd -qdrant=../../.qdrant-bin/qdrant -data=../../data/siftsmall
```

This starts a real `nucladbd` and a real `qdrant` as subprocesses, loads
the same 10,000-vector SIFT dataset into both over their own network APIs
(gRPC for NuclaDB, REST for Qdrant), sweeps the same `ef` values, and
measures recall@10 against the dataset's official groundtruth, QPS, and
RSS memory for both. It writes `results.md` and prints the same tables to
stdout.

## Reproducing: NuclaDB single-node vs multi-node cluster

```sh
go build -o ../bin/nucladbd ../cmd/nucladbd  # from bench/, if not already built
cd cmd/compare-cluster
go run . -nucladbd=../../../bin/nucladbd -data=../../data/siftsmall -shards=4
```

This runs the same dataset and `ef` sweep against a single `nucladbd`
process, then against a real 4-process cluster (one `nucladbd` per shard)
addressed through the real scatter-gather router
(`internal/cluster/router`) — not an in-process shortcut. It writes
`results-cluster.md` and prints the same tables to stdout.

## Reproducing: concurrent load test

```sh
go build -o ../bin/nucladbd ../cmd/nucladbd  # from bench/, if not already built
go run ./cmd/loadtest
```

Starts a real `nucladbd`, bulk-loads siftsmall, then sweeps concurrency
levels running a closed-loop Search load (plus a mixed read/write run and
a sustained run) over a pool of gRPC connections. Writes
`results-loadtest.md`.

## What's not committed, and why

- `data/` — the SIFT-small corpus (texmex.irisa.fr), a third-party
  dataset, ~18MB extracted.
- `.qdrant-bin/` — a platform-specific Qdrant binary, 70MB+.

Both are fetched by `download.sh` and are `.gitignore`d.

## Design notes

- **Both systems are benchmarked over their real network APIs**, not an
  in-process shortcut through NuclaDB's graph — the numbers reflect what a
  real client actually experiences, including (de)serialization and
  network round-trips.
- **Qdrant's `full_scan_threshold` is explicitly forced to HNSW** (see
  `qdrant_backend.go`). Its default lets small collections silently fall
  back to exact brute-force search — undetected, that would have made
  this an exact-vs-approximate comparison rather than HNSW-vs-HNSW. This
  was caught by noticing suspiciously perfect 1.0 recall at every `ef` on
  the first run; see `docs/writeups/` for the full story.
- **Qdrant's `indexing_threshold` is set to 1 KB and build time waits for
  indexing.** The `full_scan_threshold` fix alone wasn't enough: Qdrant
  also skips building an index for segments under `indexing_threshold`
  (10,000 KB by default), so earlier runs timed point ingest against
  NuclaDB's full HNSW build, and Qdrant's searches were still brute force.
  The harness now polls until `indexed_vectors_count` covers every vector.
- **`cmd/compare`'s QPS is single-connection and sequential** — it
  measures latency-bound throughput. For saturated concurrent throughput
  and tail latency, `cmd/loadtest` runs a closed-loop load generator (a
  pool of gRPC connections, N workers, fixed window, p50/p90/p99/max)
  against the same real `nucladbd`; results in `results-loadtest.md`.
- Recall/QPS results at only 10,000 vectors converge close to 1.0 for both
  engines by moderate `ef` — see `results.md`'s Notes section for why a
  larger run (SIFT1M) would show a more separated tradeoff curve, and why
  that wasn't run here (NuclaDB's fsync-per-write WAL makes a 1M-vector
  build impractically slow in its current form — itself one of the
  benchmark's most useful findings).
