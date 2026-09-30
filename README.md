# NuclaDB

A vector search engine written from scratch in Go: HNSW indexing, product
quantization, a crash-safe write-ahead log, mmap-backed snapshot
persistence, tenant-isolated multi-tenancy with quotas and rate limiting,
OpenTelemetry tracing, Prometheus metrics, a gRPC + REST API, and a CLI,
benchmarked head-to-head against a real Qdrant instance, not a wrapper
around one.

Every number in this README and in `bench/results.md` comes from actually
running the code. Where NuclaDB loses to Qdrant, that's reported too: see
[Benchmarks](#benchmarks) and `docs/writeups/`.

## Why this exists

Most "vector database" side projects wrap an existing engine (Qdrant,
Pinecone, pgvector) behind an app. NuclaDB is the other direction: the
internals those engines are built from, implemented and tested directly
(the HNSW graph, the durability layer, the compression, the multi-tenant
isolation), so the interesting engineering is in this repo, not imported
from one.

## Architecture

```
                     ┌─────────────────────────┐
   gRPC (:9090) ───▶ │                         │
                     │   internal/api/grpc     │
   REST  (:8080) ──▶ │   internal/api/gateway  │
                     │                         │
                     └───────────┬─────────────┘
                                 │
                     ┌───────────▼─────────────┐
                     │   internal/engine.Store  │  tenant routing,
                     │                          │  quotas, rate limits
                     └───────────┬─────────────┘
                                 │  one Engine per tenant
                     ┌───────────▼─────────────┐
                     │   internal/engine.Engine │
                     │                          │
                     │  ┌────────┐  ┌─────────┐│
                     │  │  WAL   │─▶│  HNSW   ││  internal/index/hnsw
                     │  │ (fsync)│  │  graph  ││  internal/index/pq (optional)
                     │  └────────┘  └────┬────┘│
                     │                    │     │
                     │              ┌─────▼───┐│
                     │              │ snapshot ││  internal/storage/segment
                     │              │  (mmap)  ││  internal/storage/wal
                     │              └─────────┘│
                     └──────────────────────────┘
```

Every write is WAL-logged (fsync'd) before it touches the graph; periodic
snapshots let restart skip replaying the WAL from empty. See
`docs/writeups/01-wal-then-snapshot.md` for what that durability guarantee
actually costs, measured.

## Quickstart

```sh
curl -fsSL https://raw.githubusercontent.com/Rakshit-gen/NuclaDB/main/install.sh | sh
nucladb-cli quickstart
```

That installs both binaries and runs a scripted insert/search demo against
a throwaway local server, which it then leaves running (until you hit
Ctrl+C) so you can try more commands against it in another terminal, per
the address it prints:

```sh
export NUCLADB_ADDR=127.0.0.1:<port from quickstart's output>
nucladb-cli insert -id=1 -vector=1,0,0,0 -meta=team=search
nucladb-cli search -vector=1,0,0,0 -top-k=5
```

Building from source instead:

```sh
go build -o bin/nucladbd ./cmd/nucladbd
go build -o bin/nucladb-cli ./cmd/nucladb-cli

./bin/nucladbd -data-dir=./data -dim=4 -metric=l2 &

export NUCLADB_ADDR=localhost:9090
./bin/nucladb-cli insert -id=1 -vector=1,0,0,0 -meta=team=search
./bin/nucladb-cli search -vector=1,0,0,0 -top-k=5
```

Full command reference: [`docs/cli.md`](docs/cli.md), or as a browsable
page (live at https://nucladb-demo.onrender.com/docs), or open
[`docs/site/index.html`](docs/site/index.html) directly, no build step
(it covers both CLIs end to end).

There's also a Python client and CLI, `pip install`-able as a single
command: [`clients/python`](clients/python).

## Benchmarks

Real, reproducible measurements from `bench/`, comparing NuclaDB against a
real Qdrant instance over their own network APIs on the same machine, same
10,000-vector SIFT dataset, same recall@10 target:

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS |
|---|---|---|---|---|
| 10 | 0.906 | 1.000 | 7443 | 3730 |
| 50 | 0.997 | 1.000 | 4979 | 4838 |
| 200 | 1.000 | 1.000 | 2258 | 5076 |

| Backend | Build time (10K vectors) | RSS after build |
|---|---|---|
| NuclaDB | 3.2s | 44.8 MB |
| Qdrant | 118ms | 97.1 MB |

NuclaDB still builds ~27x slower than Qdrant. It used to be ~350x (43.9s),
when every vector paid its own fsync; `BatchUpsert` now writes a whole
batch with one shared fsync (group commit), and what's left is
single-threaded HNSW construction. See
[`docs/writeups/01-wal-then-snapshot.md`](docs/writeups/01-wal-then-snapshot.md)
for the durability side. Full table,
methodology, and the Qdrant config bug this benchmark caught (its default
`full_scan_threshold` would have silently made this an exact-vs-approximate
comparison) are in [`bench/results.md`](bench/results.md) and
[`bench/README.md`](bench/README.md).

### Concurrent throughput

The table above is single-connection and sequential (latency-bound).
`bench/cmd/loadtest` is the closed-loop version: a pool of gRPC
connections, N workers looping searches for a fixed window, against the
same real `nucladbd`. 10,000 vectors, `ef=100` (recall@10 0.999),
median of three runs on a 10-core machine shared by the load generator,
server, and mock:

| connections | searches/s | p50 | p99 |
|---|---|---|---|
| 1 | 3,260 | 0.31 ms | 0.55 ms |
| 8 | 16,400 | 0.46 ms | 1.1 ms |
| 16 | 19,000 | 0.70 ms | 2.7 ms |
| 64 | 19,500 | 1.8 ms | 19 ms |
| 128 | 19,100 | 3.0 ms | 46 ms |

Throughput saturates near **~19,500 searches/s** (peak 20,600); past
~16 connections, added concurrency buys latency, not throughput —
searches hold the HNSW graph's read lock for the whole traversal, and
that single `RWMutex` is the ceiling (per-segment locking is the planned
next step). A 60-second sustained run at 128 connections held ~16,500
searches/s with **0 errored requests** (over 5M requests across the full
sweep, 0 errors). Full numbers, including a mixed read/write run, are in
[`bench/results-loadtest.md`](bench/results-loadtest.md).

Product quantization: 57.7% recall@10 at a 16x memory reduction with flat
PQ alone, 99.3% when the top 100 codes are re-ranked against full vectors
(`pq.Index.SearchRerank`), see
[`docs/writeups/03-product-quantization-cost.md`](docs/writeups/03-product-quantization-cost.md).

## Design writeups

- [Why WAL-then-snapshot, and what it actually costs](docs/writeups/01-wal-then-snapshot.md)
- [Tuning HNSW: what the recall/latency curve actually looks like](docs/writeups/02-hnsw-ef-tuning.md)
- [What product quantization actually cost](docs/writeups/03-product-quantization-cost.md)
- [What Raft gave the system and what it cost](docs/writeups/04-what-raft-gave-and-cost.md)

## Multi-tenancy

Every collection is tenant-isolated: separate graph, WAL, and snapshot
files on disk per tenant, with independent storage quotas and rate
limits enforced before a request reaches the engine
(`internal/engine/store.go`). See `docs/cli.md`'s multi-tenancy section.

## Observability

Every gRPC call gets an OpenTelemetry trace span and is recorded into
Prometheus request-count/duration histograms; `/metrics` is served
alongside the REST API. Span export is off by default (a server that
serializes a span per RPC pays for it on the hot path whether or not
anyone reads the traces); set `OTEL_EXPORTER_OTLP_ENDPOINT` to send spans
to a collector, or `NUCLADB_TRACE_STDOUT=1` to pretty-print them to
stdout for local inspection.

## Deploying

**Live demo**: https://nucladb-demo.onrender.com/ (REST API + `/metrics`;
free tier, so the first request after idle may take ~30s to spin up).

`render.yaml` is a Render Blueprint: connect this repo at
[render.com](https://render.com) (New → Blueprint), and it builds
`Dockerfile` and deploys the REST/JSON API on Render's free web-service
tier, with `/metrics` as the health check.

`docker-entrypoint.sh` binds the REST gateway to Render's dynamically
assigned `$PORT` (Docker's exec-form `ENTRYPOINT` can't expand that
itself, hence the small shell wrapper), verified locally by simulating
Render's `$PORT` injection against the real binary before trusting it.

**Honest limitation**: the free tier has no persistent disk attached
here, so the demo instance's data resets on restart/redeploy/inactivity
spin-down. That's fine for a live demo proving the API works, but it's
not a durability claim: the WAL/snapshot durability guarantees are real
and tested (see `test/chaos/`), they just need an actual persistent
volume attached (a Render paid disk, or any real deployment target) to
apply across restarts of the demo itself.

## Status

Actively built in phases; see the project plan for the full roadmap
(product quantization and multi-tenancy are done, not deferred; Docker
packaging, chaos testing in CI, and a distributed Phase 2 with
Raft-replicated sharding and Jepsen-style linearizability testing are in
progress). Test suite: `go test ./... -race`.
