# NuclaDB

A vector search engine written from scratch in Go. The server,
`nucladbd`, runs HNSW indexing, a crash-safe write-ahead log, mmap-backed
snapshot persistence, tenant-isolated multi-tenancy with quotas and rate
limiting, OpenTelemetry tracing, Prometheus metrics, and a gRPC + REST API,
with a CLI alongside. It is benchmarked head-to-head against a real Qdrant
instance, and it is not a wrapper around one.

Two more parts exist as tested Go packages but are not wired into
`nucladbd` yet, so a server you start today does not use them:

- product quantization (`internal/index/pq`)
- Raft-replicated sharding (`internal/cluster`)

The sharded cluster is exercised in-process by `bench/cmd/compare-cluster`
and by the Jepsen-style tests in `test/jepsen`. See [Status](#status).

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

Each ef gets one warm-up pass, then 5 measured passes over the 100
queries. QPS is the median pass (min-max in brackets); p50/p95 latency is
over all 500 measured queries.

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 |
|---|---|---|---|---|---|---|
| 10 | 0.932 | 0.959 | 13914 (12289-18904) | 7624 (6193-7691) | 0.07 / 0.09 ms | 0.13 / 0.17 ms |
| 20 | 0.981 | 0.989 | 13966 (13137-14075) | 7451 (7436-7797) | 0.07 / 0.09 ms | 0.13 / 0.15 ms |
| 50 | 0.996 | 0.998 | 10653 (10070-10719) | 7051 (7033-7237) | 0.09 / 0.11 ms | 0.14 / 0.16 ms |
| 100 | 0.998 | 1.000 | 7542 (7453-8083) | 6465 (6438-6686) | 0.13 / 0.16 ms | 0.15 / 0.17 ms |
| 200 | 1.000 | 1.000 | 5822 (5486-5864) | 5512 (5167-5567) | 0.17 / 0.21 ms | 0.18 / 0.22 ms |

| Backend | Build time (10K vectors) | RSS after build |
|---|---|---|
| NuclaDB | 416ms | 45.7 MB |
| Qdrant | 557ms | 115.4 MB |

NuclaDB now builds this index faster than Qdrant. Earlier
versions of this table showed Qdrant at ~120ms and NuclaDB ~27x slower,
but that compared different work: Qdrant skips building an HNSW index for
segments under its `indexing_threshold`, so it had only ingested points
and was answering every search by brute force. The benchmark now forces
Qdrant to index and waits until it has. NuclaDB's own build went from
43.9s (fsync per vector) to 3.2s (group commit) to 415ms, through faster
distance kernels, a slice-based graph layout, and a parallel batch build
(`hnsw.Graph.InsertBatch`). Neighbors are picked with the HNSW paper's
diversity heuristic, which raised ef=10 recall from 0.84-0.91 (it moved
with the random level seed) to about 0.93; Qdrant is still a little higher
at ef=10 and ef=20, and both reach 1.000 by ef=200. Earlier versions of
this table used a single pass per ef, and Qdrant's QPS jumped around
(5,214, then 1,956, then 4,730 for ef 50/100/200); with a warm-up pass and
the median of 5, both engines slow down steadily as ef grows. See
[`docs/writeups/01-wal-then-snapshot.md`](docs/writeups/01-wal-then-snapshot.md)
for the durability side. Full table and methodology, including the two
Qdrant config settings that would each have made this an unfair
comparison (`full_scan_threshold` and `indexing_threshold`), are in
[`bench/results.md`](bench/results.md) and
[`bench/README.md`](bench/README.md).

### Concurrent throughput

The table above is single-connection and sequential (latency-bound).
`bench/cmd/loadtest` is the closed-loop version: a pool of gRPC
connections, N workers looping searches for a fixed window, against the
same real `nucladbd`. 10,000 vectors, `ef=100` (recall@10 1.000),
median of three runs on a 10-core machine shared by the load generator,
server, and mock:

| connections | searches/s | p50 | p99 |
|---|---|---|---|
| 1 | 3,800 | 0.26 ms | 0.54 ms |
| 8 | 16,450 | 0.46 ms | 1.2 ms |
| 16 | 18,260 | 0.72 ms | 2.9 ms |
| 32 | 20,900 | 1.3 ms | 5.7 ms |
| 64 | 20,000 | 2.4 ms | 14 ms |
| 128 | 21,900 | 4.4 ms | 30 ms |

Throughput levels off around **~21,000 searches/s** (peak 23,300); past
~32 connections, added concurrency buys latency, not throughput.
Searches hold the HNSW graph's read lock for the whole traversal; an
insert now takes the write lock only to link a node in, after finding
its neighbors without it. A 60-second sustained run at
128 connections held ~22,100 searches/s with **0 errored requests**.
Full numbers, including a mixed read/write run, are in
[`bench/results-loadtest.md`](bench/results-loadtest.md).

Product quantization: 57.7% recall@10 at a 16x memory reduction with flat
PQ alone, 99.3% when the top 100 codes are re-ranked against full vectors
(`pq.Index.SearchRerank`). `pq.IVFIndex` adds an inverted file so a search
scans only the lists nearest the query instead of every code. See
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

Single node, done and running in `nucladbd`: HNSW, WAL, snapshots,
multi-tenancy, the gRPC/REST API and the CLI. Docker packaging is in
`Dockerfile` and `docker-compose.yml`, and crash tests are in `test/chaos`.

Built and tested as packages, but not yet run by the server:

- product quantization (flat and IVF)
- the distributed layer: Raft control plane, consistent-hash sharding,
  WAL-stream replication, scatter-gather router, health checks and
  failover
- Jepsen-style linearizability tests in `test/jepsen`, which run a real
  in-process 2-node cluster through `porcupine`

Not done: a `nucladbd` cluster mode and PQ index option, CI, and the
remaining replication gaps (divergence detection, rebalance moving data).
Test suite: `go test ./... -race`.
