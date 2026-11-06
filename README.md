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

NuclaDB against a real Qdrant 1.19 on SIFT-1M (1,000,000 vectors, 128
dims, the standard 10,000 queries with their official groundtruth). Local
numbers from an Apple M4 (10 cores, 16 GB), measured 2026-11-03, not
production. Both engines run as their own server processes and are driven
over gRPC, with the same HNSW settings (M=16, ef_construct=200), one
graph each, and builds capped at 4 threads:

```sh
# from bench/, after SIFT1M=1 ./download.sh
go build -o ../bin/nucladbd ../cmd/nucladbd
cd cmd/compare && go run . -nucladbd=../../../bin/nucladbd -qdrant=../../.qdrant-bin/qdrant \
  -data=../../data/sift -threads=4 -out=../../results-sift1m.md
```

Single connection, sequential queries. Each ef gets one warm-up pass, then
5 measured passes over all 10,000 queries; QPS is the median pass (min-max
in brackets), p50/p95 are over all 50,000 measured queries.

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 |
|---|---|---|---|---|---|---|
| 10 | 0.725 | 0.721 | 8368 (6115-9082) | 4672 (4586-4790) | 0.11 / 0.18 ms | 0.21 / 0.27 ms |
| 20 | 0.853 | 0.850 | 7557 (4908-7877) | 3521 (3340-3670) | 0.13 / 0.23 ms | 0.29 / 0.35 ms |
| 50 | 0.955 | 0.954 | 5259 (3988-5358) | 2390 (2381-2440) | 0.19 / 0.29 ms | 0.42 / 0.50 ms |
| 100 | 0.986 | 0.987 | 2444 (2211-3456) | 1637 (1605-1677) | 0.37 / 0.53 ms | 0.62 / 0.75 ms |
| 200 | 0.997 | 0.997 | 1475 (1461-1515) | 949 (943-974) | 0.67 / 0.82 ms | 1.07 / 1.35 ms |

| Backend | Build time (1M vectors, 4 threads) |
|---|---|
| NuclaDB | 2m42s |
| Qdrant | 4m20s |

What this shows: both build graphs of the same quality (recall matches at
every ef), and NuclaDB now answers 1.5-2.2x more queries per second at
every ef and builds the index 1.6x faster. Memory goes the other way:
NuclaDB keeps every vector and link in RAM (1.1-1.6 GB RSS here), while
Qdrant's RSS drops to about 0.67 GB after indexing because it pages vector
storage out. Build times are rough: Qdrant's moved between 2m43s and 4m20s
across runs on identical settings.

Since that run (2026-11-07), vectors and layer-0 links live in mmap'd
blocks outside the Go heap, and snapshots load one node at a time instead
of decoding the whole graph first. Reopening the persisted 1M store from
that run now sits at about 805 MB RSS after two GCs, down from about
1.7-2.0 GB. The raw vectors and links alone are about 644 MB. This is a
reopen measurement, not a rerun of the build-then-query compare above, so
the 1.1-1.6 GB figure there stands until that compare is rerun.

Earlier runs, kept for the record (same harness, but run under `nice -n
15`, which on Apple Silicon tends to put both servers on efficiency cores):

| run | NuclaDB QPS at ef 10 / 100 / 200 | Qdrant QPS at ef 10 / 100 / 200 | builds (NuclaDB / Qdrant) |
|---|---|---|---|
| 2026-11-01 | 5818 / 1348 / 800 | 4828 / 1578 / 969 | 5m53s / 2m43s |
| 2026-11-02 | 6429 / 1204 / 717 | 4568 / 1585 / 967 | 5m34s / 3m29s |

Qdrant's own numbers barely moved between the niced runs and this one, so
NuclaDB's jump comes from the search-path changes below, not from
dropping `nice`.

**What changed in the search path.** Measured in-process on SIFT-1M
(`engine.Store.Search`, one query at a time, 4 threads for the build),
each step against the one before it:

| change | ef=100 search | build |
|---|---|---|
| vectors in flat 8 MB blocks, reused visited sets and heaps, NEON L2 kernel (2.5x faster than the unrolled Go loop at 128 dims), parallel batch linking | 497 µs | 3m53s |
| layer-0 links in flat slot-indexed blocks: one memory access per neighbor list instead of three pointer hops | 335 µs | 3m07s |
| prefetch every 128-byte cache line of each neighbor's vector before scoring, not just the first (measured on a graph restored from a snapshot, 337 µs before) | 274 µs | |

The same 100K-vector test at the commit before this work took 727 µs at
ef=100; the first row's changes alone brought it to 325 µs. An earlier
version of this README showed a 2.3-2.6x in-process gain whose
before-numbers came from a one-thread run that doesn't agree with the old
server's own results, so that table is withdrawn. Over gRPC each query
still pays about 60-140 µs of loopback round trip on this machine (an
empty RPC costs about 62 µs), which Qdrant pays too.

Three settings would each have made this comparison unfair, and earlier
versions of this README were affected by them:

- **Qdrant's segments.** By default Qdrant splits a collection into
  several segments, each its own HNSW graph (5 on this machine), and
  searches every one at the requested ef before merging. Its "ef=10" was
  then about five times NuclaDB's search work. With those defaults Qdrant
  reached recall 0.82 at ef=10 against NuclaDB's 0.71, and was about 2.9x
  faster at equal recall, partly because it searches segments in
  parallel. The harness now sets `default_segment_number: 1` and checks
  that one indexed segment holds every point.
- **REST vs gRPC.** Qdrant used to be driven over REST/JSON while NuclaDB
  used gRPC. Moving Qdrant to gRPC raised its QPS 25-34% at 10K.
- **`full_scan_threshold` and `indexing_threshold`.** Their defaults make
  Qdrant skip the HNSW index on small collections and answer by brute
  force, which gave it 1.0 recall at every ef and a build time that was
  only point ingest. Both are set so Qdrant always builds and uses HNSW,
  and build time waits for indexing to finish.

### SIFT-small (10K vectors)

Same harness and settings on the 10,000-vector SIFT-small set, 100
queries (`-data=../../data/siftsmall`). At this size almost any index
reaches near-perfect recall, so treat it as a smoke test:

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS |
|---|---|---|---|---|
| 10 | 0.929 | 0.937 | 15334 | 12776 |
| 50 | 0.996 | 0.995 | 10457 | 10665 |
| 100 | 0.998 | 0.998 | 7573 | 9312 |
| 200 | 1.000 | 1.000 | 5641 | 7146 |

Build: NuclaDB 562ms, Qdrant 685ms (run 2026-11-02, before the NEON and
link-layout changes). NuclaDB's own build of this set went
from 43.9s to 3.2s by sharing one WAL fsync per batch instead of one per
vector, then to about 0.4s through faster distance kernels, a slice-based
graph layout and a parallel batch build (`hnsw.Graph.InsertBatch`). Full
tables are in [`bench/results-sift1m.md`](bench/results-sift1m.md) and
[`bench/results.md`](bench/results.md); methodology is in
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

## API

The same operations are on gRPC (`proto/nucladb.proto`) and REST:

| REST | What it does |
|---|---|
| `POST /v1/tenants` | create a tenant, optionally with its own `dim`, `metric` (`cosine`, `l2`, `dot`) and quota |
| `GET /v1/tenants` | list tenants with their usage |
| `DELETE /v1/tenants/{id}` | drop a tenant and its files |
| `PUT /v1/tenants/{id}/quota` | change `max_vectors` / `max_qps` |
| `POST /v1/vectors`, `POST /v1/vectors:batch` | upsert one or many vectors |
| `GET`, `PATCH`, `DELETE /v1/vectors/{id}` | read, replace metadata, delete |
| `GET /v1/vectors?page_size=&page_token=` | page through ids |
| `GET /v1/vectors:count` | vector count |
| `POST /v1/search` | top-k search with optional metadata filters |

Search filters go in `where`, a list of `{key, op, value | values}`
clauses that all have to match. Ops: `eq`, `ne`, `in`, `not_in`, `gt`,
`gte`, `lt`, `lte` (compared as numbers) and `exists`:

```sh
curl -s localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":5,
  "where":[{"key":"team","op":"in","values":["search","ads"]},
           {"key":"year","op":"gte","value":"2024"}]}'
```

A filter that matches only a few vectors (under `-exact-filter-limit`,
4096 by default) is answered by scoring every match exactly, so a
selective filter still returns a full top-k instead of whatever the graph
walk happened to pass.

**Auth and TLS.** Start `nucladbd` with `-api-keys=keys.json` to require a
key on every request:

```json
[{"key": "at-least-16-characters", "tenants": ["acme"]},
 {"key": "an-admin-key-for-ops", "tenants": ["*"], "admin": true}]
```

A key only reaches the tenants it lists (`*` for all); creating, listing,
deleting and re-quota'ing tenants needs an admin key. Send it as
`Authorization: Bearer <key>` or `X-API-Key: <key>`. The CLI reads
`NUCLADB_API_KEY`, the Python client takes `api_key=`. Add
`-tls-cert`/`-tls-key` to serve both gRPC and REST over TLS
(`NUCLADB_TLS=1` for the CLI, `tls=True` in Python). Without
`-api-keys`, every request is allowed only on the default loopback listeners,
which is what the local quickstart uses. A key can also be supplied directly
through `NUCLADB_API_KEY`; it becomes an all-tenant admin key. Binding either
listener to a non-loopback address without authentication is refused unless
you explicitly pass `-allow-unauthenticated`. When authentication is enabled,
`/metrics` requires an admin key; `/healthz` remains public for orchestrators.

Other server limits worth knowing: `-max-message-bytes` (64 MiB) caps a
gRPC message or REST body, `top_k` is capped at 1000, `ef_search` at
10000, and `-tenant-idle-timeout` (30m) closes tenants nobody has used so
thousands of them don't all hold files open.

## Feature guides

Plain-language guides to each part, with the measured numbers behind it:
[`docs/features`](docs/features/README.md).

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
alongside the REST API (and requires an admin key when API authentication is
enabled). Span export is off by default (a server that
serializes a span per RPC pays for it on the hot path whether or not
anyone reads the traces); set `OTEL_EXPORTER_OTLP_ENDPOINT` to send spans
to a collector, or `NUCLADB_TRACE_STDOUT=1` to pretty-print them to
stdout for local inspection.

## Deploying

**Live demo**: https://nucladb-demo.onrender.com/ (authenticated REST API;
free tier, so the first request after idle may take ~30s to spin up).

`render.yaml` is a Render Blueprint: connect this repo at
[render.com](https://render.com) (New → Blueprint), and it builds
`Dockerfile` and deploys the REST/JSON API on Render's free web-service
tier, with the public `/healthz` endpoint as the health check. Render generates
the deployment's admin key as `NUCLADB_API_KEY`.

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
multi-tenancy with per-tenant dimension and metric, the gRPC/REST API with
filters, API keys and TLS, and the CLI. Docker packaging is in
`Dockerfile` and `docker-compose.yml`, crash tests are in `test/chaos`, and
CI runs gofmt, vet and the race-enabled test suite on every push.

Built and tested as packages, but not yet run by the server:

- product quantization (flat and IVF)
- the distributed layer: Raft control plane, consistent-hash sharding,
  WAL-stream replication (with divergence detection and streamed
  snapshot bootstrap), a scatter-gather router that retries and returns
  partial results, parallel health checks, and failover that promotes
  the most caught-up replica
- Jepsen-style linearizability tests in `test/jepsen`, which run a real
  in-process 2-node cluster through `porcupine`

Not done: a `nucladbd` cluster mode and PQ index option. Until the server
runs the cluster, nothing starts replication streams per shard or
repoints them after a failover, and a rebalance changes shard ownership
without copying the data across. Test suite: `go test ./... -race`.
