# NuclaDB concurrent load test

Saturated concurrent throughput and tail latency over the real gRPC API — the closed-loop counterpart to `results.md`'s single-connection QPS. Every number is measured by running a real `nucladbd` on this machine, not estimated.

- **10000 vectors**, dim=128, metric l2, `ef=100`, `top-k=10` — recall@10 = **1.0000** against the dataset's official groundtruth (so throughput is anchored to a known accuracy point)
- Load generator and server share this machine; `N` closed-loop goroutines round-robin over a pool of gRPC client connections
- Server RSS after load: 43.8 MB
- 15s measured window, 3s warmup, per level

## Search throughput vs concurrency

| conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---|---|---|---|---|
| 1 | 3833 | 259µs | 313µs | 526µs | 6.935ms | 0 |
| 8 | 16451 | 456µs | 676µs | 1.216ms | 38.604ms | 0 |
| 16 | 18262 | 717µs | 1.407ms | 3.162ms | 141.016ms | 0 |
| 32 | 20914 | 1.292ms | 2.715ms | 5.669ms | 24.736ms | 0 |
| 64 | 19489 | 2.564ms | 6.185ms | 15.231ms | 64.136ms | 0 |
| 128 | 20492 | 4.445ms | 12.687ms | 33.208ms | 99.992ms | 0 |

## Mixed read/write at conns=128 (10% Insert)

These mixed numbers were measured when every insert held the HNSW graph's write lock for its whole neighbor search, blocking searches the entire time. Inserts now take that lock only to link the new node in, and single Inserts share fsyncs under concurrency, so this section needs a rerun of `cmd/loadtest`.

| op | req/s | p50 | p99 |
|---|---|---|---|
| search | 2425 | 283µs | 1.124ms |
| insert | 278 | 454.345ms | 557.078ms |

Errors: 0

## Sustained read-only at conns=128 for 1m0s

| req/s | p50 | p90 | p99 | p99.9 | max | errors |
|---|---|---|---|---|---|---|
| 20192 | 5.124ms | 12.152ms | 26.422ms | 45.944ms | 120.274ms | 0 |

## Reproduce

From `bench/`:

```sh
go build -o ../bin/nucladbd ../cmd/nucladbd
go run ./cmd/loadtest -nucladbd=../bin/nucladbd -data=./data/siftsmall
```

Numbers vary ~10% run to run — the load generator, server, and mock all
share one machine. This file is one representative run; medians of three
are in the project README.
