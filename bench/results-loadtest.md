# NuclaDB concurrent load test

Saturated concurrent throughput and tail latency over the real gRPC API — the closed-loop counterpart to `results.md`'s single-connection QPS. Every number is measured by running a real `nucladbd` on this machine, not estimated.

- **10000 vectors**, dim=128, metric l2, `ef=100`, `top-k=10` — recall@10 = **0.9980** against the dataset's official groundtruth (so throughput is anchored to a known accuracy point)
- Load generator and server share this machine; `N` closed-loop goroutines round-robin over a pool of gRPC client connections
- Server RSS after load: 44.2 MB
- 15s measured window, 3s warmup, per level

## Search throughput vs concurrency

| conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---|---|---|---|---|
| 1 | 3298 | 304µs | 366µs | 507µs | 1.985ms | 0 |
| 8 | 16042 | 471µs | 739µs | 1.136ms | 3.326ms | 0 |
| 16 | 18382 | 719µs | 1.537ms | 2.776ms | 17.722ms | 0 |
| 32 | 19379 | 1.211ms | 3.383ms | 7.07ms | 23.902ms | 0 |
| 64 | 19166 | 1.884ms | 8.704ms | 19.244ms | 60.302ms | 0 |
| 128 | 18422 | 3.214ms | 20.582ms | 48.993ms | 137.702ms | 0 |

## Mixed read/write at conns=128 (10% Insert)

Every write takes the HNSW graph's single write lock, which blocks concurrent searches for its duration — this is the cost of that design (see `internal/index/hnsw` doc comment).

| op | req/s | p50 | p99 |
|---|---|---|---|
| search | 2420 | 291µs | 1.115ms |
| insert | 278 | 454.973ms | 504.142ms |

Errors: 0

## Sustained read-only at conns=128 for 1m0s

| req/s | p50 | p90 | p99 | p99.9 | max | errors |
|---|---|---|---|---|---|---|
| 17139 | 4.823ms | 15.384ms | 45.741ms | 72.112ms | 159.44ms | 0 |

## Reproduce

From `bench/`:

```sh
go build -o ../bin/nucladbd ../cmd/nucladbd
go run ./cmd/loadtest -nucladbd=../bin/nucladbd -data=./data/siftsmall
```

Numbers vary ~10% run to run — the load generator, server, and mock all
share one machine. This file is one representative run; medians of three
are in the project README.
