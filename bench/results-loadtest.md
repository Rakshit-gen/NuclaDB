# NuclaDB concurrent load test

Saturated concurrent throughput and tail latency over the real gRPC API — the closed-loop counterpart to `results.md`'s single-connection QPS. Every number is measured by running a real `nucladbd` on this machine, not estimated.

- **10000 vectors**, dim=128, metric l2, `ef=100`, `top-k=10` — recall@10 = **0.9900** against the dataset's official groundtruth (so throughput is anchored to a known accuracy point)
- Load generator and server share this machine; `N` closed-loop goroutines round-robin over a pool of gRPC client connections
- Server RSS after load: 44.2 MB
- 15s measured window, 3s warmup, per level

## Search throughput vs concurrency

| conns | req/s | p50 | p90 | p99 | max | errors |
|---|---|---|---|---|---|---|
| 1 | 4305 | 235µs | 279µs | 348µs | 2.61ms | 0 |
| 8 | 19129 | 397µs | 592µs | 972µs | 51.1ms | 0 |
| 16 | 21031 | 635µs | 1.243ms | 2.601ms | 43.833ms | 0 |
| 32 | 22390 | 1.195ms | 2.557ms | 5.377ms | 52.572ms | 0 |
| 64 | 23059 | 2.167ms | 5.238ms | 12.966ms | 76.929ms | 0 |
| 128 | 25752 | 3.466ms | 10.048ms | 28.516ms | 94.115ms | 0 |

## Mixed read/write at conns=128 (10% Insert)

Inserts run one at a time. Each one finds its neighbors without the graph write lock and takes it only to link the new node in, so searches wait for that step alone (see the `hnsw.Graph` doc comment).

| op | req/s | p50 | p99 |
|---|---|---|---|
| search | 10980 | 1.251ms | 7.237ms |
| insert | 1215 | 87.052ms | 179.857ms |

Errors: 0

## Sustained read-only at conns=128 for 1m0s

| req/s | p50 | p90 | p99 | p99.9 | max | errors |
|---|---|---|---|---|---|---|
| 25103 | 4.269ms | 9.771ms | 17.334ms | 33.099ms | 87.373ms | 0 |

## Reproduce

From `bench/`:

```sh
go build -o ../bin/nucladbd ../cmd/nucladbd
go run ./cmd/loadtest -nucladbd=../bin/nucladbd -data=./data/siftsmall
```

Numbers vary ~10% run to run — the load generator, server, and mock all
share one machine. This file is one representative run; medians of three
are in the project README.
