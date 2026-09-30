# NuclaDB single-node vs 4-shard cluster: SIFT-small benchmark

Real measurements from running a single nucladbd process against a real 4-shard cluster (independent nucladbd processes behind the real scatter-gather router in internal/cluster/router), same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Topology | Build time | Total RSS after build |
|---|---|---|
| NuclaDB | 45.386837375s | 44.1 MB |
| NuclaDB-cluster(4 shards) | 36.97815325s | 119.4 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | NuclaDB-cluster(4 shards) recall@10 | NuclaDB QPS | NuclaDB-cluster(4 shards) QPS | NuclaDB RSS | NuclaDB-cluster(4 shards) RSS |
|---|---|---|---|---|---|---|
| 10 | 0.9030 | 0.9630 | 5236.8 | 3092.0 | 44.1 MB | 120.0 MB |
| 20 | 0.9600 | 0.9740 | 5867.2 | 3424.6 | 44.1 MB | 120.9 MB |
| 50 | 0.9960 | 0.9850 | 4359.4 | 2616.0 | 44.6 MB | 121.1 MB |
| 100 | 0.9990 | 0.9860 | 2951.8 | 2125.6 | 44.7 MB | 121.6 MB |
| 200 | 0.9990 | 0.9860 | 2100.9 | 1635.9 | 44.7 MB | 122.8 MB |

## Notes

- **These build times are from before batching.** Both were measured when every vector paid its own fsync and the router sent one RPC per vector. Since then the single node batches (about 3s for this build in `results.md`) and the router has `InsertBatch`, which `compare-cluster` now uses. Rerun `go run ./cmd/compare-cluster` to refresh the build row; the search numbers are unaffected.
- **Search fans out to every shard and merges.** `Router.Search` queries all 4 shards concurrently per request and merges each shard's own top-k into one globally-ranked top-k — recall should track the single-node numbers closely (sharding by id doesn't change which vectors exist, only where), while QPS reflects added network hops (client -> router -> N shards) and per-shard candidate lists shrinking as vectors spread across more processes.
- **RSS is summed across all shard processes**, so it's the cluster's total memory footprint, not comparable 1:1 to a single process's number without accounting for 4 processes' worth of fixed overhead (goroutine stacks, gRPC server state, OS-level per-process baseline) on top of the actual vector data.
