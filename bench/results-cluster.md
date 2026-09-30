# NuclaDB single-node vs 4-shard cluster: SIFT-small benchmark

Real measurements from running a single nucladbd process against a real 4-shard cluster (independent nucladbd processes behind the real scatter-gather router in internal/cluster/router), same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Topology | Build time | Total RSS after build |
|---|---|---|
| NuclaDB | 2.897076375s | 45.1 MB |
| NuclaDB-cluster(4 shards) | 814.45225ms | 124.7 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | NuclaDB-cluster(4 shards) recall@10 | NuclaDB QPS | NuclaDB-cluster(4 shards) QPS | NuclaDB RSS | NuclaDB-cluster(4 shards) RSS |
|---|---|---|---|---|---|---|
| 10 | 0.9100 | 0.9780 | 7740.8 | 4440.3 | 45.2 MB | 124.9 MB |
| 20 | 0.9590 | 0.9890 | 6987.0 | 3897.3 | 45.2 MB | 125.0 MB |
| 50 | 0.9960 | 0.9940 | 5648.2 | 3131.3 | 45.2 MB | 125.2 MB |
| 100 | 0.9990 | 0.9950 | 4164.1 | 2562.9 | 45.2 MB | 125.2 MB |
| 200 | 0.9990 | 0.9950 | 2744.3 | 1650.7 | 45.5 MB | 125.2 MB |

## Notes

- **Build uses batches on both paths.** The single node gets `BatchUpsert` calls of 500 vectors; the cluster gets the same batches through `Router.InsertBatch`, which splits each one by shard and sends every shard its part as one `BatchUpsert`, all shards in parallel.
- **Search fans out to every shard and merges.** `Router.Search` queries all 4 shards concurrently per request and merges each shard's own top-k into one globally-ranked top-k — recall should track the single-node numbers closely (sharding by id doesn't change which vectors exist, only where), while QPS reflects added network hops (client -> router -> N shards) and per-shard candidate lists shrinking as vectors spread across more processes.
- **RSS is summed across all shard processes**, so it's the cluster's total memory footprint, not comparable 1:1 to a single process's number without accounting for 4 processes' worth of fixed overhead (goroutine stacks, gRPC server state, OS-level per-process baseline) on top of the actual vector data.
