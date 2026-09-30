# NuclaDB vs Qdrant: SIFT-small benchmark

Real measurements from running both systems over their own network APIs on the same machine, same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 3.212119208s | 44.8 MB |
| Qdrant | 117.986667ms | 97.1 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|
| 10 | 0.9060 | 1.0000 | 7442.5 | 3730.3 | 44.8 MB | 103.4 MB |
| 20 | 0.9520 | 1.0000 | 6609.6 | 4798.8 | 45.0 MB | 103.4 MB |
| 50 | 0.9970 | 1.0000 | 4978.9 | 4837.5 | 45.0 MB | 103.4 MB |
| 100 | 1.0000 | 1.0000 | 3400.4 | 5013.0 | 45.1 MB | 103.4 MB |
| 200 | 1.0000 | 1.0000 | 2257.7 | 5075.6 | 45.1 MB | 103.4 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~5120 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Build time is still the standout gap.** NuclaDB loads through BatchUpsert in batches of 500, and each batch shares one WAL fsync; before group commit every vector paid its own fsync and this build took 43.9s. What remains is single-threaded HNSW construction (ef_construct=200) under one graph lock; Qdrant builds its index differently.
- At only 10000 vectors, recall for both engines converges close to 1.0 by moderate ef. A clearer recall/QPS separation would show at larger scale (SIFT1M), which has not been run.
