# NuclaDB vs Qdrant: SIFT-small benchmark

Real measurements from running both systems over their own network APIs on the same machine, same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 43.90628575s | 45.2 MB |
| Qdrant | 124.000458ms | 96.4 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|
| 10 | 0.8960 | 1.0000 | 7432.4 | 3661.1 | 45.2 MB | 102.7 MB |
| 20 | 0.9620 | 1.0000 | 6298.9 | 4522.6 | 45.2 MB | 102.7 MB |
| 50 | 0.9970 | 1.0000 | 4932.9 | 5057.7 | 45.3 MB | 102.7 MB |
| 100 | 1.0000 | 1.0000 | 3675.8 | 4951.5 | 45.4 MB | 102.7 MB |
| 200 | 1.0000 | 1.0000 | 2367.5 | 5063.3 | 45.6 MB | 102.7 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~5120 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Build time is the standout gap.** NuclaDB's WAL fsyncs on every single write for crash-safety durability; Qdrant batches durability differently, hence the build-time difference above. This is a genuine, unhidden weakness — see docs/writeups.
- At only 10000 vectors, recall for both engines converges close to 1.0 by moderate ef — a real recall/QPS tradeoff separation is more visible at larger scale (SIFT1M); rerunning there is documented future work, not run here due to build-time cost at this fsync-per-write rate.
