# NuclaDB vs Qdrant: SIFT-small benchmark

Real measurements from running both systems over their own network APIs on the same machine, same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 416.374167ms | 45.7 MB |
| Qdrant | 557.42475ms | 115.4 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.9320 | 0.9590 | 13914 (12289-18904) | 7624 (6193-7691) | 0.07 / 0.09 ms | 0.13 / 0.17 ms | 45.8 MB | 116.1 MB |
| 20 | 0.9810 | 0.9890 | 13966 (13137-14075) | 7451 (7436-7797) | 0.07 / 0.09 ms | 0.13 / 0.15 ms | 45.8 MB | 116.2 MB |
| 50 | 0.9960 | 0.9980 | 10653 (10070-10719) | 7051 (7033-7237) | 0.09 / 0.11 ms | 0.14 / 0.16 ms | 45.9 MB | 116.3 MB |
| 100 | 0.9980 | 1.0000 | 7542 (7453-8083) | 6465 (6438-6686) | 0.13 / 0.16 ms | 0.15 / 0.17 ms | 45.9 MB | 116.5 MB |
| 200 | 1.0000 | 1.0000 | 5822 (5486-5864) | 5512 (5167-5567) | 0.17 / 0.21 ms | 0.18 / 0.22 ms | 45.9 MB | 116.6 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~5120 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Qdrant's `indexing_threshold` is set to 1 KB, and its build time runs until every vector is indexed.** Its default (10,000 KB) is also above this dataset's size, so Qdrant never built an HNSW index in earlier runs: its build time was point ingest only (~120ms) and every search was brute force, which is why its recall was 1.0 at every ef. Upserts with wait=true return before indexing, so the harness polls until `indexed_vectors_count` covers every vector.
- **NuclaDB builds through BatchUpsert in batches of 500.** Each batch shares one WAL fsync, and the graph searches for the batch's neighbors on every core before linking them in order (`hnsw.Graph.InsertBatch`).
- At only 10000 vectors, recall for both engines converges close to 1.0 by moderate ef. A clearer recall/QPS separation would show at larger scale (SIFT1M), which has not been run.
