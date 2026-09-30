# NuclaDB vs Qdrant: SIFT-small benchmark

Real measurements from running both systems over their own network APIs on the same machine, same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 414.936792ms | 44.7 MB |
| Qdrant | 550.786208ms | 114.8 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|
| 10 | 0.9320 | 0.9640 | 10545.3 | 5551.8 | 44.8 MB | 115.6 MB |
| 20 | 0.9830 | 0.9940 | 9338.7 | 5914.5 | 44.8 MB | 115.7 MB |
| 50 | 0.9980 | 0.9990 | 7762.3 | 5214.1 | 44.8 MB | 115.9 MB |
| 100 | 1.0000 | 0.9990 | 7189.7 | 1956.3 | 44.8 MB | 116.0 MB |
| 200 | 1.0000 | 1.0000 | 4972.7 | 4729.8 | 44.9 MB | 116.2 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~5120 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Qdrant's `indexing_threshold` is set to 1 KB, and its build time runs until every vector is indexed.** Its default (10,000 KB) is also above this dataset's size, so Qdrant never built an HNSW index in earlier runs: its build time was point ingest only (~120ms) and every search was brute force, which is why its recall was 1.0 at every ef. Upserts with wait=true return before indexing, so the harness polls until `indexed_vectors_count` covers every vector.
- **NuclaDB builds through BatchUpsert in batches of 500.** Each batch shares one WAL fsync, and the graph searches for the batch's neighbors on every core before linking them in order (`hnsw.Graph.InsertBatch`).
- At only 10000 vectors, recall for both engines converges close to 1.0 by moderate ef. A clearer recall/QPS separation would show at larger scale (SIFT1M), which has not been run.
