# NuclaDB vs Qdrant: sift benchmark

Real measurements from running both systems over gRPC on the same machine, same dataset, same recall@10 target — not estimates.

1000000 base vectors, 10000 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 5m33.591681458s | 2249.7 MB |
| Qdrant | 3m29.496795333s | 2064.8 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.7230 | 0.7224 | 6429 (4367-6486) | 4568 (4233-4967) | 0.16 / 0.25 ms | 0.21 / 0.31 ms | 2002.8 MB | 717.7 MB |
| 20 | 0.8521 | 0.8502 | 3519 (3011-3580) | 3211 (3164-3327) | 0.30 / 0.41 ms | 0.30 / 0.42 ms | 2003.7 MB | 716.4 MB |
| 50 | 0.9548 | 0.9546 | 2029 (1911-2195) | 2356 (2351-2364) | 0.47 / 0.71 ms | 0.43 / 0.51 ms | 1991.5 MB | 716.2 MB |
| 100 | 0.9862 | 0.9866 | 1204 (1131-1208) | 1585 (1494-1606) | 0.86 / 1.11 ms | 0.65 / 0.79 ms | 1977.6 MB | 716.2 MB |
| 200 | 0.9966 | 0.9964 | 717 (674-720) | 967 (953-1045) | 1.44 / 1.82 ms | 1.04 / 1.37 ms | 1893.1 MB | 716.1 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~512000 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Qdrant's `indexing_threshold` is set to 1 KB, and its build time runs until every vector is indexed.** Its default (10,000 KB) is also above this dataset's size, so Qdrant never built an HNSW index in earlier runs: its build time was point ingest only (~120ms) and every search was brute force, which is why its recall was 1.0 at every ef. Upserts with wait=true return before indexing, so the harness polls until `indexed_vectors_count` covers every vector.
- **NuclaDB builds through BatchUpsert in batches of 500.** Each batch shares one WAL fsync, and the graph searches for the batch's neighbors on every core before linking them in order (`hnsw.Graph.InsertBatch`).
- **Qdrant is held to one segment** (`default_segment_number: 1`), so both engines search one HNSW graph per query. Its default on this machine is 5 segments, each searched at the requested ef and then merged, which made its ef=10 about five times the search work of NuclaDB's.
- **Both engines are driven over gRPC.** Qdrant's upserts and searches use its official Go client (`github.com/qdrant/go-client`); only collection setup and the index-progress poll use its REST API.
