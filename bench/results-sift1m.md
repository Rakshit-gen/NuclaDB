# NuclaDB vs Qdrant: sift benchmark

Real measurements from running both systems over gRPC on the same machine, same dataset, same recall@10 target — not estimates.

1000000 base vectors, 10000 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 2m42.3924225s | 1535.4 MB |
| Qdrant | 4m20.330646458s | 2004.2 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.7246 | 0.7210 | 8368 (6115-9082) | 4672 (4586-4790) | 0.11 / 0.18 ms | 0.21 / 0.27 ms | 1513.9 MB | 668.7 MB |
| 20 | 0.8527 | 0.8499 | 7557 (4908-7877) | 3521 (3340-3670) | 0.13 / 0.23 ms | 0.29 / 0.35 ms | 1566.0 MB | 668.0 MB |
| 50 | 0.9550 | 0.9535 | 5259 (3988-5358) | 2390 (2381-2440) | 0.19 / 0.29 ms | 0.42 / 0.50 ms | 1469.1 MB | 667.8 MB |
| 100 | 0.9863 | 0.9866 | 2444 (2211-3456) | 1637 (1605-1677) | 0.37 / 0.53 ms | 0.62 / 0.75 ms | 1118.2 MB | 666.9 MB |
| 200 | 0.9965 | 0.9966 | 1475 (1461-1515) | 949 (943-974) | 0.67 / 0.82 ms | 1.07 / 1.35 ms | 1201.6 MB | 658.6 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~512000 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Qdrant's `indexing_threshold` is set to 1 KB, and its build time runs until every vector is indexed.** Its default (10,000 KB) is also above this dataset's size, so Qdrant never built an HNSW index in earlier runs: its build time was point ingest only (~120ms) and every search was brute force, which is why its recall was 1.0 at every ef. Upserts with wait=true return before indexing, so the harness polls until `indexed_vectors_count` covers every vector.
- **NuclaDB builds through BatchUpsert in batches of 500.** Each batch shares one WAL fsync, and the graph searches for the batch's neighbors on every core before linking them in order (`hnsw.Graph.InsertBatch`).
- **Qdrant is held to one segment** (`default_segment_number: 1`), so both engines search one HNSW graph per query. Its default on this machine is 5 segments, each searched at the requested ef and then merged, which made its ef=10 about five times the search work of NuclaDB's.
- **Both engines are driven over gRPC.** Qdrant's upserts and searches use its official Go client (`github.com/qdrant/go-client`); only collection setup and the index-progress poll use its REST API.
