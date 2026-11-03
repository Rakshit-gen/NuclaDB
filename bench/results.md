# NuclaDB vs Qdrant: siftsmall benchmark

Real measurements from running both systems over gRPC on the same machine, same dataset, same recall@10 target — not estimates.

10000 base vectors, 100 queries, dim=128.

## Build

| Backend | Build time | RSS after build |
|---|---|---|
| NuclaDB | 562.358292ms | 41.5 MB |
| Qdrant | 684.749917ms | 120.4 MB |

## Recall / QPS / memory vs ef

| ef | NuclaDB recall@10 | Qdrant recall@10 | NuclaDB QPS | Qdrant QPS | NuclaDB p50 / p95 | Qdrant p50 / p95 | NuclaDB RSS | Qdrant RSS |
|---|---|---|---|---|---|---|---|---|
| 10 | 0.9290 | 0.9370 | 15334 (11923-16115) | 12776 (9916-13119) | 0.07 / 0.09 ms | 0.08 / 0.11 ms | 42.0 MB | 121.4 MB |
| 20 | 0.9770 | 0.9810 | 13318 (11282-14150) | 12419 (11916-12737) | 0.07 / 0.10 ms | 0.08 / 0.09 ms | 42.0 MB | 121.4 MB |
| 50 | 0.9960 | 0.9950 | 10457 (9167-10651) | 10665 (9782-10931) | 0.10 / 0.12 ms | 0.09 / 0.11 ms | 42.1 MB | 121.4 MB |
| 100 | 0.9980 | 0.9980 | 7573 (7437-8148) | 9312 (9028-9491) | 0.13 / 0.16 ms | 0.11 / 0.13 ms | 42.1 MB | 121.4 MB |
| 200 | 1.0000 | 1.0000 | 5641 (5228-5740) | 7146 (7023-7521) | 0.18 / 0.21 ms | 0.14 / 0.17 ms | 42.1 MB | 121.5 MB |

## Notes

- **Qdrant's `full_scan_threshold` is set explicitly to 10 (its API-enforced minimum) here.** Its default (10,000 KB) is comfortably above this dataset's raw size (~5120 KB), which means an out-of-the-box comparison at this scale would silently have been exact-search-vs-HNSW, not HNSW-vs-HNSW. Discovered by noticing suspiciously perfect 1.0 recall at every ef on the first run; see the writeup.
- **Qdrant's `indexing_threshold` is set to 1 KB, and its build time runs until every vector is indexed.** Its default (10,000 KB) is also above this dataset's size, so Qdrant never built an HNSW index in earlier runs: its build time was point ingest only (~120ms) and every search was brute force, which is why its recall was 1.0 at every ef. Upserts with wait=true return before indexing, so the harness polls until `indexed_vectors_count` covers every vector.
- **NuclaDB builds through BatchUpsert in batches of 500.** Each batch shares one WAL fsync, and the graph searches for the batch's neighbors on every core before linking them in order (`hnsw.Graph.InsertBatch`).
- **Qdrant is held to one segment** (`default_segment_number: 1`), so both engines search one HNSW graph per query. Its default on this machine is 5 segments, each searched at the requested ef and then merged, which made its ef=10 about five times the search work of NuclaDB's.
- **Both engines are driven over gRPC.** Qdrant's upserts and searches use its official Go client (`github.com/qdrant/go-client`); only collection setup and the index-progress poll use its REST API.
- At only 10000 vectors, recall for both engines converges close to 1.0 by moderate ef. See `results-sift1m.md` for the 1M-vector run.
