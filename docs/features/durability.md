# Crash-safe writes

Source: [`internal/storage/wal`](../../internal/storage/wal)

## The promise

When NuclaDB answers OK to a write, that write is on disk. If the process
is killed, the machine loses power, or the operating system crashes a
moment later, the write is still there after restart.

## How it keeps that promise

Every insert, update and delete goes through the same steps, in this order:

1. Write the change to the end of a log file, called the write-ahead log
   (WAL).
2. Ask the operating system to flush that file to the physical disk
   (`fsync`), and wait until it confirms.
3. Apply the change to the in-memory search graph.
4. Answer OK.

Because the log is written before anything else, a crash at any point
leaves either a complete record (which is replayed on restart) or a
half-written one at the very end, which was never acknowledged and is
safely dropped.

Each record carries a CRC32 checksum, so a torn or damaged record is
always caught. If damage shows up in the middle of the log rather than at
the end, NuclaDB refuses to start instead of quietly throwing away the
writes after it, and leaves the file untouched for you to inspect.

## What it costs, and how that cost was cut

Waiting for the disk is slow. The first version flushed once per vector,
and building a 10,000-vector index took 43.9 seconds, about 4.4 ms per
write.

The fix is group commit: when many writes arrive together, they share a
single flush. Batch uploads use one flush per batch, and separate clients
writing at the same time share flushes automatically. Nobody gets an OK
before their write is on disk, so the promise is unchanged.

| Version | Time to load 10,000 vectors |
|---|---|
| One flush per vector | 43.9 s |
| One flush per batch of 500 | 3.2 s |
| Plus faster distance code and a parallel graph build | 416 ms |
| Qdrant, for comparison | 557 ms |

Use `batch-upsert` (CLI), `POST /v1/vectors:batch` (REST) or `BatchUpsert`
(gRPC) when loading a lot of data.

## How it is tested

- `test/chaos/kill_test.go` starts the real server, writes 50 vectors,
  kills it with `kill -9` (no clean shutdown), and repeats 4 times. After
  the last restart, all 200 acknowledged vectors must be found.
- The WAL tests cut records in half, flip checksum bytes and write
  impossible length values, then check that recovery keeps every good
  record and stops safely at the bad one.
- `internal/engine` tests crash between steps: in the middle of a batch,
  during a snapshot, and between saving metadata and the graph.

More detail: [Why WAL-then-snapshot](../writeups/01-wal-then-snapshot.md).
