# Why WAL-then-snapshot, and what it actually costs

NuclaDB's durability story is two pieces: a write-ahead log (WAL) that
fsyncs every insert/delete before it touches the in-memory HNSW graph, and
a periodic snapshot that lets restart skip replaying the WAL from empty.
This is a standard design (it's how Redis's AOF+RDB combination and most
embedded databases work), but "standard" doesn't mean "free," and the
benchmark in `bench/results.md` put a number on the cost that's worth
being honest about.

## The number

Building a 10,000-vector index from an empty store:

| Backend | Build time |
|---|---|
| NuclaDB | 43.9s |
| Qdrant | 124ms |

That's roughly **350x**. Not a rounding error, but a real, structural
difference, and it's worth explaining rather than burying.

## Where it comes from

Every `Insert` in NuclaDB does, in order: encode the record, append it to
the WAL file, **call `fsync`**, then apply it to the HNSW graph
(`internal/storage/wal/wal.go`, `internal/engine/engine.go`). The `fsync`
is the expensive part: it's a synchronous call into the OS that doesn't
return until the write is actually durable on disk, not just sitting in a
page cache buffer that a crash could lose. On this machine that's
consistently costing a few milliseconds per call:

```
43.9s / 10,000 inserts ≈ 4.4ms per insert
```

That lines up with typical `fsync` latency on this filesystem. It isn't a
bug or an inefficient encoding: it's the direct, expected cost of
"durable before the call returns," paid once per write, with nothing
batched.

## Why it's still the right default

An insert that returns success but can be lost on the next crash isn't
actually inserted; it's a promise the system can't keep. For a system
whose entire pitch is "your vectors are safe here," fsync-per-write is the
conservative, correct starting point. Qdrant's 124ms build time doesn't
mean it skipped durability; it means it batches differently (write-ahead
logging with less synchronous per-point overhead, group-committed rather
than one `fsync` per point), which is a legitimate design choice with its
own tradeoffs, not a free lunch NuclaDB is missing out on by accident.

## What would close the gap

The standard fix is **group commit**: batch N pending writes and issue one
`fsync` for the batch instead of one per write, trading a small,
bounded increase in "how much could be lost in the worst-case crash
window" for a large throughput win. `Engine.Insert` already takes a single
mutex around the WAL append + graph mutation, so group commit is a
natural extension — buffer writes arriving within a short window, flush
them together — without changing the crash-recovery contract that's
already tested (`internal/engine/engine_test.go`'s `TestCrashRecovery`
and the WAL-level torn-write tests in `internal/storage/wal/wal_test.go`
would need to keep passing against the batched writer, which is the real
constraint on how aggressive the batching can be).

This is a documented follow-up, not implemented yet — the honest state of
the system today is "durable and slow to bulk-load," not "durable and
fast." Framing it as anything else would undersell exactly the kind of
tradeoff this project exists to demonstrate understanding of.
