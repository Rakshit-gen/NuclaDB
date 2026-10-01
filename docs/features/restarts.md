# Quick restarts

Source: [`internal/storage/segment`](../../internal/storage/segment)

## The problem

The write-ahead log (see [Crash-safe writes](durability.md)) holds every
change ever made. Rebuilding the search graph from it on every restart
means redoing all of that work, and the log only grows.

## What NuclaDB does

Every 5 minutes, and on a clean shutdown, NuclaDB saves the whole search
graph to a snapshot file. On restart it loads the newest snapshot and
replays only the log records written after it.

The snapshot is written to a temporary file first and then renamed into
place, so a crash halfway through a save never leaves a broken snapshot
behind. The old one stays until the new one is complete.

## Measured

10,000 vectors of 128 numbers, Apple M4, 3 runs, all three gave the same
result:

| How the server starts | Time to be ready |
|---|---|
| Replaying the whole log (no snapshot yet) | about 300 ms |
| Loading a snapshot | about 5 ms |

That is roughly 60 times faster, and the gap grows with the size of the
collection, since replaying means rebuilding the graph item by item while
loading a snapshot only reads it back.

## Safety checks on load

- The file ends with a CRC32 checksum of everything before it. A single
  flipped byte is caught and the snapshot is rejected.
- The header records the vector size and distance metric. Starting a
  server with different settings against the same data is refused instead
  of returning wrong answers.
- The file is opened with `mmap`, so loading does not need to hold a second
  full copy of it in memory. The graph itself still has to fit in RAM.

## Settings

| Flag | Default | What it changes |
|---|---|---|
| `-snapshot-interval` | 5m | How often to save. Shorter means less log to replay after a crash, at the cost of more disk writes |
| `-data-dir` | ./data | Where the log and snapshot files live |
