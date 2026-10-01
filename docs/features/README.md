# What NuclaDB does

NuclaDB stores vectors (lists of numbers that describe text, images or
anything else) and finds the ones closest to a vector you ask about. Each
page below explains one part in plain words, shows how to use it, and gives
the measured numbers behind it.

| Part | In plain words | Running in the server today? |
|---|---|---|
| [Fast similarity search](search.md) | Finds the closest matches without checking every item | Yes |
| [Crash-safe writes](durability.md) | Once it says OK, your data survives a crash or power cut | Yes |
| [Quick restarts](restarts.md) | Reloads from a saved file instead of starting over | Yes |
| [Many apps, one server](tenants.md) | Each app gets its own space, key and limits | Yes |
| [Works from any language](api.md) | gRPC, REST, a CLI and a Python client | Yes |
| [Compression](compression.md) | Stores items in about a sixteenth of the space | Not yet, library only |
| [Multiple machines](cluster.md) | Splits one collection across several servers | Not yet, library only |

## Where the numbers come from

Unless a page says otherwise, numbers were measured on an Apple M4 laptop
(10 cores, 16 GB) with the SIFT-small dataset: 10,000 vectors of 128
numbers each, plus 100 test searches with known correct answers. The
benchmark code lives in [`bench/`](../../bench) and the full results are in
[`bench/results.md`](../../bench/results.md),
[`bench/results-loadtest.md`](../../bench/results-loadtest.md) and
[`bench/results-cluster.md`](../../bench/results-cluster.md).

Two words come up a lot:

- **Recall** is the share of the true 10 closest matches a search found.
  0.99 means it found 99 of every 100.
- **QPS** is searches answered per second.
