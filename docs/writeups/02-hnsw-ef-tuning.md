# Tuning HNSW: what the recall/latency curve actually looks like

HNSW has three knobs that matter: `M` (graph connectivity), `efConstruction`
(search width during index build), and `efSearch` (search width at query
time). The first two are fixed when the graph is built; `efSearch` is
free to change per query. This post is about that last one, because it's
the one every caller actually has to choose, and "higher is better" only
tells you the direction, not how much it costs.

## The real curve

From `bench/results.md`, NuclaDB against SIFT-small (10,000 vectors,
dim=128, `M=16`, `efConstruction=200`), sweeping `efSearch`:

| ef | recall@10 | QPS |
|---|---|---|
| 10 | 0.896 | 7432 |
| 20 | 0.962 | 6299 |
| 50 | 0.997 | 4933 |
| 100 | 1.000 | 3676 |
| 200 | 1.000 | 2368 |

Two things worth noticing:

**Recall saturates well before `ef` does.** Going from `ef=10` to
`ef=50` buys +10 points of recall (0.896 → 0.997). Going from `ef=50` to
`ef=200` buys nothing measurable (0.997 → 1.000, and even that last 0.3%
might just be noise at only 10,000 vectors) while cutting QPS by more than
half. Past the saturation point, raising `ef` further is pure latency cost
with no accuracy benefit — a genuinely easy mistake to make by reasoning
"more thorough search = better" without looking at where the curve
actually flattens.

**The relationship isn't linear.** QPS roughly halves from `ef=10` to
`ef=200` (7432 → 2368), but recall gain is front-loaded almost entirely
into the first jump. The efficient operating point for this dataset is
closer to `ef=50` than either extreme — 99.7% recall at nearly 5,000 QPS,
versus paying for `ef=200`'s marginal 0.3% at less than half the
throughput.

## Why this isn't a fixed answer

This curve is specific to this dataset's intrinsic dimensionality and
size. A higher-dimensional or more clustered dataset needs a different
`ef` to hit the same recall target — there's no universal "good" value,
which is why `SearchRequest.ef_search` is exposed as a per-query
parameter (`proto/nucladb.proto`) rather than baked into the server config.
The right way to pick it for a real workload is exactly what this
benchmark did: sweep it against representative data and representative
queries, and look at where the curve actually bends, not where intuition
says it should.

## What `M` and `efConstruction` would show

This post only covers `efSearch` because it's the knob every query
controls; `M` and `efConstruction` are build-time decisions baked into a
running graph, so measuring their effect means building multiple graphs at
different settings and comparing recall/build-time/memory across them —
a natural next benchmark, not run here yet. The expectation (from the
HNSW paper and from `internal/index/hnsw/graph_test.go`'s recall test,
which holds `M=16, efConstruction=200` and gets 99.8% recall@10 on random
uniform vectors) is that higher `M` trades more memory (more neighbor
edges per node) for a better-connected graph that needs less `efSearch` to
hit the same recall — but that's a claim to verify with numbers, the same
way the `efSearch` curve above was, not to state without evidence.
