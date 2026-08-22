# What product quantization actually cost

`internal/index/pq` trades recall for a fixed, large memory reduction:
a vector of `Dim` float32s (4 bytes each) becomes an `M`-byte code, one
byte per subspace, each byte indexing a trained k-means centroid. At
`M=16` on a `Dim=64` vector, that's 256 bytes down to 16 — a 16x
reduction, always, regardless of the data. The question worth measuring
isn't whether it's lossy (it is, by construction), but how lossy at a
compression ratio someone would actually want to use.

## The number

From `internal/index/pq/pq_test.go`'s `TestIndexRecallVsExact`, on 3,000
uniform-random 64-dimensional vectors, `M=16`, `K=256` centroids per
subspace, flat (unindexed, brute-force-over-codes) search:

**57.7% recall@10** against exact brute-force search on the same data.

That's a real, deterministic, reproducible number — not a rough estimate.
Run five times, it came back 0.577 every time (fixed seeds, and the
result turned out not to depend on Go's randomized map iteration order the
way an earlier version of this test's *ground truth* generation
accidentally did — see the git history for that fix).

## Why 57.7% and not higher

Two things are stacked against recall here, both deliberately:

1. **No re-ranking stage.** Production PQ setups (Faiss's `IndexIVFPQ`,
   for instance) typically retrieve a wider candidate set via approximate
   distance, then re-rank the top candidates using their real,
   uncompressed vectors for a final accurate ordering. NuclaDB's
   `pq.Index.Search` does neither — it's PQ alone, unindexed, which is
   close to the worst-case configuration of the technique. It exists as a
   correctness-and-tradeoff demonstration, not (yet) a
   production-recommended search path.
2. **No coarse quantizer (no IVF).** Without a first-stage clustering step
   to narrow the candidate set before scoring, every query scores every
   stored code — O(N·M) per query. That's a cost problem more than a
   recall problem, but it's the reason nobody ships flat PQ alone at real
   scale.

## The honest takeaway

16x memory reduction for roughly half the recall, with no re-ranking and
no IVF, is a real and unglamorous number — and reporting it instead of a
cherrier one (a smaller `M`, a friendlier dataset, a re-ranked variant) is
the point. The asymmetric distance computation itself is verified exact
(`TestDistanceTableMatchesExactDistanceToDecodedCode` checks ADC distance
against directly computing exact distance to the decoded code, agreeing to
float32 precision) — the recall loss is entirely attributable to
quantization error, not a bug in how it's scored. Closing the gap without
touching the underlying compression ratio means adding re-ranking or IVF,
both documented as follow-up work rather than claimed as done.
