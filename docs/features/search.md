# Fast similarity search

Source: [`internal/index/hnsw`](../../internal/index/hnsw)

## The problem

To find the 10 items closest to a question, the simple way is to measure
the distance to every stored item and keep the best 10. That is exact, but
the work grows with every item you add. At a million items, every search
does a million distance calculations.

## What NuclaDB does instead

It keeps the items in a graph called HNSW (Hierarchical Navigable Small
World). Every item is linked to a handful of nearby items. A few items also
sit on higher "express" layers with longer links, like highways above
local roads.

A search starts at the top, takes the long links to get roughly close,
then drops down a layer and keeps walking toward the question until no
neighbour is any closer. It only ever measures the items it walks past, so
it checks a small fraction of the collection.

The catch: it can miss a true match now and then. How often depends on how
hard you let it look.

## How hard it looks: `ef_search`

`ef_search` is how many candidates the search keeps in mind while walking.
Higher means more right answers and slower searches. You can set it on
every search, so one app can trade speed for accuracy without restarting
anything.

Measured on 10,000 vectors, single connection, median of 5 runs:

| ef_search | Right answers (recall@10) | Searches per second | Typical time per search |
|---|---|---|---|
| 10 | 93.2% | 13,914 | 0.07 ms |
| 20 | 98.1% | 13,966 | 0.07 ms |
| 50 | 99.6% | 10,653 | 0.09 ms |
| 100 | 99.8% | 7,542 | 0.13 ms |
| 200 | 100% | 5,822 | 0.17 ms |

Accuracy stops improving much after 50. Going from 50 to 200 adds 0.4% for
nearly half the speed. For this data, 50 is the sensible default. Your data
may differ, so try a few values on your own vectors.

## Compared with Qdrant

Same machine, same data, both called over their network APIs:

| ef_search | NuclaDB searches/s | Qdrant searches/s | NuclaDB recall | Qdrant recall |
|---|---|---|---|---|
| 10 | 13,914 | 7,624 | 93.2% | 95.9% |
| 50 | 10,653 | 7,051 | 99.6% | 99.8% |
| 200 | 5,822 | 5,512 | 100% | 100% |

NuclaDB answers more searches per second at every setting and uses 46 MB
of memory against Qdrant's 116 MB. Qdrant gets slightly more right at the
lowest settings. Building the index takes 416 ms for NuclaDB and 557 ms for
Qdrant.

## Under load

With many clients at once (10,000 vectors, `ef_search` 100, 99% recall):

| Connections | Searches/s | Median time | Slowest 1% |
|---|---|---|---|
| 1 | 4,305 | 0.24 ms | 0.35 ms |
| 8 | 19,129 | 0.40 ms | 0.97 ms |
| 32 | 22,390 | 1.2 ms | 5.4 ms |
| 128 | 25,752 | 3.5 ms | 28.5 ms |

A one-minute run at 128 connections held 25,103 searches per second with
zero errors. Past about 32 connections, more clients mostly add waiting
time rather than throughput.

## Filters

You can limit a search to items whose metadata matches, for example only
`team = search` from 2024 onwards:

```sh
curl -s localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":5,
  "where":[{"key":"team","op":"eq","value":"search"},
           {"key":"year","op":"gte","value":"2024"}]}'
```

Operators: `eq`, `ne`, `in`, `not_in`, `gt`, `gte`, `lt`, `lte` and
`exists`. When a filter matches only a few items (4,096 or fewer by
default, set with `-exact-filter-limit`), NuclaDB skips the graph and checks
each match directly, so a narrow filter still returns a full, exact top 10.

## Settings

| Flag | Default | What it changes |
|---|---|---|
| `-m` | 16 | Links per item. More links use more memory but make the graph easier to search |
| `-ef-construction` | 200 | How hard it looks when adding an item. Higher builds a better graph, more slowly |
| `ef_search` (per request) | set by caller | How hard each search looks, see above |
| `-metric` | cosine | How distance is measured: `cosine`, `l2` or `dot` |

More detail: [Tuning HNSW](../writeups/02-hnsw-ef-tuning.md).
