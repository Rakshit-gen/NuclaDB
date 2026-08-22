# nucladb-cli command reference

`nucladb-cli` is a thin gRPC client for a running `nucladbd` server. Every
subcommand below was run against a real local server to produce the
example output — nothing here is invented.

## Connecting

The CLI talks to `localhost:9090` by default. Override with an environment
variable:

```
export NUCLADB_ADDR=localhost:9090
```

## `ping`

Checks that the server is reachable, via the standard gRPC health-checking
protocol (`grpc.health.v1.Health/Check`).

```
$ nucladb-cli ping
ok
```

## `insert`

Insert or update (upsert) a single vector.

| Flag       | Required | Description                                          |
|------------|----------|-------------------------------------------------------|
| `-id`      | yes      | Vector id, a decimal `uint64` (e.g. `1`, `42`)         |
| `-vector`  | yes      | Comma-separated `float32` values, e.g. `0.1,0.2,0.3`   |
| `-meta`    | no       | `key=value` metadata pair; repeat the flag for more    |

```
$ nucladb-cli insert -id=1 -vector=1,0,0,0 -meta=team=search
inserted id=1
```

## `batch-upsert`

Insert or update many vectors at once from a JSON file.

| Flag    | Required | Description             |
|---------|----------|---------------------------|
| `-file` | yes      | Path to a JSON array file  |

File format:

```json
[
  {"id": "10", "values": [1, 1, 0, 0]},
  {"id": "11", "values": [1, 1, 1, 0], "metadata": {"team": "infra"}}
]
```

```
$ nucladb-cli batch-upsert -file=batch.json
upserted 2 vectors
```

## `search`

Find the nearest neighbors of a query vector. Lower `score` means closer,
matching the configured distance metric (the server-wide metric — a
`nucladbd` instance is built with one fixed metric, since HNSW bakes the
metric into which neighbors get linked at construction time).

| Flag       | Required | Description                                                   |
|------------|----------|-----------------------------------------------------------------|
| `-vector`  | yes      | Comma-separated `float32` query vector                          |
| `-top-k`   | no       | Number of results to return (default `10`)                      |
| `-ef`      | no       | Candidate beam width; higher = better recall, slower (default: `top-k`) |
| `-filter`  | no       | `key=value` metadata the result must match; repeat for AND of multiple filters |

```
$ nucladb-cli search -vector=1,0,0,0 -top-k=3
1	score=0.000000	map[team:search]
3	score=2.000000	map[]
2	score=2.000000	map[team:infra]

$ nucladb-cli search -vector=1,0,0,0 -top-k=3 -filter=team=infra
2	score=2.000000	map[team:infra]
```

Filtering is a post-filter over an overfetched candidate set (see
`internal/engine/engine.go`): if a filter is very selective, the search
widens its internal candidate window and retries a bounded number of
times before returning fewer than `top-k` results.

## `delete`

Delete a vector by id. Deleting an id that doesn't exist is not an error —
deletes are idempotent, since WAL replay during crash recovery may safely
re-apply the same delete.

| Flag  | Required | Description        |
|-------|----------|----------------------|
| `-id` | yes      | Vector id to delete   |

```
$ nucladb-cli delete -id=1
deleted=true
```

## REST equivalent

Every command above has a REST/JSON equivalent served by `nucladbd` on its
HTTP port (default `:8080`), documented inline in
`internal/api/gateway/gateway.go`:

```
POST   /v1/vectors        (Insert)
POST   /v1/vectors:batch  (BatchUpsert)
DELETE /v1/vectors/{id}   (Delete)
POST   /v1/search         (Search)
```

```
$ curl -s -X POST localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":2}'
{"matches":[{"id":"3","score":2},{"id":"2","score":2,"metadata":{"team":"infra"}}]}
```
