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

## Multi-tenancy

Every data command (`insert`, `batch-upsert`, `search`, `delete`) accepts
`-tenant`, scoping the call to a tenant-isolated index: its own graph, WAL,
and snapshot files on disk, invisible to every other tenant. Omitting
`-tenant` uses the reserved `default` tenant, which has no quota — so
single-tenant usage needs no flags at all.

A tenant other than `default` must be created first via `create-tenant`.

## `ping`

Checks that the server is reachable, via the standard gRPC health-checking
protocol (`grpc.health.v1.Health/Check`).

```
$ nucladb-cli ping
ok
```

## `create-tenant`

Provisions a new, isolated tenant with an optional storage quota and
request-rate limit. Creating a tenant id that already exists is an error.

| Flag           | Required | Description                                             |
|----------------|----------|------------------------------------------------------------|
| `-id`          | yes      | Tenant id                                                   |
| `-max-vectors` | no       | Storage quota: max vectors this tenant may hold (default: unlimited) |
| `-max-qps`     | no       | Rate limit: max requests/sec for this tenant (default: unlimited)    |

```
$ nucladb-cli create-tenant -id=acme -max-vectors=1000000
created tenant "acme"
```

## `insert`

Insert or update (upsert) a single vector.

| Flag       | Required | Description                                          |
|------------|----------|-------------------------------------------------------|
| `-id`      | yes      | Vector id, a decimal `uint64` (e.g. `1`, `42`)         |
| `-vector`  | yes      | Comma-separated `float32` values, e.g. `0.1,0.2,0.3`   |
| `-meta`    | no       | `key=value` metadata pair; repeat the flag for more    |
| `-tenant`  | no       | Tenant id (default: the reserved `default` tenant)     |

```
$ nucladb-cli insert -id=1 -vector=1,0,0,0 -meta=team=search
inserted id=1

$ nucladb-cli insert -id=1 -tenant=acme -vector=1,0,0,0 -meta=who=acme
inserted id=1
```

Note the second example reuses id `1` under a different tenant — this does
not collide with the first insert, since tenants are fully isolated
indexes, not a shared id space with a tenant label attached.

## `batch-upsert`

Insert or update many vectors at once from a JSON file.

| Flag      | Required | Description                                                   |
|-----------|----------|------------------------------------------------------------------|
| `-file`   | yes      | Path to a JSON array file                                         |
| `-tenant` | no       | Tenant id applied to every item that doesn't set its own `tenant_id` |

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
| `-tenant`  | no       | Tenant id (default: the reserved `default` tenant)               |

```
$ nucladb-cli search -vector=1,0,0,0 -top-k=3
1	score=0.000000	map[team:search]
3	score=2.000000	map[]
2	score=2.000000	map[team:infra]

$ nucladb-cli search -vector=1,0,0,0 -top-k=3 -filter=team=infra
2	score=2.000000	map[team:infra]

$ nucladb-cli search -vector=1,0,0,0 -top-k=5 -tenant=acme
1	score=0.000000	map[who:acme]
```

The `acme` search above returns only `acme`'s own data — a search on
`default` for the same query vector never sees it, and vice versa.

Filtering is a post-filter over an overfetched candidate set (see
`internal/engine/engine.go`): if a filter is very selective, the search
widens its internal candidate window and retries a bounded number of
times before returning fewer than `top-k` results.

## `delete`

Delete a vector by id. Deleting an id that doesn't exist is not an error —
deletes are idempotent, since WAL replay during crash recovery may safely
re-apply the same delete.

| Flag      | Required | Description                                       |
|-----------|----------|------------------------------------------------------|
| `-id`     | yes      | Vector id to delete                                    |
| `-tenant` | no       | Tenant id (default: the reserved `default` tenant)      |

```
$ nucladb-cli delete -id=1
deleted=true
```

## Quota and rate-limit errors

Exceeding a tenant's `-max-vectors` returns `ResourceExhausted`:

```
$ nucladb-cli insert -id=4 -tenant=acme -vector=1,1,1,1
nucladb-cli: rpc error: code = ResourceExhausted desc = engine: tenant storage quota exceeded
```

Referencing a tenant that was never created returns `NotFound`:

```
$ nucladb-cli insert -id=5 -tenant=doesnotexist -vector=1,1,1,1
nucladb-cli: rpc error: code = NotFound desc = engine: tenant not found
```

Exceeding `-max-qps` returns the same `ResourceExhausted` code with a
"rate limit exceeded" message.

## REST equivalent

Every command above has a REST/JSON equivalent served by `nucladbd` on its
HTTP port (default `:8080`), documented inline in
`internal/api/gateway/gateway.go`. `tenant_id` is a JSON body field
(`insert`, `batch-upsert`, `search`) or a `?tenant_id=` query parameter
(`delete`, since it has no body):

```
POST   /v1/tenants        (CreateTenant)
POST   /v1/vectors        (Insert)
POST   /v1/vectors:batch  (BatchUpsert)
DELETE /v1/vectors/{id}   (Delete)
POST   /v1/search         (Search)
```

```
$ curl -s -X POST localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":2}'
{"matches":[{"id":"3","score":2},{"id":"2","score":2,"metadata":{"team":"infra"}}]}
```

## Known limitation: quota is not yet persisted

A tenant's quota (`-max-vectors` / `-max-qps`) lives in server process
memory, set at `create-tenant` time. It is not written to disk, so a
`nucladbd` restart brings every tenant back with quota reset to unlimited
— vectors and metadata are fully durable across restart (WAL + snapshot,
same as any other tenant data), only the quota policy is not. Re-applying
quotas on startup is a documented follow-up, not a silent gap.
