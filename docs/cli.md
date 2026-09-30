# nucladb-cli command reference

`nucladb-cli` is a thin gRPC client for a running `nucladbd` server. Every
subcommand below was run against a real local server to produce the
example output, nothing here is invented.

## Connecting

The CLI talks to `localhost:9090` by default. Override with an environment
variable:

```
export NUCLADB_ADDR=localhost:9090
```

If the server runs with `-api-keys`, set the key to send with every call,
and set `NUCLADB_TLS=1` if it serves TLS (`-tls-cert`/`-tls-key`):

```
export NUCLADB_API_KEY=your-key
export NUCLADB_TLS=1
```

Without a key, such a server answers `Unauthenticated`; with a key that
doesn't cover the tenant, `PermissionDenied`. Creating, listing,
deleting and re-quota'ing tenants needs an admin key.

## Multi-tenancy

Every data command (`insert`, `batch-upsert`, `search`, `delete`) accepts
`-tenant`, scoping the call to a tenant-isolated index: its own graph, WAL,
and snapshot files on disk, invisible to every other tenant. Omitting
`-tenant` uses the reserved `default` tenant, which has no quota, so
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
| `-dim`         | no       | Vector dimension (default: the server's `-dim`)            |
| `-metric`      | no       | `cosine`, `l2` or `dot` (default: the server's `-metric`)  |

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

Note the second example reuses id `1` under a different tenant. This does
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
under the tenant's distance metric (set when the tenant is created and
fixed after that, since HNSW bakes the metric into which neighbors get
linked at construction time).

| Flag       | Required | Description                                                   |
|------------|----------|-----------------------------------------------------------------|
| `-vector`  | yes      | Comma-separated `float32` query vector                          |
| `-top-k`   | no       | Number of results to return (default `10`)                      |
| `-ef`      | no       | Candidate beam width; higher = better recall, slower (default: `top-k`) |
| `-filter`  | no       | `key=value` metadata the result must match; repeat for AND of multiple filters |
| `-where`   | no       | `key:op` or `key:op:value` filter, op one of `eq ne in not_in gt gte lt lte exists`; `in`/`not_in` take comma-separated values; repeatable |
| `-tenant`  | no       | Tenant id (default: the reserved `default` tenant)               |

```
$ nucladb-cli search -vector=1,0,0,0 -top-k=3
1	score=0.000000	map[team:search]
3	score=2.000000	map[]
2	score=2.000000	map[team:infra]

$ nucladb-cli search -vector=1,0,0,0 -top-k=3 -filter=team=infra
2	score=2.000000	map[team:infra]

$ nucladb-cli search -vector=1,0,0,0 -where=year:gte:2024 -where=team:in:search,infra
2	score=2.000000	map[team:infra year:2025]

$ nucladb-cli search -vector=1,0,0,0 -top-k=5 -tenant=acme
1	score=0.000000	map[who:acme]
```

The `acme` search above returns only `acme`'s own data. A search on
`default` for the same query vector never sees it, and vice versa.

`gt`, `gte`, `lt` and `lte` compare as numbers; a stored value that isn't
a number never matches them.

Filtering checks a key/value index first. If the filter matches at most
4096 vectors (the server's `-exact-filter-limit`), the search scores just those vectors and returns the exact
nearest `top-k` among them. A broader filter post-filters a widened graph
search instead (see `internal/engine/engine.go`).

## `get`, `update-metadata`, `list`, `count`

```
$ nucladb-cli get -id=2
2	[0 1 0 0]	map[team:ads year:2025]

$ nucladb-cli update-metadata -id=2 -meta=team=infra -meta=year=2025
updated id=2

$ nucladb-cli list
1
2

$ nucladb-cli count
2
```

`update-metadata` replaces all of a vector's metadata and leaves the
vector alone. `list` prints ids in order, fetching `-page-size` at a time
(up to 1000). All four take `-tenant`.

## `tenants`, `set-quota`, `delete-tenant`

```
$ nucladb-cli tenants
default	vectors=2	dim=4	metric=l2	max_vectors=0	max_qps=0
t2	vectors=0	dim=2	metric=cosine	max_vectors=5	max_qps=0

$ nucladb-cli set-quota -id=t2 -max-vectors=5
set quota for "t2"

$ nucladb-cli delete-tenant -id=t2
deleted tenant "t2"
```

`delete-tenant` removes the tenant's files. The `default` tenant can't be
deleted.

## `delete`

Delete a vector by id. Deleting an id that doesn't exist is not an error:
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
on requests with a body (`insert`, `batch-upsert`, `search`, the `PATCH`)
or a `?tenant_id=` query parameter on the rest:

```
POST   /v1/tenants               (CreateTenant)
GET    /v1/tenants               (ListTenants)
DELETE /v1/tenants/{id}          (DeleteTenant)
PUT    /v1/tenants/{id}/quota    (SetQuota)
POST   /v1/vectors               (Insert)
POST   /v1/vectors:batch         (BatchUpsert)
GET    /v1/vectors/{id}          (Get)
PATCH  /v1/vectors/{id}          (UpdateMetadata)
DELETE /v1/vectors/{id}          (Delete)
GET    /v1/vectors               (List, ?page_size=&page_token=)
GET    /v1/vectors:count         (Count)
POST   /v1/search                (Search, filters in "where")
```

Send the API key as `Authorization: Bearer <key>` or `X-API-Key: <key>`.

```
$ curl -s -X POST localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":2}'
{"matches":[{"id":"3","score":2},{"id":"2","score":2,"metadata":{"team":"infra"}}]}
```

## Quotas across restarts

A tenant's quota (`-max-vectors` / `-max-qps`) is saved to `quota.json` in
the tenant's data directory when it is created, and read back when
`nucladbd` starts, so a restart keeps every tenant's limits. Tenants
created by older versions have no `quota.json` and come back unlimited.
