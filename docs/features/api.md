# Works from any language

Sources: [`internal/api`](../../internal/api), [`proto/nucladb.proto`](../../proto/nucladb.proto), [`clients/python`](../../clients/python)

The server offers the same operations four ways. Pick whichever fits.

| Way in | Port | Good for |
|---|---|---|
| gRPC | 9090 | Services in Go, Java, Rust, Node and anything else with gRPC support. Fastest option |
| REST (JSON) | 8080 | Quick tests with `curl`, languages without gRPC, browsers behind a proxy |
| `nucladb-cli` | uses gRPC | Trying things from a terminal, scripts |
| Python client | uses gRPC | Python apps and notebooks |

## The same search, four ways

**curl**

```sh
curl -s localhost:8080/v1/search -d '{"query":[1,0,0,0],"top_k":5}'
```

**CLI**

```sh
nucladb-cli search -vector=1,0,0,0 -top-k=5
```

**Python**

```python
from nucladb import Client

with Client("localhost:9090") as db:
    db.insert("1", [1.0, 0.0, 0.0, 0.0], metadata={"team": "search"})
    for match in db.search([1.0, 0.0, 0.0, 0.0], top_k=5):
        print(match.id, match.score)
```

**gRPC** (any language): generate a client from
[`proto/nucladb.proto`](../../proto/nucladb.proto) and call `Search`.

## REST endpoints

| Endpoint | What it does |
|---|---|
| `POST /v1/vectors` | Add or replace one vector |
| `POST /v1/vectors:batch` | Add or replace many vectors with one disk flush |
| `GET /v1/vectors/{id}` | Read one vector |
| `PATCH /v1/vectors/{id}` | Replace its metadata |
| `DELETE /v1/vectors/{id}` | Delete it |
| `GET /v1/vectors` | Page through ids |
| `GET /v1/vectors:count` | Count vectors |
| `POST /v1/search` | Find the closest matches, with optional filters |
| `POST /v1/tenants`, `GET /v1/tenants` | Create and list tenants |
| `DELETE /v1/tenants/{id}` | Delete a tenant and its files |
| `PUT /v1/tenants/{id}/quota` | Change a tenant's limits |
| `GET /metrics` | Prometheus metrics |

The REST layer is written by hand on top of the same service the gRPC
server uses, so both always behave the same.

## Speed over the network

Measured through the real gRPC API, 10,000 vectors, `ef_search` 100:

| Clients at once | Searches/s | Median time |
|---|---|---|
| 1 | 4,305 | 0.24 ms |
| 8 | 19,129 | 0.40 ms |
| 128 | 25,752 | 3.5 ms |

With 10% of requests being inserts at 128 connections, the server handled
10,980 searches and 1,215 inserts per second with zero errors. Full
results: [`bench/results-loadtest.md`](../../bench/results-loadtest.md).

## Limits worth knowing

- A single request or gRPC message can be up to 64 MiB
  (`-max-message-bytes`).
- `top_k` is capped at 1,000 and `ef_search` at 10,000.

## Watching it run

`/metrics` on port 8080 serves request counts and durations for
Prometheus, plus per-tenant usage. Every gRPC call also gets an
OpenTelemetry trace span. Sending spans is off by default; set
`OTEL_EXPORTER_OTLP_ENDPOINT` to send them to a collector, or
`NUCLADB_TRACE_STDOUT=1` to print them.

Full CLI reference: [`docs/cli.md`](../cli.md).
