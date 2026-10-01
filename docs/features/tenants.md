# Many apps, one server

Source: [`internal/engine`](../../internal/engine) (see `store.go`)

## What a tenant is

A tenant is a separate space inside one NuclaDB server. Think of it as a
separate database per app or per customer. Each tenant has:

- its own search graph, log file and snapshot file on disk
- its own vector size and distance metric, so one app can store
  1536-number OpenAI embeddings while another stores 384-number ones
- its own limits: a maximum number of vectors and a maximum number of
  requests per second

Searches in one tenant never see another tenant's data, because the data
is not in the same files.

## Creating one

```sh
nucladb-cli create-tenant -id=acme -dim=384 -metric=cosine -max-vectors=100000 -max-qps=200
nucladb-cli insert -tenant=acme -id=1 -vector=...
nucladb-cli tenants
```

Or over REST:

```sh
curl -s localhost:8080/v1/tenants -d '{"tenant_id":"acme","dim":384,"metric":"cosine",
  "max_vectors":100000,"max_qps":200}'
```

## Limits

- **Vector limit.** A write that would go over `max_vectors` is rejected
  before it reaches the log, so nothing is half applied.
- **Request rate.** Each tenant has a token bucket refilled at `max_qps`
  per second. A batch of 500 vectors costs 500 tokens, so batches cannot
  be used to get around the limit. Requests over the limit get
  `ResourceExhausted` (gRPC) or HTTP 429.
- Limits can be changed at any time with `set-quota` or
  `PUT /v1/tenants/{id}/quota`, and they survive restarts.

## API keys

Start the server with `-api-keys=keys.json`:

```json
[{"key": "at-least-16-characters", "tenants": ["acme"]},
 {"key": "an-admin-key-for-ops", "tenants": ["*"], "admin": true}]
```

A key only reaches the tenants it lists. Creating, deleting and changing
limits on tenants needs an admin key. Clients send the key as
`Authorization: Bearer <key>` or `X-API-Key: <key>`. Add `-tls-cert` and
`-tls-key` to encrypt traffic. Without `-api-keys`, every request is
allowed, which is fine on your own machine but not on a network.

## Lots of tenants

Tenants nobody has used for 30 minutes are closed so thousands of them do
not all hold files open (`-tenant-idle-timeout`). The next request opens
the tenant again from its snapshot, which takes a few milliseconds (see
[Quick restarts](restarts.md)).

There is no separate benchmark for multi-tenant load yet. Each tenant runs
the same engine measured in [Fast similarity search](search.md).
