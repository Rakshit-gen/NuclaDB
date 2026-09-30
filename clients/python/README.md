# nucladb (Python client)

A thin Python client for [NuclaDB](../../README.md), generated from the
same `proto/nucladb.proto` gRPC contract the Go server implements,
wrapped in a small pythonic layer (a context manager, plain dicts/lists
instead of protobuf messages) plus a `nucladb` command-line tool with
the same commands as the Go `nucladb-cli` (see `../../docs/cli.md`).

## Install

```
pip install ./clients/python
```

That's the one command: it puts both the `nucladb` library and the
`nucladb` CLI on your `PATH` (via the `[project.scripts]` entry point in
`pyproject.toml`), no separate build step. (Named `nucladb`, not
`nucladb-cli`, to avoid colliding with the Go binary of that name if
both are ever installed on the same machine.)

## CLI

```
$ export NUCLADB_ADDR=localhost:9090   # or pass --addr
$ nucladb ping
ok
$ nucladb insert --id 1 --vector 1,0,0,0 --meta team=search
inserted id=1
$ nucladb search --vector 1,0,0,0 --top-k 3
1	score=0.000000	{'team': 'search'}
$ nucladb delete --id 1
deleted=true
```

Run `nucladb <command> --help` for a command's full flag list. Every
data command also accepts `--tenant` at the top level, same semantics
as the Go CLI: omit it for the reserved `default` tenant. For a server
run with `-api-keys`, set `NUCLADB_API_KEY`; set `NUCLADB_TLS=1` if it
serves TLS.

## Library

```python
from nucladb import Client, DistanceMetric

with Client("localhost:9090") as db:
    db.insert("1", [1.0, 0.0, 0.0, 0.0], metadata={"team": "search", "year": "2025"})
    for match in db.search([1.0, 0.0, 0.0, 0.0], top_k=3, metric=DistanceMetric.L2):
        print(match.id, match.score, match.metadata)

    # where clauses: (key, op) or (key, op, value); all must match
    db.search([1.0, 0.0, 0.0, 0.0], where=[("year", "gte", 2024), ("team", "in", ["search", "ads"])])

    db.get("1")                                 # (vector, metadata), or None
    db.update_metadata("1", {"team": "infra"})  # replaces all metadata
    ids = list(db.list())                       # pages through every id
    db.count()

    # tenant admin (needs an admin key when the server uses -api-keys)
    db.create_tenant("acme", dim=384, metric=DistanceMetric.COSINE, max_vectors=1_000_000)
    db.set_quota("acme", max_qps=50)
    db.list_tenants()
    db.delete_tenant("acme")
```

`Client(address, tenant_id="", api_key="", tls=False)`: `tenant_id` scopes
every data call, `api_key` is sent as a bearer token, `tls=True` connects
with the system's root certificates.

## Regenerating the protobuf stubs

`nucladb/nucladb_pb2.py` and `nucladb/nucladb_pb2_grpc.py` are generated,
not hand-written; regenerate them after changing `proto/nucladb.proto`:

```
python -m grpc_tools.protoc -I ../../proto \
  --python_out=nucladb --grpc_python_out=nucladb --pyi_out=nucladb \
  ../../proto/nucladb.proto
```

`nucladb_pb2_grpc.py`'s generated `import nucladb_pb2` needs to stay a
relative import (`from . import nucladb_pb2`) for the package to import
correctly: the generator doesn't know it's producing a package, so fix
that line back up after regenerating.

## Tests

`tests/smoke_test.py` is a real end-to-end check, not a mock: it builds
the actual `nucladbd` Go binary, runs it as a subprocess, and drives it
through insert/batch_upsert/search/filter/delete via this package's own
`Client`, the same pattern `test/chaos/kill_test.go` uses on the Go
side. Run from a checkout with Go available:

```
python tests/smoke_test.py
```
