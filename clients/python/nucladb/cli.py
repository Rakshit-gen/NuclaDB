"""`nucladb`: a thin command-line client over this package's own Client,
mirroring cmd/nucladb-cli's commands and semantics (see docs/cli.md in
the main repo) so anyone who's used the Go CLI already knows this one.
Installed as a single console-script entry point (see pyproject.toml)
so `pip install nucladb` (or `pipx install nucladb`) is enough to get
the `nucladb` command on PATH — no separate build step.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from .client import Client, DistanceMetric

DEFAULT_ADDR = os.environ.get("NUCLADB_ADDR", "localhost:9090")

_METRICS = {
    "cosine": DistanceMetric.COSINE,
    "l2": DistanceMetric.L2,
    "dot": DistanceMetric.DOT,
}


def _parse_vector(s: str) -> list[float]:
    return [float(x) for x in s.split(",")]


def _parse_meta(pairs: list[str] | None) -> dict[str, str]:
    meta = {}
    for pair in pairs or []:
        key, _, value = pair.partition("=")
        meta[key] = value
    return meta


def cmd_ping(client: Client, args: argparse.Namespace) -> None:
    stub = health_pb2_grpc.HealthStub(client.channel)
    resp = stub.Check(health_pb2.HealthCheckRequest())
    if resp.status == health_pb2.HealthCheckResponse.SERVING:
        print("ok")
    else:
        print(f"not serving: {health_pb2.HealthCheckResponse.ServingStatus.Name(resp.status)}")
        sys.exit(1)


def cmd_create_tenant(client: Client, args: argparse.Namespace) -> None:
    client.create_tenant(args.id, max_vectors=args.max_vectors, max_qps=args.max_qps)
    print(f'created tenant "{args.id}"')


def cmd_insert(client: Client, args: argparse.Namespace) -> None:
    id_ = client.insert(args.id, _parse_vector(args.vector), _parse_meta(args.meta))
    print(f"inserted id={id_}")


def cmd_batch_upsert(client: Client, args: argparse.Namespace) -> None:
    with open(args.file) as f:
        items = json.load(f)
    vectors = [
        (item["id"], item["values"], item.get("metadata")) for item in items
    ]
    n = client.batch_upsert(vectors)
    print(f"upserted {n} vectors")


def cmd_search(client: Client, args: argparse.Namespace) -> None:
    metric = _METRICS.get(args.metric, DistanceMetric.UNSPECIFIED) if args.metric else DistanceMetric.UNSPECIFIED
    results = client.search(
        _parse_vector(args.vector),
        top_k=args.top_k,
        metric=metric,
        ef_search=args.ef or 0,
        filters=_parse_meta(args.filter),
    )
    for r in results:
        print(f"{r.id}\tscore={r.score:.6f}\t{r.metadata}")


def cmd_delete(client: Client, args: argparse.Namespace) -> None:
    deleted = client.delete(args.id)
    print(f"deleted={str(deleted).lower()}")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="nucladb", description="NuclaDB command-line client")
    p.add_argument("--addr", default=DEFAULT_ADDR, help=f"server address (default: {DEFAULT_ADDR}, or $NUCLADB_ADDR)")
    p.add_argument("--tenant", default="", help="tenant id (default: the reserved 'default' tenant)")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="check the server is reachable").set_defaults(func=cmd_ping)

    ct = sub.add_parser("create-tenant", help="provision a new isolated tenant")
    ct.add_argument("--id", required=True)
    ct.add_argument("--max-vectors", type=int, default=0)
    ct.add_argument("--max-qps", type=float, default=0)
    ct.set_defaults(func=cmd_create_tenant)

    ins = sub.add_parser("insert", help="insert or update a single vector")
    ins.add_argument("--id", required=True)
    ins.add_argument("--vector", required=True, help="comma-separated floats, e.g. 0.1,0.2,0.3")
    ins.add_argument("--meta", action="append", help="key=value metadata pair; repeat for more")
    ins.set_defaults(func=cmd_insert)

    bu = sub.add_parser("batch-upsert", help="insert or update many vectors from a JSON file")
    bu.add_argument("--file", required=True, help='JSON array of {"id", "values", "metadata"}')
    bu.set_defaults(func=cmd_batch_upsert)

    se = sub.add_parser("search", help="find the nearest neighbors of a query vector")
    se.add_argument("--vector", required=True)
    se.add_argument("--top-k", type=int, default=10)
    se.add_argument("--ef", type=int, default=0, help="candidate beam width (default: top-k)")
    se.add_argument("--metric", choices=sorted(_METRICS), default=None)
    se.add_argument("--filter", action="append", help="key=value metadata filter; repeat for AND")
    se.set_defaults(func=cmd_search)

    de = sub.add_parser("delete", help="delete a vector by id (idempotent)")
    de.add_argument("--id", required=True)
    de.set_defaults(func=cmd_delete)

    return p


def main() -> None:
    args = build_parser().parse_args()
    with Client(args.addr, tenant_id=args.tenant) as client:
        try:
            args.func(client, args)
        except grpc.RpcError as e:
            print(f"nucladb: {e.code()}: {e.details()}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
