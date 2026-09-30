"""A thin, pythonic wrapper around the generated NuclaDB gRPC stubs.

Every data method maps 1:1 onto one RPC in proto/nucladb.proto — this
package adds ergonomics (a context manager, plain dicts/lists instead of
protobuf messages, keyword defaults matching the server's own), not new
behavior. See the Go CLI (cmd/nucladb-cli, docs/cli.md) for the same API
surface from the reference implementation's own side.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Iterable

import grpc

from . import nucladb_pb2 as pb
from . import nucladb_pb2_grpc as pb_grpc


class DistanceMetric(IntEnum):
    UNSPECIFIED = pb.DISTANCE_METRIC_UNSPECIFIED
    COSINE = pb.DISTANCE_METRIC_COSINE
    L2 = pb.DISTANCE_METRIC_L2
    DOT = pb.DISTANCE_METRIC_DOT


_FILTER_OPS = {
    "eq": pb.FILTER_OP_EQ,
    "ne": pb.FILTER_OP_NE,
    "in": pb.FILTER_OP_IN,
    "not_in": pb.FILTER_OP_NOT_IN,
    "gt": pb.FILTER_OP_GT,
    "gte": pb.FILTER_OP_GTE,
    "lt": pb.FILTER_OP_LT,
    "lte": pb.FILTER_OP_LTE,
    "exists": pb.FILTER_OP_EXISTS,
}


@dataclass
class Tenant:
    id: str
    vector_count: int
    max_vectors: int
    max_qps: float
    dim: int
    metric: DistanceMetric


@dataclass
class ScoredVector:
    id: str
    score: float
    metadata: dict[str, str]


class _APIKey(grpc.UnaryUnaryClientInterceptor):
    """Adds the API key to every call."""

    def __init__(self, key: str):
        self._metadata = (("authorization", "Bearer " + key),)

    def intercept_unary_unary(self, continuation, details, request):
        details = details._replace(metadata=tuple(details.metadata or ()) + self._metadata)
        return continuation(details, request)


class Client:
    """A connection to one NuclaDB server. Use as a context manager:

        with Client("localhost:9090") as db:
            db.insert("v1", [0.1, 0.2, 0.3])
            db.search([0.1, 0.2, 0.3], top_k=5)
    """

    def __init__(
        self, address: str, tenant_id: str = "", api_key: str = "", tls: bool = False
    ):
        self.tenant_id = tenant_id
        if tls:
            self.channel = grpc.secure_channel(address, grpc.ssl_channel_credentials())
        else:
            self.channel = grpc.insecure_channel(address)
        channel = self.channel
        if api_key:
            channel = grpc.intercept_channel(channel, _APIKey(api_key))
        self._stub = pb_grpc.NuclaDBStub(channel)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self.channel.close()

    def create_tenant(
        self,
        tenant_id: str,
        max_vectors: int = 0,
        max_qps: float = 0,
        dim: int = 0,
        metric: DistanceMetric = DistanceMetric.UNSPECIFIED,
    ) -> None:
        """dim and metric default to the server's -dim and -metric."""
        self._stub.CreateTenant(
            pb.CreateTenantRequest(
                tenant_id=tenant_id,
                quota=pb.TenantQuota(max_vectors=max_vectors, max_qps=max_qps),
                dim=dim,
                metric=int(metric),
            )
        )

    def list_tenants(self) -> list[Tenant]:
        resp = self._stub.ListTenants(pb.ListTenantsRequest())
        return [
            Tenant(
                id=t.tenant_id,
                vector_count=t.vector_count,
                max_vectors=t.quota.max_vectors,
                max_qps=t.quota.max_qps,
                dim=t.dim,
                metric=DistanceMetric(t.metric),
            )
            for t in resp.tenants
        ]

    def delete_tenant(self, tenant_id: str) -> None:
        self._stub.DeleteTenant(pb.DeleteTenantRequest(tenant_id=tenant_id))

    def set_quota(self, tenant_id: str, max_vectors: int = 0, max_qps: float = 0) -> None:
        self._stub.SetQuota(
            pb.SetQuotaRequest(
                tenant_id=tenant_id,
                quota=pb.TenantQuota(max_vectors=max_vectors, max_qps=max_qps),
            )
        )

    def insert(
        self, id: str, vector: list[float], metadata: dict[str, str] | None = None
    ) -> str:
        resp = self._stub.Insert(
            pb.InsertRequest(vector=self._vector(id, vector, metadata))
        )
        return resp.id

    def batch_upsert(
        self, vectors: Iterable[tuple[str, list[float], dict[str, str] | None]]
    ) -> int:
        """vectors is an iterable of (id, vector, metadata) tuples."""
        resp = self._stub.BatchUpsert(
            pb.BatchUpsertRequest(
                vectors=[self._vector(id, v, meta) for id, v, meta in vectors]
            )
        )
        return resp.upserted

    def delete(self, id: str) -> bool:
        # deletes are idempotent by design (see internal/engine/engine.go's
        # Delete doc comment) — deleting an unknown or already-deleted id
        # is not an error, and the server always reports Deleted: true.
        resp = self._stub.Delete(
            pb.DeleteRequest(id=id, tenant_id=self.tenant_id)
        )
        return resp.deleted

    def get(self, id: str) -> tuple[list[float], dict[str, str]] | None:
        """Returns (vector, metadata), or None if id isn't stored."""
        try:
            resp = self._stub.Get(pb.GetRequest(id=id, tenant_id=self.tenant_id))
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                return None
            raise
        return list(resp.vector.values), dict(resp.vector.metadata)

    def update_metadata(self, id: str, metadata: dict[str, str]) -> None:
        """Replaces id's metadata without touching its vector."""
        self._stub.UpdateMetadata(
            pb.UpdateMetadataRequest(id=id, metadata=metadata, tenant_id=self.tenant_id)
        )

    def list(self, page_size: int = 0) -> Iterable[str]:
        """Yields every stored id, fetching a page at a time."""
        token = ""
        while True:
            resp = self._stub.List(
                pb.ListRequest(tenant_id=self.tenant_id, page_token=token, page_size=page_size)
            )
            yield from resp.ids
            token = resp.next_page_token
            if not token:
                return

    def count(self) -> int:
        return self._stub.Count(pb.CountRequest(tenant_id=self.tenant_id)).count

    def search(
        self,
        query: list[float],
        top_k: int = 10,
        metric: DistanceMetric = DistanceMetric.UNSPECIFIED,
        ef_search: int = 0,
        filters: dict[str, str] | None = None,
        where: Iterable[tuple] = (),
    ) -> list[ScoredVector]:
        """filters are key=value matches. where takes (key, op) or
        (key, op, value) tuples, op one of eq, ne, in, not_in, gt, gte, lt,
        lte, exists; value is a list for in and not_in. Every filter has
        to match."""
        clauses = [pb.MetadataFilter(key=k, value=v) for k, v in (filters or {}).items()]
        for key, op, *rest in where:
            if op not in _FILTER_OPS:
                raise ValueError(f"unknown filter op {op!r}")
            value = rest[0] if rest else None
            clause = pb.MetadataFilter(key=key, op=_FILTER_OPS[op])
            if isinstance(value, (list, tuple)):
                clause.values.extend(str(v) for v in value)
            elif value is not None:
                clause.value = str(value)
            clauses.append(clause)
        resp = self._stub.Search(
            pb.SearchRequest(
                query=query,
                top_k=top_k,
                metric=int(metric),
                ef_search=ef_search,
                filters=clauses,
                tenant_id=self.tenant_id,
            )
        )
        return [
            ScoredVector(id=m.id, score=m.score, metadata=dict(m.metadata))
            for m in resp.matches
        ]

    def _vector(
        self, id: str, values: list[float], metadata: dict[str, str] | None
    ) -> pb.Vector:
        return pb.Vector(
            id=id,
            values=values,
            metadata=metadata or {},
            tenant_id=self.tenant_id,
        )
