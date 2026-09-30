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


@dataclass
class ScoredVector:
    id: str
    score: float
    metadata: dict[str, str]


class Client:
    """A connection to one NuclaDB server. Use as a context manager:

        with Client("localhost:9090") as db:
            db.insert("v1", [0.1, 0.2, 0.3])
            db.search([0.1, 0.2, 0.3], top_k=5)
    """

    def __init__(self, address: str, tenant_id: str = ""):
        self.tenant_id = tenant_id
        self.channel = grpc.insecure_channel(address)
        self._stub = pb_grpc.NuclaDBStub(self.channel)

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        self.channel.close()

    def create_tenant(
        self, tenant_id: str, max_vectors: int = 0, max_qps: float = 0
    ) -> None:
        self._stub.CreateTenant(
            pb.CreateTenantRequest(
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

    def search(
        self,
        query: list[float],
        top_k: int = 10,
        metric: DistanceMetric = DistanceMetric.UNSPECIFIED,
        ef_search: int = 0,
        filters: dict[str, str] | None = None,
    ) -> list[ScoredVector]:
        resp = self._stub.Search(
            pb.SearchRequest(
                query=query,
                top_k=top_k,
                metric=int(metric),
                ef_search=ef_search,
                filters=[
                    pb.MetadataFilter(key=k, value=v)
                    for k, v in (filters or {}).items()
                ],
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
