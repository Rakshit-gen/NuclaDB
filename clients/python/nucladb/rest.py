"""The calls nucladb-mcp makes, over NuclaDB's REST/JSON API instead of
gRPC. Hosts like Render only expose one HTTP port, so a hosted server is
reachable over REST but not gRPC. Standard library only.

Methods match Client's, so code written against one works with the other.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import warnings
from typing import Iterable

from .client import DistanceMetric, ScoredVector, Tenant, _is_local

_METRIC_NAMES = {
    DistanceMetric.UNSPECIFIED: "",
    DistanceMetric.COSINE: "cosine",
    DistanceMetric.L2: "l2",
    DistanceMetric.DOT: "dot",
}


class RestError(Exception):
    """A request the server refused, with its HTTP status and reason."""

    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.message = message


class RestClient:
    """A NuclaDB server reached over REST, e.g. https://nucladb-demo.onrender.com.

    batch_upsert sends batch_size vectors per request, because a tenant
    with a rate limit counts every vector and refuses a batch bigger than
    its limit. When the server answers 429, a call waits and tries again,
    up to retries times.
    """

    def __init__(
        self,
        url: str,
        tenant_id: str = "",
        api_key: str = "",
        timeout: float | None = 60,
        batch_size: int = 10,
        retries: int = 30,
    ):
        self.url = url.rstrip("/")
        self.tenant_id = tenant_id
        self.timeout = timeout
        self.batch_size = max(1, batch_size)
        self.retries = retries
        self._headers = {"Content-Type": "application/json"}
        if api_key:
            self._headers["Authorization"] = "Bearer " + api_key
            host = urllib.parse.urlsplit(self.url).netloc
            if self.url.startswith("http://") and not _is_local(host):
                warnings.warn(
                    f"sending an API key to {self.url} without TLS; anyone on the path can read it. "
                    "Use an https:// URL.",
                    stacklevel=2,
                )

    def __enter__(self) -> "RestClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def close(self) -> None:
        pass

    def _call(self, method: str, path: str, body=None, query: dict | None = None) -> dict:
        url = self.url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        data = None if body is None else json.dumps(body).encode()
        for attempt in range(self.retries + 1):
            req = urllib.request.Request(url, data=data, method=method, headers=self._headers)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    text = resp.read()
                    return json.loads(text) if text else {}
            except urllib.error.HTTPError as e:
                text = e.read().decode(errors="replace")
                if e.code == 429 and attempt < self.retries:
                    time.sleep(1)
                    continue
                try:
                    message = json.loads(text).get("error", text)
                except ValueError:
                    message = text
                raise RestError(e.code, message) from None
            except urllib.error.URLError as e:
                raise RestError(503, f"can't reach {self.url}: {e.reason}") from None
        raise AssertionError("unreachable")

    def _tenant(self) -> dict:
        return {"tenant_id": self.tenant_id} if self.tenant_id else {}

    def create_tenant(
        self,
        tenant_id: str,
        max_vectors: int = 0,
        max_qps: float = 0,
        dim: int = 0,
        metric: DistanceMetric = DistanceMetric.UNSPECIFIED,
    ) -> None:
        body = {"tenant_id": tenant_id, "max_vectors": max_vectors, "max_qps": max_qps, "dim": dim}
        if _METRIC_NAMES[metric]:
            body["metric"] = _METRIC_NAMES[metric]
        self._call("POST", "/v1/tenants", body)

    def list_tenants(self) -> list[Tenant]:
        return [
            Tenant(
                id=t["tenant_id"],
                vector_count=t.get("vector_count", 0),
                max_vectors=t.get("quota", {}).get("max_vectors", 0),
                max_qps=t.get("quota", {}).get("max_qps", 0),
                dim=t.get("dim", 0),
                metric=DistanceMetric(t.get("metric", 0)),
            )
            for t in self._call("GET", "/v1/tenants").get("tenants", [])
        ]

    def batch_upsert(
        self, vectors: Iterable[tuple[str, list[float], dict[str, str] | None]]
    ) -> int:
        """vectors is an iterable of (id, vector, metadata) tuples."""
        items = [
            {"id": id, "values": list(v), "metadata": meta or {}, **self._tenant()}
            for id, v, meta in vectors
        ]
        upserted = 0
        for i in range(0, len(items), self.batch_size):
            resp = self._call("POST", "/v1/vectors:batch", {"vectors": items[i : i + self.batch_size]})
            upserted += resp.get("upserted", 0)
        return upserted

    def delete(self, id: str) -> bool:
        path = "/v1/vectors/" + urllib.parse.quote(id, safe="")
        return self._call("DELETE", path, query=self._tenant()).get("deleted", False)

    def get(self, id: str) -> tuple[list[float], dict[str, str]] | None:
        """Returns (vector, metadata), or None if id isn't stored."""
        path = "/v1/vectors/" + urllib.parse.quote(id, safe="")
        try:
            v = self._call("GET", path, query=self._tenant()).get("vector", {})
        except RestError as e:
            if e.status == 404:
                return None
            raise
        return v.get("values", []), v.get("metadata", {})

    def count(self) -> int:
        return self._call("GET", "/v1/vectors:count", query=self._tenant()).get("count", 0)

    def search(
        self,
        query: list[float],
        top_k: int = 10,
        ef_search: int = 0,
        filters: dict[str, str] | None = None,
    ) -> list[ScoredVector]:
        body = {"query": list(query), "top_k": top_k, **self._tenant()}
        if ef_search:
            body["ef_search"] = ef_search
        if filters:
            body["filters"] = filters
        return [
            ScoredVector(id=m["id"], score=m.get("score", 0.0), metadata=m.get("metadata", {}))
            for m in self._call("POST", "/v1/search", body).get("matches", [])
        ]
