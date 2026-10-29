"""MCP server that gives an agent text search over a NuclaDB tenant.

NuclaDB stores vectors, and an agent can't write a 384-float embedding by
hand, so every tool here takes text and embeds it locally before it
reaches the server.

    pip install './clients/python[mcp]'
    nucladb-mcp
"""
from __future__ import annotations

import contextlib
import hashlib
import os
from typing import Callable, Iterator

import grpc
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp_types import ToolAnnotations

from .client import Client, DistanceMetric

# texts in, one vector per text out
Embedder = Callable[[list[str]], list[list[float]]]

DEFAULT_MODEL = "all-MiniLM-L6-v2"  # 384-dim, small enough for CPU
MAX_TOP_K = 50  # more than this floods the agent's context with text
MAX_BATCH = 256  # one embed call; bigger batches stall the agent with no feedback

READ_ONLY = ToolAnnotations(read_only_hint=True)
DESTRUCTIVE = ToolAnnotations(destructive_hint=True, idempotent_hint=True)
# upserts keyed on a hash of the text, so resending the same call is harmless
ADDITIVE = ToolAnnotations(destructive_hint=False, idempotent_hint=True)


def text_id(text: str) -> str:
    """Same text, same id, so adding it twice upserts instead of storing a
    duplicate. The server wants decimal uint64 ids; 60 bits of sha256 fit."""
    return str(int(hashlib.sha256(text.encode()).hexdigest()[:15], 16))


@contextlib.contextmanager
def _rpc() -> Iterator[None]:
    # Anything but ToolError reaches the agent as a bare "Error executing
    # tool", so pass the server's own reason through instead.
    try:
        yield
    except grpc.RpcError as e:
        msg = f"nucladb: {e.code().name}: {e.details()}"
        if "dimension mismatch" in (e.details() or ""):
            msg += " (the tenant's dim has to equal the embedding model's, 384 for all-MiniLM-L6-v2)"
        raise ToolError(msg) from e


def build(db: Client, embed: Embedder) -> MCPServer:
    server = MCPServer(
        "nucladb",
        instructions="Long-term text memory backed by a NuclaDB vector index.",
    )

    @server.tool(annotations=ADDITIVE)
    def add_texts(texts: list[str], metadata: dict[str, str] | None = None) -> list[str]:
        """Store texts so search can find them later by meaning. metadata is
        attached to every text in this call. Returns one id per text."""
        if not texts:
            raise ToolError("texts is empty")
        if len(texts) > MAX_BATCH:
            raise ToolError(f"at most {MAX_BATCH} texts per call, got {len(texts)}")
        if any(not t.strip() for t in texts):
            raise ToolError("every text has to have some non-whitespace content")
        if metadata and "text" in metadata:
            raise ToolError("metadata key \"text\" is reserved for the text itself")
        ids = [text_id(t) for t in texts]
        vectors = embed(texts)
        with _rpc():
            db.batch_upsert(
                (i, v, {**(metadata or {}), "text": t})
                for i, v, t in zip(ids, vectors, texts)
            )
        return ids

    @server.tool(annotations=READ_ONLY)
    def search(
        query: str, top_k: int = 5, filters: dict[str, str] | None = None
    ) -> list[dict]:
        """Find stored texts closest in meaning to query, best match first.
        filters keeps only texts whose metadata has every key=value given.
        score is a distance: lower is closer, 0 is the same meaning."""
        if not query.strip():
            raise ToolError("query is empty")
        if not 1 <= top_k <= MAX_TOP_K:
            raise ToolError(f"top_k must be between 1 and {MAX_TOP_K}")
        vector = embed([query])[0]
        with _rpc():
            matches = db.search(vector, top_k=top_k, filters=filters)
        return [
            {
                "id": m.id,
                "score": m.score,
                "text": m.metadata.pop("text", ""),
                "metadata": m.metadata,
            }
            for m in matches
        ]

    @server.tool(annotations=READ_ONLY)
    def get(id: str) -> dict | None:
        """Fetch one stored text and its metadata by id, or null if absent."""
        with _rpc():
            found = db.get(id)
        if found is None:
            return None
        metadata = found[1]
        return {"id": id, "text": metadata.pop("text", ""), "metadata": metadata}

    @server.tool(annotations=DESTRUCTIVE)
    def delete(id: str) -> bool:
        """Forget one stored text. Deleting an unknown id is not an error."""
        with _rpc():
            return db.delete(id)

    @server.tool(annotations=READ_ONLY)
    def count() -> int:
        """How many texts are stored."""
        with _rpc():
            return db.count()

    return server


def sentence_transformer(model_name: str) -> Embedder:
    # imported here so the plain client never needs torch installed
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    # unit length, so cosine, dot and l2 tenants all rank the same way
    return lambda texts: model.encode(texts, normalize_embeddings=True).tolist()


def check_tenant(db: Client, dim: int) -> None:
    """Fail at startup, not on the agent's first add, when the tenant was
    made for vectors of a different size than the model produces. A tenant
    that doesn't exist yet is created to fit the model."""
    try:
        tenants = {t.id: t for t in db.list_tenants()}
    except grpc.RpcError as e:
        # listing tenants is an admin call; a tenant-scoped key just skips the check
        if e.code() in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAUTHENTICATED):
            return
        raise
    name = db.tenant_id or "default"
    tenant = tenants.get(name)
    if tenant is None:
        db.create_tenant(name, dim=dim, metric=DistanceMetric.COSINE)
        return
    if tenant.dim != dim:
        raise SystemExit(
            f"nucladb-mcp: tenant {name!r} holds {tenant.dim}-dim vectors but the "
            f"embedding model makes {dim}-dim ones; use a tenant created with dim={dim}"
        )


def main() -> None:
    embed = sentence_transformer(os.environ.get("NUCLADB_MCP_MODEL", DEFAULT_MODEL))
    with Client(
        os.environ.get("NUCLADB_ADDR", "localhost:9090"),
        tenant_id=os.environ.get("NUCLADB_TENANT", ""),
        api_key=os.environ.get("NUCLADB_API_KEY", ""),
        tls=os.environ.get("NUCLADB_TLS") == "1",
    ) as db:
        check_tenant(db, len(embed(["dimension probe"])[0]))
        build(db, embed).run("stdio")


if __name__ == "__main__":
    main()
