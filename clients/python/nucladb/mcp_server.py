"""MCP server that gives an agent text search over a NuclaDB tenant.

NuclaDB stores vectors, and an agent can't write a 384-float embedding by
hand, so every tool here takes text and embeds it locally before it
reaches the server.

    pip install './clients/python[mcp]'
    nucladb-mcp
"""
from __future__ import annotations

import hashlib
import os
from typing import Callable

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from .client import Client

# texts in, one vector per text out
Embedder = Callable[[list[str]], list[list[float]]]

DEFAULT_MODEL = "all-MiniLM-L6-v2"  # 384-dim, small enough for CPU


def text_id(text: str) -> str:
    """Same text, same id, so adding it twice upserts instead of storing a
    duplicate. The server wants decimal uint64 ids; 60 bits of sha256 fit."""
    return str(int(hashlib.sha256(text.encode()).hexdigest()[:15], 16))


def build(db: Client, embed: Embedder) -> MCPServer:
    server = MCPServer(
        "nucladb",
        instructions="Long-term text memory backed by a NuclaDB vector index.",
    )

    @server.tool()
    def add_texts(texts: list[str], metadata: dict[str, str] | None = None) -> list[str]:
        """Store texts so search can find them later by meaning. metadata is
        attached to every text in this call. Returns one id per text."""
        if not texts:
            raise ToolError("texts is empty")
        if any(not t.strip() for t in texts):
            raise ToolError("every text has to have some non-whitespace content")
        ids = [text_id(t) for t in texts]
        vectors = embed(texts)
        db.batch_upsert(
            (i, v, {**(metadata or {}), "text": t})
            for i, v, t in zip(ids, vectors, texts)
        )
        return ids

    @server.tool()
    def search(
        query: str, top_k: int = 5, filters: dict[str, str] | None = None
    ) -> list[dict]:
        """Find stored texts closest in meaning to query, best match first.
        filters keeps only texts whose metadata has every key=value given."""
        matches = db.search(embed([query])[0], top_k=top_k, filters=filters)
        return [
            {
                "id": m.id,
                "score": m.score,
                "text": m.metadata.pop("text", ""),
                "metadata": m.metadata,
            }
            for m in matches
        ]

    @server.tool()
    def get(id: str) -> dict | None:
        """Fetch one stored text and its metadata by id, or null if absent."""
        found = db.get(id)
        if found is None:
            return None
        metadata = found[1]
        return {"id": id, "text": metadata.pop("text", ""), "metadata": metadata}

    @server.tool()
    def delete(id: str) -> bool:
        """Forget one stored text. Deleting an unknown id is not an error."""
        return db.delete(id)

    @server.tool()
    def count() -> int:
        """How many texts are stored."""
        return db.count()

    return server


def sentence_transformer(model_name: str) -> Embedder:
    # imported here so the plain client never needs torch installed
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    # unit length, so cosine, dot and l2 tenants all rank the same way
    return lambda texts: model.encode(texts, normalize_embeddings=True).tolist()


def main() -> None:
    embed = sentence_transformer(os.environ.get("NUCLADB_MCP_MODEL", DEFAULT_MODEL))
    with Client(
        os.environ.get("NUCLADB_ADDR", "localhost:9090"),
        tenant_id=os.environ.get("NUCLADB_TENANT", ""),
        api_key=os.environ.get("NUCLADB_API_KEY", ""),
        tls=os.environ.get("NUCLADB_TLS") == "1",
    ) as db:
        build(db, embed).run("stdio")


if __name__ == "__main__":
    main()
