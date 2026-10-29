"""MCP server that gives an agent text search over a NuclaDB tenant.

NuclaDB stores vectors, and an agent can't write a 384-float embedding by
hand, so every tool here takes text and embeds it locally before it
reaches the server.

    pip install './clients/python[mcp]'
    nucladb-mcp
"""
from __future__ import annotations

import hashlib
from typing import Callable

from mcp.server.mcpserver import MCPServer

from .client import Client

# texts in, one vector per text out
Embedder = Callable[[list[str]], list[list[float]]]


def text_id(text: str) -> str:
    """Same text, same id, so adding it twice upserts instead of storing a
    duplicate. The server wants decimal uint64 ids; 60 bits of sha256 fit."""
    return str(int(hashlib.sha256(text.encode()).hexdigest()[:15], 16))


def build(db: Client, embed: Embedder) -> MCPServer:
    server = MCPServer(
        "nucladb",
        instructions="Long-term text memory backed by a NuclaDB vector index.",
    )
    return server
