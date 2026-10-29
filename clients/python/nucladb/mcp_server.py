"""MCP server that gives an agent text search over a NuclaDB tenant.

NuclaDB stores vectors, and an agent can't write a 384-float embedding by
hand, so every tool here takes text and embeds it locally before it
reaches the server.

    pip install './clients/python[mcp]'
    nucladb-mcp
"""
from __future__ import annotations

from typing import Callable

from mcp.server.mcpserver import MCPServer

from .client import Client

# texts in, one vector per text out
Embedder = Callable[[list[str]], list[list[float]]]


def build(db: Client, embed: Embedder) -> MCPServer:
    server = MCPServer(
        "nucladb",
        instructions="Long-term text memory backed by a NuclaDB vector index.",
    )
    return server
