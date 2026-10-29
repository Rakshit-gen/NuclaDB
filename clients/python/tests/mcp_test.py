#!/usr/bin/env python3
"""Assert-based self-check for nucladb-mcp: drives the MCP tools against a
real nucladbd subprocess, like smoke_test.py does for the client. The
embedder is a stand-in that counts four words, so the test needs neither
torch nor a model download and the expected ranking is obvious."""

import asyncio
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from nucladb import Client  # noqa: E402
from nucladb.mcp_server import build, check_tenant  # noqa: E402
from smoke_test import REPO_ROOT, free_port, wait_ready  # noqa: E402

WORDS = ["cat", "dog", "car", "boat"]


def embed(texts: list[str]) -> list[list[float]]:
    # the 0.01 keeps a text with none of the words off the zero vector,
    # which has no cosine direction
    return [[t.lower().count(w) + 0.01 for w in WORDS] for t in texts]


def call(server, tool: str, **args):
    return asyncio.run(server.call_tool(tool, args)).structured_content["result"]


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        bin_path = os.path.join(tmp, "nucladbd")
        subprocess.run(
            ["go", "build", "-o", bin_path, "./cmd/nucladbd"],
            cwd=REPO_ROOT, check=True,
        )

        grpc_port = free_port()
        proc = subprocess.Popen([
            bin_path,
            "-data-dir", os.path.join(tmp, "data"),
            "-grpc-addr", f"127.0.0.1:{grpc_port}",
            "-http-addr", f"127.0.0.1:{free_port()}",
            "-dim", "4",
        ])
        try:
            addr = f"127.0.0.1:{grpc_port}"
            with Client(addr) as default:
                wait_ready(default)
            with Client(addr, tenant_id="memory") as db:
                # "memory" doesn't exist yet, so this also covers creating it
                check_tenant(db, len(WORDS))
                server = build(db, embed)

                ids = call(server, "add_texts", texts=[
                    "the cat sat on the mat",
                    "a dog chased the ball",
                    "my car needs new tyres",
                ])
                assert len(ids) == 3 and len(set(ids)) == 3, ids

                top = call(server, "search", query="where is the cat", top_k=1)
                assert [m["text"] for m in top] == ["the cat sat on the mat"], top

            print("OK: add_texts and search verified against a real nucladbd subprocess")
        finally:
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    main()
