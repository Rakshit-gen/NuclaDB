#!/usr/bin/env python3
"""Assert-based self-check: builds the real nucladbd binary and drives it
through insert/batch_upsert/search/filter/delete via this package's own
Client — the same pattern test/chaos/kill_test.go uses on the Go side
(a real subprocess, not a mock server)."""

import contextlib
import os
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from nucladb import Client, DistanceMetric  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_ready(client: Client, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            client.search([0.0] * 4, top_k=1)
            return
        except Exception as e:  # noqa: BLE001 - server not up yet, keep retrying
            last_err = e
            time.sleep(0.05)
    raise RuntimeError(f"server never became ready: {last_err}")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        bin_path = os.path.join(tmp, "nucladbd")
        subprocess.run(
            ["go", "build", "-o", bin_path, "./cmd/nucladbd"],
            cwd=REPO_ROOT, check=True,
        )

        grpc_port = free_port()
        http_port = free_port()
        proc = subprocess.Popen([
            bin_path,
            "-data-dir", os.path.join(tmp, "data"),
            "-grpc-addr", f":{grpc_port}",
            "-http-addr", f":{http_port}",
            "-dim", "4",
            "-metric", "l2",
        ])
        try:
            with Client(f"127.0.0.1:{grpc_port}") as db:
                wait_ready(db)

                # vector ids are decimal uint64 strings (internal/api/grpc
                # parses them that way), not arbitrary text.
                assert db.insert("1", [1, 0, 0, 0]) == "1"
                assert db.insert("2", [0, 1, 0, 0], metadata={"color": "red"}) == "2"
                assert db.batch_upsert([
                    ("3", [0, 0, 1, 0], None),
                    ("4", [0, 0, 0, 1], {"color": "blue"}),
                ]) == 2

                top = db.search([1, 0, 0, 0], top_k=1, metric=DistanceMetric.L2)
                assert len(top) == 1 and top[0].id == "1", top

                filtered = db.search([0, 0, 0, 0], top_k=10, filters={"color": "red"})
                assert [r.id for r in filtered] == ["2"], filtered

                # deletes are idempotent by design (internal/engine's own
                # doc comment) — a second delete of the same id still
                # reports success, not failure.
                assert db.delete("1") is True
                assert db.delete("1") is True

                after_delete = db.search([1, 0, 0, 0], top_k=10)
                assert "1" not in [r.id for r in after_delete], after_delete

            print("OK: insert, batch_upsert, search, metadata filter, and "
                  "delete all verified against a real nucladbd subprocess")
        finally:
            proc.terminate()
            proc.wait(timeout=5)


if __name__ == "__main__":
    main()
