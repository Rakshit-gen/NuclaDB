#!/usr/bin/env python3
"""Semantic search over this repo's own markdown docs, backed by NuclaDB.

Splits docs/*.md and README.md into paragraphs, embeds them with a small
local sentence-transformers model, and stores them in a running nucladbd.
Then lets you search those paragraphs by meaning, not keyword.

Setup:
    pip install ./clients/python sentence-transformers
    ./bin/nucladbd -data-dir=./data -dim=384 -metric=cosine &

Usage:
    python examples/semantic_search_docs.py index
    python examples/semantic_search_docs.py query "how does the WAL work"
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

from nucladb import Client

ADDR = os.environ.get("NUCLADB_ADDR", "localhost:9090")
MODEL_NAME = "all-MiniLM-L6-v2"  # 384-dim, small enough to run on CPU


def load_paragraphs(root: str) -> list[tuple[str, int, str]]:
    paths = sorted(glob.glob(os.path.join(root, "**/*.md"), recursive=True))
    chunks = []
    for path in paths:
        text = open(path, encoding="utf-8").read()
        for i, para in enumerate(text.split("\n\n")):
            para = para.strip()
            if len(para) >= 40:
                chunks.append((path, i, para))
    return chunks


def index(args: argparse.Namespace) -> None:
    from sentence_transformers import SentenceTransformer

    chunks = load_paragraphs(args.path)
    if not chunks:
        print(f"no markdown paragraphs found under {args.path}", file=sys.stderr)
        return

    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode([c[2] for c in chunks], show_progress_bar=True)

    with Client(ADDR) as db:
        upserted = db.batch_upsert(
            (
                str(n),
                vec.tolist(),
                {"file": path, "chunk": str(i), "text": text[:500]},
            )
            for n, ((path, i, text), vec) in enumerate(zip(chunks, embeddings), start=1)
        )
    print(f"indexed {upserted} paragraphs from {len(set(c[0] for c in chunks))} files")


def query(args: argparse.Namespace) -> None:
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(MODEL_NAME)
    vec = model.encode([args.text])[0].tolist()

    with Client(ADDR) as db:
        for m in db.search(vec, top_k=args.top_k):
            print(f"[{m.score:.4f}] {m.metadata['file']}")
            print(f"    {m.metadata['text'][:200]}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_index = sub.add_parser("index", help="embed and store docs")
    p_index.add_argument("--path", default=".", help="root to scan for *.md files")
    p_index.set_defaults(func=index)

    p_query = sub.add_parser("query", help="search stored docs")
    p_query.add_argument("text")
    p_query.add_argument("--top-k", type=int, default=5)
    p_query.set_defaults(func=query)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
