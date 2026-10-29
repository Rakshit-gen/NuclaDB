# Memory for AI agents

Sources: [`clients/python/nucladb/mcp_server.py`](../../clients/python/nucladb/mcp_server.py), [`clients/python/tests/mcp_test.py`](../../clients/python/tests/mcp_test.py)

`nucladb-mcp` lets an AI agent such as Claude Code or Claude Desktop save
notes into NuclaDB and find them again later by meaning. It speaks MCP,
the protocol those apps use to call outside tools.

The catch it solves: NuclaDB stores vectors, and an agent can't write one
by hand. So the MCP server turns text into a vector itself, using a small
model that runs on the same machine (`all-MiniLM-L6-v2`, 384 numbers per
text, no API key, no per-call cost). The original text is kept next to its
vector, so a search hands back the text and not just a number.

It is a client of the server, like the CLI. Nothing in `nucladbd` changes.

## Try it

```sh
pip install './clients/python[mcp]'
./bin/nucladbd -data-dir=./data &
claude mcp add nucladb -e NUCLADB_TENANT=memory -- nucladb-mcp
```

Then ask Claude to remember something, and in a later session ask about
it. The full tool list and settings are in the
[Python client README](../../clients/python/README.md#mcp-server).

## What a search looks like

Four notes stored through the real model, then searched over MCP with the
SDK's own client, one result each:

| Asked | Top match | Score |
|---|---|---|
| where did I leave my car | I parked the car on level 3 of the airport garage. | 0.450 |
| how is durability handled | The WAL is fsynced before a write touches the HNSW graph. | 0.911 |
| rate limiting per customer | Tenants get quotas on vector count and queries per second. | 0.629 |

All three came back right, and none of them share a key word with the
note they found. The score is a distance (1 minus cosine similarity), so
lower is closer. 0.911 is a weak match that still won because the other
three notes were further off. With only four notes, top-1 will always
return something, so an agent should treat a high score as "probably not
stored".

## Startup time

| | Time to ready |
|---|---|
| First start (downloads the 90 MB model) | 54.5 s |
| Every start after | 6.9 s |

Both measured on the Apple M4 laptop from the [index](README.md), with
the model cache empty and then warm.

## Limits

- One tenant per running server. To keep work and personal notes apart,
  register two servers with different `NUCLADB_TENANT` values.
- Up to 256 texts per `add_texts` call and 50 results per search, to keep
  a single tool call from stalling or flooding the agent's context.
- Search filters are exact key=value matches. The richer `where` operators
  the client has (ranges, `in`, `exists`) aren't exposed yet.
- The model only reads the first 256 tokens of a text (roughly 200
  words) and ignores the rest, so split long documents before adding them.
