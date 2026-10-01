# Multiple machines (not in the server yet)

Source: [`internal/cluster`](../../internal/cluster)

These are finished and tested Go packages, but `nucladbd` has no cluster
mode yet. They run today in the benchmark (`bench/cmd/compare-cluster`) and
in the tests under `test/jepsen` and `test/chaos/network`.

## The idea

One collection is split into shards, and each shard lives on a different
server. Every vector belongs to exactly one shard, chosen from its id with
consistent hashing, so adding a server only moves a small part of the data.

- **Writes** go to the server that owns the shard. It streams its log to a
  replica server, which keeps a copy.
- **Searches** go to every shard at once. Each returns its own top 10, and
  a router merges them into one final top 10.
- **Who owns which shard** is agreed with Raft, so two servers can never
  disagree about it, even during a network split. If a shard's server stops
  answering, its replica is promoted in its place.

## Measured

4 shards against 1 server, same 10,000 vectors, same machine:

| ef_search | 1 server, searches/s | 4 shards, searches/s | 1 server, recall | 4 shards, recall |
|---|---|---|---|---|
| 10 | 7,741 | 4,440 | 91.0% | 97.8% |
| 50 | 5,648 | 3,131 | 99.6% | 99.4% |
| 200 | 2,744 | 1,651 | 99.9% | 99.5% |

Splitting costs 38% to 45% of the searches per second here, because every
search makes extra network hops to all 4 shards. At this size one server is
faster. Sharding pays off when the data no longer fits on one machine.

This comparison was run on an older build than the one in
[Fast similarity search](search.md), so compare the two columns with each
other, not with the numbers on that page.

## The known gap

Replication to the replica is asynchronous: the leader answers OK before
the replica has the write. If the leader dies in that moment, the replica
takes over without that write. The Jepsen-style test
`TestFailoverLinearizability` checks for exactly this and reports whether
it happened, because depending on timing, it can.

## What is missing before the server can use it

- a cluster mode in `nucladbd` that starts replication for each shard and
  points it at the new owner after a failover
- copying data when shards move between servers (today a rebalance changes
  ownership without moving the data)

More detail: [What Raft gave the system, and what it cost](../writeups/04-what-raft-gave-and-cost.md).
