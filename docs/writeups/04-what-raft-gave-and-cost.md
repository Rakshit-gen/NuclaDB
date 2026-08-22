# What Raft gave the system and what it cost

`internal/cluster/raft` runs hashicorp/raft over exactly one kind of
data: cluster *metadata* — which nodes exist, which node currently leads
each shard (`internal/cluster/raft/fsm.go`). It does not touch a single
vector write. Every insert, delete, and search stays on the fast path
documented in `internal/engine` and streamed leader-to-replica by
`internal/cluster/replication` — a plain TCP WAL tail, no consensus round
trip per write. Running every vector write through Raft would be
correct and dramatically slower; the actual tradeoff worth writing up is
what that split buys and what it gives up.

## What it gave

**Topology stays agreed-upon through faults, for free.** Because shard
leadership is Raft-committed state rather than something each node
computes independently, `Cluster.ShardOwner` and the router's dial
target can never diverge between nodes the way two independently-run
"who owns shard 7" calculations could after a partition. `TestFullPartitionLosesQuorumThenHeals`
(`test/chaos/network`) is the direct proof: black-hole a 2-node
cluster's traffic with toxiproxy, and writes correctly stop rather than
each side quietly disagreeing about who's in charge — then heal the
partition and a leader re-emerges with no manual reconciliation step.

**Failover is a Raft propose, not a race.** `internal/cluster/health.Checker`
promotes a replica after `failureThreshold` missed probes and evicts the
node entirely (rebalancing its shards away) after `evictThreshold` — both
paths are just `Cluster.FailoverNode` / `Cluster.Leave` proposals, so
every node's view of "who owns this shard now" updates atomically and
identically, without a separate gossip or leader-election protocol
layered on top for the metadata itself.

## What it cost

**The write path Raft doesn't cover is exactly where linearizability can
break.** `test/jepsen`'s two tests make this concrete rather than
theoretical:

- `TestLinearizableUnderNormalOperation` — steady-state, single leader,
  no faults — passes the real `porcupine` checker every run. Raft's
  metadata layer isn't in this path at all once a leader is settled; this
  is really validating the engine's own concurrency model.
- `TestFailoverLinearizability` kills the shard leader mid-workload and
  lets `Checker` fail over to the replica, then runs the same porcupine
  check. Because `internal/cluster/replication` streams the WAL
  asynchronously — not as a Raft-committed quorum write — a write the old
  leader had already acknowledged to its client can still be sitting
  unreplicated when the leader dies. The test doesn't assert either
  outcome; it reports whichever the checker finds, because both are
  real depending on timing. That's the actual cost of choosing
  async replication for the fast path: better write latency, at the
  price of a failover window where an acknowledged write can be lost.

**Sharded routing has a real, measured throughput cost.**
`bench/results-cluster.md` (4 shards vs. single-node, same SIFT-small
dataset, same recall target) shows QPS dropping between **22% (ef=200)
and 42% (ef=10)** depending on `ef` — every query now makes a router hop
plus a fan-out to all 4 shards instead of one in-process search, and each
shard's own candidate list is thinner than the single-node graph's. In
exchange, recall@10 actually tracks the single-node numbers closely (at
ef=10 the cluster's 0.963 even beats single-node's 0.903, since sharding
by id doesn't remove any vectors, it just changes which process holds
them). Raft's own metadata layer isn't the source of this cost — the
scatter-gather query pattern is — but it's the tradeoff that only exists
*because* topology is dynamically Raft-managed rather than a fixed,
hand-configured shard map.

## The honest takeaway

Raft bought correctness for the one thing that's genuinely hard to get
right without it — a cluster-wide, partition-tolerant, always-agreed-upon
answer to "who owns this shard right now" — and nothing more. It doesn't
make writes linearizable across a failover (that's a property of the
replication mechanism, which is deliberately not Raft, for latency), and
it doesn't make the query path faster (that's a property of sharding
itself, not of how shard ownership is tracked). Reporting the async
failover window as a real, sometimes-triggered outcome rather than a
theoretical footnote — and the 22-42% QPS cost as a measured range
instead of a single rounded number — is the point: both are the actual
price of this design, not a hedge against a design that might have a
price.
