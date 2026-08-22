// Package health monitors cluster node liveness on top of
// internal/cluster and drives automatic shard failover and eviction. Only
// the current Raft leader actually probes and acts: probing from every
// node would be redundant work, and worse, every node could reach a
// different verdict about who's down with no consensus on what to do
// about it — so this follows the same "only the leader may propose" rule
// the rest of the control plane already lives by.
package health

import (
	"context"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/cluster"
)

// Prober checks whether the node reachable at addr is alive. GRPCProber is
// the production implementation, dialing the standard gRPC health
// protocol every nucladbd process already serves (see cmd/nucladbd);
// tests use a fake that can be told to fail on demand.
type Prober interface {
	Probe(ctx context.Context, addr string) error
}

// Checker periodically probes every known node other than itself and
// reacts to sustained failures in two stages: a fast per-shard failover
// (promote a healthy replica to shard leader — cheap, no Raft membership
// change) once a node has missed FailureThreshold consecutive probes,
// then full eviction (Cluster.Leave, which also rebalances every shard
// away from the dead node) once it's missed EvictThreshold. Both stages
// retry on the next tick if their propose calls fail (e.g. a transient
// leadership change), rather than giving up permanently.
type Checker struct {
	cluster *cluster.Cluster
	prober  Prober
	selfID  string

	interval         time.Duration
	probeTimeout     time.Duration
	failureThreshold int
	evictThreshold   int

	failures   map[string]int
	failedOver map[string]bool
}

// New creates a Checker. FailureThreshold must be <= EvictThreshold.
// interval/probeTimeout/failureThreshold/evictThreshold are all
// parameters (rather than hardcoded) so tests can run a full
// failure-detection-to-eviction cycle in milliseconds instead of waiting
// on production-realistic timings.
func New(c *cluster.Cluster, prober Prober, selfID string, interval, probeTimeout time.Duration, failureThreshold, evictThreshold int) *Checker {
	return &Checker{
		cluster:          c,
		prober:           prober,
		selfID:           selfID,
		interval:         interval,
		probeTimeout:     probeTimeout,
		failureThreshold: failureThreshold,
		evictThreshold:   evictThreshold,
		failures:         make(map[string]int),
		failedOver:       make(map[string]bool),
	}
}

// Run probes every other known node once per interval until ctx is
// cancelled. A tick on a node that doesn't currently believe it's the
// Raft leader is a no-op check-and-skip — see the package doc for why.
func (c *Checker) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Checker) tick(ctx context.Context) {
	if !c.cluster.IsLeader() {
		return
	}
	for id, addr := range c.cluster.Nodes() {
		if id == c.selfID {
			continue
		}

		probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout)
		err := c.prober.Probe(probeCtx, addr)
		cancel()

		if err == nil {
			delete(c.failures, id)
			delete(c.failedOver, id)
			continue
		}

		c.failures[id]++
		switch {
		case c.failures[id] >= c.evictThreshold:
			if err := c.cluster.Leave(id, c.probeTimeout); err == nil {
				delete(c.failures, id)
				delete(c.failedOver, id)
			}
		case c.failures[id] >= c.failureThreshold && !c.failedOver[id]:
			if err := c.cluster.FailoverNode(id); err == nil {
				c.failedOver[id] = true
			}
		}
	}
}
