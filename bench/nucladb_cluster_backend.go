package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/cluster/router"
)

// staticResolver implements router.ShardResolver over a fixed shard->addr
// table. This benchmark exercises real query routing (fan-out/merge cost,
// multi-shard recall) over a real deployed topology of independent
// nucladbd processes — it deliberately doesn't exercise dynamic Raft
// membership changes, which internal/cluster's own tests already cover;
// a benchmark run isn't the place to also be testing cluster convergence.
type staticResolver struct {
	addrs []string
}

func (s *staticResolver) NumShards() int { return len(s.addrs) }

func (s *staticResolver) ShardOwner(shard int) (nodeID, addr string, err error) {
	if shard < 0 || shard >= len(s.addrs) {
		return "", "", fmt.Errorf("staticResolver: shard %d out of range", shard)
	}
	return fmt.Sprintf("shard-%d", shard), s.addrs[shard], nil
}

// NuclaDBClusterBackend drives numShards real nucladbd subprocesses (one
// per shard) through the real scatter-gather router
// (internal/cluster/router) — the same client-facing path a production
// multi-node deployment routes queries through — instead of a single
// process's in-memory HNSW graph. This measures the router's fan-out/merge
// overhead and aggregate recall/QPS/memory across shards, not just one
// node's index.
type NuclaDBClusterBackend struct {
	nodes  []*NuclaDBBackend
	router *router.Router
}

// StartNuclaDBCluster launches numShards nucladbd subprocesses on
// consecutive ports starting at basePort, each with its own data
// directory under dataDirBase, and wires a Router in front of them.
func StartNuclaDBCluster(binPath, dataDirBase string, dim int, metric string, numShards, basePort int) (*NuclaDBClusterBackend, error) {
	nodes := make([]*NuclaDBBackend, 0, numShards)
	addrs := make([]string, numShards)

	cleanup := func() {
		for _, n := range nodes {
			_ = n.Close()
		}
	}

	for i := 0; i < numShards; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", basePort+i)
		dataDir := filepath.Join(dataDirBase, fmt.Sprintf("shard-%d", i))
		node, err := StartNuclaDB(binPath, dataDir, dim, metric, addr)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("starting shard %d: %w", i, err)
		}
		nodes = append(nodes, node)
		addrs[i] = addr
	}

	return &NuclaDBClusterBackend{
		nodes:  nodes,
		router: router.New(&staticResolver{addrs: addrs}, ""),
	}, nil
}

func (b *NuclaDBClusterBackend) Name() string {
	return fmt.Sprintf("NuclaDB-cluster(%d shards)", len(b.nodes))
}

// Upsert sends vectors through the router in batches of 500. Each batch is
// split by shard and sent as one BatchUpsert per shard (Router.InsertBatch).
func (b *NuclaDBClusterBackend) Upsert(vectors [][]float32) error {
	const batchSize = 500
	for start := 0; start < len(vectors); start += batchSize {
		end := min(start+batchSize, len(vectors))
		items := make([]router.Item, 0, end-start)
		for i := start; i < end; i++ {
			items = append(items, router.Item{ID: uint64(i), Vector: vectors[i]})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := b.router.InsertBatch(ctx, items)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *NuclaDBClusterBackend) Search(query []float32, topK, ef int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	matches, err := b.router.Search(ctx, query, topK, ef, nil)
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(matches))
	for i, m := range matches {
		ids[i] = m.ID
	}
	return ids, nil
}

// RSSBytes sums every shard process's RSS — the real aggregate memory
// footprint of the whole cluster, not just one node's.
func (b *NuclaDBClusterBackend) RSSBytes() (uint64, error) {
	var total uint64
	for _, n := range b.nodes {
		rss, err := n.RSSBytes()
		if err != nil {
			return 0, err
		}
		total += rss
	}
	return total, nil
}

// Close terminates every shard subprocess.
func (b *NuclaDBClusterBackend) Close() error {
	_ = b.router.Close()
	var firstErr error
	for _, n := range b.nodes {
		if err := n.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
