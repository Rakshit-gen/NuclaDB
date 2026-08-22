package bench

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
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

// Upsert routes every vector through the router individually — it has no
// batch-insert API yet (unlike the single-node BatchUpsert RPC), a real,
// unhidden cost of the cluster path worth reporting alongside the
// recall/QPS numbers. Inserts fan out over a bounded worker pool so the
// lack of batching doesn't make a 10k-vector build prohibitively slow to
// run, while every insert is still its own real RPC through the real
// router — no batching added on the sly to make the number look better.
func (b *NuclaDBClusterBackend) Upsert(vectors [][]float32) error {
	const concurrency = 32
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, v := range vectors {
		wg.Add(1)
		sem <- struct{}{}
		go func(id uint64, vec []float32) {
			defer wg.Done()
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := b.router.Insert(ctx, id, vec, nil); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
		}(uint64(i), v)
	}
	wg.Wait()
	return firstErr
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
