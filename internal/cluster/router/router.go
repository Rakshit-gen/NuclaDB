// Package router implements scatter-gather query routing over a sharded
// NuclaDB cluster: Insert and Delete go to the single shard a vector id
// hashes to, Search fans out to every shard concurrently and merges each
// shard's own top-K into one globally-ranked top-K.
package router

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// ShardResolver resolves which node currently owns a shard. Satisfied by
// *internal/cluster.Cluster in production; Router only ever needs this
// narrow view of it (hashing ids to shards, then scattering/gathering
// across whichever shards exist doesn't care how ownership was decided),
// which keeps this package testable against a trivial fake instead of a
// full Raft cluster.
type ShardResolver interface {
	NumShards() int
	ShardOwner(shardID int) (nodeID, addr string, err error)
}

// ShardFor deterministically maps a vector id to one of numShards fixed
// shards via FNV-1a — fast, and good enough distribution for a
// non-adversarial id space. This is a different hash from
// internal/cluster/ring's SHA-256-based one deliberately: the ring hashes
// a changing set of *nodes* onto a hash circle so shard *ownership* moves
// minimally as nodes join/leave; here the shard *count* itself is fixed
// for the cluster's lifetime, so a plain, fast, uniform hash of the id is
// all that's needed to pick which of the fixed shards owns it.
func ShardFor(id uint64, numShards int) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)
	h := fnv.New64a()
	_, _ = h.Write(buf[:]) // hash.Hash.Write never errors
	return int(h.Sum64() % uint64(numShards))
}

// Router scatters/gathers requests across a sharded cluster, dialing each
// shard's current owner's gRPC API and reusing connections across calls.
type Router struct {
	resolver ShardResolver
	tenantID string

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn // API addr -> reused connection
}

// New creates a Router over resolver, scoping every request to tenantID
// (the reserved "default" tenant if empty).
func New(resolver ShardResolver, tenantID string) *Router {
	return &Router{resolver: resolver, tenantID: tenantID, conns: make(map[string]*grpc.ClientConn)}
}

// Close closes every pooled connection.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for addr, conn := range r.conns {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("router: close conn to %s: %w", addr, err)
		}
	}
	return firstErr
}

func (r *Router) clientFor(addr string) (pb.NuclaDBClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if conn, ok := r.conns[addr]; ok {
		return pb.NewNuclaDBClient(conn), nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("router: dial %s: %w", addr, err)
	}
	r.conns[addr] = conn
	return pb.NewNuclaDBClient(conn), nil
}

func (r *Router) ownerClient(shard int) (pb.NuclaDBClient, error) {
	_, addr, err := r.resolver.ShardOwner(shard)
	if err != nil {
		return nil, fmt.Errorf("router: resolve shard %d owner: %w", shard, err)
	}
	return r.clientFor(addr)
}

// Insert routes id/vector to the single shard id hashes to.
func (r *Router) Insert(ctx context.Context, id uint64, vector []float32, metadata map[string]string) error {
	client, err := r.ownerClient(ShardFor(id, r.resolver.NumShards()))
	if err != nil {
		return err
	}
	_, err = client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{
		Id:       strconv.FormatUint(id, 10),
		Values:   vector,
		Metadata: metadata,
		TenantId: r.tenantID,
	}})
	return err
}

// Item is one vector in an InsertBatch call.
type Item struct {
	ID       uint64
	Vector   []float32
	Metadata map[string]string
}

// InsertBatch splits items by shard and sends each shard a single
// BatchUpsert, all shards in parallel, so each shard logs its part with one
// fsync. If a shard fails, the other shards' parts may still have been
// written; upserts are idempotent, so the caller can retry the whole batch.
func (r *Router) InsertBatch(ctx context.Context, items []Item) error {
	numShards := r.resolver.NumShards()
	byShard := make(map[int][]*pb.Vector)
	for _, it := range items {
		shard := ShardFor(it.ID, numShards)
		byShard[shard] = append(byShard[shard], &pb.Vector{
			Id:       strconv.FormatUint(it.ID, 10),
			Values:   it.Vector,
			Metadata: it.Metadata,
			TenantId: r.tenantID,
		})
	}

	errs := make(chan error, len(byShard))
	for shard, vecs := range byShard {
		go func(shard int, vecs []*pb.Vector) {
			client, err := r.ownerClient(shard)
			if err == nil {
				_, err = client.BatchUpsert(ctx, &pb.BatchUpsertRequest{Vectors: vecs})
			}
			if err != nil {
				err = fmt.Errorf("shard %d: %w", shard, err)
			}
			errs <- err
		}(shard, vecs)
	}
	var firstErr error
	for range byShard {
		if err := <-errs; err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Delete routes id to the single shard it hashes to.
func (r *Router) Delete(ctx context.Context, id uint64) error {
	client, err := r.ownerClient(ShardFor(id, r.resolver.NumShards()))
	if err != nil {
		return err
	}
	_, err = client.Delete(ctx, &pb.DeleteRequest{Id: strconv.FormatUint(id, 10), TenantId: r.tenantID})
	return err
}

// Match is one scored result from a scatter-gather Search, merged across
// shards.
type Match struct {
	ID       uint64
	Score    float32
	Metadata map[string]string
}

// PartialError reports shards that still failed after a retry. Search
// returns it together with the merged results of the shards that did
// answer, so a caller that can live with partial results checks for it
// with errors.As and keeps the matches; one that can't just sees an error.
type PartialError struct {
	Failed map[int]error // shard -> last error
}

func (e *PartialError) Error() string {
	shards := make([]int, 0, len(e.Failed))
	for s := range e.Failed {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	msgs := make([]string, len(shards))
	for i, s := range shards {
		msgs[i] = e.Failed[s].Error()
	}
	return fmt.Sprintf("router: %d shard(s) failed: %s", len(shards), strings.Join(msgs, "; "))
}

func (e *PartialError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failed))
	for _, err := range e.Failed {
		errs = append(errs, err)
	}
	return errs
}

// searchAttempts is how many times one shard is tried. The owner is
// resolved again before each try, so a retry after a failover reaches the
// newly promoted replica.
const searchAttempts = 2

// Search fans query out to every shard's current owner concurrently,
// waits for all of them, and merges each shard's own top-K results into
// one globally-ranked top-K. This is correct, not an approximation,
// because Insert/Delete route by ShardFor, so shards partition the id
// space disjointly: no result can appear on two shards needing
// deduplication, only re-ranking across shards' already-sorted lists.
//
// A shard that fails is retried once. If some shards still fail, Search
// returns the other shards' merged results with a *PartialError; if every
// shard fails, it returns no results.
func (r *Router) Search(ctx context.Context, query []float32, topK, efSearch int, filters map[string]string) ([]Match, error) {
	numShards := r.resolver.NumShards()

	var pbFilters []*pb.MetadataFilter
	for k, v := range filters {
		pbFilters = append(pbFilters, &pb.MetadataFilter{Key: k, Value: v})
	}
	req := &pb.SearchRequest{
		Query:    query,
		TopK:     int32(topK),
		EfSearch: int32(efSearch),
		Filters:  pbFilters,
		TenantId: r.tenantID,
	}

	type shardResult struct {
		matches []*pb.ScoredVector
		err     error
	}
	results := make([]shardResult, numShards)

	var wg sync.WaitGroup
	for shard := 0; shard < numShards; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			var err error
			for attempt := 0; attempt < searchAttempts && ctx.Err() == nil; attempt++ {
				var client pb.NuclaDBClient
				if client, err = r.ownerClient(shard); err != nil {
					continue
				}
				var resp *pb.SearchResponse
				if resp, err = client.Search(ctx, req); err == nil {
					results[shard] = shardResult{matches: resp.GetMatches()}
					return
				}
			}
			if err == nil {
				err = ctx.Err()
			}
			results[shard] = shardResult{err: fmt.Errorf("shard %d: %w", shard, err)}
		}(shard)
	}
	wg.Wait()

	var merged []Match
	failed := map[int]error{}
	for shard, res := range results {
		if res.err != nil {
			failed[shard] = res.err
			continue
		}
		for _, m := range res.matches {
			id, err := strconv.ParseUint(m.GetId(), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("shard %d: invalid match id %q: %w", shard, m.GetId(), err)
			}
			merged = append(merged, Match{ID: id, Score: m.GetScore(), Metadata: m.GetMetadata()})
		}
	}

	sort.Slice(merged, func(i, j int) bool { return merged[i].Score < merged[j].Score })
	if len(merged) > topK {
		merged = merged[:topK]
	}
	if len(failed) == numShards {
		return nil, &PartialError{Failed: failed}
	}
	if len(failed) > 0 {
		return merged, &PartialError{Failed: failed}
	}
	return merged, nil
}
