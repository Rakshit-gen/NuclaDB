package raft

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// DiskDeps builds a production Deps: a BoltDB-backed log/stable store (the
// same durability class as the engine's own WAL — fsync'd, crash-safe) and
// a real file-backed snapshot store, both rooted at dataDir. This is what
// a real cluster node uses; tests use the in-memory equivalents directly
// (see node_test.go and cluster_test.go) since spinning up disk state per
// test case would only add noise, not coverage.
func DiskDeps(dataDir string, transport raft.Transport) (Deps, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return Deps{}, fmt.Errorf("raft: create data dir: %w", err)
	}

	store, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return Deps{}, fmt.Errorf("raft: open bolt store: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return Deps{}, fmt.Errorf("raft: open snapshot store: %w", err)
	}

	return Deps{
		Logs:      store,
		Stable:    store,
		Snapshots: snapshots,
		Transport: transport,
	}, nil
}

// NewTCPTransport wraps raft.NewTCPTransport with the defaults this
// package uses everywhere: bindAddr is both the listen and advertise
// address (no NAT/reverse-proxy support needed for a cluster of peers
// that all dial each other directly), a small connection pool, and a
// bounded per-RPC timeout so a single unreachable peer can't hang the
// caller indefinitely.
func NewTCPTransport(bindAddr string) (*raft.NetworkTransport, error) {
	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("raft: resolve bind addr %q: %w", bindAddr, err)
	}
	return raft.NewTCPTransport(bindAddr, addr, 3, 5*time.Second, io.Discard)
}
