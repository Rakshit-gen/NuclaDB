// Package engine wires the HNSW graph, write-ahead log, and on-disk
// snapshot together into a single durable, queryable database: every
// mutation is WAL-logged before it touches the graph, and a snapshot lets
// restart skip replaying the log from empty.
package engine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	"github.com/Rakshit-gen/nucladb/internal/storage/segment"
	"github.com/Rakshit-gen/nucladb/internal/storage/wal"
)

var ErrNotFound = errors.New("engine: id not found")

const (
	snapshotFile = "snapshot.bin"
	metadataFile = "metadata.json"
	walFile      = "wal.log"
)

// Engine is a single-node, durable vector database: HNSW search over an
// in-memory graph, with every write made crash-safe via the WAL before
// it's applied, and periodic snapshots to keep restart fast.
//
// mu serializes all mutating operations (Insert, Delete, Snapshot) so the
// WAL, the graph, and the metadata map can never diverge from each other —
// the graph has its own finer-grained internal lock for search
// concurrency, but write ordering across all three needs a single writer.
type Engine struct {
	mu  sync.Mutex
	dir string
	cfg hnsw.Config

	graph *hnsw.Graph
	w     *wal.Writer
	seq   uint64

	metaMu   sync.RWMutex
	metadata map[uint64]map[string]string
}

// Open loads dir's snapshot (if any), replays any WAL records written
// since that snapshot, and returns a ready-to-use Engine. A brand-new,
// empty dir produces an empty database.
func Open(dir string, cfg hnsw.Config) (*Engine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	snapshotPath := filepath.Join(dir, snapshotFile)
	walPath := filepath.Join(dir, walFile)

	g, snapshotSeq, err := segment.Load(snapshotPath, cfg)
	if err != nil {
		return nil, err
	}
	metadata, err := loadMetadata(filepath.Join(dir, metadataFile))
	if err != nil {
		return nil, err
	}

	lastSeq := snapshotSeq
	_, err = wal.Replay(walPath, func(rec wal.Record) error {
		if rec.Seq <= snapshotSeq {
			// Already reflected in the snapshot we just loaded.
			return nil
		}
		switch rec.Op {
		case wal.OpInsert:
			if err := g.Insert(rec.ID, rec.Vector); err != nil {
				return err
			}
			if len(rec.Extra) > 0 {
				var m map[string]string
				if err := json.Unmarshal(rec.Extra, &m); err != nil {
					return err
				}
				metadata[rec.ID] = m
			} else {
				delete(metadata, rec.ID)
			}
		case wal.OpDelete:
			_ = g.Delete(rec.ID) // idempotent: fine if already absent
			delete(metadata, rec.ID)
		}
		lastSeq = rec.Seq
		return nil
	})
	if err != nil {
		return nil, err
	}

	w, err := wal.OpenWriter(walPath, lastSeq)
	if err != nil {
		return nil, err
	}

	return &Engine{
		dir:      dir,
		cfg:      cfg,
		graph:    g,
		w:        w,
		seq:      lastSeq,
		metadata: metadata,
	}, nil
}

// Insert durably upserts id -> (vector, metadata). metadata may be nil.
func (e *Engine) Insert(id uint64, vector []float32, metadata map[string]string) error {
	var extra []byte
	if len(metadata) > 0 {
		b, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		extra = b
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	seq, err := e.w.AppendWithExtra(wal.OpInsert, id, vector, extra)
	if err != nil {
		return err
	}
	if err := e.graph.Insert(id, vector); err != nil {
		return err
	}

	e.metaMu.Lock()
	if len(metadata) > 0 {
		e.metadata[id] = metadata
	} else {
		delete(e.metadata, id)
	}
	e.metaMu.Unlock()

	e.seq = seq
	return nil
}

// Delete durably removes id. It is not an error to delete an id that was
// already deleted or never existed — deletes are idempotent by design so
// WAL replay can safely re-apply them.
func (e *Engine) Delete(id uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	seq, err := e.w.AppendWithExtra(wal.OpDelete, id, nil, nil)
	if err != nil {
		return err
	}
	_ = e.graph.Delete(id)

	e.metaMu.Lock()
	delete(e.metadata, id)
	e.metaMu.Unlock()

	e.seq = seq
	return nil
}

// Result is one scored match from Search, including its metadata payload.
type Result struct {
	ID       uint64
	Distance float32
	Metadata map[string]string
}

// Search returns up to topK nearest neighbors of query, optionally
// restricted to vectors whose metadata matches every key/value in filters
// (a plain equality AND across all filter keys).
//
// Filtering is applied as a post-filter over an overfetched candidate set
// rather than steering graph traversal itself: it's simpler to reason
// about correctly and works well as long as the filter isn't extremely
// selective. If the first pass returns fewer than topK matches, ef is
// widened and retried, up to a bounded number of attempts — documented
// tradeoff, see README.
func (e *Engine) Search(query []float32, topK, ef int, filters map[string]string) ([]Result, error) {
	if ef < topK {
		ef = topK
	}
	overfetch := ef
	if len(filters) > 0 {
		overfetch = ef * 4
	}

	const maxAttempts = 3
	var candidates []hnsw.SearchResult
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res, err := e.graph.Search(query, overfetch, overfetch)
		if err != nil {
			return nil, err
		}
		candidates = res

		matched := e.filterMatches(candidates, filters, topK)
		if len(matched) >= topK || len(candidates) < overfetch {
			// Either we have enough, or we've exhausted the whole graph
			// (fewer candidates returned than requested) so widening
			// further can't help.
			return matched, nil
		}
		overfetch *= 4
	}
	return e.filterMatches(candidates, filters, topK), nil
}

func (e *Engine) filterMatches(candidates []hnsw.SearchResult, filters map[string]string, topK int) []Result {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()

	out := make([]Result, 0, topK)
	for _, c := range candidates {
		md := e.metadata[c.ID]
		if !matchesFilters(md, filters) {
			continue
		}
		out = append(out, Result{ID: c.ID, Distance: c.Distance, Metadata: md})
		if len(out) == topK {
			break
		}
	}
	return out
}

func matchesFilters(metadata map[string]string, filters map[string]string) bool {
	for k, want := range filters {
		if metadata[k] != want {
			return false
		}
	}
	return true
}

// Snapshot writes the current graph and metadata to disk and truncates the
// WAL, since everything in it up to the current sequence number is now
// captured in the snapshot. Called periodically in the background (see
// cmd/vectorbased) and once more on clean shutdown.
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := segment.Save(filepath.Join(e.dir, snapshotFile), e.graph, e.seq); err != nil {
		return err
	}

	e.metaMu.RLock()
	err := saveMetadata(filepath.Join(e.dir, metadataFile), e.metadata)
	e.metaMu.RUnlock()
	if err != nil {
		return err
	}

	if err := e.w.Close(); err != nil {
		return err
	}
	if err := os.Truncate(filepath.Join(e.dir, walFile), 0); err != nil {
		return err
	}
	w, err := wal.OpenWriter(filepath.Join(e.dir, walFile), e.seq)
	if err != nil {
		return err
	}
	e.w = w
	return nil
}

// Close snapshots the current state and releases the WAL file handle.
func (e *Engine) Close() error {
	if err := e.Snapshot(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.Close()
}

// Len returns the number of live (non-deleted) vectors.
func (e *Engine) Len() int {
	return e.graph.Len()
}

func loadMetadata(path string) (map[uint64]map[string]string, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[uint64]map[string]string), nil
	}
	if err != nil {
		return nil, err
	}
	m := make(map[uint64]map[string]string)
	if len(b) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func saveMetadata(path string, metadata map[uint64]map[string]string) error {
	b, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
