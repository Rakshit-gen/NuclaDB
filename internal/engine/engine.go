// Package engine wires the HNSW graph, write-ahead log, and on-disk
// snapshot together into a single durable, queryable database: every
// mutation is WAL-logged before it touches the graph, and a snapshot lets
// restart skip replaying the log from empty.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

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
//
// graph is an atomic pointer, not a mu-guarded field: Search only ever
// reads it, and gating every search on the writer's Mutex just to copy a
// pointer serialized the read path under concurrency for no reason.
// LoadSnapshot (the only reassignment) still holds mu, so it can't race a
// concurrent Insert.
type Engine struct {
	mu  sync.Mutex
	dir string
	cfg hnsw.Config

	graph       atomic.Pointer[hnsw.Graph]
	w           *wal.Writer
	seq         uint64
	snapshotSeq uint64 // the seq the on-disk snapshot currently reflects

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

	e := &Engine{
		dir:         dir,
		cfg:         cfg,
		w:           w,
		seq:         lastSeq,
		snapshotSeq: snapshotSeq,
		metadata:    metadata,
	}
	e.graph.Store(g)
	return e, nil
}

// Insert durably upserts id -> (vector, metadata). metadata may be nil.
func (e *Engine) Insert(id uint64, vector []float32, metadata map[string]string) error {
	if len(vector) != e.cfg.Dim {
		return hnsw.ErrDimensionMismatch
	}
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
	if err := e.graph.Load().Insert(id, vector); err != nil {
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

// InsertItem is one vector in an InsertBatch call, the same shape Insert
// takes for a single record.
type InsertItem struct {
	ID       uint64
	Vector   []float32
	Metadata map[string]string
}

// InsertBatch durably upserts every item as one group: the WAL records share
// a single fsync (see wal.Writer.AppendBatch) instead of one each. The
// durability guarantee is the same as calling Insert per item. An fsync per
// record is why building the 10K SIFT benchmark took 44s.
//
// The whole batch is made durable before any item touches the graph, so if
// the shared fsync fails nothing in the batch is applied.
func (e *Engine) InsertBatch(items []InsertItem) error {
	if len(items) == 0 {
		return nil
	}

	// Validate everything before logging anything. A record the graph
	// rejects would also be rejected on replay, and Open would fail forever.
	for _, it := range items {
		if len(it.Vector) != e.cfg.Dim {
			return hnsw.ErrDimensionMismatch
		}
	}

	records := make([]wal.Record, len(items))
	for i, it := range items {
		var extra []byte
		if len(it.Metadata) > 0 {
			b, err := json.Marshal(it.Metadata)
			if err != nil {
				return err
			}
			extra = b
		}
		records[i] = wal.Record{Op: wal.OpInsert, ID: it.ID, Vector: it.Vector, Extra: extra}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	seqs, err := e.w.AppendBatch(records)
	if err != nil {
		return err
	}

	graph := e.graph.Load()
	for i, it := range items {
		if err := graph.Insert(it.ID, it.Vector); err != nil {
			return err
		}

		e.metaMu.Lock()
		if len(it.Metadata) > 0 {
			e.metadata[it.ID] = it.Metadata
		} else {
			delete(e.metadata, it.ID)
		}
		e.metaMu.Unlock()

		e.seq = seqs[i]
	}
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
	_ = e.graph.Load().Delete(id)

	e.metaMu.Lock()
	delete(e.metadata, id)
	e.metaMu.Unlock()

	e.seq = seq
	return nil
}

// LastSeq returns the sequence number of the most recently applied
// operation — where a replication follower should resume from after a
// reconnect (see internal/cluster/replication).
func (e *Engine) LastSeq() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq
}

// SnapshotSeq returns the sequence number the current on-disk snapshot
// reflects. A replication leader uses this to decide whether a follower
// requesting a given sequence can be served from the live WAL alone, or
// needs a full snapshot transfer first — see wal.Follow's doc comment on
// exactly where that boundary is.
func (e *Engine) SnapshotSeq() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotSeq
}

// WALPath returns the path to this Engine's current WAL file, for a
// replication leader to stream via wal.Follow. The path itself is stable
// for the Engine's lifetime even though Snapshot rotates the file it
// points to (see wal.Follow's doc comment on how it handles that).
func (e *Engine) WALPath() string {
	return filepath.Join(e.dir, walFile)
}

// ApplyReplicated durably applies rec — a WAL record produced by another
// Engine, typically the leader of this shard — to this Engine's own WAL
// and graph, preserving its original sequence number rather than
// assigning a new one. This is how a replica follower stays in sync: it
// never originates writes, only ever applies what the leader already
// committed, in the same order and under the same sequence numbers, so a
// promoted-to-leader replica's WAL is byte-for-byte continuable.
func (e *Engine) ApplyReplicated(rec wal.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.w.AppendRecord(rec); err != nil {
		return err
	}

	switch rec.Op {
	case wal.OpInsert:
		if err := e.graph.Load().Insert(rec.ID, rec.Vector); err != nil {
			return err
		}
		e.metaMu.Lock()
		if len(rec.Extra) > 0 {
			var m map[string]string
			if err := json.Unmarshal(rec.Extra, &m); err != nil {
				e.metaMu.Unlock()
				return err
			}
			e.metadata[rec.ID] = m
		} else {
			delete(e.metadata, rec.ID)
		}
		e.metaMu.Unlock()
	case wal.OpDelete:
		_ = e.graph.Load().Delete(rec.ID)
		e.metaMu.Lock()
		delete(e.metadata, rec.ID)
		e.metaMu.Unlock()
	}

	e.seq = rec.Seq
	return nil
}

// SnapshotBytes forces a fresh Snapshot and returns the resulting
// snapshot.bin and metadata.json contents verbatim, plus the sequence
// number they reflect. Used by internal/cluster/replication to bootstrap
// a new follower that's too far behind for WAL streaming alone to catch
// up (see wal.Follow's doc comment on that boundary) — the same role a
// base backup plays before streaming replication in a system like
// Postgres.
func (e *Engine) SnapshotBytes() (snapshot, metadata []byte, seq uint64, err error) {
	if err := e.Snapshot(); err != nil {
		return nil, nil, 0, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	snapshot, err = os.ReadFile(filepath.Join(e.dir, snapshotFile))
	if err != nil {
		return nil, nil, 0, err
	}
	metadata, err = os.ReadFile(filepath.Join(e.dir, metadataFile))
	if err != nil {
		return nil, nil, 0, err
	}
	return snapshot, metadata, e.snapshotSeq, nil
}

// LoadSnapshot replaces this Engine's entire state — graph, metadata, and
// WAL — with the given snapshot, atomically from the caller's perspective
// (the whole operation runs under e.mu). It's the follower side of
// SnapshotBytes: once loaded, WAL replication can resume live from seq,
// the same as if this Engine had just restarted from a local snapshot at
// that point.
func (e *Engine) LoadSnapshot(snapshotBytes, metadataBytes []byte, seq uint64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Metadata before the graph snapshot, for the same reason as Snapshot.
	metadataPath := filepath.Join(e.dir, metadataFile)
	if err := writeFileAtomic(metadataPath, metadataBytes); err != nil {
		return err
	}
	snapshotPath := filepath.Join(e.dir, snapshotFile)
	if err := writeFileAtomic(snapshotPath, snapshotBytes); err != nil {
		return err
	}

	g, gotSeq, err := segment.Load(snapshotPath, e.cfg)
	if err != nil {
		return err
	}
	if gotSeq != seq {
		return fmt.Errorf("engine: loaded snapshot seq %d does not match expected %d", gotSeq, seq)
	}
	metadata, err := loadMetadata(metadataPath)
	if err != nil {
		return err
	}

	if err := e.w.Close(); err != nil {
		return err
	}
	walPath := filepath.Join(e.dir, walFile)
	tmpPath := walPath + ".new"
	w, err := wal.OpenWriter(tmpPath, gotSeq)
	if err != nil {
		return err
	}
	if err := os.Rename(tmpPath, walPath); err != nil {
		_ = w.Close()
		return err
	}

	e.graph.Store(g)
	e.w = w
	e.seq = gotSeq
	e.snapshotSeq = gotSeq
	e.metaMu.Lock()
	e.metadata = metadata
	e.metaMu.Unlock()
	return nil
}

// writeFileAtomic replaces path with data so that after a crash the file
// holds either the old contents or the new, never a torn or empty mix: the
// temp file is fsynced before the rename, and the directory after it so the
// rename itself is durable.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

	// LoadSnapshot is the only thing that ever reassigns e.graph (Insert and
	// Delete mutate the existing graph in place under its own internal
	// lock). An atomic load here means Search never touches the writer's
	// Mutex — the traversal below runs fully concurrently with other
	// searches, bounded only by the graph's own RWMutex.
	graph := e.graph.Load()

	const maxAttempts = 3
	var candidates []hnsw.SearchResult
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res, err := graph.Search(query, overfetch, overfetch)
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

// Snapshot writes the current graph and metadata to disk and rotates the
// WAL, since everything in it up to the current sequence number is now
// captured in the snapshot. Called periodically in the background (see
// cmd/nucladbd) and once more on clean shutdown.
func (e *Engine) Snapshot() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Metadata goes first. Replay skips every WAL record at or below the
	// snapshot's seq, so if the graph snapshot landed and a crash hit before
	// metadata.json did, the metadata for those records was gone for good.
	// In this order a crash in between leaves metadata newer than the graph,
	// and replaying the WAL on top of it is harmless: each record sets or
	// clears its id's metadata outright.
	e.metaMu.RLock()
	err := saveMetadata(filepath.Join(e.dir, metadataFile), e.metadata)
	e.metaMu.RUnlock()
	if err != nil {
		return err
	}

	if err := segment.Save(filepath.Join(e.dir, snapshotFile), e.graph.Load(), e.seq); err != nil {
		return err
	}

	if err := e.w.Close(); err != nil {
		return err
	}
	walPath := filepath.Join(e.dir, walFile)
	tmpPath := walPath + ".new"
	w, err := wal.OpenWriter(tmpPath, e.seq)
	if err != nil {
		return err
	}
	// Rename, not truncate-in-place: a replication follower (see
	// internal/storage/wal.Follow) may have this file open mid-tail, and
	// a same-path truncate can race its size-based staleness check. A
	// rename gives the post-snapshot WAL a genuinely new file identity at
	// the same path — a follower detects that via os.SameFile and reopens
	// cleanly, while its existing fd on the old (now unlinked) file stays
	// valid and fully readable until it does.
	if err := os.Rename(tmpPath, walPath); err != nil {
		_ = w.Close()
		return err
	}
	e.w = w
	e.snapshotSeq = e.seq
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
	return e.graph.Load().Len()
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
	return writeFileAtomic(path, b)
}
