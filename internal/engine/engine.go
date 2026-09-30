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
	mu sync.Mutex
	// qmu guards queue and flushing, the group commit state for Insert.
	qmu      sync.Mutex
	queue    []*pendingInsert
	flushing bool

	dir string
	cfg hnsw.Config

	graph       atomic.Pointer[hnsw.Graph]
	w           *wal.Writer
	seq         uint64
	snapshotSeq uint64 // the seq the on-disk snapshot currently reflects

	metaMu   sync.RWMutex
	metadata map[uint64]map[string]string
	// postings maps "key\x00value" to the ids whose metadata has that pair,
	// so a filtered search can see how selective its filter is.
	postings map[string]map[uint64]struct{}
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

	// Replayed inserts are queued and applied with InsertBatch, so a long
	// WAL rebuilds on every core. A delete flushes the queue first, keeping
	// the graph's operations in log order.
	var pendingIDs []uint64
	var pendingVecs [][]float32
	flush := func() error {
		err := g.InsertBatch(pendingIDs, pendingVecs)
		pendingIDs, pendingVecs = pendingIDs[:0], pendingVecs[:0]
		return err
	}

	lastSeq := snapshotSeq
	walBytes, _, err := wal.Recover(walPath, func(rec wal.Record) error {
		if rec.Seq <= snapshotSeq {
			// Already reflected in the snapshot we just loaded.
			return nil
		}
		switch rec.Op {
		case wal.OpInsert:
			pendingIDs = append(pendingIDs, rec.ID)
			pendingVecs = append(pendingVecs, rec.Vector)
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
			if err := flush(); err != nil {
				return err
			}
			_ = g.Delete(rec.ID) // idempotent: fine if already absent
			delete(metadata, rec.ID)
		}
		lastSeq = rec.Seq
		return nil
	})
	if err == nil {
		err = flush()
	}
	if err != nil {
		return nil, err
	}

	w, err := wal.OpenWriterAt(walPath, lastSeq, walBytes)
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
		postings:    buildPostings(metadata),
	}
	e.graph.Store(g)
	return e, nil
}

// Insert durably upserts id -> (vector, metadata). metadata may be nil.
//
// Concurrent Inserts are group committed: the first caller becomes the
// leader and logs every insert queued behind it with one shared fsync (the
// same path InsertBatch takes). A caller still returns only once its own
// record is durable and applied. Under one writer this is one fsync per
// insert, same as before; under N concurrent writers it is roughly one per
// round instead of N.
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

	p := &pendingInsert{
		item: InsertItem{ID: id, Vector: vector, Metadata: metadata},
		rec:  wal.Record{Op: wal.OpInsert, ID: id, Vector: vector, Extra: extra},
		done: make(chan groupResult, 1),
	}

	e.qmu.Lock()
	e.queue = append(e.queue, p)
	lead := !e.flushing
	e.flushing = true
	e.qmu.Unlock()

	if !lead {
		r := <-p.done
		if !r.lead {
			return r.err
		}
	}

	e.qmu.Lock()
	group := e.queue
	e.queue = nil
	e.qmu.Unlock()

	items := make([]InsertItem, len(group))
	records := make([]wal.Record, len(group))
	for i, q := range group {
		items[i], records[i] = q.item, q.rec
	}
	e.mu.Lock()
	err := e.commitLocked(items, records)
	e.mu.Unlock()

	for _, q := range group {
		if q != p {
			q.done <- groupResult{err: err}
		}
	}

	// Hand leadership to the next waiter instead of looping here, so one
	// caller never gets stuck flushing everyone else's writes.
	e.qmu.Lock()
	if len(e.queue) == 0 {
		e.flushing = false
	} else {
		e.queue[0].done <- groupResult{lead: true}
	}
	e.qmu.Unlock()
	return err
}

type pendingInsert struct {
	item InsertItem
	rec  wal.Record
	done chan groupResult // buffered, receives exactly one value
}

// groupResult is either a finished insert's error, or lead=true telling a
// waiter to flush the queue itself.
type groupResult struct {
	err  error
	lead bool
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
	return e.commitLocked(items, records)
}

// commitLocked logs records with one fsync, then applies items to the graph
// in the same order. items must already be validated. Caller holds e.mu.
func (e *Engine) commitLocked(items []InsertItem, records []wal.Record) error {
	seqs, err := e.w.AppendBatch(records)
	if err != nil {
		return err
	}

	ids := make([]uint64, len(items))
	vectors := make([][]float32, len(items))
	for i, it := range items {
		ids[i], vectors[i] = it.ID, it.Vector
	}
	if err := e.graph.Load().InsertBatch(ids, vectors); err != nil {
		return err
	}
	for _, it := range items {
		e.setMeta(it.ID, it.Metadata)
	}
	e.seq = seqs[len(seqs)-1]
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
	e.setMeta(id, nil)

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
		var m map[string]string
		if len(rec.Extra) > 0 {
			if err := json.Unmarshal(rec.Extra, &m); err != nil {
				return err
			}
		}
		e.setMeta(rec.ID, m)
	case wal.OpDelete:
		_ = e.graph.Load().Delete(rec.ID)
		e.setMeta(rec.ID, nil)
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
	e.postings = buildPostings(metadata)
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

// exactFilterLimit is the most filter matches Search will score by brute
// force. Below it, scanning the matches is exact and costs about as much as
// a graph walk; above it, the filter is broad enough that post-filtering
// the graph's candidates finds topK matches.
// ponytail: fixed cutoff, tune from the ef/dataset size if it ever matters.
const exactFilterLimit = 4096

// Search returns up to topK nearest neighbors of query, optionally
// restricted to vectors whose metadata matches every key/value in filters
// (a plain equality AND across all filter keys).
//
// With a filter, Search first checks how many vectors match it using the
// postings index. A selective filter (up to exactFilterLimit matches) is
// answered exactly by scoring just those vectors, so it can't come back
// short. A broad filter post-filters an overfetched graph search, widening
// ef and retrying a bounded number of times if too few candidates match.
func (e *Engine) Search(query []float32, topK, ef int, filters map[string]string) ([]Result, error) {
	if ef < topK {
		ef = topK
	}

	// LoadSnapshot is the only thing that ever reassigns e.graph (Insert and
	// Delete mutate the existing graph in place under its own lock), so an
	// atomic load keeps Search off the writer's mutex.
	graph := e.graph.Load()

	if len(filters) > 0 {
		if ids, ok := e.filterCandidates(filters); ok {
			res, err := graph.ExactSearch(query, ids, topK)
			if err != nil {
				return nil, err
			}
			return e.withMetadata(res), nil
		}
	}

	overfetch := ef
	if len(filters) > 0 {
		overfetch = ef * 4
	}
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

// filterCandidates returns the ids matching every filter when the most
// selective filter pair has at most exactFilterLimit ids. ok is false when
// the filter is too broad for an exact scan.
func (e *Engine) filterCandidates(filters map[string]string) (ids []uint64, ok bool) {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()

	var smallest map[uint64]struct{}
	first := true
	for k, v := range filters {
		p := e.postings[postingKey(k, v)]
		if first || len(p) < len(smallest) {
			smallest, first = p, false
		}
	}
	if len(smallest) > exactFilterLimit {
		return nil, false
	}
	ids = make([]uint64, 0, len(smallest))
	for id := range smallest {
		if matchesFilters(e.metadata[id], filters) {
			ids = append(ids, id)
		}
	}
	return ids, true
}

func (e *Engine) withMetadata(res []hnsw.SearchResult) []Result {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	out := make([]Result, len(res))
	for i, r := range res {
		out[i] = Result{ID: r.ID, Distance: r.Distance, Metadata: e.metadata[r.ID]}
	}
	return out
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

func postingKey(k, v string) string { return k + "\x00" + v }

func buildPostings(metadata map[uint64]map[string]string) map[string]map[uint64]struct{} {
	p := make(map[string]map[uint64]struct{})
	for id, md := range metadata {
		for k, v := range md {
			addPosting(p, postingKey(k, v), id)
		}
	}
	return p
}

func addPosting(p map[string]map[uint64]struct{}, key string, id uint64) {
	ids := p[key]
	if ids == nil {
		ids = make(map[uint64]struct{})
		p[key] = ids
	}
	ids[id] = struct{}{}
}

// setMeta replaces id's metadata (nil or empty clears it) and keeps the
// postings index in step.
func (e *Engine) setMeta(id uint64, md map[string]string) {
	e.metaMu.Lock()
	defer e.metaMu.Unlock()
	for k, v := range e.metadata[id] {
		key := postingKey(k, v)
		delete(e.postings[key], id)
		if len(e.postings[key]) == 0 {
			delete(e.postings, key)
		}
	}
	if len(md) == 0 {
		delete(e.metadata, id)
		return
	}
	e.metadata[id] = md
	for k, v := range md {
		addPosting(e.postings, postingKey(k, v), id)
	}
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
