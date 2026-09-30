// Package engine wires the HNSW graph, write-ahead log, and on-disk
// snapshot together into a single durable, queryable database: every
// mutation is WAL-logged before it touches the graph, and a snapshot lets
// restart skip replaying the log from empty.
package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	// snapMu serializes snapshots, which drop mu while writing files. Taken
	// before mu, never after.
	snapMu sync.Mutex
	// qmu guards queue and flushing, the group commit state for Insert and
	// Delete.
	qmu      sync.Mutex
	queue    []*pendingOp
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

	exactFilterLimit atomic.Int64
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
		case wal.OpInsert, wal.OpMeta:
			if rec.Op == wal.OpInsert {
				pendingIDs = append(pendingIDs, rec.ID)
				pendingVecs = append(pendingVecs, rec.Vector)
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
	// A metadata update logged just before a concurrent delete of the same
	// id can leave metadata for an id the graph no longer has.
	for id := range metadata {
		if _, ok := g.Get(id); !ok {
			delete(metadata, id)
		}
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
	e.exactFilterLimit.Store(DefaultExactFilterLimit)
	e.graph.Store(g)
	return e, nil
}

// SetExactFilterLimit changes the filter size up to which Search scores the
// matching vectors directly instead of walking the graph.
func (e *Engine) SetExactFilterLimit(n int) {
	e.exactFilterLimit.Store(int64(n))
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

	return e.group(&pendingOp{
		rec:  wal.Record{Op: wal.OpInsert, ID: id, Vector: vector, Extra: extra},
		meta: metadata,
		done: make(chan groupResult, 1),
	})
}

// group queues p for the next group commit and returns once it is durable
// and applied. The first caller in becomes the leader and commits every op
// queued behind it with one shared fsync.
func (e *Engine) group(p *pendingOp) error {
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

	records := make([]wal.Record, len(group))
	metas := make([]map[string]string, len(group))
	for i, q := range group {
		records[i], metas[i] = q.rec, q.meta
	}
	e.mu.Lock()
	err := e.commitLocked(records, metas)
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

type pendingOp struct {
	rec  wal.Record
	meta map[string]string // inserts and metadata updates
	done chan groupResult  // buffered, receives exactly one value
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
	metas := make([]map[string]string, len(items))
	for i, it := range items {
		metas[i] = it.Metadata
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
	return e.commitLocked(records, metas)
}

// commitLocked logs records with one fsync, then applies them to the graph
// in the same order. Runs of inserts go through InsertBatch; a delete ends
// the run so the graph sees operations in log order. metas[i] is record
// i's metadata. Records must already be validated. Caller holds e.mu.
func (e *Engine) commitLocked(records []wal.Record, metas []map[string]string) error {
	seqs, err := e.w.AppendBatch(records)
	if err != nil {
		return err
	}

	g := e.graph.Load()
	var ids []uint64
	var vectors [][]float32
	flush := func() error {
		err := g.InsertBatch(ids, vectors)
		ids, vectors = ids[:0], vectors[:0]
		return err
	}
	for _, rec := range records {
		switch rec.Op {
		case wal.OpInsert:
			ids = append(ids, rec.ID)
			vectors = append(vectors, rec.Vector)
		case wal.OpDelete:
			if err := flush(); err != nil {
				return err
			}
			_ = g.Delete(rec.ID)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	for i, rec := range records {
		if _, ok := g.Get(rec.ID); ok || rec.Op == wal.OpDelete {
			e.setMeta(rec.ID, metas[i])
		}
	}
	e.seq = seqs[len(seqs)-1]
	return nil
}

// UpdateMetadata durably replaces id's metadata without touching its
// vector. nil or empty clears it. Returns ErrNotFound if id isn't stored.
func (e *Engine) UpdateMetadata(id uint64, metadata map[string]string) error {
	if e.CountNew([]uint64{id}) == 1 {
		return ErrNotFound
	}
	var extra []byte
	if len(metadata) > 0 {
		b, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		extra = b
	}
	return e.group(&pendingOp{
		rec:  wal.Record{Op: wal.OpMeta, ID: id, Extra: extra},
		meta: metadata,
		done: make(chan groupResult, 1),
	})
}

// applyMeta applies a replicated OpMeta record. Caller holds e.mu.
func (e *Engine) applyMeta(rec wal.Record) error {
	var m map[string]string
	if len(rec.Extra) > 0 {
		if err := json.Unmarshal(rec.Extra, &m); err != nil {
			return err
		}
	}
	if _, ok := e.graph.Load().Get(rec.ID); ok {
		e.setMeta(rec.ID, m)
	}
	return nil
}

// Delete durably removes id. It is not an error to delete an id that was
// already deleted or never existed; nothing is logged for one, so it costs
// no fsync. Concurrent deletes and inserts share group commits.
func (e *Engine) Delete(id uint64) error {
	if e.CountNew([]uint64{id}) == 1 {
		return nil
	}
	return e.group(&pendingOp{
		rec:  wal.Record{Op: wal.OpDelete, ID: id},
		done: make(chan groupResult, 1),
	})
}

// LastSeq returns the sequence number of the most recently applied
// operation — where a replication follower should resume from after a
// reconnect (see internal/cluster/replication).
func (e *Engine) LastSeq() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq
}

// RecordSum returns the WAL checksum of the record numbered seq, if it is
// still in this Engine's WAL. Replication compares it between leader and
// follower to catch two logs that agree on length but not on content.
// ponytail: scans the WAL from the start, fine at connect time; keep a
// seq -> offset index if reconnects over large WALs get slow.
func (e *Engine) RecordSum(seq uint64) (sum uint32, found bool, err error) {
	return wal.SumAt(e.WALPath(), seq)
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
	case wal.OpMeta:
		if err := e.applyMeta(rec); err != nil {
			return err
		}
	}

	e.seq = rec.Seq
	return nil
}

// OpenSnapshot forces a fresh Snapshot and opens the resulting
// snapshot.bin and metadata.json for reading, plus the sequence number
// they reflect. The caller streams and closes both. Used by
// internal/cluster/replication to bootstrap a follower that's too far
// behind for WAL streaming alone (see wal.Follow's doc comment on that
// boundary), the same role a base backup plays before streaming
// replication in Postgres. The open files stay readable even if a later
// snapshot replaces them on disk, since that is a rename over the path.
func (e *Engine) OpenSnapshot() (snapshot, metadata *os.File, seq uint64, err error) {
	// Holding snapMu keeps another snapshot from replacing the files
	// between the two opens.
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	if err := e.snapshotLocked(); err != nil {
		return nil, nil, 0, err
	}

	snapshot, err = os.Open(filepath.Join(e.dir, snapshotFile))
	if err != nil {
		return nil, nil, 0, err
	}
	metadata, err = os.Open(filepath.Join(e.dir, metadataFile))
	if err != nil {
		snapshot.Close()
		return nil, nil, 0, err
	}
	return snapshot, metadata, e.SnapshotSeq(), nil
}

// LoadSnapshot replaces this Engine's entire state (graph, metadata and
// WAL) with the snapshot read from snapshotR and then metadataR, in that
// order. It's the follower side of OpenSnapshot: once loaded, WAL
// replication can resume live from seq, the same as if this Engine had
// just restarted from a local snapshot at that point. Both readers are
// copied to disk first without holding e.mu, so searches keep running
// during a large transfer; only the swap itself blocks them.
func (e *Engine) LoadSnapshot(snapshotR, metadataR io.Reader, seq uint64) error {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()

	snapshotPath := filepath.Join(e.dir, snapshotFile)
	metadataPath := filepath.Join(e.dir, metadataFile)
	snapshotIn, metadataIn := snapshotPath+".incoming", metadataPath+".incoming"
	defer os.Remove(snapshotIn)
	defer os.Remove(metadataIn)
	if err := writeFileFrom(snapshotIn, snapshotR); err != nil {
		return err
	}
	if err := writeFileFrom(metadataIn, metadataR); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Metadata before the graph snapshot, for the same reason as Snapshot.
	if err := os.Rename(metadataIn, metadataPath); err != nil {
		return err
	}
	if err := os.Rename(snapshotIn, snapshotPath); err != nil {
		return err
	}
	if err := syncDir(e.dir); err != nil {
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
// writeFileFrom copies r into a new file at path and fsyncs it.
func writeFileFrom(path string, r io.Reader) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

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

// DefaultExactFilterLimit is the most filter matches Search scores by brute
// force up front. Below it, scanning the matches is exact and costs about as
// much as a graph walk; above it, the filter is broad enough that
// post-filtering the graph's candidates usually finds topK matches.
const DefaultExactFilterLimit = 4096

// DefaultEf is the search beam width used when a caller doesn't pick one.
// In the SIFT benchmark at topK=10, ef=10 gave recall 0.93 and ef=50 gave
// 0.996 at about 75% of the QPS.
const DefaultEf = 64

// Search returns up to topK nearest neighbors of query, optionally
// restricted to vectors whose metadata matches every filter.
//
// With a filter, Search first uses the postings index to count the vectors
// an Eq or In filter allows. A selective filter (up to the exact filter
// limit) is
// answered exactly by scoring just those vectors, so it can't come back
// short. A broad filter post-filters an overfetched graph search, widening
// ef and retrying a bounded number of times; if that still finds fewer
// than topK matches, it scores every match exactly.
func (e *Engine) Search(query []float32, topK, ef int, filters []Filter) ([]Result, error) {
	for _, f := range filters {
		if err := f.validate(); err != nil {
			return nil, err
		}
	}
	if ef <= 0 {
		ef = DefaultEf
	}
	ef = max(ef, topK)

	// LoadSnapshot is the only thing that ever reassigns e.graph (Insert and
	// Delete mutate the existing graph in place under its own lock), so an
	// atomic load keeps Search off the writer's mutex.
	graph := e.graph.Load()

	if len(filters) > 0 {
		if ids, ok := e.filterCandidates(graph, filters, int(e.exactFilterLimit.Load())); ok {
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
	// The filter matched few of the graph's nearest vectors. Score all of
	// its matches instead, so the caller still gets topK when they exist.
	ids, _ := e.filterCandidates(graph, filters, math.MaxInt)
	res, err := graph.ExactSearch(query, ids, topK)
	if err != nil {
		return nil, err
	}
	return e.withMetadata(res), nil
}

// filterCandidates returns the ids matching every filter, if the most
// selective Eq or In filter allows at most limit ids. ok is false when
// the filter is too broad for an exact scan. With no Eq or In filter, only
// an unlimited call scans, and it checks every stored id.
func (e *Engine) filterCandidates(g *hnsw.Graph, filters []Filter, limit int) (ids []uint64, ok bool) {
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()

	var best []map[uint64]struct{}
	bestSize := -1
	for _, f := range filters {
		if f.Op != OpEq && f.Op != OpIn {
			continue
		}
		var sets []map[uint64]struct{}
		size := 0
		for _, v := range f.Values {
			p := e.postings[postingKey(f.Key, v)]
			sets, size = append(sets, p), size+len(p)
		}
		if bestSize < 0 || size < bestSize {
			best, bestSize = sets, size
		}
	}

	var pool []uint64
	switch {
	case bestSize >= 0 && bestSize <= limit:
		pool = make([]uint64, 0, bestSize)
		for _, set := range best {
			for id := range set {
				pool = append(pool, id)
			}
		}
	case bestSize < 0 && limit == math.MaxInt:
		pool = g.IDs()
	default:
		return nil, false
	}
	ids = pool[:0]
	for _, id := range pool {
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

func (e *Engine) filterMatches(candidates []hnsw.SearchResult, filters []Filter, topK int) []Result {
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
// WAL, since everything in it up to the snapshot's sequence number is now
// captured in the snapshot. Called periodically in the background (see
// cmd/nucladbd) and once more on clean shutdown.
//
// Writes are blocked only while the state is copied and while the WAL is
// rotated, not while the snapshot is written and fsynced.
func (e *Engine) Snapshot() error {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	return e.snapshotLocked()
}

// snapshotLocked is Snapshot. Caller holds snapMu, not mu.
// ponytail: the copy under mu is O(vectors); copy-on-write graph pages if
// that pause starts to matter for very large tenants.
func (e *Engine) snapshotLocked() error {
	e.mu.Lock()
	if err := e.compactLocked(); err != nil {
		e.mu.Unlock()
		return err
	}
	g := e.graph.Load()
	seq := e.seq
	nodes, entryPoint, maxLevel, hasEntry := g.Snapshot()
	e.metaMu.RLock()
	metaJSON, err := json.Marshal(e.metadata)
	e.metaMu.RUnlock()
	var walOffset int64
	if err == nil {
		walOffset, err = e.w.Size()
	}
	e.mu.Unlock()
	if err != nil {
		return err
	}

	// Metadata goes first. Replay skips every WAL record at or below the
	// snapshot's seq, so if the graph snapshot landed and a crash hit before
	// metadata.json did, the metadata for those records was gone for good.
	// In this order a crash in between leaves metadata newer than the graph,
	// and replaying the WAL on top of it is harmless: each record sets or
	// clears its id's metadata outright.
	if err := writeFileAtomic(filepath.Join(e.dir, metadataFile), metaJSON); err != nil {
		return err
	}
	if err := segment.SaveState(filepath.Join(e.dir, snapshotFile), g.Config(), nodes, entryPoint, maxLevel, hasEntry, seq); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rotateWALLocked(seq, walOffset)
}

// rotateWALLocked replaces the WAL with one holding only what was appended
// after byte offset from, i.e. the records after seq, which the snapshot
// just written doesn't cover. Caller holds mu.
func (e *Engine) rotateWALLocked(seq uint64, from int64) error {
	walPath := filepath.Join(e.dir, walFile)
	tmpPath := walPath + ".new"
	if err := copyTail(walPath, tmpPath, from); err != nil {
		return err
	}
	w, err := wal.OpenWriter(tmpPath, e.seq)
	if err != nil {
		return err
	}
	// Rename, not truncate-in-place: a replication follower (see
	// internal/storage/wal.Follow) may have this file open mid-tail, and
	// a same-path truncate can race its size-based staleness check. A
	// rename gives the post-snapshot WAL a genuinely new file identity at
	// the same path. A follower detects that via os.SameFile and reopens
	// cleanly, while its existing fd on the old (now unlinked) file stays
	// valid and fully readable until it does.
	if err := os.Rename(tmpPath, walPath); err != nil {
		_ = w.Close()
		return err
	}
	if err := syncDir(e.dir); err != nil {
		_ = w.Close()
		return err
	}
	_ = e.w.Close()
	e.w = w
	e.snapshotSeq = seq
	return nil
}

// copyTail writes src's bytes from offset on into a new, fsynced dst.
func copyTail(src, dst string, offset int64) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, io.NewSectionReader(in, offset, math.MaxInt64-offset)); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// compactLocked rebuilds the graph without its deleted nodes once they
// outnumber the live ones. Caller holds e.mu.
// ponytail: a full rebuild with writes blocked, fine while rebuilds take
// well under a second; an incremental repair (hnswlib's markDelete plus
// slot reuse) if they stop being rare or quick.
func (e *Engine) compactLocked() error {
	g := e.graph.Load()
	if dead := g.Tombstones(); dead < minCompact || dead <= g.Len() {
		return nil
	}
	ng, err := g.Compact()
	if err != nil {
		return err
	}
	e.graph.Store(ng)
	return nil
}

// minCompact keeps small graphs from being rebuilt over a few deletes.
const minCompact = 256

// discard releases the WAL file handle without snapshotting, for an engine
// whose files are about to be deleted.
func (e *Engine) discard() error {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.Close()
}

// Close snapshots the current state and releases the WAL file handle.
func (e *Engine) Close() error {
	e.snapMu.Lock()
	defer e.snapMu.Unlock()
	if err := e.snapshotLocked(); err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.w.Close()
}

// Get returns id's stored vector and metadata. ok is false if id isn't
// stored.
func (e *Engine) Get(id uint64) (vector []float32, metadata map[string]string, ok bool) {
	vector, ok = e.graph.Load().Get(id)
	if !ok {
		return nil, nil, false
	}
	e.metaMu.RLock()
	defer e.metaMu.RUnlock()
	return vector, e.metadata[id], true
}

// List returns up to limit stored ids that are >= start, in ascending
// order, and whether more follow.
// ponytail: sorts every id per page, O(n log n); keep a sorted index if
// paging through big tenants gets slow.
func (e *Engine) List(start uint64, limit int) (ids []uint64, more bool) {
	all := e.graph.Load().IDs()
	slices.Sort(all)
	i, _ := slices.BinarySearch(all, start)
	all = all[i:]
	if len(all) > limit {
		return all[:limit], true
	}
	return all, false
}

// CountNew returns how many distinct ids are not stored yet.
func (e *Engine) CountNew(ids []uint64) int {
	return e.graph.Load().CountNew(ids)
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
