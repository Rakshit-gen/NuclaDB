// Package wal implements a crash-safe, append-only write-ahead log used to
// make every insert/delete durable before it is applied to the in-memory
// HNSW graph. On restart, the log is replayed to reconstruct any writes
// made since the last snapshot (see internal/storage/segment).
//
// Record wire format (little-endian):
//
//	[4]  length   uint32  length of (crc + payload)
//	[4]  crc32    uint32  IEEE crc32 of payload
//	[..] payload
//
// payload:
//
//	[8]  seq      uint64
//	[1]  op       byte     1=insert, 2=delete
//	[8]  id       uint64
//	[4]  dim      uint32   0 for deletes
//	[dim*4]       []float32, little-endian
//	[4]  extraLen uint32   length of an opaque, caller-defined sidecar blob
//	[extraLen]    []byte   e.g. JSON-encoded metadata; the WAL never
//	                       interprets this, it just makes it durable
//	                       alongside the vector op
package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

// Op identifies the kind of operation a Record represents.
type Op byte

const (
	OpInsert Op = 1
	OpDelete Op = 2
)

// Record is one durable operation in the log.
type Record struct {
	Seq    uint64
	Op     Op
	ID     uint64
	Vector []float32 // nil for OpDelete
	Extra  []byte    // opaque caller payload, e.g. JSON metadata; nil if none
}

// ErrCorruptTail is returned internally during replay when the log ends in
// a partial record — the expected shape of a write that was interrupted by
// a crash. Replay treats it as end-of-log rather than a fatal error.
var errCorruptTail = errors.New("wal: corrupt or partial tail record")

// Writer appends records to a log file, fsyncing after every write so a
// successful Append call is a durability guarantee, not just a buffering
// promise.
type Writer struct {
	mu      sync.Mutex
	f       *os.File
	nextSeq uint64
}

// OpenWriter opens (creating if necessary) the log file at path for
// appending, starting sequence numbers at startSeq+1. Callers recovering
// from a prior snapshot should pass the snapshot's LastSeq here.
func OpenWriter(path string, startSeq uint64) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, nextSeq: startSeq + 1}, nil
}

// Append durably writes rec's operation to the log and returns the
// sequence number assigned to it. Equivalent to AppendWithExtra with a nil
// sidecar payload.
func (w *Writer) Append(op Op, id uint64, vector []float32) (uint64, error) {
	return w.AppendWithExtra(op, id, vector, nil)
}

// AppendWithExtra is Append plus an opaque sidecar byte payload, durably
// logged alongside the operation. Used to make caller-level state (e.g.
// per-vector metadata) crash-safe without the WAL needing to understand
// its contents.
func (w *Writer) AppendWithExtra(op Op, id uint64, vector []float32, extra []byte) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.nextSeq
	payload := encodePayload(seq, op, id, vector, extra)

	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(4+len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)

	if _, err := w.f.Write(buf); err != nil {
		return 0, err
	}
	if err := w.f.Sync(); err != nil {
		return 0, err
	}
	w.nextSeq++
	return seq, nil
}

// AppendRecord durably appends rec exactly as given — including its own
// sequence number — instead of assigning the next one itself. It's for a
// replication follower applying records streamed from a shard leader
// (see internal/cluster/replication): the sequence must match the
// leader's exactly for the two WALs to represent the same history. It
// errors if rec.Seq isn't the next expected sequence, catching a gap (a
// dropped or reordered record) rather than silently creating one.
func (w *Writer) AppendRecord(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if rec.Seq != w.nextSeq {
		return fmt.Errorf("wal: out-of-order append: got seq %d, want %d", rec.Seq, w.nextSeq)
	}
	if _, err := w.f.Write(EncodeRecord(rec)); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.nextSeq++
	return nil
}

// Close flushes and closes the underlying file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// EncodeRecord serializes rec into the same on-disk frame format Writer
// uses, for callers that need the raw bytes directly — e.g. streaming a
// record to a replication follower over the network instead of writing it
// to a local file.
func EncodeRecord(rec Record) []byte {
	payload := encodePayload(rec.Seq, rec.Op, rec.ID, rec.Vector, rec.Extra)
	buf := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(4+len(payload)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(payload))
	copy(buf[8:], payload)
	return buf
}

// DecodeRecord reads one frame from r using the same format Replay
// consumes, for a caller reading a raw record stream directly — e.g. a
// replication follower reading records off a network connection instead
// of a local file.
func DecodeRecord(r *bufio.Reader) (Record, error) {
	return readRecord(r)
}

func encodePayload(seq uint64, op Op, id uint64, vector []float32, extra []byte) []byte {
	buf := make([]byte, 8+1+8+4+len(vector)*4+4+len(extra))
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	buf[8] = byte(op)
	binary.LittleEndian.PutUint64(buf[9:17], id)
	binary.LittleEndian.PutUint32(buf[17:21], uint32(len(vector)))
	off := 21
	for _, f := range vector {
		binary.LittleEndian.PutUint32(buf[off:off+4], math.Float32bits(f))
		off += 4
	}
	binary.LittleEndian.PutUint32(buf[off:off+4], uint32(len(extra)))
	off += 4
	copy(buf[off:], extra)
	return buf
}

func decodePayload(payload []byte) (Record, error) {
	if len(payload) < 21 {
		return Record{}, errCorruptTail
	}
	seq := binary.LittleEndian.Uint64(payload[0:8])
	op := Op(payload[8])
	id := binary.LittleEndian.Uint64(payload[9:17])
	dim := binary.LittleEndian.Uint32(payload[17:21])

	off := 21
	var vec []float32
	if dim > 0 {
		if len(payload) < off+int(dim)*4 {
			return Record{}, errCorruptTail
		}
		vec = make([]float32, dim)
		for i := range vec {
			vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(payload[off : off+4]))
			off += 4
		}
	}

	if len(payload) < off+4 {
		return Record{}, errCorruptTail
	}
	extraLen := binary.LittleEndian.Uint32(payload[off : off+4])
	off += 4
	if len(payload) != off+int(extraLen) {
		return Record{}, errCorruptTail
	}
	var extra []byte
	if extraLen > 0 {
		extra = make([]byte, extraLen)
		copy(extra, payload[off:off+int(extraLen)])
	}

	return Record{Seq: seq, Op: op, ID: id, Vector: vec, Extra: extra}, nil
}

// Replay reads every well-formed record in the log at path in order,
// invoking fn for each. If the file does not exist, Replay treats that as
// an empty log and returns (0, nil).
//
// A trailing partial record — the signature of a write interrupted by a
// crash between the length prefix and a full fsync — is not an error: it
// is silently dropped, and Replay returns the sequence number of the last
// *complete* record applied. This is the log's crash-recovery contract.
func Replay(path string, fn func(Record) error) (lastSeq uint64, err error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		rec, err := readRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, errCorruptTail) {
				break
			}
			return lastSeq, err
		}
		if err := fn(rec); err != nil {
			return lastSeq, err
		}
		lastSeq = rec.Seq
	}
	return lastSeq, nil
}

func readRecord(r *bufio.Reader) (Record, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return Record{}, err
	}
	length := binary.LittleEndian.Uint32(header[0:4])
	wantCRC := binary.LittleEndian.Uint32(header[4:8])
	if length < 4 {
		return Record{}, errCorruptTail
	}

	payload := make([]byte, length-4)
	if _, err := io.ReadFull(r, payload); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, io.ErrUnexpectedEOF
		}
		return Record{}, err
	}
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return Record{}, errCorruptTail
	}
	return decodePayload(payload)
}

// Follow behaves like Replay but never stops at end-of-file: after
// delivering every currently-available complete record with Seq >
// fromSeq, it polls at pollInterval for more, until ctx is cancelled or
// fn returns an error. It's the leader side of WAL replication (see
// internal/cluster/replication) — a follower streams a shard's writes
// live off the leader's own WAL file, the same durable source of truth
// restart/replay already trusts, rather than a separate in-memory
// broadcast that could silently drop a record a slow or disconnected
// follower never saw.
//
// It reads via absolute-offset ReadAt on the file, not the buffered
// sequential Reader Replay uses, so it doesn't have to track how many
// bytes a bufio.Reader silently buffered ahead of what's actually been
// parsed (which would need unreading on a partial record).
//
// Engine.Snapshot rotates the WAL file when it runs (see engine.go):
// rather than truncating the file Follow already has open — which would
// race a fast truncate-then-immediate-rewrite past size-based detection,
// since the file could coincidentally be back to (or past) the same size
// before Follow's next poll ever observes it having shrunk — it writes
// the new, empty WAL to a temp path and renames it into place, giving the
// post-snapshot file a genuinely new identity at the same path. Follow
// checks path's current identity against the fd it has open every
// iteration (os.SameFile, an inode/device comparison, not a size guess);
// on a mismatch it reopens path fresh and resets its offset to 0 — the
// same non-racy rotation-following pattern `tail -F` uses, and unlike a
// size comparison it can't be fooled by the new file happening to reach
// the same length. The old, now-unlinked file stays fully readable
// through the fd Follow already had on it until that reopen happens, so
// no record in flight is ever lost, only possibly read a little late.
//
// A newly Follow-ing reader whose fromSeq predates the oldest record
// still physically present in the WAL file (the leader already
// snapshotted past it) will simply see no records for that range — those
// writes only still exist in the leader's snapshot, not its WAL, and
// getting them requires transferring that snapshot first (see
// replication.Bootstrap), not something Follow can serve from a WAL file
// alone.
func Follow(ctx context.Context, path string, fromSeq uint64, pollInterval time.Duration, fn func(Record) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	// A closure, not a bound method value: f is reassigned on rotation
	// below, and this must close whichever file is current when Follow
	// returns, not the one open when the defer statement ran.
	defer func() { _ = f.Close() }()

	var offset int64
	header := make([]byte, 8)
	for {
		if pathInfo, statErr := os.Stat(path); statErr == nil {
			if fdInfo, fdErr := f.Stat(); fdErr == nil && !os.SameFile(fdInfo, pathInfo) {
				if next, openErr := os.Open(path); openErr == nil {
					_ = f.Close()
					f = next
					offset = 0
				}
			}
		}

		n, err := f.ReadAt(header, offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if n < len(header) {
			if err := sleepOrDone(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}

		length := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])
		if length < 4 {
			// Only reachable via a torn concurrent write of the length
			// prefix itself (the whole frame is written in one syscall,
			// so this is rare) — treated as "not written yet", same as a
			// short header read, rather than fatal: Follow always assumes
			// more may still be coming.
			if err := sleepOrDone(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}

		payload := make([]byte, length-4)
		pn, err := f.ReadAt(payload, offset+int64(len(header)))
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if pn < len(payload) || crc32.ChecksumIEEE(payload) != wantCRC {
			if err := sleepOrDone(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}

		rec, err := decodePayload(payload)
		if err != nil {
			if err := sleepOrDone(ctx, pollInterval); err != nil {
				return err
			}
			continue
		}

		offset += int64(len(header) + len(payload))
		if rec.Seq > fromSeq {
			if err := fn(rec); err != nil {
				return err
			}
			fromSeq = rec.Seq
		}
	}
}

func sleepOrDone(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
