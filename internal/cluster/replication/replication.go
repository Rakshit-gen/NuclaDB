// Package replication streams a shard leader's WAL to follower replicas
// over a plain TCP connection, keeping each replica's Engine durably in
// sync without going through Raft — see internal/cluster/raft's own
// package doc for why Raft here governs cluster *metadata*, not the
// high-throughput vector write path.
//
// Wire protocol, deliberately minimal:
//
//  1. The follower connects and sends its current sequence number (8
//     bytes, big-endian; 0 for a brand-new replica), then one byte that is
//     1 if it still has that record in its WAL, then that record's WAL
//     checksum (4 bytes, big-endian; zero when the flag is 0).
//  2. The leader replies with one marker byte: 'S' if a snapshot transfer
//     follows, or 'N' if the follower can be served directly from the
//     live WAL. It sends a snapshot when the follower is too far behind
//     for WAL streaming alone (its sequence predates the oldest record
//     still on disk, see wal.Follow's doc comment on that boundary), and
//     also when the follower may have diverged: it is ahead of the leader
//     (an old leader's writes that never replicated), or its record at
//     that sequence has a different checksum, or either side no longer
//     has the record to compare. Resyncing from a snapshot throws the
//     follower's divergent tail away.
//  3. If 'S': an 8-byte sequence number the snapshot reflects, then the
//     snapshot and metadata blobs, each an 8-byte big-endian length
//     followed by that many bytes. Both are streamed from and to disk,
//     never held whole in memory.
//  4. Either way, the connection then carries a live stream of raw WAL
//     record frames (internal/storage/wal's own on-disk frame format) for
//     every record after the sequence point established above, continuing
//     as the leader accepts new writes, until the connection closes.
package replication

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/storage/wal"
)

// pollInterval is a fallback: the stream wakes on e.WALChanged as soon as
// a write lands, and only re-checks the file this often if it somehow
// misses one.
const pollInterval = time.Second

// headerLen is the follower's opening message: seq, has-sum flag, sum.
const headerLen = 8 + 1 + 4

const (
	markerSnapshotFollows byte = 'S'
	markerNoSnapshot      byte = 'N'
)

// Serve accepts replication connections on ln, streaming e to each
// connected follower per the protocol above. It blocks until ln is closed
// or ctx is cancelled, at which point it returns nil.
func Serve(ctx context.Context, ln net.Listener, e *engine.Engine) error {
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}
		go func() {
			_ = serveConn(ctx, conn, e)
		}()
	}
}

func serveConn(ctx context.Context, conn net.Conn, e *engine.Engine) error {
	defer func() { _ = conn.Close() }()

	header := make([]byte, headerLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("replication: read from-seq: %w", err)
	}
	fromSeq := binary.BigEndian.Uint64(header[0:8])
	hasSum := header[8] == 1
	sum := binary.BigEndian.Uint32(header[9:13])

	fromSeq, err := sendSnapshotIfNeeded(conn, e, fromSeq, hasSum, sum)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(conn)
	return wal.Follow(ctx, e.WALPath(), fromSeq, pollInterval, e.WALChanged, func(rec wal.Record) error {
		if _, err := w.Write(wal.EncodeRecord(rec)); err != nil {
			return err
		}
		return w.Flush()
	})
}

// sendSnapshotIfNeeded implements steps 2-3 of the protocol, returning the
// sequence number the caller should now wal.Follow from — either fromSeq
// unchanged, or the snapshot's sequence if one was sent.
func sendSnapshotIfNeeded(conn net.Conn, e *engine.Engine, fromSeq uint64, hasSum bool, sum uint32) (uint64, error) {
	resync, err := needsSnapshot(e, fromSeq, hasSum, sum)
	if err != nil {
		return 0, err
	}
	if !resync {
		if _, err := conn.Write([]byte{markerNoSnapshot}); err != nil {
			return 0, fmt.Errorf("replication: send no-snapshot marker: %w", err)
		}
		return fromSeq, nil
	}

	snapshot, metadata, seq, err := e.OpenSnapshot()
	if err != nil {
		return 0, fmt.Errorf("replication: snapshot for bootstrap: %w", err)
	}
	defer snapshot.Close()
	defer metadata.Close()

	if _, err := conn.Write([]byte{markerSnapshotFollows}); err != nil {
		return 0, fmt.Errorf("replication: send snapshot marker: %w", err)
	}
	seqBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(seqBuf, seq)
	if _, err := conn.Write(seqBuf); err != nil {
		return 0, fmt.Errorf("replication: send snapshot seq: %w", err)
	}
	if err := writeBlob(conn, snapshot); err != nil {
		return 0, fmt.Errorf("replication: send snapshot blob: %w", err)
	}
	if err := writeBlob(conn, metadata); err != nil {
		return 0, fmt.Errorf("replication: send metadata blob: %w", err)
	}
	return seq, nil
}

// needsSnapshot reports whether the follower at fromSeq has to be rebuilt
// from a snapshot rather than streamed the WAL after fromSeq.
func needsSnapshot(e *engine.Engine, fromSeq uint64, hasSum bool, sum uint32) (bool, error) {
	// SnapshotSeq only ever advances, and is 0 for a brand-new Engine, so a
	// fresh follower joining a fresh leader streams without a snapshot.
	if fromSeq < e.SnapshotSeq() || fromSeq > e.LastSeq() {
		return true, nil
	}
	if fromSeq == 0 {
		return false, nil
	}
	leaderSum, found, err := e.RecordSum(fromSeq)
	if err != nil {
		return false, fmt.Errorf("replication: read record %d: %w", fromSeq, err)
	}
	// ponytail: a record either side can't compare (rotated out by a
	// snapshot) counts as diverged, so a follower reconnecting right after
	// a snapshot pays a full resync. Carry the snapshot's last checksum if
	// that gets expensive.
	return !found || !hasSum || leaderSum != sum, nil
}

// writeBlob sends f's length and then its contents.
func writeBlob(w io.Writer, f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	lenBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(lenBuf, uint64(info.Size()))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err = io.CopyN(w, f, info.Size())
	return err
}

// blobReader reads one length-prefixed blob. The length is read on the
// first Read, so two blobs sent back to back can be handed to one consumer
// that reads them in order. It fails with io.ErrUnexpectedEOF if the
// stream ends early, so a dropped connection can't load as a short,
// valid-looking snapshot.
type blobReader struct {
	r       io.Reader
	n       uint64
	started bool
}

func (b *blobReader) Read(p []byte) (int, error) {
	if !b.started {
		lenBuf := make([]byte, 8)
		if _, err := io.ReadFull(b.r, lenBuf); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return 0, err
		}
		b.n, b.started = binary.BigEndian.Uint64(lenBuf), true
	}
	if b.n == 0 {
		return 0, io.EOF
	}
	if uint64(len(p)) > b.n {
		p = p[:b.n]
	}
	n, err := b.r.Read(p)
	b.n -= uint64(n)
	if err == io.EOF && b.n > 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// Follow connects to a leader's replication listener at addr and applies
// every streamed record to e in order, resuming from e's own last applied
// sequence (or bootstrapping from a leader-sent snapshot first, if e is
// too far behind — see the package doc). A reconnect after a network blip
// or leader failover picks up exactly where it left off: never
// re-applying or skipping a record, since Engine.ApplyReplicated's own
// out-of-order check catches any gap that would otherwise be silent.
// Blocks until ctx is cancelled or the connection drops, returning the
// resulting error either way.
func Follow(ctx context.Context, addr string, e *engine.Engine) error {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("replication: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	fromSeq := e.LastSeq()
	header := make([]byte, headerLen)
	binary.BigEndian.PutUint64(header[0:8], fromSeq)
	if fromSeq > 0 {
		sum, found, err := e.RecordSum(fromSeq)
		if err != nil {
			return fmt.Errorf("replication: read own record %d: %w", fromSeq, err)
		}
		if found {
			header[8] = 1
			binary.BigEndian.PutUint32(header[9:13], sum)
		}
	}
	if _, err := conn.Write(header); err != nil {
		return fmt.Errorf("replication: send from-seq: %w", err)
	}

	r := bufio.NewReader(conn)
	marker, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("replication: read marker: %w", err)
	}
	if marker == markerSnapshotFollows {
		seqBuf := make([]byte, 8)
		if _, err := io.ReadFull(r, seqBuf); err != nil {
			return fmt.Errorf("replication: read snapshot seq: %w", err)
		}
		seq := binary.BigEndian.Uint64(seqBuf)
		if err := e.LoadSnapshot(&blobReader{r: r}, &blobReader{r: r}, seq); err != nil {
			return fmt.Errorf("replication: load snapshot: %w", err)
		}
	}

	for {
		rec, err := wal.DecodeRecord(r)
		if err != nil {
			return err
		}
		if err := e.ApplyReplicated(rec); err != nil {
			return fmt.Errorf("replication: apply seq %d: %w", rec.Seq, err)
		}
	}
}
