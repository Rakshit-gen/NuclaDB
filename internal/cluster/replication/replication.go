// Package replication streams a shard leader's WAL to follower replicas
// over a plain TCP connection, keeping each replica's Engine durably in
// sync without going through Raft — see internal/cluster/raft's own
// package doc for why Raft here governs cluster *metadata*, not the
// high-throughput vector write path.
//
// Wire protocol, deliberately minimal:
//
//  1. The follower connects and sends its current sequence number (8
//     bytes, big-endian) — 0 for a brand-new replica.
//  2. The leader replies with one marker byte: 'S' if the follower is too
//     far behind for WAL streaming alone (its sequence predates the
//     oldest record still on disk — see wal.Follow's doc comment on that
//     boundary) and a snapshot transfer follows, or 'N' if the follower
//     can be served directly from the live WAL.
//  3. If 'S': an 8-byte sequence number the snapshot reflects, then the
//     snapshot and metadata blobs, each a 4-byte big-endian length
//     followed by that many bytes.
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
	"time"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/storage/wal"
)

const pollInterval = 20 * time.Millisecond

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

	header := make([]byte, 8)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("replication: read from-seq: %w", err)
	}
	fromSeq := binary.BigEndian.Uint64(header)

	fromSeq, err := sendSnapshotIfNeeded(conn, e, fromSeq)
	if err != nil {
		return err
	}

	w := bufio.NewWriter(conn)
	return wal.Follow(ctx, e.WALPath(), fromSeq, pollInterval, func(rec wal.Record) error {
		if _, err := w.Write(wal.EncodeRecord(rec)); err != nil {
			return err
		}
		return w.Flush()
	})
}

// sendSnapshotIfNeeded implements steps 2-3 of the protocol, returning the
// sequence number the caller should now wal.Follow from — either fromSeq
// unchanged, or the snapshot's sequence if one was sent.
func sendSnapshotIfNeeded(conn net.Conn, e *engine.Engine, fromSeq uint64) (uint64, error) {
	if fromSeq >= e.SnapshotSeq() {
		// The live WAL alone covers everything from fromSeq forward — no
		// snapshot needed. (SnapshotSeq only ever advances, and equals 0
		// for a brand-new Engine with fromSeq 0 too, so a fresh follower
		// joining a fresh leader correctly takes this branch.)
		if _, err := conn.Write([]byte{markerNoSnapshot}); err != nil {
			return 0, fmt.Errorf("replication: send no-snapshot marker: %w", err)
		}
		return fromSeq, nil
	}

	snapshot, metadata, seq, err := e.SnapshotBytes()
	if err != nil {
		return 0, fmt.Errorf("replication: snapshot for bootstrap: %w", err)
	}

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

func writeBlob(w io.Writer, data []byte) error {
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
	if _, err := w.Write(lenBuf); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readBlob(r io.Reader) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	data := make([]byte, binary.BigEndian.Uint32(lenBuf))
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
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
	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, fromSeq)
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
		snapshot, err := readBlob(r)
		if err != nil {
			return fmt.Errorf("replication: read snapshot blob: %w", err)
		}
		metadata, err := readBlob(r)
		if err != nil {
			return fmt.Errorf("replication: read metadata blob: %w", err)
		}
		if err := e.LoadSnapshot(snapshot, metadata, seq); err != nil {
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
