package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 2, []float32{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpDelete, 1, nil); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	lastSeq, err := Replay(path, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 3 {
		t.Fatalf("lastSeq = %d, want 3", lastSeq)
	}
	if len(got) != 3 {
		t.Fatalf("replayed %d records, want 3", len(got))
	}
	if got[0].Op != OpInsert || got[0].ID != 1 || len(got[0].Vector) != 3 {
		t.Fatalf("unexpected record 0: %+v", got[0])
	}
	if got[2].Op != OpDelete || got[2].ID != 1 || got[2].Vector != nil {
		t.Fatalf("unexpected record 2: %+v", got[2])
	}
}

func TestReplayMissingFile(t *testing.T) {
	lastSeq, err := Replay(filepath.Join(t.TempDir(), "does-not-exist.log"), func(Record) error {
		t.Fatal("fn should not be called for a missing log")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if lastSeq != 0 {
		t.Fatalf("lastSeq = %d, want 0", lastSeq)
	}
}

func TestSequenceNumbersResumeFromStartSeq(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := w.Append(OpInsert, 1, []float32{1})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 101 {
		t.Fatalf("seq = %d, want 101", seq)
	}
	_ = w.Close()
}

// TestCrashRecoveryTruncatedTail simulates a process killed mid-write: the
// final record is chopped off partway through, leaving a truncated length
// prefix or a payload shorter than its declared length. Replay must
// recover every complete record before the tear and silently drop the
// partial one, rather than erroring or corrupting earlier data.
func TestCrashRecoveryTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 2, []float32{5, 6, 7, 8}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 3, []float32{9, 10, 11, 12}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Chop off the last 10 bytes, guaranteed to land inside record 3's
	// payload since each record here is well over 10 bytes.
	truncated := full[:len(full)-10]
	if err := os.WriteFile(path, truncated, 0o644); err != nil {
		t.Fatal(err)
	}

	var got []Record
	lastSeq, err := Replay(path, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay should tolerate a truncated tail, got err: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("recovered %d records, want 2 (record 3 should be dropped)", len(got))
	}
	if lastSeq != 2 {
		t.Fatalf("lastSeq = %d, want 2", lastSeq)
	}
	if got[0].ID != 1 || got[1].ID != 2 {
		t.Fatalf("unexpected recovered ids: %+v", got)
	}
}

// TestCrashRecoveryCorruptChecksum simulates bit-rot / a torn write that
// left a full-length record whose bytes don't match its CRC. This must
// also be treated as end-of-log, not a fatal error — a torn write can land
// exactly on a record boundary and still fail its checksum.
func TestCrashRecoveryCorruptChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 2, []float32{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the last record's payload (after the first
	// record's header+payload) without changing the file's length, so the
	// length prefix still parses but the CRC won't match.
	full[len(full)-1] ^= 0xFF
	if err := os.WriteFile(path, full, 0o644); err != nil {
		t.Fatal(err)
	}

	var got []Record
	_, err = Replay(path, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Replay should tolerate a corrupt tail record, got err: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("recovered %d records, want 1 (corrupt record 2 should be dropped)", len(got))
	}
}
