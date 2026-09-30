package wal

import (
	"errors"
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

func TestReplayStopsAtHugeLengthPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{0xff, 0xff, 0xff, 0xff, 0, 0, 0, 0}) // 4 GiB length, no payload
	f.Close()

	n := 0
	last, err := Replay(path, func(Record) error { n++; return nil })
	if err != nil || n != 1 || last != 1 {
		t.Fatalf("n=%d last=%d err=%v, want 1 record and clean stop", n, last, err)
	}
}

func TestWriterTruncatesTornTailSoLaterAppendsSurvive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := OpenWriter(path, 0)
	w.Append(OpInsert, 1, []float32{1})
	w.Close()

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{9, 0, 0, 0, 1, 2}) // torn frame
	f.Close()

	w, err := OpenWriter(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(OpInsert, 2, []float32{2})
	w.Close()

	n := 0
	last, err := Replay(path, func(Record) error { n++; return nil })
	if err != nil || n != 2 || last != 2 {
		t.Fatalf("n=%d last=%d err=%v, want both records visible", n, last, err)
	}
}

func TestRecoverOffsetLetsOpenWriterAtSkipTheTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := OpenWriter(path, 0)
	w.Append(OpInsert, 1, []float32{1})
	w.Append(OpInsert, 2, []float32{2})
	w.Close()
	info, _ := os.Stat(path)

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.Write([]byte{9, 0, 0, 0, 1, 2}) // torn frame
	f.Close()

	valid, last, err := Recover(path, func(Record) error { return nil })
	if err != nil || last != 2 || valid != info.Size() {
		t.Fatalf("valid=%d last=%d err=%v, want valid=%d last=2", valid, last, err, info.Size())
	}

	w, err = OpenWriterAt(path, last, valid)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(OpInsert, 3, []float32{3})
	w.Close()

	n := 0
	last, err = Replay(path, func(Record) error { n++; return nil })
	if err != nil || n != 3 || last != 3 {
		t.Fatalf("n=%d last=%d err=%v, want all three records", n, last, err)
	}
}

// A bad record followed by good ones is not a torn tail. Recovery must
// refuse to open rather than truncate away the acknowledged writes after it.
func TestCorruptMidLogRecordFailsInsteadOfTruncating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 3; id++ {
		if _, err := w.Append(OpInsert, id, []float32{1, 2, 3}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	frame := len(full) / 3
	full[frame+frame/2] ^= 0xFF // inside record 2's payload
	if err := os.WriteFile(path, full, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := Replay(path, func(Record) error { return nil }); !errors.Is(err, ErrCorruptMidLog) {
		t.Fatalf("Replay err = %v, want ErrCorruptMidLog", err)
	}
	if _, err := OpenWriter(path, 0); !errors.Is(err, ErrCorruptMidLog) {
		t.Fatalf("OpenWriter err = %v, want ErrCorruptMidLog", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(full) {
		t.Fatalf("file was truncated from %d to %d bytes", len(full), len(after))
	}
}
