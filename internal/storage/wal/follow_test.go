package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestFollowStreamsRecordsLive verifies the core replication-support
// property: Follow delivers records that are appended *after* it starts
// watching, not just whatever was on disk at call time — a plain Replay
// call, run once, could never do this.
func TestFollowStreamsRecordsLive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var got []Record
	done := make(chan error, 1)
	go func() {
		done <- Follow(ctx, path, 0, 5*time.Millisecond, nil, func(rec Record) error {
			mu.Lock()
			got = append(got, rec)
			mu.Unlock()
			return nil
		})
	}()

	// Give Follow a moment to observe the first record, then append two
	// more live — this is the part a one-shot Replay couldn't do.
	time.Sleep(30 * time.Millisecond)
	if _, err := w.Append(OpInsert, 2, []float32{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpDelete, 1, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Follow delivered %d records within timeout, want 3", n)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Follow returned %v, want context.Canceled", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got[0].ID != 1 || got[0].Op != OpInsert {
		t.Fatalf("got[0] = %+v, want insert id=1", got[0])
	}
	if got[1].ID != 2 || got[1].Op != OpInsert {
		t.Fatalf("got[1] = %+v, want insert id=2", got[1])
	}
	if got[2].ID != 1 || got[2].Op != OpDelete {
		t.Fatalf("got[2] = %+v, want delete id=1", got[2])
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestFollowSurvivesRotation simulates what happens when Engine.Snapshot
// rotates the WAL file (renaming a fresh empty file into place) out from
// under an attached follower — Follow must notice the path now points to
// a different file and resume from its content, not get stuck reading a
// stale offset in the old (now-unlinked) file it still has open.
func TestFollowSurvivesRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(OpInsert, 1, []float32{1, 2, 3}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var got []Record
	done := make(chan error, 1)
	go func() {
		done <- Follow(ctx, path, 0, 5*time.Millisecond, nil, func(rec Record) error {
			mu.Lock()
			got = append(got, rec)
			mu.Unlock()
			return nil
		})
	}()

	time.Sleep(30 * time.Millisecond) // let Follow observe id=1 and advance past it

	// Simulate Engine.Snapshot's rotation: close the old writer, build the
	// new (empty) WAL at a temp path, and rename it into place — a
	// genuinely new file at the same path, exactly like engine.go now
	// does, deliberately choosing a record shape identical in length to
	// the pre-rotation one (still a 3-float vector, no extra) so this test
	// would still catch a regression to size-based detection, which this
	// exact same-length coincidence used to defeat.
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	tmpPath := path + ".new"
	w2, err := OpenWriter(tmpPath, 1) // as if the snapshot captured up through seq 1
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Append(OpInsert, 2, []float32{4, 5, 6}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Follow delivered %d records after rotation within timeout, want 2 (got so far: %+v)", n, got)
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if got[0].ID != 1 {
		t.Fatalf("got[0] = %+v, want id=1 (pre-rotation)", got[0])
	}
	if got[1].ID != 2 || got[1].Seq != 2 {
		t.Fatalf("got[1] = %+v, want id=2 seq=2 (post-rotation, not re-delivered)", got[1])
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
}

// A replication handshake may open the old WAL just before a snapshot
// rotates the path. FollowOpened must drain that descriptor before switching
// to the new file, or acknowledged records in its unread tail are skipped.
func TestFollowOpenedDrainsOldWALBeforeFollowingRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")
	w, err := OpenWriter(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 2; id++ {
		if _, err := w.Append(OpInsert, id, []float32{float32(id)}); err != nil {
			t.Fatal(err)
		}
	}
	opened, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	newPath := path + ".new"
	w2, err := CreateWriter(newPath, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w2.Append(OpInsert, 3, []float32{3}); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(newPath, path); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan uint64, 3)
	done := make(chan error, 1)
	go func() {
		done <- FollowOpened(ctx, path, opened, 0, time.Millisecond, nil, func(rec Record) error {
			got <- rec.Seq
			if rec.Seq == 3 {
				cancel()
			}
			return nil
		})
	}()
	for want := uint64(1); want <= 3; want++ {
		select {
		case seq := <-got:
			if seq != want {
				t.Fatalf("sequence = %d, want %d", seq, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for sequence %d", want)
		}
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("FollowOpened returned %v, want context.Canceled", err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
}
