package engine

import (
	"testing"

	"github.com/Rakshit-gen/nucladb/internal/storage/wal"
)

// TestApplyReplicatedMirrorsInsert verifies a replica follower applying a
// record captured from a leader's real Insert ends up with the same
// searchable state as the leader — the actual property replication needs,
// not just "AppendRecord doesn't error."
func TestApplyReplicatedMirrorsInsert(t *testing.T) {
	leader, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.Insert(1, []float32{1, 0, 0, 0, 0, 0, 0, 0}, map[string]string{"team": "search"}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Insert(2, []float32{0, 1, 0, 0, 0, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := leader.Delete(2); err != nil {
		t.Fatal(err)
	}

	// Recover exactly what a replication follower would receive: the raw
	// WAL records the leader durably wrote, in order.
	var records []wal.Record
	if _, err := wal.Replay(leader.WALPath(), func(rec wal.Record) error {
		records = append(records, rec)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Fatalf("got %d WAL records, want 3", len(records))
	}

	follower, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	for _, rec := range records {
		if err := follower.ApplyReplicated(rec); err != nil {
			t.Fatalf("ApplyReplicated(seq=%d): %v", rec.Seq, err)
		}
	}

	if follower.LastSeq() != leader.LastSeq() {
		t.Fatalf("follower.LastSeq()=%d, leader.LastSeq()=%d", follower.LastSeq(), leader.LastSeq())
	}
	if follower.Len() != leader.Len() {
		t.Fatalf("follower.Len()=%d, leader.Len()=%d", follower.Len(), leader.Len())
	}

	res, err := follower.Search([]float32{1, 0, 0, 0, 0, 0, 0, 0}, 5, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].ID != 1 || res[0].Metadata["team"] != "search" {
		t.Fatalf("follower.Search() = %+v, want a single match id=1 team=search", res)
	}
}

// TestApplyReplicatedRejectsGap verifies the out-of-order guard actually
// fires — a follower must never silently accept a record that skips a
// sequence number, since that would mean a write it never applied.
func TestApplyReplicatedRejectsGap(t *testing.T) {
	follower, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	rec := wal.Record{Seq: 5, Op: wal.OpInsert, ID: 1, Vector: []float32{1, 0, 0, 0, 0, 0, 0, 0}}
	if err := follower.ApplyReplicated(rec); err == nil {
		t.Fatal("expected ApplyReplicated to reject seq=5 on a fresh (seq=0) follower, got nil error")
	}
}

// TestSnapshotBytesLoadSnapshotRoundTrip verifies the bootstrap path a
// late-joining replica uses when it's too far behind for WAL streaming
// alone: pull a fresh snapshot from the leader, load it, and continue
// applying replicated writes from that point.
func TestSnapshotBytesLoadSnapshotRoundTrip(t *testing.T) {
	leader, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer leader.Close()

	if err := leader.Insert(1, []float32{1, 0, 0, 0, 0, 0, 0, 0}, map[string]string{"team": "search"}); err != nil {
		t.Fatal(err)
	}
	if err := leader.Insert(2, []float32{0, 1, 0, 0, 0, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	snapshotBytes, metadataBytes, seq, err := leader.SnapshotBytes()
	if err != nil {
		t.Fatal(err)
	}
	if seq != leader.LastSeq() {
		t.Fatalf("SnapshotBytes seq=%d, leader.LastSeq()=%d", seq, leader.LastSeq())
	}

	follower, err := Open(t.TempDir(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer follower.Close()

	if err := follower.LoadSnapshot(snapshotBytes, metadataBytes, seq); err != nil {
		t.Fatal(err)
	}
	if follower.LastSeq() != seq || follower.SnapshotSeq() != seq {
		t.Fatalf("after LoadSnapshot: LastSeq()=%d SnapshotSeq()=%d, want both %d", follower.LastSeq(), follower.SnapshotSeq(), seq)
	}
	if follower.Len() != leader.Len() {
		t.Fatalf("follower.Len()=%d, leader.Len()=%d", follower.Len(), leader.Len())
	}

	// A write made on the leader *after* the snapshot was taken should
	// apply cleanly on top of the loaded state, at the next sequence.
	if err := leader.Insert(3, []float32{0, 0, 1, 0, 0, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	var post []wal.Record
	if _, err := wal.Replay(leader.WALPath(), func(rec wal.Record) error {
		if rec.Seq > seq {
			post = append(post, rec)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(post) != 1 {
		t.Fatalf("got %d post-snapshot records, want 1", len(post))
	}
	if err := follower.ApplyReplicated(post[0]); err != nil {
		t.Fatalf("ApplyReplicated after LoadSnapshot: %v", err)
	}
	if follower.Len() != leader.Len() {
		t.Fatalf("after post-snapshot replication: follower.Len()=%d, leader.Len()=%d", follower.Len(), leader.Len())
	}
}
