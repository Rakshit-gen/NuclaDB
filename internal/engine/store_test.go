package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

func testStoreConfig() hnsw.Config {
	return hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1}
}

func TestStoreDefaultTenantAutoProvisioned(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// The default tenant must accept writes with no explicit CreateTenant
	// call and no quota, unlike every other tenant.
	if err := s.Insert("", 1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	stats, err := s.Stats(DefaultTenant)
	if err != nil {
		t.Fatal(err)
	}
	if stats.VectorCount != 1 {
		t.Fatalf("VectorCount = %d, want 1", stats.VectorCount)
	}
}

func TestStoreTenantIsolation(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.CreateTenant("acme", Quota{}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTenant("globex", Quota{}); err != nil {
		t.Fatal(err)
	}

	// Same id, different vectors, different tenants: must not collide or
	// leak into each other's search results.
	if err := s.Insert("acme", 1, []float32{1, 0, 0, 0}, map[string]string{"tenant": "acme"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("globex", 1, []float32{0, 1, 0, 0}, map[string]string{"tenant": "globex"}); err != nil {
		t.Fatal(err)
	}

	res, err := s.Search("acme", []float32{1, 0, 0, 0}, 5, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Metadata["tenant"] != "acme" {
		t.Fatalf("acme search leaked cross-tenant data: %+v", res)
	}

	res, err = s.Search("globex", []float32{1, 0, 0, 0}, 5, 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Metadata["tenant"] != "globex" {
		t.Fatalf("globex search leaked cross-tenant data: %+v", res)
	}

	statsA, _ := s.Stats("acme")
	statsG, _ := s.Stats("globex")
	if statsA.VectorCount != 1 || statsG.VectorCount != 1 {
		t.Fatalf("unexpected per-tenant counts: acme=%d globex=%d", statsA.VectorCount, statsG.VectorCount)
	}

	if err := s.Delete("acme", 1); err != nil {
		t.Fatal(err)
	}
	statsA, _ = s.Stats("acme")
	statsG, _ = s.Stats("globex")
	if statsA.VectorCount != 0 {
		t.Fatalf("delete in acme should not affect acme's own remaining count wrongly: got %d", statsA.VectorCount)
	}
	if statsG.VectorCount != 1 {
		t.Fatalf("delete in acme leaked into globex: globex count = %d, want 1", statsG.VectorCount)
	}
}

func TestStoreUnknownTenantRejected(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Insert("does-not-exist", 1, []float32{1, 0, 0, 0}, nil); err != ErrTenantNotFound {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

func TestStoreDuplicateTenantRejected(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.CreateTenant("acme", Quota{}); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTenant("acme", Quota{}); err != ErrTenantExists {
		t.Fatalf("expected ErrTenantExists, got %v", err)
	}
}

func TestStoreInvalidTenantIDRejected(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, bad := range []string{"../escape", "a/b", ".", ".."} {
		if err := s.CreateTenant(bad, Quota{}); err != ErrInvalidTenantID {
			t.Fatalf("tenant id %q: expected ErrInvalidTenantID, got %v", bad, err)
		}
	}
}

func TestStoreStorageQuotaEnforced(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.CreateTenant("tiny", Quota{MaxVectors: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("tiny", 1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("tiny", 2, []float32{0, 1, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("tiny", 3, []float32{0, 0, 1, 0}, nil); err != ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded on the 3rd insert into a MaxVectors=2 tenant, got %v", err)
	}

	// Deleting frees quota headroom back up.
	if err := s.Delete("tiny", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("tiny", 3, []float32{0, 0, 1, 0}, nil); err != nil {
		t.Fatalf("insert after delete should succeed within quota, got %v", err)
	}
}

func TestStoreRateLimitEnforced(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// A tight rate limit (burst 1, refilling extremely slowly) so the
	// second call in the same instant is guaranteed to be rejected
	// without the test needing real wall-clock waits.
	if err := s.CreateTenant("ratelimited", Quota{MaxQPS: 0.0001}); err != nil {
		t.Fatal(err)
	}

	err = s.Insert("ratelimited", 1, []float32{1, 0, 0, 0}, nil)
	if err != nil {
		t.Fatalf("first request within burst should succeed, got %v", err)
	}
	err = s.Insert("ratelimited", 2, []float32{0, 1, 0, 0}, nil)
	if err != ErrRateLimited {
		t.Fatalf("expected ErrRateLimited on the request past burst, got %v", err)
	}
}

func TestStoreUnlimitedTenantNeverRateLimited(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.CreateTenant("unlimited", Quota{}); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 50; i++ {
		if err := s.Insert("unlimited", i, []float32{float32(i), 0, 0, 0}, nil); err != nil {
			t.Fatalf("insert %d: unexpected error for an unlimited-quota tenant: %v", i, err)
		}
	}
}

// TestStoreConcurrentMultiTenant exercises many tenants under concurrent
// load simultaneously, verifying with -race that isolation holds (no data
// races between tenants' independent Engines) and every tenant ends up
// with exactly the vectors written to it.
func TestStoreConcurrentMultiTenant(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const numTenants = 8
	const perTenant = 50
	tenantIDs := make([]string, numTenants)
	for i := range tenantIDs {
		tenantIDs[i] = "tenant-" + string(rune('a'+i))
		if err := s.CreateTenant(tenantIDs[i], Quota{}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for _, tid := range tenantIDs {
		wg.Add(1)
		go func(tid string) {
			defer wg.Done()
			for i := uint64(0); i < perTenant; i++ {
				v := []float32{float32(i), float32(i), float32(i), float32(i)}
				if err := s.Insert(tid, i, v, nil); err != nil {
					t.Errorf("tenant %s insert %d: %v", tid, i, err)
				}
				if _, err := s.Search(tid, v, 3, 10, nil); err != nil {
					t.Errorf("tenant %s search: %v", tid, err)
				}
			}
		}(tid)
	}
	wg.Wait()

	for _, tid := range tenantIDs {
		stats, err := s.Stats(tid)
		if err != nil {
			t.Fatal(err)
		}
		if stats.VectorCount != perTenant {
			t.Fatalf("tenant %s: VectorCount = %d, want %d", tid, stats.VectorCount, perTenant)
		}
	}
}

func TestStoreReopenRediscoversTenants(t *testing.T) {
	dir := t.TempDir()
	cfg := testStoreConfig()

	s, err := OpenStore(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CreateTenant("acme", Quota{MaxVectors: 100}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("acme", 1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := OpenStore(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	stats, err := s2.Stats("acme")
	if err != nil {
		t.Fatal(err)
	}
	if stats.VectorCount != 1 {
		t.Fatalf("VectorCount after reopen = %d, want 1", stats.VectorCount)
	}
	if stats.Quota.MaxVectors != 100 {
		t.Fatalf("quota after reopen = %+v, want MaxVectors 100", stats.Quota)
	}
}

// Several goroutines hitting tenants that haven't been opened yet: the lazy
// open must not race with the fast-path check (run under -race).
func TestStoreConcurrentFirstAccess(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for n := 0; n < 30; n++ {
		id := fmt.Sprintf("lazy-%d", n)
		if err := s.CreateTenant(id, Quota{}); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := s.Stats(id); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
	}
}

func TestStoreSnapshotContinuesPastAFailingTenant(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root, testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := s.CreateTenant(id, Quota{}); err != nil {
			t.Fatal(err)
		}
		if err := s.Insert(id, 1, []float32{1, 0, 0, 0}, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Tenant a can't write its snapshot files.
	badDir := filepath.Join(root, "a")
	if err := os.Chmod(badDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(badDir, 0o755)

	if err := s.Snapshot(); err == nil {
		t.Fatal("Snapshot should report tenant a's failure")
	}
	if _, err := os.Stat(filepath.Join(root, "b", snapshotFile)); err != nil {
		t.Fatalf("tenant b was not snapshotted: %v", err)
	}
}

func TestStoreQuotaIgnoresUpsertsAndHoldsUnderConcurrency(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateTenant("q", Quota{MaxVectors: 10}); err != nil {
		t.Fatal(err)
	}

	// 50 writers race for 10 slots.
	var wg sync.WaitGroup
	for i := uint64(0); i < 50; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()
			_ = s.Insert("q", id, []float32{float32(id), 0, 0, 0}, nil)
		}(i)
	}
	wg.Wait()
	stats, _ := s.Stats("q")
	if stats.VectorCount != 10 {
		t.Fatalf("VectorCount = %d, want exactly the quota of 10", stats.VectorCount)
	}

	// At the quota, overwriting stored ids is still allowed.
	var stored []InsertItem
	for i := uint64(0); i < 50 && len(stored) < 10; i++ {
		if s.tenants["q"].engine.CountNew([]uint64{i}) == 0 {
			stored = append(stored, InsertItem{ID: i, Vector: []float32{0, 0, 0, 1}})
		}
	}
	if err := s.Insert("q", stored[0].ID, []float32{0, 0, 1, 0}, nil); err != nil {
		t.Fatalf("upsert of a stored id at the quota: %v", err)
	}
	if err := s.InsertBatch("q", stored); err != nil {
		t.Fatalf("batch upsert of stored ids at the quota: %v", err)
	}
	if err := s.Insert("q", 1000, []float32{1, 1, 1, 1}, nil); err != ErrQuotaExceeded {
		t.Fatalf("new id past the quota: got %v, want ErrQuotaExceeded", err)
	}
}

func TestStoreRateLimitCountsBatchVectors(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Burst of 5 tokens, refilled slowly enough not to matter here.
	if err := s.CreateTenant("r", Quota{MaxQPS: 5}); err != nil {
		t.Fatal(err)
	}
	batch := func(n int) []InsertItem {
		items := make([]InsertItem, n)
		for i := range items {
			items[i] = InsertItem{ID: uint64(i), Vector: []float32{float32(i), 0, 0, 0}}
		}
		return items
	}
	if err := s.InsertBatch("r", batch(4)); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertBatch("r", batch(2)); err != ErrRateLimited {
		t.Fatalf("a 2-vector batch with 1 token left: got %v, want ErrRateLimited", err)
	}
}

func TestStoreInsertBatchesRefusesWholeRequest(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateTenant("small", Quota{MaxVectors: 1}); err != nil {
		t.Fatal(err)
	}
	ok := TenantBatch{TenantID: DefaultTenant, Items: []InsertItem{{ID: 1, Vector: []float32{1, 0, 0, 0}}}}
	cases := map[string]TenantBatch{
		"unknown tenant": {TenantID: "nope", Items: ok.Items},
		"bad dimension":  {TenantID: "small", Items: []InsertItem{{ID: 1, Vector: []float32{1}}}},
		"over quota":     {TenantID: "small", Items: []InsertItem{{ID: 1, Vector: []float32{1, 0, 0, 0}}, {ID: 2, Vector: []float32{0, 1, 0, 0}}}},
	}
	for name, bad := range cases {
		if err := s.InsertBatches([]TenantBatch{ok, bad}); err == nil {
			t.Fatalf("%s: expected an error", name)
		}
		if st, _ := s.Stats(DefaultTenant); st.VectorCount != 0 {
			t.Fatalf("%s: default tenant was written before the request was refused", name)
		}
	}
}

func TestStoreCloseIdleReopensOnNextUse(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateTenant("idle", Quota{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("idle", 1, []float32{1, 0, 0, 0}, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}

	if n, err := s.CloseIdle(time.Hour); err != nil || n != 0 {
		t.Fatalf("recently used tenant closed: n=%d err=%v", n, err)
	}
	n, err := s.CloseIdle(0)
	if err != nil || n < 1 {
		t.Fatalf("CloseIdle(0): n=%d err=%v", n, err)
	}
	if s.tenants["idle"].engine != nil {
		t.Fatal("engine still open")
	}
	if got := s.AllStats()["idle"].VectorCount; got != 1 {
		t.Fatalf("AllStats for a closed tenant = %d, want 1", got)
	}

	res, err := s.Search("idle", []float32{1, 0, 0, 0}, 1, 0, nil)
	if err != nil || len(res) != 1 || res[0].Metadata["k"] != "v" {
		t.Fatalf("search after reopen: %v, %v", res, err)
	}
}

// Requests racing CloseIdle must never see a closed engine.
func TestStoreCloseIdleUnderLoad(t *testing.T) {
	s, err := OpenStore(t.TempDir(), testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				id := uint64(w*50 + i)
				if err := s.Insert("", id, []float32{float32(id), 1, 0, 0}, nil); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = s.CloseIdle(0)
			}
		}
	}()
	wg.Wait()
	close(stop)
	if st, _ := s.Stats(""); st.VectorCount != 200 {
		t.Fatalf("VectorCount = %d, want 200", st.VectorCount)
	}
}

func TestStoreTenantAdmin(t *testing.T) {
	root := t.TempDir()
	s, err := OpenStore(root, testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.CreateTenant("gone", Quota{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("gone", 1, []float32{1, 0, 0, 0}, nil); err != nil {
		t.Fatal(err)
	}

	// SetQuota applies to the next request.
	if err := s.SetQuota("gone", Quota{MaxVectors: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.Insert("gone", 2, []float32{0, 1, 0, 0}, nil); err != ErrQuotaExceeded {
		t.Fatalf("insert past the new quota: got %v", err)
	}
	if st, _ := s.Stats("gone"); st.Quota.MaxVectors != 1 {
		t.Fatalf("Stats quota = %+v", st.Quota)
	}

	if err := s.DeleteTenant(DefaultTenant); err != ErrDefaultTenant {
		t.Fatalf("deleting the default tenant: got %v", err)
	}
	if err := s.DeleteTenant("gone"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "gone")); !os.IsNotExist(err) {
		t.Fatalf("tenant dir still there: %v", err)
	}
	if err := s.Insert("gone", 1, []float32{1, 0, 0, 0}, nil); err != ErrTenantNotFound {
		t.Fatalf("insert into a deleted tenant: got %v", err)
	}
	if _, ok := s.AllStats()["gone"]; ok {
		t.Fatal("deleted tenant still listed")
	}
	// The id can be reused as a fresh, empty tenant.
	if err := s.CreateTenant("gone", Quota{}); err != nil {
		t.Fatal(err)
	}
	if st, _ := s.Stats("gone"); st.VectorCount != 0 {
		t.Fatalf("recreated tenant has %d vectors", st.VectorCount)
	}
}

func TestStoreIgnoresStrayDirectories(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "lost+found"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := OpenStore(root, testStoreConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, ok := s.AllStats()["lost+found"]; ok {
		t.Fatal("a directory with no tenant files was loaded as a tenant")
	}
}
