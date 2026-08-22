package engine

import (
	"sync"
	"testing"

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
	// Quota is process-local config (not yet persisted to disk), so a
	// reopened tenant comes back with a zero/unlimited quota rather than
	// the one it was created with — documented behavior, not a bug: quota
	// policy is expected to be re-applied by whatever's provisioning the
	// server, the same way dim/metric/M are passed on every OpenStore
	// call rather than persisted per tenant.
	if stats.Quota.MaxVectors != 0 {
		t.Fatalf("expected quota to reset to unlimited on reopen (documented behavior), got %+v", stats.Quota)
	}
}
