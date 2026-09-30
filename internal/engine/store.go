package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
)

// DefaultTenant is the reserved tenant id used when a caller supplies an
// empty tenant_id, so existing single-tenant callers need no changes.
const DefaultTenant = "default"

var (
	ErrTenantExists    = errors.New("engine: tenant already exists")
	ErrTenantNotFound  = errors.New("engine: tenant not found")
	ErrQuotaExceeded   = errors.New("engine: tenant storage quota exceeded")
	ErrRateLimited     = errors.New("engine: tenant request rate limit exceeded")
	ErrInvalidTenantID = errors.New("engine: tenant_id must not contain path separators")
)

// Quota bounds one tenant's resource usage. A zero value for either field
// means unlimited — this is what the auto-provisioned DefaultTenant gets,
// so single-tenant usage is never quota-limited.
type Quota struct {
	MaxVectors int64   // 0 = unlimited
	MaxQPS     float64 // 0 = unlimited
}

type tenant struct {
	engine  *Engine
	quota   Quota
	limiter *rate.Limiter // nil when MaxQPS == 0 (unlimited)
	// reserved counts new vectors whose inserts passed the quota check but
	// haven't finished yet, so concurrent inserts can't all squeeze past
	// the same headroom.
	reserved atomic.Int64
}

// Store manages multiple tenant-isolated Engines under one root directory:
// each tenant gets its own subdirectory, and therefore its own graph, WAL,
// and snapshot files, completely isolated from every other tenant's data.
// A per-tenant token-bucket rate limiter and vector-count quota are
// enforced before any mutating or search call reaches that tenant's
// Engine.
type Store struct {
	mu      sync.RWMutex
	rootDir string
	cfg     hnsw.Config
	tenants map[string]*tenant
}

// OpenStore discovers existing tenant subdirectories under rootDir (each
// one reopened lazily, on first use) and returns a Store ready to serve
// them, plus accept newly created tenants. cfg is applied to every
// tenant's graph (dimension, metric, HNSW parameters are store-wide, not
// per-tenant, since the same client integration is expected to speak one
// vector shape across all its tenants).
func OpenStore(rootDir string, cfg hnsw.Config) (*Store, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		return nil, err
	}

	s := &Store{rootDir: rootDir, cfg: cfg, tenants: make(map[string]*tenant)}
	foundDefault := false
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		quota, err := loadQuota(filepath.Join(rootDir, e.Name()))
		if err != nil {
			return nil, err
		}
		s.tenants[e.Name()] = newTenant(quota)
		if e.Name() == DefaultTenant {
			foundDefault = true
		}
	}
	if !foundDefault {
		if err := s.createTenantLocked(DefaultTenant, Quota{}); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func validTenantID(id string) bool {
	return id != "" && id != "." && id != ".." && filepath.Base(id) == id
}

// CreateTenant provisions a new, empty tenant with the given quota. It is
// an error to create a tenant id that already exists — use quota (0,0)
// for "unlimited" if that's genuinely intended, rather than relying on an
// implicit default.
func (s *Store) CreateTenant(tenantID string, quota Quota) error {
	if !validTenantID(tenantID) {
		return ErrInvalidTenantID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tenants[tenantID]; exists {
		return ErrTenantExists
	}
	return s.createTenantLocked(tenantID, quota)
}

// createTenantLocked registers tenantID with quota but does not open its
// Engine — that happens lazily on first access via getTenant, so
// provisioning many tenants up front doesn't hold many open WAL file
// handles for ones that see no traffic.
func (s *Store) createTenantLocked(tenantID string, quota Quota) error {
	dir := filepath.Join(s.rootDir, tenantID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := saveQuota(dir, quota); err != nil {
		return err
	}
	s.tenants[tenantID] = newTenant(quota)
	return nil
}

// allow takes n tokens from the tenant's rate limiter, so a batch of n
// vectors costs the same as n single inserts. A batch bigger than the whole
// bucket takes the whole bucket instead of being refused forever.
func (t *tenant) allow(n int) bool {
	if t.limiter == nil {
		return true
	}
	return t.limiter.AllowN(time.Now(), min(max(n, 1), t.limiter.Burst()))
}

func newTenant(quota Quota) *tenant {
	t := &tenant{quota: quota}
	if quota.MaxQPS > 0 {
		t.limiter = rate.NewLimiter(rate.Limit(quota.MaxQPS), burstFor(quota.MaxQPS))
	}
	return t
}

// quota.json sits next to the tenant's WAL and snapshot so a restart brings
// the tenant back with the limits it was created with. A missing file (a
// tenant created before quotas were saved) means unlimited.
const quotaFile = "quota.json"

func loadQuota(dir string) (Quota, error) {
	var q Quota
	b, err := os.ReadFile(filepath.Join(dir, quotaFile))
	if errors.Is(err, os.ErrNotExist) {
		return q, nil
	}
	if err != nil {
		return q, err
	}
	err = json.Unmarshal(b, &q)
	return q, err
}

func saveQuota(dir string, q Quota) error {
	b, err := json.Marshal(q)
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, quotaFile), b)
}

func burstFor(qps float64) int {
	b := int(qps)
	if b < 1 {
		b = 1
	}
	return b
}

// getTenant returns the tenant's entry, opening its Engine on first
// access.
func (s *Store) getTenant(tenantID string) (*tenant, error) {
	if tenantID == "" {
		tenantID = DefaultTenant
	}

	// t.engine is written under s.mu by the lazy open below, so the fast
	// path has to read it under the lock too.
	s.mu.RLock()
	t, ok := s.tenants[tenantID]
	opened := ok && t.engine != nil
	s.mu.RUnlock()
	if !ok {
		return nil, ErrTenantNotFound
	}
	if opened {
		return t, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	// Re-check under the write lock: another goroutine may have opened it
	// while we waited.
	t = s.tenants[tenantID]
	if t.engine != nil {
		return t, nil
	}
	eng, err := Open(filepath.Join(s.rootDir, tenantID), s.cfg)
	if err != nil {
		return nil, err
	}
	t.engine = eng
	return t, nil
}

// Engine returns tenantID's underlying Engine, opening it on first access
// if needed — an escape hatch for callers that need a lower-level API
// Store doesn't expose itself, such as wiring per-tenant WAL replication.
func (s *Store) Engine(tenantID string) (*Engine, error) {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return nil, err
	}
	return t.engine, nil
}

// reserve checks the quota for ids and holds room for the ones that are new.
// Upserting an id that already exists doesn't use any. The caller must call
// the returned release once the insert is done.
func (s *Store) reserve(t *tenant, ids []uint64) (release func(), err error) {
	if t.quota.MaxVectors <= 0 {
		return func() {}, nil
	}
	n := int64(t.engine.CountNew(ids))
	if int64(t.engine.Len())+t.reserved.Add(n) > t.quota.MaxVectors {
		t.reserved.Add(-n)
		return nil, ErrQuotaExceeded
	}
	return func() { t.reserved.Add(-n) }, nil
}

// Insert durably upserts id -> (vector, metadata) within tenantID.
func (s *Store) Insert(tenantID string, id uint64, vector []float32, metadata map[string]string) error {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return err
	}
	if !t.allow(1) {
		return ErrRateLimited
	}
	release, err := s.reserve(t, []uint64{id})
	if err != nil {
		return err
	}
	defer release()
	return t.engine.Insert(id, vector, metadata)
}

// InsertBatch durably upserts items within tenantID with one shared fsync.
// Used by the BatchUpsert RPC for bulk loads.
func (s *Store) InsertBatch(tenantID string, items []InsertItem) error {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return err
	}
	if !t.allow(len(items)) {
		return ErrRateLimited
	}
	ids := make([]uint64, len(items))
	for i, it := range items {
		ids[i] = it.ID
	}
	release, err := s.reserve(t, ids)
	if err != nil {
		return err
	}
	defer release()
	return t.engine.InsertBatch(items)
}

// Delete durably removes id within tenantID.
func (s *Store) Delete(tenantID string, id uint64) error {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return err
	}
	if !t.allow(1) {
		return ErrRateLimited
	}
	return t.engine.Delete(id)
}

// Search finds the nearest neighbors of query within tenantID.
func (s *Store) Search(tenantID string, query []float32, topK, ef int, filters map[string]string) ([]Result, error) {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return nil, err
	}
	if !t.allow(1) {
		return nil, ErrRateLimited
	}
	return t.engine.Search(query, topK, ef, filters)
}

// TenantStats reports one tenant's current usage against its quota.
type TenantStats struct {
	VectorCount int
	Quota       Quota
}

// Stats returns tenantID's current usage.
func (s *Store) Stats(tenantID string) (TenantStats, error) {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return TenantStats{}, err
	}
	return TenantStats{VectorCount: t.engine.Len(), Quota: t.quota}, nil
}

// AllStats reports every known tenant's current usage, without forcing an
// unopened tenant's Engine open just to answer the question — a tenant
// that's never been written to or read from reports VectorCount 0
// directly, rather than paying the cost of opening its (necessarily
// empty) WAL and snapshot files. Intended for a periodic metrics sweep,
// where touching every tenant on every tick would defeat the point of
// lazy-opening them in the first place.
func (s *Store) AllStats() map[string]TenantStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]TenantStats, len(s.tenants))
	for id, t := range s.tenants {
		count := 0
		if t.engine != nil {
			count = t.engine.Len()
		}
		out[id] = TenantStats{VectorCount: count, Quota: t.quota}
	}
	return out
}

// Snapshot snapshots every currently-open tenant engine. One tenant failing
// doesn't stop the others; every failure is returned, joined.
func (s *Store) Snapshot() error {
	s.mu.RLock()
	engines := make([]*Engine, 0, len(s.tenants))
	for _, t := range s.tenants {
		if t.engine != nil {
			engines = append(engines, t.engine)
		}
	}
	s.mu.RUnlock()

	var errs []error
	for _, e := range engines {
		if err := e.Snapshot(); err != nil {
			errs = append(errs, fmt.Errorf("snapshot %s: %w", e.dir, err))
		}
	}
	return errors.Join(errs...)
}

// Close snapshots and closes every currently-open tenant engine.
func (s *Store) Close() error {
	s.mu.RLock()
	engines := make([]*Engine, 0, len(s.tenants))
	for _, t := range s.tenants {
		if t.engine != nil {
			engines = append(engines, t.engine)
		}
	}
	s.mu.RUnlock()

	var errs []error
	for _, e := range engines {
		if err := e.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close %s: %w", e.dir, err))
		}
	}
	return errors.Join(errs...)
}
