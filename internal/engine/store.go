package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

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
		s.tenants[e.Name()] = &tenant{quota: Quota{}}
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
	if err := os.MkdirAll(filepath.Join(s.rootDir, tenantID), 0o755); err != nil {
		return err
	}
	t := &tenant{quota: quota}
	if quota.MaxQPS > 0 {
		t.limiter = rate.NewLimiter(rate.Limit(quota.MaxQPS), burstFor(quota.MaxQPS))
	}
	s.tenants[tenantID] = t
	return nil
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

	s.mu.RLock()
	t, ok := s.tenants[tenantID]
	s.mu.RUnlock()
	if !ok {
		return nil, ErrTenantNotFound
	}
	if t.engine != nil {
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

func (s *Store) checkAndReserve(t *tenant, extra int) error {
	if t.limiter != nil && !t.limiter.Allow() {
		return ErrRateLimited
	}
	if t.quota.MaxVectors > 0 && int64(t.engine.Len()+extra) > t.quota.MaxVectors {
		return ErrQuotaExceeded
	}
	return nil
}

// Insert durably upserts id -> (vector, metadata) within tenantID.
func (s *Store) Insert(tenantID string, id uint64, vector []float32, metadata map[string]string) error {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return err
	}
	if err := s.checkAndReserve(t, 1); err != nil {
		return err
	}
	return t.engine.Insert(id, vector, metadata)
}

// Delete durably removes id within tenantID.
func (s *Store) Delete(tenantID string, id uint64) error {
	t, err := s.getTenant(tenantID)
	if err != nil {
		return err
	}
	if t.limiter != nil && !t.limiter.Allow() {
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
	if t.limiter != nil && !t.limiter.Allow() {
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

// Snapshot snapshots every currently-open tenant engine.
func (s *Store) Snapshot() error {
	s.mu.RLock()
	engines := make([]*Engine, 0, len(s.tenants))
	for _, t := range s.tenants {
		if t.engine != nil {
			engines = append(engines, t.engine)
		}
	}
	s.mu.RUnlock()

	for _, e := range engines {
		if err := e.Snapshot(); err != nil {
			return err
		}
	}
	return nil
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

	var firstErr error
	for _, e := range engines {
		if err := e.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
