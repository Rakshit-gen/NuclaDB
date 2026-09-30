// Package grpc implements the NuclaDB gRPC service by translating protobuf
// requests into calls on internal/engine.Store. IDs are exchanged as
// decimal strings over the wire (matching common vector-DB client
// ergonomics) but stored internally as uint64, since the HNSW graph keys
// nodes by uint64 for compact adjacency-list storage.
package grpc

import (
	"context"
	"errors"
	"slices"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Rakshit-gen/nucladb/internal/auth"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// Server implements pb.NuclaDBServer over a tenant-isolated Store. Every
// data RPC's tenant_id is passed straight through to the Store, which
// resolves an empty tenant_id to engine.DefaultTenant.
type Server struct {
	pb.UnimplementedNuclaDBServer
	store *engine.Store
	keys  *auth.Keys // nil: no API keys required
}

// RequireKeys makes every RPC check the caller's API key against keys.
// Callers must also install auth.UnaryServerInterceptor (or, for REST,
// put the key in the context with auth.WithKey).
func (s *Server) RequireKeys(keys *auth.Keys) { s.keys = keys }

func (s *Server) allow(ctx context.Context, tenantID string, admin bool) error {
	if s.keys == nil {
		return nil
	}
	switch err := s.keys.Allow(ctx, tenantID, admin); {
	case errors.Is(err, auth.ErrNoKey):
		return status.Error(codes.Unauthenticated, err.Error())
	case err != nil:
		return status.Error(codes.PermissionDenied, err.Error())
	}
	return nil
}

// New wraps store as a gRPC service.
func New(store *engine.Store) *Server {
	return &Server{store: store}
}

var metricNames = map[pb.DistanceMetric]string{
	pb.DistanceMetric_DISTANCE_METRIC_COSINE: "cosine",
	pb.DistanceMetric_DISTANCE_METRIC_L2:     "l2",
	pb.DistanceMetric_DISTANCE_METRIC_DOT:    "dot",
}

func metricEnum(name string) pb.DistanceMetric {
	for m, n := range metricNames {
		if n == name {
			return m
		}
	}
	return pb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED
}

// Caps on per-search work, so one request can't ask the server to walk
// the whole graph or sort millions of results.
const (
	MaxTopK = 1000
	MaxEf   = 10000
)

func parseID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "id must be a decimal uint64, got %q", s)
	}
	return id, nil
}

func (s *Server) CreateTenant(ctx context.Context, req *pb.CreateTenantRequest) (*pb.CreateTenantResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), true); err != nil {
		return nil, err
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	q := req.GetQuota()
	err := s.store.CreateTenantWithIndex(req.GetTenantId(), engine.Quota{
		MaxVectors: q.GetMaxVectors(),
		MaxQPS:     q.GetMaxQps(),
	}, engine.IndexSpec{Dim: int(req.GetDim()), Metric: metricNames[req.GetMetric()]})
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CreateTenantResponse{}, nil
}

func (s *Server) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	if err := s.allow(ctx, req.GetVector().GetTenantId(), false); err != nil {
		return nil, err
	}
	v := req.GetVector()
	if v == nil {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}
	id, err := parseID(v.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.store.Insert(v.GetTenantId(), id, v.GetValues(), v.GetMetadata()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.InsertResponse{Id: v.GetId()}, nil
}

// BatchUpsert groups the request's vectors by tenant (tenant_id is set per
// vector on the wire, so one request can mix tenants) and writes each group
// as one durable batch: one fsync per tenant instead of one per vector. A
// bulk load into a single tenant gets the full benefit. Every tenant is
// checked before any is written (see engine.Store.InsertBatches).
func (s *Server) BatchUpsert(ctx context.Context, req *pb.BatchUpsertRequest) (*pb.BatchUpsertResponse, error) {
	byTenant := make(map[string][]engine.InsertItem)
	var tenantOrder []string
	for _, v := range req.GetVectors() {
		id, err := parseID(v.GetId())
		if err != nil {
			return nil, err
		}
		tenantID := v.GetTenantId()
		if _, ok := byTenant[tenantID]; !ok {
			if err := s.allow(ctx, tenantID, false); err != nil {
				return nil, err
			}
			tenantOrder = append(tenantOrder, tenantID)
		}
		byTenant[tenantID] = append(byTenant[tenantID], engine.InsertItem{
			ID:       id,
			Vector:   v.GetValues(),
			Metadata: v.GetMetadata(),
		})
	}

	batches := make([]engine.TenantBatch, len(tenantOrder))
	var n int64
	for i, tenantID := range tenantOrder {
		batches[i] = engine.TenantBatch{TenantID: tenantID, Items: byTenant[tenantID]}
		n += int64(len(byTenant[tenantID]))
	}
	if err := s.store.InsertBatches(batches); err != nil {
		return nil, toStatus(err)
	}
	return &pb.BatchUpsertResponse{Upserted: n}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.store.Delete(req.GetTenantId(), id); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DeleteResponse{Deleted: true}, nil
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	// HNSW bakes its metric into which neighbors get linked, so a search
	// can't switch metrics the way a brute-force scan could. A request
	// naming a different one is refused rather than silently ignored.
	if want := req.GetMetric(); want != pb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED {
		spec, err := s.store.IndexOf(req.GetTenantId())
		if err != nil {
			return nil, toStatus(err)
		}
		if metricNames[want] != spec.Metric {
			return nil, status.Errorf(codes.InvalidArgument,
				"tenant's index was built with metric %s; search cannot use a different one", spec.Metric)
		}
	}
	topK := int(req.GetTopK())
	if topK <= 0 || topK > MaxTopK {
		return nil, status.Errorf(codes.InvalidArgument, "top_k must be between 1 and %d", MaxTopK)
	}
	ef := int(req.GetEfSearch())
	if ef < 0 || ef > MaxEf {
		return nil, status.Errorf(codes.InvalidArgument, "ef_search must be between 0 (server default) and %d", MaxEf)
	}

	// engine.FilterOp and pb.FilterOp number their ops the same way.
	filters := make([]engine.Filter, len(req.GetFilters()))
	for i, f := range req.GetFilters() {
		filters[i] = engine.Filter{Key: f.GetKey(), Op: engine.FilterOp(f.GetOp()), Values: f.GetValues()}
		if len(f.GetValues()) == 0 && f.GetOp() != pb.FilterOp_FILTER_OP_EXISTS {
			filters[i].Values = []string{f.GetValue()}
		}
	}

	results, err := s.store.Search(req.GetTenantId(), req.GetQuery(), topK, ef, filters)
	if err != nil {
		return nil, toStatus(err)
	}

	matches := make([]*pb.ScoredVector, len(results))
	for i, r := range results {
		matches[i] = &pb.ScoredVector{
			Id:       strconv.FormatUint(r.ID, 10),
			Score:    r.Distance,
			Metadata: r.Metadata,
		}
	}
	return &pb.SearchResponse{Matches: matches}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	v, md, err := s.store.Get(req.GetTenantId(), id)
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.GetResponse{Vector: &pb.Vector{
		Id: req.GetId(), Values: v, Metadata: md, TenantId: req.GetTenantId(),
	}}, nil
}

// MaxPageSize caps List's page_size.
const MaxPageSize = 1000

func (s *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	size := int(req.GetPageSize())
	if size < 0 || size > MaxPageSize {
		return nil, status.Errorf(codes.InvalidArgument, "page_size must be between 0 (default 100) and %d", MaxPageSize)
	}
	if size == 0 {
		size = 100
	}
	var start uint64
	if tok := req.GetPageToken(); tok != "" {
		var err error
		if start, err = strconv.ParseUint(tok, 10, 64); err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad page_token")
		}
	}
	ids, more, err := s.store.List(req.GetTenantId(), start, size)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &pb.ListResponse{Ids: make([]string, len(ids))}
	for i, id := range ids {
		resp.Ids[i] = strconv.FormatUint(id, 10)
	}
	if more {
		resp.NextPageToken = strconv.FormatUint(ids[len(ids)-1]+1, 10)
	}
	return resp, nil
}

func (s *Server) Count(ctx context.Context, req *pb.CountRequest) (*pb.CountResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	st, err := s.store.Stats(req.GetTenantId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &pb.CountResponse{Count: int64(st.VectorCount)}, nil
}

func (s *Server) UpdateMetadata(ctx context.Context, req *pb.UpdateMetadataRequest) (*pb.UpdateMetadataResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), false); err != nil {
		return nil, err
	}
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.store.UpdateMetadata(req.GetTenantId(), id, req.GetMetadata()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.UpdateMetadataResponse{}, nil
}

func (s *Server) DeleteTenant(ctx context.Context, req *pb.DeleteTenantRequest) (*pb.DeleteTenantResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), true); err != nil {
		return nil, err
	}
	if err := s.store.DeleteTenant(req.GetTenantId()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DeleteTenantResponse{}, nil
}

func (s *Server) SetQuota(ctx context.Context, req *pb.SetQuotaRequest) (*pb.SetQuotaResponse, error) {
	if err := s.allow(ctx, req.GetTenantId(), true); err != nil {
		return nil, err
	}
	q := req.GetQuota()
	if err := s.store.SetQuota(req.GetTenantId(), engine.Quota{MaxVectors: q.GetMaxVectors(), MaxQPS: q.GetMaxQps()}); err != nil {
		return nil, toStatus(err)
	}
	return &pb.SetQuotaResponse{}, nil
}

func (s *Server) ListTenants(ctx context.Context, req *pb.ListTenantsRequest) (*pb.ListTenantsResponse, error) {
	if err := s.allow(ctx, "", true); err != nil {
		return nil, err
	}
	all := s.store.AllStats()
	ids := make([]string, 0, len(all))
	for id := range all {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	resp := &pb.ListTenantsResponse{}
	for _, id := range ids {
		st := all[id]
		resp.Tenants = append(resp.Tenants, &pb.TenantInfo{
			TenantId:    id,
			VectorCount: int64(st.VectorCount),
			Quota:       &pb.TenantQuota{MaxVectors: st.Quota.MaxVectors, MaxQps: st.Quota.MaxQPS},
			Dim:         int32(st.Index.Dim),
			Metric:      metricEnum(st.Index.Metric),
		})
	}
	return resp, nil
}

// toStatus maps engine and store sentinel errors (wrapped or not) to gRPC
// codes. Anything unrecognized is an internal error.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	switch {
	case errors.Is(err, engine.ErrTenantNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, engine.ErrTenantExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, engine.ErrInvalidTenantID), errors.Is(err, hnsw.ErrDimensionMismatch),
		errors.Is(err, engine.ErrDefaultTenant), errors.Is(err, engine.ErrBadFilter),
		errors.Is(err, engine.ErrBadIndexSpec):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, engine.ErrQuotaExceeded), errors.Is(err, engine.ErrRateLimited):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, engine.ErrNotFound), errors.Is(err, hnsw.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
