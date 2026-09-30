// Package gateway exposes the gRPC API over plain HTTP/JSON, so the
// database is curl-able and usable from a browser without a gRPC client.
// It's a thin hand-written translation layer rather than grpc-gateway,
// since grpc-gateway's code generation needs the full googleapis proto
// tree vendored in for five routes — not worth the dependency weight here.
package gateway

import (
	"cmp"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	site "github.com/Rakshit-gen/nucladb/docs/site"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// Handler serves the REST facade over an in-process gRPC server
// implementation (internal/api/grpc.Server satisfies pb.NuclaDBServer).
type Handler struct {
	svc     pb.NuclaDBServer
	mux     *http.ServeMux
	maxBody int64
}

// New builds the REST handler, routing:
//
//	POST   /v1/tenants        -> CreateTenant
//	POST   /v1/vectors        -> Insert
//	POST   /v1/vectors:batch  -> BatchUpsert
//	DELETE /v1/vectors/{id}   -> Delete
//	POST   /v1/search         -> Search
//	GET    /v1/vectors/{id}   -> Get
//	PATCH  /v1/vectors/{id}   -> UpdateMetadata
//	GET    /v1/vectors        -> List (page_token, page_size query params)
//	GET    /v1/vectors:count  -> Count
//	GET    /v1/tenants        -> ListTenants
//	DELETE /v1/tenants/{id}   -> DeleteTenant
//	PUT    /v1/tenants/{id}/quota -> SetQuota
//	GET    /docs              -> CLI documentation page (docs/site/index.html)
//
// Every route except tenant creation accepts an optional tenant_id (JSON
// body field, or a query parameter for GET and DELETE); omitting it uses the
// reserved default tenant. Request bodies over maxBodyBytes are refused
// with 413.
func New(svc pb.NuclaDBServer, maxBodyBytes int64) *Handler {
	h := &Handler{svc: svc, mux: http.NewServeMux(), maxBody: maxBodyBytes}
	h.mux.HandleFunc("POST /v1/tenants", h.createTenant)
	h.mux.HandleFunc("POST /v1/vectors", h.insert)
	h.mux.HandleFunc("POST /v1/vectors:batch", h.batchUpsert)
	h.mux.HandleFunc("DELETE /v1/vectors/{id}", h.delete)
	h.mux.HandleFunc("POST /v1/search", h.search)
	h.mux.HandleFunc("GET /v1/vectors/{id}", h.get)
	h.mux.HandleFunc("PATCH /v1/vectors/{id}", h.updateMetadata)
	h.mux.HandleFunc("GET /v1/vectors", h.list)
	h.mux.HandleFunc("GET /v1/vectors:count", h.count)
	h.mux.HandleFunc("GET /v1/tenants", h.listTenants)
	h.mux.HandleFunc("DELETE /v1/tenants/{id}", h.deleteTenant)
	h.mux.HandleFunc("PUT /v1/tenants/{id}/quota", h.setQuota)
	h.mux.HandleFunc("GET /docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(site.Index)
	})
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBody)
	h.mux.ServeHTTP(w, r)
}

type tenantJSON struct {
	TenantID   string  `json:"tenant_id"`
	MaxVectors int64   `json:"max_vectors,omitempty"`
	MaxQPS     float64 `json:"max_qps,omitempty"`
}

func (h *Handler) createTenant(w http.ResponseWriter, r *http.Request) {
	var body tenantJSON
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := h.svc.CreateTenant(r.Context(), &pb.CreateTenantRequest{
		TenantId: body.TenantID,
		Quota:    &pb.TenantQuota{MaxVectors: body.MaxVectors, MaxQps: body.MaxQPS},
	})
	writeResult(w, resp, err)
}

type vectorJSON struct {
	ID       string            `json:"id"`
	Values   []float32         `json:"values"`
	Metadata map[string]string `json:"metadata,omitempty"`
	TenantID string            `json:"tenant_id,omitempty"`
}

func (h *Handler) insert(w http.ResponseWriter, r *http.Request) {
	var body vectorJSON
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := h.svc.Insert(r.Context(), &pb.InsertRequest{Vector: &pb.Vector{
		Id: body.ID, Values: body.Values, Metadata: body.Metadata, TenantId: body.TenantID,
	}})
	writeResult(w, resp, err)
}

func (h *Handler) batchUpsert(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Vectors []vectorJSON `json:"vectors"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	vectors := make([]*pb.Vector, len(body.Vectors))
	for i, v := range body.Vectors {
		vectors[i] = &pb.Vector{Id: v.ID, Values: v.Values, Metadata: v.Metadata, TenantId: v.TenantID}
	}
	resp, err := h.svc.BatchUpsert(r.Context(), &pb.BatchUpsertRequest{Vectors: vectors})
	writeResult(w, resp, err)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.Delete(r.Context(), &pb.DeleteRequest{
		Id:       r.PathValue("id"),
		TenantId: r.URL.Query().Get("tenant_id"),
	})
	writeResult(w, resp, err)
}

// Filters is the short form, equality only. Where takes any operator:
// {"key": "price", "op": "lt", "value": "10"} or
// {"key": "tier", "op": "in", "values": ["pro", "team"]}.
type searchJSON struct {
	Query    []float32         `json:"query"`
	TopK     int32             `json:"top_k"`
	EfSearch int32             `json:"ef_search,omitempty"`
	Filters  map[string]string `json:"filters,omitempty"`
	Where    []whereJSON       `json:"where,omitempty"`
	TenantID string            `json:"tenant_id,omitempty"`
}

type whereJSON struct {
	Key    string   `json:"key"`
	Op     string   `json:"op"`
	Value  string   `json:"value,omitempty"`
	Values []string `json:"values,omitempty"`
}

var filterOps = map[string]pb.FilterOp{
	"": pb.FilterOp_FILTER_OP_EQ, "eq": pb.FilterOp_FILTER_OP_EQ, "ne": pb.FilterOp_FILTER_OP_NE,
	"in": pb.FilterOp_FILTER_OP_IN, "not_in": pb.FilterOp_FILTER_OP_NOT_IN,
	"gt": pb.FilterOp_FILTER_OP_GT, "gte": pb.FilterOp_FILTER_OP_GTE,
	"lt": pb.FilterOp_FILTER_OP_LT, "lte": pb.FilterOp_FILTER_OP_LTE,
	"exists": pb.FilterOp_FILTER_OP_EXISTS,
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	var body searchJSON
	if !decodeJSON(w, r, &body) {
		return
	}
	filters := make([]*pb.MetadataFilter, 0, len(body.Filters))
	for k, v := range body.Filters {
		filters = append(filters, &pb.MetadataFilter{Key: k, Value: v})
	}
	for _, f := range body.Where {
		op, ok := filterOps[f.Op]
		if !ok {
			writeError(w, http.StatusBadRequest, "unknown filter op "+strconv.Quote(f.Op))
			return
		}
		filters = append(filters, &pb.MetadataFilter{Key: f.Key, Op: op, Value: f.Value, Values: f.Values})
	}
	resp, err := h.svc.Search(r.Context(), &pb.SearchRequest{
		Query: body.Query, TopK: body.TopK, EfSearch: body.EfSearch, Filters: filters, TenantId: body.TenantID,
	})
	writeResult(w, resp, err)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.Get(r.Context(), &pb.GetRequest{
		Id: r.PathValue("id"), TenantId: r.URL.Query().Get("tenant_id"),
	})
	writeResult(w, resp, err)
}

func (h *Handler) updateMetadata(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Metadata map[string]string `json:"metadata"`
		TenantID string            `json:"tenant_id,omitempty"`
	}
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := h.svc.UpdateMetadata(r.Context(), &pb.UpdateMetadataRequest{
		Id: r.PathValue("id"), Metadata: body.Metadata, TenantId: body.TenantID,
	})
	writeResult(w, resp, err)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	size, err := strconv.Atoi(cmp.Or(q.Get("page_size"), "0"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "page_size must be an integer")
		return
	}
	resp, err := h.svc.List(r.Context(), &pb.ListRequest{
		TenantId: q.Get("tenant_id"), PageToken: q.Get("page_token"), PageSize: int32(size),
	})
	writeResult(w, resp, err)
}

func (h *Handler) count(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.Count(r.Context(), &pb.CountRequest{TenantId: r.URL.Query().Get("tenant_id")})
	writeResult(w, resp, err)
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.ListTenants(r.Context(), &pb.ListTenantsRequest{})
	writeResult(w, resp, err)
}

func (h *Handler) deleteTenant(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.DeleteTenant(r.Context(), &pb.DeleteTenantRequest{TenantId: r.PathValue("id")})
	writeResult(w, resp, err)
}

func (h *Handler) setQuota(w http.ResponseWriter, r *http.Request) {
	var body tenantJSON
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := h.svc.SetQuota(r.Context(), &pb.SetQuotaRequest{
		TenantId: r.PathValue("id"),
		Quota:    &pb.TenantQuota{MaxVectors: body.MaxVectors, MaxQps: body.MaxQPS},
	})
	writeResult(w, resp, err)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		code := http.StatusBadRequest
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			code = http.StatusRequestEntityTooLarge
		}
		writeError(w, code, err.Error())
		return false
	}
	return true
}

func writeResult(w http.ResponseWriter, resp any, err error) {
	if err != nil {
		writeError(w, statusCodeFor(err), err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func statusCodeFor(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
