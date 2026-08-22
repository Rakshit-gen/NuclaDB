// Package gateway exposes the gRPC API over plain HTTP/JSON, so the
// database is curl-able and usable from a browser without a gRPC client.
// It's a thin hand-written translation layer rather than grpc-gateway,
// since grpc-gateway's code generation needs the full googleapis proto
// tree vendored in for four routes — not worth the dependency weight here.
package gateway

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// Handler serves the REST facade over an in-process gRPC server
// implementation (internal/api/grpc.Server satisfies pb.NuclaDBServer).
type Handler struct {
	svc pb.NuclaDBServer
	mux *http.ServeMux
}

// New builds the REST handler, routing:
//
//	POST   /v1/vectors        -> Insert
//	POST   /v1/vectors:batch  -> BatchUpsert
//	DELETE /v1/vectors/{id}   -> Delete
//	POST   /v1/search         -> Search
func New(svc pb.NuclaDBServer) *Handler {
	h := &Handler{svc: svc, mux: http.NewServeMux()}
	h.mux.HandleFunc("POST /v1/vectors", h.insert)
	h.mux.HandleFunc("POST /v1/vectors:batch", h.batchUpsert)
	h.mux.HandleFunc("DELETE /v1/vectors/{id}", h.delete)
	h.mux.HandleFunc("POST /v1/search", h.search)
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

type vectorJSON struct {
	ID       string            `json:"id"`
	Values   []float32         `json:"values"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

func (h *Handler) insert(w http.ResponseWriter, r *http.Request) {
	var body vectorJSON
	if !decodeJSON(w, r, &body) {
		return
	}
	resp, err := h.svc.Insert(r.Context(), &pb.InsertRequest{Vector: &pb.Vector{
		Id: body.ID, Values: body.Values, Metadata: body.Metadata,
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
		vectors[i] = &pb.Vector{Id: v.ID, Values: v.Values, Metadata: v.Metadata}
	}
	resp, err := h.svc.BatchUpsert(r.Context(), &pb.BatchUpsertRequest{Vectors: vectors})
	writeResult(w, resp, err)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	resp, err := h.svc.Delete(r.Context(), &pb.DeleteRequest{Id: r.PathValue("id")})
	writeResult(w, resp, err)
}

type searchJSON struct {
	Query    []float32         `json:"query"`
	TopK     int32             `json:"top_k"`
	EfSearch int32             `json:"ef_search,omitempty"`
	Filters  map[string]string `json:"filters,omitempty"`
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
	resp, err := h.svc.Search(r.Context(), &pb.SearchRequest{
		Query: body.Query, TopK: body.TopK, EfSearch: body.EfSearch, Filters: filters,
	})
	writeResult(w, resp, err)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
	default:
		return http.StatusInternalServerError
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
