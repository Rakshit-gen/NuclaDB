// Package grpc implements the NuclaDB gRPC service by translating protobuf
// requests into calls on internal/engine.Engine. IDs are exchanged as
// decimal strings over the wire (matching common vector-DB client
// ergonomics) but stored internally as uint64, since the HNSW graph keys
// nodes by uint64 for compact adjacency-list storage.
package grpc

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// Server implements pb.NuclaDBServer over a single Engine.
//
// metric is the distance metric the underlying graph was built with. HNSW
// bakes its metric into which neighbors get linked at construction time,
// so search can't switch metrics per-query the way a brute-force scan
// could — a SearchRequest.metric that disagrees with it is rejected rather
// than silently ignored.
type Server struct {
	pb.UnimplementedNuclaDBServer
	engine *engine.Engine
	metric pb.DistanceMetric
}

// New wraps eng as a gRPC service, validating incoming search requests
// against metric (the metric the graph was actually configured with).
func New(eng *engine.Engine, metric pb.DistanceMetric) *Server {
	return &Server{engine: eng, metric: metric}
}

func parseID(s string) (uint64, error) {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "id must be a decimal uint64, got %q", s)
	}
	return id, nil
}

func (s *Server) Insert(ctx context.Context, req *pb.InsertRequest) (*pb.InsertResponse, error) {
	v := req.GetVector()
	if v == nil {
		return nil, status.Error(codes.InvalidArgument, "vector is required")
	}
	id, err := parseID(v.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.engine.Insert(id, v.GetValues(), v.GetMetadata()); err != nil {
		return nil, toStatus(err)
	}
	return &pb.InsertResponse{Id: v.GetId()}, nil
}

func (s *Server) BatchUpsert(ctx context.Context, req *pb.BatchUpsertRequest) (*pb.BatchUpsertResponse, error) {
	var n int64
	for _, v := range req.GetVectors() {
		id, err := parseID(v.GetId())
		if err != nil {
			return nil, err
		}
		if err := s.engine.Insert(id, v.GetValues(), v.GetMetadata()); err != nil {
			return nil, toStatus(err)
		}
		n++
	}
	return &pb.BatchUpsertResponse{Upserted: n}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	id, err := parseID(req.GetId())
	if err != nil {
		return nil, err
	}
	if err := s.engine.Delete(id); err != nil {
		return nil, toStatus(err)
	}
	return &pb.DeleteResponse{Deleted: true}, nil
}

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	if req.GetMetric() != pb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED && req.GetMetric() != s.metric {
		return nil, status.Errorf(codes.InvalidArgument,
			"index was built with metric %s; search cannot use a different metric (HNSW neighbor selection is metric-specific)",
			s.metric)
	}
	topK := int(req.GetTopK())
	if topK <= 0 {
		return nil, status.Error(codes.InvalidArgument, "top_k must be > 0")
	}
	ef := int(req.GetEfSearch())

	filters := make(map[string]string, len(req.GetFilters()))
	for _, f := range req.GetFilters() {
		filters[f.GetKey()] = f.GetValue()
	}

	results, err := s.engine.Search(req.GetQuery(), topK, ef, filters)
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

// toStatus classifies engine errors for the client: dimension mismatches
// and unknown-id errors are the caller's fault (InvalidArgument); anything
// else is treated as an internal error.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	msg := err.Error()
	if strings.Contains(msg, "dimension mismatch") || strings.Contains(msg, "not found") {
		return status.Error(codes.InvalidArgument, msg)
	}
	return status.Error(codes.Internal, msg)
}
