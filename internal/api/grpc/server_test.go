package grpc

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func startTestServer(t *testing.T) pb.NuclaDBClient {
	t.Helper()

	store, err := engine.OpenStore(t.TempDir(), hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterNuclaDBServer(grpcServer, New(store, pb.DistanceMetric_DISTANCE_METRIC_L2))
	go func() {
		_ = grpcServer.Serve(lis)
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewNuclaDBClient(conn)
}

func TestGRPCInsertAndSearch(t *testing.T) {
	client := startTestServer(t)
	ctx := context.Background()

	_, err := client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{
		Id:       "1",
		Values:   []float32{1, 0, 0, 0},
		Metadata: map[string]string{"lang": "go"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{
		Id:     "2",
		Values: []float32{0, 1, 0, 0},
	}})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := client.Search(ctx, &pb.SearchRequest{
		Query: []float32{1, 0, 0, 0},
		TopK:  1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Matches) != 1 || resp.Matches[0].Id != "1" || resp.Matches[0].Metadata["lang"] != "go" {
		t.Fatalf("unexpected search response: %+v", resp)
	}
}

func TestGRPCDelete(t *testing.T) {
	client := startTestServer(t)
	ctx := context.Background()

	_, err := client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{Id: "1", Values: []float32{1, 0, 0, 0}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Delete(ctx, &pb.DeleteRequest{Id: "1"}); err != nil {
		t.Fatal(err)
	}

	resp, err := client.Search(ctx, &pb.SearchRequest{Query: []float32{1, 0, 0, 0}, TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range resp.Matches {
		if m.Id == "1" {
			t.Fatalf("deleted id=1 still returned: %+v", resp.Matches)
		}
	}
}

func TestGRPCBatchUpsert(t *testing.T) {
	client := startTestServer(t)
	ctx := context.Background()

	resp, err := client.BatchUpsert(ctx, &pb.BatchUpsertRequest{Vectors: []*pb.Vector{
		{Id: "1", Values: []float32{1, 0, 0, 0}},
		{Id: "2", Values: []float32{0, 1, 0, 0}},
		{Id: "3", Values: []float32{0, 0, 1, 0}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Upserted != 3 {
		t.Fatalf("Upserted = %d, want 3", resp.Upserted)
	}
}

func TestGRPCRejectsMismatchedMetric(t *testing.T) {
	client := startTestServer(t)
	ctx := context.Background()

	_, err := client.Search(ctx, &pb.SearchRequest{
		Query:  []float32{1, 0, 0, 0},
		TopK:   1,
		Metric: pb.DistanceMetric_DISTANCE_METRIC_COSINE,
	})
	if err == nil {
		t.Fatal("expected an error for a metric mismatch, got nil")
	}
}

func TestGRPCRejectsNonNumericID(t *testing.T) {
	client := startTestServer(t)
	ctx := context.Background()

	_, err := client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{Id: "not-a-number", Values: []float32{1, 0, 0, 0}}})
	if err == nil {
		t.Fatal("expected an error for a non-numeric id, got nil")
	}
}
