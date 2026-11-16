package grpc

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Rakshit-gen/nucladb/internal/auth"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

const rootKey = "root-admin-key-0123456789"

// startKeyedServer is startTestServer with API keys required, rootKey as
// the configured admin key, and issued keys saved under the store's dir.
func startKeyedServer(t *testing.T, withKeys bool) pb.NuclaDBClient {
	t.Helper()
	dir := t.TempDir()
	store, err := engine.OpenStore(dir, hnsw.Config{Dim: 2, Metric: hnsw.L2(), Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	svc := New(store)
	opts := []grpc.ServerOption{grpc.UnaryInterceptor(auth.UnaryServerInterceptor())}
	if withKeys {
		keys, err := auth.New([]auth.Key{{Key: rootKey, Tenants: []string{"*"}, Admin: true}})
		if err != nil {
			t.Fatal(err)
		}
		if err := keys.IssuedFrom(filepath.Join(dir, "api_keys.json")); err != nil {
			t.Fatal(err)
		}
		svc.RequireKeys(keys)
		opts = append(opts, grpc.InTapHandle(auth.TapHandle(keys)))
	}

	lis := bufconn.Listen(1 << 20)
	s := grpc.NewServer(opts...)
	pb.RegisterNuclaDBServer(s, svc)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewNuclaDBClient(conn)
}

func TestAPIKeyLifecycle(t *testing.T) {
	c := startKeyedServer(t, true)
	ctx := func(key string) context.Context {
		return metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+key)
	}
	code := func(err error) codes.Code { return status.Code(err) }

	if _, err := c.CreateTenant(ctx(rootKey), &pb.CreateTenantRequest{TenantId: "acme"}); err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateAPIKey(ctx(rootKey), &pb.CreateAPIKeyRequest{Name: "ci", Tenants: []string{"acme"}})
	if err != nil {
		t.Fatal(err)
	}
	key, id := created.GetKey(), created.GetInfo().GetId()

	insert := &pb.InsertRequest{Vector: &pb.Vector{Id: "1", Values: []float32{1, 0}, TenantId: "acme"}}
	if _, err := c.Insert(ctx(key), insert); err != nil {
		t.Fatalf("issued key can't insert into its tenant: %v", err)
	}
	if _, err := c.Count(ctx(key), &pb.CountRequest{}); code(err) != codes.PermissionDenied {
		t.Fatalf("issued key on the default tenant: got %v", err)
	}
	if _, err := c.CreateAPIKey(ctx(key), &pb.CreateAPIKeyRequest{Tenants: []string{"acme"}}); code(err) != codes.PermissionDenied {
		t.Fatalf("non-admin key issued a key: got %v", err)
	}
	if _, err := c.CreateAPIKey(ctx(rootKey), &pb.CreateAPIKeyRequest{}); code(err) != codes.InvalidArgument {
		t.Fatalf("key with no tenants: got %v", err)
	}
	if _, err := c.CreateAPIKey(ctx(rootKey), &pb.CreateAPIKeyRequest{Tenants: []string{"acme"}, TtlSeconds: -1}); code(err) != codes.InvalidArgument {
		t.Fatalf("negative ttl: got %v", err)
	}
	expiring, err := c.CreateAPIKey(ctx(rootKey), &pb.CreateAPIKeyRequest{Tenants: []string{"acme"}, TtlSeconds: 3600})
	if err != nil {
		t.Fatal(err)
	}
	if got := expiring.GetInfo().GetExpiresUnix() - expiring.GetInfo().GetCreatedUnix(); got != 3600 {
		t.Fatalf("expires - created = %d, want 3600", got)
	}
	if _, err := c.RevokeAPIKey(ctx(rootKey), &pb.RevokeAPIKeyRequest{Id: expiring.GetInfo().GetId()}); err != nil {
		t.Fatal(err)
	}

	list, err := c.ListAPIKeys(ctx(rootKey), &pb.ListAPIKeysRequest{})
	if err != nil || len(list.GetKeys()) != 1 || list.GetKeys()[0].GetId() != id {
		t.Fatalf("ListAPIKeys = %v, %v", list, err)
	}

	if _, err := c.RevokeAPIKey(ctx(rootKey), &pb.RevokeAPIKeyRequest{Id: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx(key), insert); code(err) != codes.Unauthenticated {
		t.Fatalf("revoked key: got %v", err)
	}
	if _, err := c.RevokeAPIKey(ctx(rootKey), &pb.RevokeAPIKeyRequest{Id: id}); code(err) != codes.NotFound {
		t.Fatalf("second revoke: got %v", err)
	}

	// Deleting a tenant takes it off every key, so a re-created tenant of
	// the same name doesn't inherit them.
	again, _ := c.CreateAPIKey(ctx(rootKey), &pb.CreateAPIKeyRequest{Tenants: []string{"acme"}})
	if _, err := c.DeleteTenant(ctx(rootKey), &pb.DeleteTenantRequest{TenantId: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateTenant(ctx(rootKey), &pb.CreateTenantRequest{TenantId: "acme"}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Insert(ctx(again.GetKey()), insert); code(err) != codes.Unauthenticated {
		t.Fatalf("key for a deleted tenant: got %v", err)
	}
}

func TestAPIKeysNeedKeyedServer(t *testing.T) {
	c := startKeyedServer(t, false)
	_, err := c.CreateAPIKey(context.Background(), &pb.CreateAPIKeyRequest{Admin: true})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("got %v, want FailedPrecondition", err)
	}
}

func TestTapRefusesUnknownKeyBeforeDecoding(t *testing.T) {
	c := startKeyedServer(t, true)
	for _, key := range []string{"", "not-a-real-key-0123456789"} {
		ctx := context.Background()
		if key != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+key)
		}
		_, err := c.Search(ctx, &pb.SearchRequest{Query: []float32{1, 0}, TopK: 1})
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("key %q: got %v, want Unauthenticated", key, err)
		}
	}
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+rootKey)
	if _, err := c.Search(ctx, &pb.SearchRequest{Query: []float32{1, 0}, TopK: 1}); err != nil {
		t.Fatalf("root key: %v", err)
	}
}
