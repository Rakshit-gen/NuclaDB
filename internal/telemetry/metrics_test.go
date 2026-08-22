package telemetry

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

type fakeReqWithTenant struct{ tenantID string }

func (f fakeReqWithTenant) GetTenantId() string { return f.tenantID }

type fakeReqWithoutTenant struct{}

func TestInterceptorRecordsSuccessWithTenant(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	interceptor := m.UnaryServerInterceptor()

	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	_, err := interceptor(context.Background(), fakeReqWithTenant{tenantID: "acme"},
		&grpc.UnaryServerInfo{FullMethod: "/nucladb.v1.NuclaDB/Search"}, handler)
	if err != nil {
		t.Fatal(err)
	}

	body := scrape(t, reg)
	if !strings.Contains(body, `method="/nucladb.v1.NuclaDB/Search"`) || !strings.Contains(body, `tenant="acme"`) {
		t.Fatalf("expected method+tenant labels in scrape output:\n%s", body)
	}
	if !strings.Contains(body, `code="OK"`) {
		t.Fatalf("expected code=\"OK\" in scrape output:\n%s", body)
	}
}

// TestInterceptorResolvesInsertRequestNestedTenant guards a real bug found
// while manually verifying metrics against a running server: InsertRequest
// has no top-level tenant_id (only its nested Vector does), so a blind
// GetTenantId() interface assertion against InsertRequest itself silently
// mislabeled every Insert call as tenant="-" regardless of which tenant it
// actually touched.
func TestInterceptorResolvesInsertRequestNestedTenant(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	interceptor := m.UnaryServerInterceptor()

	handler := func(ctx context.Context, req any) (any, error) { return nil, nil }
	req := &pb.InsertRequest{Vector: &pb.Vector{Id: "1", TenantId: "acme"}}
	_, _ = interceptor(context.Background(), req,
		&grpc.UnaryServerInfo{FullMethod: "/nucladb.v1.NuclaDB/Insert"}, handler)

	body := scrape(t, reg)
	if !strings.Contains(body, `method="/nucladb.v1.NuclaDB/Insert",tenant="acme"`) {
		t.Fatalf("InsertRequest's nested Vector.tenant_id should resolve to the request's tenant label:\n%s", body)
	}
}

func TestInterceptorDefaultsTenantLabel(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	interceptor := m.UnaryServerInterceptor()

	handler := func(ctx context.Context, req any) (any, error) { return nil, nil }

	// No tenant_id field at all on the request type.
	_, _ = interceptor(context.Background(), fakeReqWithoutTenant{},
		&grpc.UnaryServerInfo{FullMethod: "/nucladb.v1.NuclaDB/BatchUpsert"}, handler)
	// tenant_id field present but empty.
	_, _ = interceptor(context.Background(), fakeReqWithTenant{tenantID: ""},
		&grpc.UnaryServerInfo{FullMethod: "/nucladb.v1.NuclaDB/Insert"}, handler)

	body := scrape(t, reg)
	if !strings.Contains(body, `tenant="-"`) {
		t.Fatalf("expected tenant=\"-\" for a request type with no tenant_id field:\n%s", body)
	}
	if !strings.Contains(body, `tenant="default"`) {
		t.Fatalf("expected tenant=\"default\" for an empty tenant_id field:\n%s", body)
	}
}

func TestInterceptorRecordsErrorCode(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	interceptor := m.UnaryServerInterceptor()

	handler := func(ctx context.Context, req any) (any, error) {
		return nil, status.Error(codes.ResourceExhausted, "quota exceeded")
	}
	_, err := interceptor(context.Background(), fakeReqWithTenant{tenantID: "acme"},
		&grpc.UnaryServerInfo{FullMethod: "/nucladb.v1.NuclaDB/Insert"}, handler)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("interceptor should propagate the handler's error unchanged, got %v", err)
	}

	body := scrape(t, reg)
	if !strings.Contains(body, `code="ResourceExhausted"`) {
		t.Fatalf("expected code=\"ResourceExhausted\" in scrape output:\n%s", body)
	}
}

func TestSetTenantStats(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.SetTenantStats("acme", 42, 1000)

	body := scrape(t, reg)
	if !strings.Contains(body, `nucladb_tenant_vector_count{tenant="acme"} 42`) {
		t.Fatalf("expected vector count gauge for acme:\n%s", body)
	}
	if !strings.Contains(body, `nucladb_tenant_quota_max_vectors{tenant="acme"} 1000`) {
		t.Fatalf("expected quota gauge for acme:\n%s", body)
	}
}

func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	Handler(reg).ServeHTTP(rec, req)
	return rec.Body.String()
}
