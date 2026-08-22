package telemetry

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func grpcCode(err error) string {
	return status.Code(err).String()
}

// Metrics holds every Prometheus collector the server publishes. Request
// latency is exposed as a histogram (not pre-computed p50/p95/p99) so
// percentiles can be computed with whatever quantile or window a caller's
// PromQL query wants, rather than baking one choice in server-side.
type Metrics struct {
	requestsTotal     *prometheus.CounterVec
	requestDuration   *prometheus.HistogramVec
	tenantVectorCount *prometheus.GaugeVec
	tenantQuotaMax    *prometheus.GaugeVec
}

// NewMetrics registers every collector against a fresh registry and
// returns both, so tests can use an isolated registry instead of the
// global default one.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "nucladb_requests_total",
			Help: "Total gRPC requests, labeled by method, tenant, and status code.",
		}, []string{"method", "tenant", "code"}),
		requestDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "nucladb_request_duration_seconds",
			Help:    "gRPC request latency in seconds, labeled by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		tenantVectorCount: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "nucladb_tenant_vector_count",
			Help: "Current live vector count per tenant.",
		}, []string{"tenant"}),
		tenantQuotaMax: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "nucladb_tenant_quota_max_vectors",
			Help: "Configured max-vectors quota per tenant (0 = unlimited).",
		}, []string{"tenant"}),
	}
	return m
}

// Handler serves the registered collectors in the Prometheus text
// exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// SetTenantStats publishes tenantID's current usage and quota.
func (m *Metrics) SetTenantStats(tenantID string, vectorCount int, maxVectors int64) {
	m.tenantVectorCount.WithLabelValues(tenantID).Set(float64(vectorCount))
	m.tenantQuotaMax.WithLabelValues(tenantID).Set(float64(maxVectors))
}

// tenantExtractor pulls a tenant_id label out of a gRPC request message
// for the interceptor below. Every request type that carries a top-level
// tenant_id implements this narrow interface (the generated protobuf
// types already satisfy it via their GetTenantId method). InsertRequest
// is the one exception: its tenant_id lives one level down, on the nested
// Vector — a blind interface assertion against InsertRequest itself would
// silently mislabel every Insert call, so it gets a type-switch case
// instead (see tenantOf).
type tenantExtractor interface {
	GetTenantId() string
}

// tenantOf resolves the "-" (no concept of a single tenant for this
// request shape), "default", or actual tenant label for req.
func tenantOf(req any) string {
	switch r := req.(type) {
	case *pb.InsertRequest:
		return resolveTenant(r.GetVector().GetTenantId())
	case tenantExtractor:
		return resolveTenant(r.GetTenantId())
	default:
		return "-"
	}
}

func resolveTenant(id string) string {
	if id == "" {
		return "default"
	}
	return id
}

// UnaryServerInterceptor times every unary RPC and records it into
// requestsTotal/requestDuration, labeled with the method name, resolved
// tenant (falling back to a fixed "-" label for requests with no
// tenant_id field, e.g. batch-upsert, where tenancy is per-item), and the
// resulting gRPC status code.
func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		duration := time.Since(start).Seconds()

		m.requestsTotal.WithLabelValues(info.FullMethod, tenantOf(req), grpcCode(err)).Inc()
		m.requestDuration.WithLabelValues(info.FullMethod).Observe(duration)
		return resp, err
	}
}
