package health

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCProber checks liveness via the standard gRPC health-checking
// protocol (google.golang.org/grpc/health) that every nucladbd process
// already serves (see cmd/nucladbd) — no NuclaDB-specific RPC needed.
// Health checks need no API key, so DialOptions only has to carry TLS
// credentials when nodes serve TLS; empty means dial without TLS.
type GRPCProber struct {
	DialOptions []grpc.DialOption
}

// Probe dials addr and issues one health check, bounded by ctx's
// deadline. A fresh connection per call keeps the prober stateless and
// correct (no pooled-connection staleness to reason about) at the cost of
// a new TCP handshake per probe — an acceptable trade at a health-check
// cadence measured in seconds, not one worth pooling for.
func (p GRPCProber) Probe(ctx context.Context, addr string) error {
	opts := p.DialOptions
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return fmt.Errorf("health: dial %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health: check %s: %w", addr, err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("health: %s reports status %s", addr, resp.GetStatus())
	}
	return nil
}
