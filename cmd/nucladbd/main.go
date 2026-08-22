// Command nucladbd runs the NuclaDB server: it opens (or creates) a
// database directory, serves the gRPC API and a REST/JSON facade over it,
// publishes OpenTelemetry traces and Prometheus metrics, and periodically
// snapshots to disk so restart never has to replay the WAL from empty.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Rakshit-gen/nucladb/internal/api/gateway"
	grpcapi "github.com/Rakshit-gen/nucladb/internal/api/grpc"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	"github.com/Rakshit-gen/nucladb/internal/telemetry"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func main() {
	var (
		dataDir        = flag.String("data-dir", "./data", "directory holding the WAL and snapshot files")
		grpcAddr       = flag.String("grpc-addr", ":9090", "gRPC listen address")
		httpAddr       = flag.String("http-addr", ":8080", "REST/JSON + /metrics listen address")
		dim            = flag.Int("dim", 128, "fixed vector dimensionality for this database")
		metric         = flag.String("metric", "cosine", "distance metric: cosine, l2, or dot")
		m              = flag.Int("m", 16, "HNSW M: bidirectional links per node above layer 0")
		efConstruction = flag.Int("ef-construction", 200, "HNSW build-time candidate list size")
		snapshotEvery  = flag.Duration("snapshot-interval", 5*time.Minute, "how often to snapshot to disk")
		metricsEvery   = flag.Duration("metrics-interval", 15*time.Second, "how often to refresh per-tenant usage gauges")
	)
	flag.Parse()

	hnswMetric, pbMetric, err := parseMetric(*metric)
	if err != nil {
		log.Fatalf("nucladbd: %v", err)
	}

	ctx := context.Background()
	shutdownTracing, err := telemetry.SetupTracing(ctx, "nucladbd")
	if err != nil {
		log.Fatalf("nucladbd: setting up tracing: %v", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Printf("nucladbd: tracing shutdown: %v", err)
		}
	}()

	registry := prometheus.NewRegistry()
	metrics := telemetry.NewMetrics(registry)

	store, err := engine.OpenStore(*dataDir, hnsw.Config{
		Dim:            *dim,
		M:              *m,
		EfConstruction: *efConstruction,
		Metric:         hnswMetric,
	})
	if err != nil {
		log.Fatalf("nucladbd: opening store at %s: %v", *dataDir, err)
	}
	log.Printf("nucladbd: opened %s (dim=%d, metric=%s)", *dataDir, *dim, *metric)

	svc := grpcapi.New(store, pbMetric)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()),
	)
	pb.RegisterNuclaDBServer(grpcServer, svc)
	reflection.Register(grpcServer)

	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("nucladbd: listening on %s: %v", *grpcAddr, err)
	}
	go func() {
		log.Printf("nucladbd: gRPC listening on %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("nucladbd: gRPC server stopped: %v", err)
		}
	}()

	httpMux := http.NewServeMux()
	httpMux.Handle("/", gateway.New(svc))
	httpMux.Handle("/metrics", telemetry.Handler(registry))
	httpServer := &http.Server{Addr: *httpAddr, Handler: httpMux}
	go func() {
		log.Printf("nucladbd: REST listening on %s (/metrics for Prometheus)", *httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("nucladbd: REST server stopped: %v", err)
		}
	}()

	stopBackground := make(chan struct{})
	go periodicSnapshot(store, *snapshotEvery, stopBackground)
	go periodicMetricsRefresh(store, metrics, *metricsEvery, stopBackground)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("nucladbd: shutting down")

	close(stopBackground)
	grpcServer.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)

	if err := store.Close(); err != nil {
		log.Fatalf("nucladbd: final snapshot on shutdown failed: %v", err)
	}
	log.Println("nucladbd: clean shutdown, final snapshot written")
}

func periodicSnapshot(store *engine.Store, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := store.Snapshot(); err != nil {
				log.Printf("nucladbd: periodic snapshot failed: %v", err)
			} else {
				log.Printf("nucladbd: snapshot written")
			}
		case <-stop:
			return
		}
	}
}

func periodicMetricsRefresh(store *engine.Store, metrics *telemetry.Metrics, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for tenantID, stats := range store.AllStats() {
				metrics.SetTenantStats(tenantID, stats.VectorCount, stats.Quota.MaxVectors)
			}
		case <-stop:
			return
		}
	}
}

func parseMetric(name string) (hnsw.Metric, pb.DistanceMetric, error) {
	switch name {
	case "cosine":
		return hnsw.Cosine(), pb.DistanceMetric_DISTANCE_METRIC_COSINE, nil
	case "l2":
		return hnsw.L2(), pb.DistanceMetric_DISTANCE_METRIC_L2, nil
	case "dot":
		return hnsw.Dot(), pb.DistanceMetric_DISTANCE_METRIC_DOT, nil
	default:
		return nil, 0, errUnknownMetric(name)
	}
}

type errUnknownMetric string

func (e errUnknownMetric) Error() string {
	return "unknown metric " + string(e) + " (want cosine, l2, or dot)"
}
