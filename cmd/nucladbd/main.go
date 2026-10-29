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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Rakshit-gen/nucladb/internal/api/gateway"
	grpcapi "github.com/Rakshit-gen/nucladb/internal/api/grpc"
	"github.com/Rakshit-gen/nucladb/internal/auth"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	"github.com/Rakshit-gen/nucladb/internal/telemetry"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func main() {
	var (
		dataDir        = flag.String("data-dir", "./data", "directory holding the WAL and snapshot files")
		grpcAddr       = flag.String("grpc-addr", "127.0.0.1:9090", "gRPC listen address")
		httpAddr       = flag.String("http-addr", "127.0.0.1:8080", "REST/JSON + /metrics listen address")
		dim            = flag.Int("dim", 128, "vector dimension for tenants created without their own")
		metric         = flag.String("metric", "cosine", "distance metric for tenants created without their own: cosine, l2, or dot")
		m              = flag.Int("m", 16, "HNSW M: bidirectional links per node above layer 0")
		efConstruction = flag.Int("ef-construction", 200, "HNSW build-time candidate list size")
		snapshotEvery  = flag.Duration("snapshot-interval", 5*time.Minute, "how often to snapshot to disk")
		maxMessage     = flag.Int("max-message-bytes", 64<<20, "largest gRPC message or REST body accepted, in bytes")
		exactFilter    = flag.Int("exact-filter-limit", engine.DefaultExactFilterLimit, "filters matching at most this many vectors are answered by scoring every match")
		idleTimeout    = flag.Duration("tenant-idle-timeout", 30*time.Minute, "close tenants unused for this long (0 keeps them open)")
		apiKeys        = flag.String("api-keys", "", "JSON file of API keys (see internal/auth); without it every request is allowed")
		allowUnauth    = flag.Bool("allow-unauthenticated", false, "explicitly allow an unauthenticated non-loopback listener")
		tlsCert        = flag.String("tls-cert", "", "TLS certificate file; with -tls-key, serves gRPC and REST over TLS")
		tlsKey         = flag.String("tls-key", "", "TLS private key file")
		metricsEvery   = flag.Duration("metrics-interval", 15*time.Second, "how often to refresh per-tenant usage gauges")
	)
	flag.Parse()
	if *dim <= 0 || *m <= 1 || *efConstruction <= 0 {
		log.Fatalf("nucladbd: -dim and -ef-construction must be positive and -m must be at least 2")
	}
	if *snapshotEvery <= 0 || *metricsEvery <= 0 || *idleTimeout < 0 || (*idleTimeout > 0 && *idleTimeout < 2*time.Nanosecond) {
		log.Fatalf("nucladbd: snapshot/metrics intervals must be positive and tenant idle timeout must be 0 or at least 2ns")
	}
	if *maxMessage <= 0 || *exactFilter < 0 {
		log.Fatalf("nucladbd: max-message-bytes must be positive and exact-filter-limit non-negative")
	}
	envAPIKey := os.Getenv("NUCLADB_API_KEY")
	if *apiKeys == "" && envAPIKey == "" && !*allowUnauth && (!loopbackAddress(*grpcAddr) || !loopbackAddress(*httpAddr)) {
		log.Fatalf("nucladbd: refusing unauthenticated non-loopback listeners; configure -api-keys or explicitly pass -allow-unauthenticated")
	}

	hnswMetric, ok := hnsw.MetricByName(*metric)
	if !ok {
		log.Fatalf("nucladbd: unknown metric %q (want cosine, l2, or dot)", *metric)
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
	store.SetExactFilterLimit(*exactFilter)
	log.Printf("nucladbd: opened %s (dim=%d, metric=%s)", *dataDir, *dim, *metric)

	svc := grpcapi.New(store)

	var configuredKeys *auth.Keys
	if *apiKeys != "" || envAPIKey != "" {
		if *apiKeys != "" {
			configuredKeys, err = auth.Load(*apiKeys)
		} else {
			configuredKeys, err = auth.New([]auth.Key{{Key: envAPIKey, Tenants: []string{"*"}, Admin: true}})
		}
		if err != nil {
			log.Fatalf("nucladbd: %v", err)
		}
		svc.RequireKeys(configuredKeys)
	} else {
		log.Printf("nucladbd: no -api-keys file, so every request is allowed")
	}
	if (*tlsCert == "") != (*tlsKey == "") {
		log.Fatalf("nucladbd: -tls-cert and -tls-key go together")
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(*maxMessage),
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor(), auth.UnaryServerInterceptor()),
	}
	if *tlsCert != "" {
		creds, err := credentials.NewServerTLSFromFile(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("nucladbd: loading TLS certificate: %v", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(opts...)
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
	httpMux.Handle("/", gateway.New(svc, int64(*maxMessage)))
	metricsHandler := telemetry.Handler(registry)
	if configuredKeys != nil {
		metricsHandler = requireAdminKey(configuredKeys, metricsHandler)
	}
	httpMux.Handle("/metrics", metricsHandler)
	httpMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	httpServer := &http.Server{
		Addr: *httpAddr, Handler: httpMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	httpLis, err := net.Listen("tcp", *httpAddr)
	if err != nil {
		_ = lis.Close()
		log.Fatalf("nucladbd: listening on %s: %v", *httpAddr, err)
	}
	go func() {
		log.Printf("nucladbd: REST listening on %s (/metrics for Prometheus)", *httpAddr)
		serve := func() error { return httpServer.Serve(httpLis) }
		if *tlsCert != "" {
			serve = func() error { return httpServer.ServeTLS(httpLis, *tlsCert, *tlsKey) }
		}
		if err := serve(); err != nil && err != http.ErrServerClosed {
			log.Printf("nucladbd: REST server stopped: %v", err)
		}
	}()

	stopBackground := make(chan struct{})
	var background sync.WaitGroup
	startBackground := func(fn func()) { background.Add(1); go func() { defer background.Done(); fn() }() }
	startBackground(func() { periodicSnapshot(store, *snapshotEvery, stopBackground) })
	startBackground(func() { periodicMetricsRefresh(store, metrics, *metricsEvery, stopBackground) })
	if *idleTimeout > 0 {
		startBackground(func() { periodicIdleClose(store, *idleTimeout, stopBackground) })
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("nucladbd: shutting down")

	close(stopBackground)
	background.Wait()
	grpcStopped := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(grpcStopped) }()
	select {
	case <-grpcStopped:
	case <-time.After(10 * time.Second):
		grpcServer.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("nucladbd: REST shutdown: %v", err)
	}

	if err := store.Close(); err != nil {
		log.Printf("nucladbd: final snapshot on shutdown failed: %v", err)
		return
	}
	log.Println("nucladbd: clean shutdown, final snapshot written")
}

func loopbackAddress(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func requireAdminKey(keys *auth.Keys, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := auth.FromHeader(r.Header.Get("Authorization"), r.Header.Get("X-API-Key"))
		if err := keys.Allow(auth.WithKey(r.Context(), key), "", true); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
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

func periodicIdleClose(store *engine.Store, idle time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(idle / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n, err := store.CloseIdle(idle)
			if err != nil {
				log.Printf("nucladbd: closing idle tenants: %v", err)
			}
			if n > 0 {
				log.Printf("nucladbd: closed %d idle tenants", n)
			}
		case <-stop:
			return
		}
	}
}

func periodicMetricsRefresh(store *engine.Store, metrics *telemetry.Metrics, interval time.Duration, stop <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	seen := make(map[string]struct{})
	for {
		select {
		case <-ticker.C:
			current := make(map[string]struct{})
			for tenantID, stats := range store.AllStats() {
				metrics.SetTenantStats(tenantID, stats.VectorCount, stats.Quota.MaxVectors)
				current[tenantID] = struct{}{}
			}
			for tenantID := range seen {
				if _, ok := current[tenantID]; !ok {
					metrics.DeleteTenantStats(tenantID)
				}
			}
			seen = current
		case <-stop:
			return
		}
	}
}
