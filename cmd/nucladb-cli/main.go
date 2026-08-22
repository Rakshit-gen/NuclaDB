// Command nucladb-cli is a thin gRPC client for driving a running nucladbd
// instance from the terminal: insert, search, delete, batch-upsert, and a
// couple of inspection commands. Every subcommand and flag is documented
// in docs/cli.md — this binary's --help output and that file are kept in
// sync by hand, so update both when a command changes.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	addr := os.Getenv("NUCLADB_ADDR")
	if addr == "" {
		addr = "localhost:9090"
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	var err error
	switch cmd {
	case "create-tenant":
		err = runCreateTenant(addr, args)
	case "insert":
		err = runInsert(addr, args)
	case "batch-upsert":
		err = runBatchUpsert(addr, args)
	case "search":
		err = runSearch(addr, args)
	case "delete":
		err = runDelete(addr, args)
	case "ping":
		err = runPing(addr, args)
	case "-h", "--help", "help":
		printUsage()
		return
	default:
		fmt.Fprintf(os.Stderr, "nucladb-cli: unknown command %q\n\n", cmd)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "nucladb-cli: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprint(os.Stderr, `nucladb-cli: command-line client for NuclaDB

Usage:
  nucladb-cli <command> [flags]

Commands:
  create-tenant  Provision a new tenant with an optional storage/rate quota
  insert         Insert or update a single vector
  batch-upsert   Insert or update many vectors from a JSON file
  search         Find the nearest neighbors of a query vector
  delete         Delete a vector by id
  ping           Check that the server is reachable

All data commands accept -tenant to scope the operation to a tenant other
than the reserved "default" one; tenants are fully isolated from each
other (separate index, separate storage/rate quota).

Environment:
  NUCLADB_ADDR   Server address (default localhost:9090)

Run "nucladb-cli <command> -h" for command-specific flags.
Full reference: docs/cli.md
`)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func ctxWithTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

func parseVector(s string) ([]float32, error) {
	parts := strings.Split(s, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector component %q: %w", p, err)
		}
		out[i] = float32(f)
	}
	return out, nil
}

func parseKV(pairs []string) map[string]string {
	if len(pairs) == 0 {
		return nil
	}
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		m[k] = v
	}
	return m
}

type kvFlags []string

func (k *kvFlags) String() string { return strings.Join(*k, ",") }
func (k *kvFlags) Set(v string) error {
	*k = append(*k, v)
	return nil
}

func runCreateTenant(addr string, args []string) error {
	fs := flag.NewFlagSet("create-tenant", flag.ExitOnError)
	id := fs.String("id", "", "tenant id (required)")
	maxVectors := fs.Int64("max-vectors", 0, "storage quota: max vectors this tenant may hold (0 = unlimited)")
	maxQPS := fs.Float64("max-qps", 0, "rate limit: max requests/sec for this tenant (0 = unlimited)")
	_ = fs.Parse(args) // flag.ExitOnError means Parse never returns on error

	if *id == "" {
		return fmt.Errorf("create-tenant: -id is required")
	}

	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewNuclaDBClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	_, err = client.CreateTenant(ctx, &pb.CreateTenantRequest{
		TenantId: *id,
		Quota:    &pb.TenantQuota{MaxVectors: *maxVectors, MaxQps: *maxQPS},
	})
	if err != nil {
		return err
	}
	fmt.Printf("created tenant %q\n", *id)
	return nil
}

func runInsert(addr string, args []string) error {
	fs := flag.NewFlagSet("insert", flag.ExitOnError)
	id := fs.String("id", "", "vector id (decimal uint64, required)")
	vec := fs.String("vector", "", "comma-separated float32 values, e.g. 0.1,0.2,0.3 (required)")
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	var meta kvFlags
	fs.Var(&meta, "meta", "metadata key=value pair; repeatable")
	_ = fs.Parse(args) // flag.ExitOnError means Parse never returns on error

	if *id == "" || *vec == "" {
		return fmt.Errorf("insert: -id and -vector are required")
	}
	values, err := parseVector(*vec)
	if err != nil {
		return err
	}

	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewNuclaDBClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	resp, err := client.Insert(ctx, &pb.InsertRequest{Vector: &pb.Vector{
		Id: *id, Values: values, Metadata: parseKV(meta), TenantId: *tenant,
	}})
	if err != nil {
		return err
	}
	fmt.Printf("inserted id=%s\n", resp.GetId())
	return nil
}

func runBatchUpsert(addr string, args []string) error {
	fs := flag.NewFlagSet("batch-upsert", flag.ExitOnError)
	file := fs.String("file", "", `path to a JSON file: [{"id":"1","values":[0.1,0.2],"metadata":{"k":"v"}}, ...] (required)`)
	tenant := fs.String("tenant", "", "tenant id applied to every item that doesn't set its own \"tenant_id\"")
	_ = fs.Parse(args) // flag.ExitOnError means Parse never returns on error

	if *file == "" {
		return fmt.Errorf("batch-upsert: -file is required")
	}
	b, err := os.ReadFile(*file)
	if err != nil {
		return err
	}
	var items []struct {
		ID       string            `json:"id"`
		Values   []float32         `json:"values"`
		Metadata map[string]string `json:"metadata"`
		TenantID string            `json:"tenant_id"`
	}
	if err := json.Unmarshal(b, &items); err != nil {
		return fmt.Errorf("parsing %s: %w", *file, err)
	}

	vectors := make([]*pb.Vector, len(items))
	for i, it := range items {
		tenantID := it.TenantID
		if tenantID == "" {
			tenantID = *tenant
		}
		vectors[i] = &pb.Vector{Id: it.ID, Values: it.Values, Metadata: it.Metadata, TenantId: tenantID}
	}

	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewNuclaDBClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	resp, err := client.BatchUpsert(ctx, &pb.BatchUpsertRequest{Vectors: vectors})
	if err != nil {
		return err
	}
	fmt.Printf("upserted %d vectors\n", resp.GetUpserted())
	return nil
}

func runSearch(addr string, args []string) error {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	vec := fs.String("vector", "", "comma-separated float32 query vector (required)")
	topK := fs.Int("top-k", 10, "number of nearest neighbors to return")
	ef := fs.Int("ef", 0, "candidate beam width (0 = use top-k)")
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	var filters kvFlags
	fs.Var(&filters, "filter", "metadata key=value the result must match; repeatable")
	_ = fs.Parse(args) // flag.ExitOnError means Parse never returns on error

	if *vec == "" {
		return fmt.Errorf("search: -vector is required")
	}
	query, err := parseVector(*vec)
	if err != nil {
		return err
	}

	var pbFilters []*pb.MetadataFilter
	for k, v := range parseKV(filters) {
		pbFilters = append(pbFilters, &pb.MetadataFilter{Key: k, Value: v})
	}

	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewNuclaDBClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	resp, err := client.Search(ctx, &pb.SearchRequest{
		Query: query, TopK: int32(*topK), EfSearch: int32(*ef), Filters: pbFilters, TenantId: *tenant,
	})
	if err != nil {
		return err
	}
	for _, m := range resp.GetMatches() {
		fmt.Printf("%s\tscore=%.6f\t%v\n", m.GetId(), m.GetScore(), m.GetMetadata())
	}
	return nil
}

func runDelete(addr string, args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	id := fs.String("id", "", "vector id to delete (required)")
	tenant := fs.String("tenant", "", "tenant id (default: the reserved \"default\" tenant)")
	_ = fs.Parse(args) // flag.ExitOnError means Parse never returns on error

	if *id == "" {
		return fmt.Errorf("delete: -id is required")
	}

	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := pb.NewNuclaDBClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	resp, err := client.Delete(ctx, &pb.DeleteRequest{Id: *id, TenantId: *tenant})
	if err != nil {
		return err
	}
	fmt.Printf("deleted=%v\n", resp.GetDeleted())
	return nil
}

func runPing(addr string, args []string) error {
	conn, err := dial(addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	client := healthpb.NewHealthClient(conn)

	ctx, cancel := ctxWithTimeout()
	defer cancel()
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server reports status %s", resp.GetStatus())
	}
	fmt.Println("ok")
	return nil
}
