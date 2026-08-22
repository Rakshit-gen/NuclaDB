package bench

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

// NuclaDBBackend drives a real nucladbd subprocess over gRPC — the same
// path any client uses, not an in-process shortcut through the graph.
type NuclaDBBackend struct {
	cmd    *exec.Cmd
	conn   *grpc.ClientConn
	client pb.NuclaDBClient
}

// StartNuclaDB launches the nucladbd binary at binPath against a fresh
// dataDir, waits for it to accept connections, and returns a ready
// backend. metric must match how groundtruth was computed (SIFT's
// groundtruth is Euclidean/L2).
func StartNuclaDB(binPath, dataDir string, dim int, metric string, grpcAddr string) (*NuclaDBBackend, error) {
	cmd := exec.Command(binPath,
		"-data-dir", dataDir,
		"-dim", strconv.Itoa(dim),
		"-metric", metric,
		"-grpc-addr", grpcAddr,
		"-http-addr", ":0",
	)
	cmd.Stdout = os.Stderr // keep server logs out of stdout, which carries the report
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting nucladbd: %w", err)
	}

	conn, err := dialWithRetry(grpcAddr, 10*time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}

	return &NuclaDBBackend{cmd: cmd, conn: conn, client: pb.NewNuclaDBClient(conn)}, nil
}

// dialWithRetry connects to addr and polls the standard gRPC health
// service until it reports SERVING — grpc.NewClient itself dials lazily
// and essentially never fails up front, so it can't be used alone to tell
// "the subprocess is still starting up" from "the server is ready."
func dialWithRetry(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", addr, err)
	}
	health := healthpb.NewHealthClient(conn)

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		resp, err := health.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return conn, nil
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	_ = conn.Close()
	return nil, fmt.Errorf("nucladbd did not become ready within %s: %w", timeout, lastErr)
}

func (b *NuclaDBBackend) Name() string { return "NuclaDB" }

func (b *NuclaDBBackend) Upsert(vectors [][]float32) error {
	const batchSize = 500
	for start := 0; start < len(vectors); start += batchSize {
		end := min(start+batchSize, len(vectors))
		batch := make([]*pb.Vector, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, &pb.Vector{Id: strconv.Itoa(i), Values: vectors[i]})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := b.client.BatchUpsert(ctx, &pb.BatchUpsertRequest{Vectors: batch})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *NuclaDBBackend) Search(query []float32, topK, ef int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := b.client.Search(ctx, &pb.SearchRequest{
		Query: query, TopK: int32(topK), EfSearch: int32(ef),
	})
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(resp.GetMatches()))
	for i, m := range resp.GetMatches() {
		id, err := strconv.ParseUint(m.GetId(), 10, 64)
		if err != nil {
			return nil, err
		}
		ids[i] = id
	}
	return ids, nil
}

func (b *NuclaDBBackend) RSSBytes() (uint64, error) {
	return rssBytesForPID(b.cmd.Process.Pid)
}

// Close terminates the nucladbd subprocess and closes the client
// connection.
func (b *NuclaDBBackend) Close() error {
	_ = b.conn.Close()
	if err := b.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = b.cmd.Wait()
	return nil
}
