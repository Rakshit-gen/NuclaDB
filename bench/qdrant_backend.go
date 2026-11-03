package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/qdrant/go-client/qdrant"
)

// QdrantBackend drives a real Qdrant subprocess. Upsert and Search, the
// timed paths, go over Qdrant's gRPC API with its official Go client, the
// same transport the NuclaDB backend uses, so neither side pays for JSON.
// Collection setup and the index-progress poll stay on REST.
type QdrantBackend struct {
	cmd        *exec.Cmd
	baseURL    string
	collection string
	httpClient *http.Client
	grpc       *qdrant.Client
}

// StartQdrant launches the qdrant binary at binPath with a fresh storage
// dir and an HNSW config matching NuclaDB's (m, efConstruct), waits for
// it to accept connections, and creates the benchmark collection.
// distance must be Qdrant's name for the metric ("Euclid", "Cosine", or
// "Dot") matching how groundtruth was computed.
func StartQdrant(binPath, storageDir string, dim int, distance string, httpPort, grpcPort int, m, efConstruct, threads int) (*QdrantBackend, error) {
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("QDRANT__STORAGE__STORAGE_PATH=%s", storageDir),
		fmt.Sprintf("QDRANT__STORAGE__SNAPSHOTS_PATH=%s/snapshots", storageDir),
		fmt.Sprintf("QDRANT__SERVICE__HTTP_PORT=%d", httpPort),
		fmt.Sprintf("QDRANT__SERVICE__GRPC_PORT=%d", grpcPort),
		"QDRANT__LOG_LEVEL=warn",
		"QDRANT__TELEMETRY_DISABLED=true",
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting qdrant: %w", err)
	}

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	b := &QdrantBackend{cmd: cmd, baseURL: baseURL, collection: "bench", httpClient: &http.Client{Timeout: 30 * time.Second}}

	if err := b.waitReady(10 * time.Second); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	if err := b.createCollection(dim, distance, m, efConstruct, threads); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
	client, err := qdrant.NewClient(&qdrant.Config{Host: "127.0.0.1", Port: grpcPort})
	if err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("qdrant gRPC client: %w", err)
	}
	b.grpc = client
	return b, nil
}

func (b *QdrantBackend) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := b.httpClient.Get(b.baseURL + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("qdrant did not become ready within %s: %w", timeout, lastErr)
}

func (b *QdrantBackend) createCollection(dim int, distance string, m, efConstruct, threads int) error {
	body := map[string]any{
		"vectors": map[string]any{
			"size":     dim,
			"distance": distance,
		},
		"hnsw_config": map[string]any{
			"m":            m,
			"ef_construct": efConstruct,
			// Qdrant falls back to exact brute-force search below this
			// size (in KB) rather than using HNSW at all — its default
			// is 10,000 KB, comfortably above a 10K-vector, 128-dim,
			// float32 dataset (~5,000 KB), which would silently turn
			// this into an exact-vs-approximate comparison instead of
			// approximate-vs-approximate. 10 is Qdrant's own API-enforced
			// minimum; setting it there forces real HNSW search
			// regardless of collection size, matching what NuclaDB
			// always does.
			"full_scan_threshold": 10,
			// 0 is Qdrant's "pick for me"; the harness passes the same cap
			// it gives nucladbd's GOMAXPROCS.
			"max_indexing_threads": threads,
		},
		// Qdrant only builds an HNSW index for a segment once it passes
		// indexing_threshold (KB). The default, 10,000 KB in 1.19, is again
		// above this dataset's size, so without this Qdrant never built an
		// index at all: indexed_vectors_count stayed 0, every search was
		// brute force, and "build time" was just point ingest. 1 KB makes
		// it index everything.
		"optimizers_config": map[string]any{
			"indexing_threshold": 1,
			// One segment, so one HNSW graph, like NuclaDB. By default
			// Qdrant splits a collection into several segments (5 on a
			// 10-core machine), each its own graph, and searches every one
			// at the requested ef before merging. At SIFT-1M that made its
			// ef=10 roughly five ef=10 searches over 200K-vector graphs, so
			// "same ef" compared different amounts of work. The size cap
			// (KB) is raised so the optimizer won't split it back up.
			"default_segment_number": 1,
			"max_segment_size":       4_000_000,
		},
	}
	return b.doJSON(http.MethodPut, "/collections/"+b.collection, body, nil)
}

func (b *QdrantBackend) Name() string { return "Qdrant" }

func (b *QdrantBackend) Upsert(vectors [][]float32) error {
	const batchSize = 500
	wait := true
	for start := 0; start < len(vectors); start += batchSize {
		end := min(start+batchSize, len(vectors))
		points := make([]*qdrant.PointStruct, 0, end-start)
		for i := start; i < end; i++ {
			points = append(points, &qdrant.PointStruct{Id: qdrant.NewIDNum(uint64(i)), Vectors: qdrant.NewVectors(vectors[i]...)})
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_, err := b.grpc.Upsert(ctx, &qdrant.UpsertPoints{CollectionName: b.collection, Wait: &wait, Points: points})
		cancel()
		if err != nil {
			return err
		}
	}
	return b.waitIndexed(len(vectors), 30*time.Minute)
}

// waitIndexed blocks until Qdrant reports every vector in its HNSW index
// and the collection green. wait=true on upsert only waits for the write,
// while the index is built in the background, so build time has to
// include this wait to compare with NuclaDB, which indexes on insert.
func (b *QdrantBackend) waitIndexed(n int, timeout time.Duration) error {
	var info struct {
		Result struct {
			Status              string `json:"status"`
			IndexedVectorsCount int    `json:"indexed_vectors_count"`
		} `json:"result"`
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := b.doJSON(http.MethodGet, "/collections/"+b.collection, nil, &info); err != nil {
			return err
		}
		if info.Result.Status == "green" && info.Result.IndexedVectorsCount >= n {
			return b.checkOneGraph(n)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("qdrant indexed %d of %d vectors within %s", info.Result.IndexedVectorsCount, n, timeout)
}

// checkOneGraph confirms one indexed segment holds all n points. Qdrant
// also keeps an empty appendable segment for new writes, so the plain
// segments_count is 2 even when the data is in one graph.
func (b *QdrantBackend) checkOneGraph(n int) error {
	var t struct {
		Result struct {
			Collections struct {
				Collections []struct {
					Shards []struct {
						Local struct {
							Segments []struct {
								Info struct {
									NumPoints int `json:"num_points"`
								} `json:"info"`
							} `json:"segments"`
						} `json:"local"`
					} `json:"shards"`
				} `json:"collections"`
			} `json:"collections"`
		} `json:"result"`
	}
	if err := b.doJSON(http.MethodGet, "/telemetry?details_level=10", nil, &t); err != nil {
		return err
	}
	var withPoints []int
	for _, c := range t.Result.Collections.Collections {
		for _, sh := range c.Shards {
			for _, seg := range sh.Local.Segments {
				if seg.Info.NumPoints > 0 {
					withPoints = append(withPoints, seg.Info.NumPoints)
				}
			}
		}
	}
	if len(withPoints) != 1 || withPoints[0] != n {
		return fmt.Errorf("qdrant points per non-empty segment = %v, want one segment of %d", withPoints, n)
	}
	return nil
}

func (b *QdrantBackend) Search(query []float32, topK, ef int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hnswEf := uint64(ef)
	resp, err := b.grpc.GetPointsClient().Search(ctx, &qdrant.SearchPoints{
		CollectionName: b.collection,
		Vector:         query,
		Limit:          uint64(topK),
		Params:         &qdrant.SearchParams{HnswEf: &hnswEf},
	})
	if err != nil {
		return nil, err
	}
	ids := make([]uint64, len(resp.GetResult()))
	for i, p := range resp.GetResult() {
		ids[i] = p.GetId().GetNum()
	}
	return ids, nil
}

func (b *QdrantBackend) RSSBytes() (uint64, error) {
	return rssBytesForPID(b.cmd.Process.Pid)
}

// Close terminates the qdrant subprocess and closes the gRPC client.
func (b *QdrantBackend) Close() error {
	_ = b.grpc.Close()
	if err := b.cmd.Process.Kill(); err != nil {
		return err
	}
	_ = b.cmd.Wait()
	return nil
}

func (b *QdrantBackend) doJSON(method, path string, body, out any) error {
	var reader *bytes.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(buf)
	} else {
		reader = bytes.NewReader(nil)
	}

	req, err := http.NewRequest(method, b.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		var errBody bytes.Buffer
		_, _ = errBody.ReadFrom(resp.Body)
		return fmt.Errorf("qdrant %s %s: HTTP %d: %s", method, path, resp.StatusCode, errBody.String())
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
