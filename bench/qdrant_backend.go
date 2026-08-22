package bench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"
)

// QdrantBackend drives a real Qdrant subprocess over its REST API — the
// same interface any Qdrant client uses.
type QdrantBackend struct {
	cmd        *exec.Cmd
	baseURL    string
	collection string
	httpClient *http.Client
}

// StartQdrant launches the qdrant binary at binPath with a fresh storage
// dir and an HNSW config matching NuclaDB's (m, efConstruct), waits for
// it to accept connections, and creates the benchmark collection.
// distance must be Qdrant's name for the metric ("Euclid", "Cosine", or
// "Dot") matching how groundtruth was computed.
func StartQdrant(binPath, storageDir string, dim int, distance string, httpPort, grpcPort int, m, efConstruct int) (*QdrantBackend, error) {
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
	if err := b.createCollection(dim, distance, m, efConstruct); err != nil {
		_ = cmd.Process.Kill()
		return nil, err
	}
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

func (b *QdrantBackend) createCollection(dim int, distance string, m, efConstruct int) error {
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
		},
	}
	return b.doJSON(http.MethodPut, "/collections/"+b.collection, body, nil)
}

func (b *QdrantBackend) Name() string { return "Qdrant" }

func (b *QdrantBackend) Upsert(vectors [][]float32) error {
	const batchSize = 500
	for start := 0; start < len(vectors); start += batchSize {
		end := start + batchSize
		if end > len(vectors) {
			end = len(vectors)
		}
		points := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			points = append(points, map[string]any{"id": i, "vector": vectors[i]})
		}
		body := map[string]any{"points": points}
		if err := b.doJSON(http.MethodPut, "/collections/"+b.collection+"/points?wait=true", body, nil); err != nil {
			return err
		}
	}
	return nil
}

type qdrantSearchResponse struct {
	Result []struct {
		ID any `json:"id"`
	} `json:"result"`
}

func (b *QdrantBackend) Search(query []float32, topK, ef int) ([]uint64, error) {
	body := map[string]any{
		"vector": query,
		"limit":  topK,
		"params": map[string]any{"hnsw_ef": ef},
	}
	var resp qdrantSearchResponse
	if err := b.doJSON(http.MethodPost, "/collections/"+b.collection+"/points/search", body, &resp); err != nil {
		return nil, err
	}
	ids := make([]uint64, len(resp.Result))
	for i, r := range resp.Result {
		// Qdrant returns numeric point ids as JSON numbers (float64 once
		// decoded into `any`).
		f, ok := r.ID.(float64)
		if !ok {
			return nil, fmt.Errorf("bench: unexpected qdrant point id type %T", r.ID)
		}
		ids[i] = uint64(f)
	}
	return ids, nil
}

func (b *QdrantBackend) RSSBytes() (uint64, error) {
	return rssBytesForPID(b.cmd.Process.Pid)
}

// Close terminates the qdrant subprocess.
func (b *QdrantBackend) Close() error {
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
