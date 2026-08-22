package gateway

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	grpcapi "github.com/Rakshit-gen/nucladb/internal/api/grpc"
	"github.com/Rakshit-gen/nucladb/internal/engine"
	"github.com/Rakshit-gen/nucladb/internal/index/hnsw"
	pb "github.com/Rakshit-gen/nucladb/proto/nucladbv1"
)

func newTestHandler(t *testing.T) *Handler {
	t.Helper()
	store, err := engine.OpenStore(t.TempDir(), hnsw.Config{Dim: 4, M: 16, EfConstruction: 100, Metric: hnsw.L2(), Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return New(grpcapi.New(store, pb.DistanceMetric_DISTANCE_METRIC_L2))
}

func doJSON(t *testing.T, h *Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	var out map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("response not JSON: %s (%v)", rec.Body.String(), err)
		}
	}
	return rec.Code, out
}

func TestRESTInsertSearchDelete(t *testing.T) {
	h := newTestHandler(t)

	code, resp := doJSON(t, h, "POST", "/v1/vectors", map[string]any{
		"id": "1", "values": []float32{1, 0, 0, 0}, "metadata": map[string]string{"team": "search"},
	})
	if code != 200 || resp["id"] != "1" {
		t.Fatalf("insert: code=%d resp=%v", code, resp)
	}

	code, resp = doJSON(t, h, "POST", "/v1/search", map[string]any{
		"query": []float32{1, 0, 0, 0}, "top_k": 1,
	})
	if code != 200 {
		t.Fatalf("search: code=%d resp=%v", code, resp)
	}
	matches, _ := resp["matches"].([]any)
	if len(matches) != 1 {
		t.Fatalf("search: expected 1 match, got %v", resp)
	}

	code, resp = doJSON(t, h, "DELETE", "/v1/vectors/1", nil)
	if code != 200 || resp["deleted"] != true {
		t.Fatalf("delete: code=%d resp=%v", code, resp)
	}
}

func TestRESTBatchUpsert(t *testing.T) {
	h := newTestHandler(t)

	code, resp := doJSON(t, h, "POST", "/v1/vectors:batch", map[string]any{
		"vectors": []map[string]any{
			{"id": "1", "values": []float32{1, 0, 0, 0}},
			{"id": "2", "values": []float32{0, 1, 0, 0}},
		},
	})
	if code != 200 || resp["upserted"] != float64(2) {
		t.Fatalf("batch-upsert: code=%d resp=%v", code, resp)
	}
}

func TestRESTBadRequest(t *testing.T) {
	h := newTestHandler(t)
	code, resp := doJSON(t, h, "POST", "/v1/vectors", map[string]any{
		"id": "not-a-number", "values": []float32{1, 0, 0, 0},
	})
	if code != 400 {
		t.Fatalf("expected 400 for a non-numeric id, got %d (%v)", code, resp)
	}
}
