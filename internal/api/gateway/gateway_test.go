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
	return New(grpcapi.New(store, pb.DistanceMetric_DISTANCE_METRIC_L2), 1<<20)
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

func TestRESTRejectsOversizedBodyAndHugeTopK(t *testing.T) {
	h := newTestHandler(t) // 1 MiB body limit

	big := make([]float32, 1_000_000) // about 2 MB of JSON
	if code, _ := doJSON(t, h, "POST", "/v1/vectors", map[string]any{"id": "1", "values": big}); code != 413 {
		t.Fatalf("oversized body: code=%d, want 413", code)
	}
	for _, body := range []map[string]any{
		{"query": []float32{1, 0, 0, 0}, "top_k": grpcapi.MaxTopK + 1},
		{"query": []float32{1, 0, 0, 0}, "top_k": 1, "ef_search": grpcapi.MaxEf + 1},
	} {
		if code, _ := doJSON(t, h, "POST", "/v1/search", body); code != 400 {
			t.Fatalf("search %v: code=%d, want 400", body, code)
		}
	}
}

func TestRESTGetListCountUpdateAndTenantAdmin(t *testing.T) {
	h := newTestHandler(t)
	for i, v := range [][]float32{{1, 0, 0, 0}, {0, 1, 0, 0}, {0, 0, 1, 0}} {
		id := string(rune('1' + i))
		if code, resp := doJSON(t, h, "POST", "/v1/vectors", map[string]any{"id": id, "values": v}); code != 200 {
			t.Fatalf("insert %s: %d %v", id, code, resp)
		}
	}

	code, resp := doJSON(t, h, "PATCH", "/v1/vectors/2", map[string]any{"metadata": map[string]string{"k": "v"}})
	if code != 200 {
		t.Fatalf("patch: %d %v", code, resp)
	}
	code, resp = doJSON(t, h, "GET", "/v1/vectors/2", nil)
	vec, _ := resp["vector"].(map[string]any)
	if code != 200 || vec["metadata"].(map[string]any)["k"] != "v" {
		t.Fatalf("get: %d %v", code, resp)
	}
	if code, _ := doJSON(t, h, "GET", "/v1/vectors/99", nil); code != 404 {
		t.Fatalf("get missing: %d", code)
	}

	code, resp = doJSON(t, h, "GET", "/v1/vectors?page_size=2", nil)
	if code != 200 || len(resp["ids"].([]any)) != 2 || resp["next_page_token"] != "3" {
		t.Fatalf("list page 1: %d %v", code, resp)
	}
	code, resp = doJSON(t, h, "GET", "/v1/vectors?page_size=2&page_token=3", nil)
	if code != 200 || len(resp["ids"].([]any)) != 1 || resp["next_page_token"] != nil {
		t.Fatalf("list page 2: %d %v", code, resp)
	}
	if code, resp = doJSON(t, h, "GET", "/v1/vectors:count", nil); code != 200 || resp["count"] != float64(3) {
		t.Fatalf("count: %d %v", code, resp)
	}

	if code, resp = doJSON(t, h, "POST", "/v1/tenants", map[string]any{"tenant_id": "t1"}); code != 200 {
		t.Fatalf("create tenant: %d %v", code, resp)
	}
	if code, resp = doJSON(t, h, "PUT", "/v1/tenants/t1/quota", map[string]any{"max_vectors": 5}); code != 200 {
		t.Fatalf("set quota: %d %v", code, resp)
	}
	code, resp = doJSON(t, h, "GET", "/v1/tenants", nil)
	tenants, _ := resp["tenants"].([]any)
	if code != 200 || len(tenants) != 2 {
		t.Fatalf("list tenants: %d %v", code, resp)
	}
	if code, _ := doJSON(t, h, "DELETE", "/v1/tenants/t1", nil); code != 200 {
		t.Fatalf("delete tenant: %d", code)
	}
	if code, _ := doJSON(t, h, "DELETE", "/v1/tenants/default", nil); code != 400 {
		t.Fatalf("delete default tenant: %d, want 400", code)
	}
}

func TestRESTWhereFilters(t *testing.T) {
	h := newTestHandler(t)
	for i, price := range []string{"5", "15", "25"} {
		v := []float32{float32(i + 1), 1, 0, 0}
		if code, resp := doJSON(t, h, "POST", "/v1/vectors", map[string]any{
			"id": string(rune('1' + i)), "values": v, "metadata": map[string]string{"price": price},
		}); code != 200 {
			t.Fatalf("insert: %d %v", code, resp)
		}
	}
	code, resp := doJSON(t, h, "POST", "/v1/search", map[string]any{
		"query": []float32{1, 1, 0, 0}, "top_k": 3,
		"where": []map[string]any{{"key": "price", "op": "gte", "value": "15"}},
	})
	if code != 200 || len(resp["matches"].([]any)) != 2 {
		t.Fatalf("where gte: %d %v", code, resp)
	}
	if code, _ := doJSON(t, h, "POST", "/v1/search", map[string]any{
		"query": []float32{1, 1, 0, 0}, "top_k": 3,
		"where": []map[string]any{{"key": "price", "op": "between", "value": "1"}},
	}); code != 400 {
		t.Fatalf("unknown op: %d, want 400", code)
	}
}
