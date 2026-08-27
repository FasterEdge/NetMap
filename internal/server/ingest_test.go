package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/FasterEdge/NetMap/internal/store"
)

// validEnvelope returns a freshly serialised topology envelope that the
// ingestion handler should accept.
func validEnvelope(t *testing.T) []byte {
	t.Helper()
	body := map[string]any{
		"version": "1",
		"source":  "test-source",
		"self": map[string]any{
			"nodeName":      "edge-1",
			"defaultIface":  "eth0",
			"interfaces":    []map[string]any{{"name": "eth0", "mac": "aa:bb:cc", "ipv4": []string{"10.0.0.1"}}},
			"hostAddresses": []string{"10.0.0.1"},
			"scannedAt":     "2026-08-26T00:00:00Z",
		},
		"peers": []map[string]any{
			{"name": "peer-a", "address": "10.0.0.2:7000", "role": "edge"},
		},
		"issuedAt": "2026-08-26T00:00:00Z",
	}
	buf, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return buf
}

func TestIngestMissingTokenReturns404(t *testing.T) {
	srv := newTestServer(t) // no token configured
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %q", rec.Code, rec.Body.String())
	}
}

func TestIngestRequiresBearerToken(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "secret-token" })

	cases := []struct {
		name    string
		headers map[string]string
		want    int
	}{
		{"no header", nil, http.StatusUnauthorized},
		{"wrong scheme", map[string]string{"Authorization": "Basic abc"}, http.StatusUnauthorized},
		{"wrong token", map[string]string{"Authorization": "Bearer nope"}, http.StatusUnauthorized},
		{"correct", map[string]string{"Authorization": "Bearer secret-token"}, http.StatusAccepted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d, body=%q", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestIngestRejectsWrongMethod(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	req := httptest.NewRequest(http.MethodPut, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec.Header().Get("Allow") == "" {
		t.Fatal("missing Allow header")
	}
}

func TestIngestRejectsBadContentType(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsOversizedBody(t *testing.T) {
	srv := newTestServer(t, func(c *Config) {
		c.IngestToken = "token"
		c.MaxIngestBytes = 64
	})
	// Build a body larger than 64 bytes.
	body := bytes.Repeat([]byte("a"), 128)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsInvalidJSON(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", strings.NewReader("{not-json"))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsUnsupportedVersion(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	body := map[string]any{
		"version": "999",
		"self":    map[string]any{"nodeName": "x"},
		"peers":   []any{},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "unsupported version") {
		t.Fatalf("expected unsupported version error, got %q", rec.Body.String())
	}
}

func TestIngestRejectsMissingSelf(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	body := map[string]any{
		"version": "1",
		"self":    map[string]any{"nodeName": ""},
		"peers":   []any{},
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestRejectsEmptyBody(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(nil))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d", rec.Code)
	}
}

func TestIngestAcceptsValidPayload(t *testing.T) {
	// Build a fresh server with an empty store so the assertion is
	// deterministic.
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	srv.store = newEmptyStore()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
	req.Header.Set("Authorization", "Bearer token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
	body, _ := io.ReadAll(rec.Body)
	var ack map[string]any
	if err := json.Unmarshal(body, &ack); err != nil {
		t.Fatal(err)
	}
	if ack["status"] != "accepted" {
		t.Fatalf("status = %v", ack["status"])
	}
	// Peer + self should be merged into the store.
	peers := srv.store.AllPeers()
	if len(peers) != 1 {
		t.Fatalf("peers = %d", len(peers))
	}
	if peers[0].Name != "peer-a" {
		t.Fatalf("peer = %+v", peers[0])
	}
}

func TestIngestAcceptsMissingContentType(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.IngestToken = "token" })
	// No Content-Type: handler should still accept the body because the
	// spec only treats a present-but-wrong content-type as an error.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/topology", bytes.NewReader(validEnvelope(t)))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body=%q", rec.Code, rec.Body.String())
	}
}

// newEmptyStore returns a store.Store with no default peers so tests can
// assert exact peer counts after a single ingest.
func newEmptyStore() *store.Store { return store.New() }
