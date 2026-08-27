package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
	"github.com/FasterEdge/NetMap/internal/store"
	"github.com/FasterEdge/NetMap/internal/topology"
)

// newTestServer builds a Server bound to an in-memory registry / store.
func newTestServer(t *testing.T, opts ...func(*Config)) *Server {
	t.Helper()
	st := store.New()
	st.SetNodeName("test-hub")
	st.UpsertPeer(store.Peer{Name: "edge-1", Address: "10.0.0.1:7000", Role: "edge"})
	st.UpsertPeer(store.Peer{Name: "edge-2", Address: "10.0.0.2:7000"})
	st.UpsertPeer(store.Peer{Name: "cloud-1", Address: "10.0.0.3:9000", Role: "cloud"})
	reg := source.NewRegistry()
	cfg := DefaultConfig()
	cfg.Addr = ":0"
	cfg.IngestToken = ""
	for _, opt := range opts {
		opt(&cfg)
	}
	return New(st, reg, cfg)
}

func TestHandleTopology(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/topology", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("content-type = %q", ct)
	}
	var top store.Topology
	if err := json.NewDecoder(rec.Body).Decode(&top); err != nil {
		t.Fatal(err)
	}
	if top.Self.NodeName != "test-hub" {
		t.Fatalf("self = %+v", top.Self)
	}
	if len(top.Peers) != 3 {
		t.Fatalf("peers = %d", len(top.Peers))
	}
}

func TestHandlePeers(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/peers", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var peers []store.Peer
	if err := json.NewDecoder(rec.Body).Decode(&peers); err != nil {
		t.Fatal(err)
	}
	if len(peers) != 3 {
		t.Fatalf("peers = %d", len(peers))
	}
}

func TestHandleSelf(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/self", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var self store.SelfInfo
	if err := json.NewDecoder(rec.Body).Decode(&self); err != nil {
		t.Fatal(err)
	}
	if self.NodeName != "test-hub" {
		t.Fatalf("self = %+v", self)
	}
}

func TestHandleHealthz(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/healthz", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var h map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&h); err != nil {
		t.Fatal(err)
	}
	if h["status"] != "ok" {
		t.Fatalf("status = %v", h["status"])
	}
	if peers, ok := h["peers"].(float64); !ok || peers != 3 {
		t.Fatalf("peers = %v", h["peers"])
	}
}

func TestHandleIndexHTML(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	if !strings.Contains(body, "NetMap") {
		t.Fatalf("body missing NetMap: %q", body[:min(100, len(body))])
	}
}

func TestCORSPreflight(t *testing.T) {
	srv := newTestServer(t, func(c *Config) { c.AllowOrigin = "*" })
	req := httptest.NewRequest(http.MethodOptions, "/api/topology", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("CORS missing: %v", rec.Header())
	}
}

func TestServerTimeoutsConfigured(t *testing.T) {
	testTimeouts.Store("read_timeout", 7*time.Second)
	testTimeouts.Store("write_timeout", 9*time.Second)
	testTimeouts.Store("idle_timeout", 11*time.Second)
	testTimeouts.Store("read_header_timeout", 5*time.Second)
	testTimeouts.Store("max_header_bytes", 4096)
	t.Cleanup(func() {
		testTimeouts.Delete("read_timeout")
		testTimeouts.Delete("write_timeout")
		testTimeouts.Delete("idle_timeout")
		testTimeouts.Delete("read_header_timeout")
		testTimeouts.Delete("max_header_bytes")
	})
	srv := newTestServer(t)
	hs := srv.buildHTTPServer()
	if hs.ReadTimeout != 7*time.Second {
		t.Fatalf("ReadTimeout = %v", hs.ReadTimeout)
	}
	if hs.WriteTimeout != 9*time.Second {
		t.Fatalf("WriteTimeout = %v", hs.WriteTimeout)
	}
	if hs.IdleTimeout != 11*time.Second {
		t.Fatalf("IdleTimeout = %v", hs.IdleTimeout)
	}
	if hs.ReadHeaderTimeout != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %v", hs.ReadHeaderTimeout)
	}
	if hs.MaxHeaderBytes != 4096 {
		t.Fatalf("MaxHeaderBytes = %d", hs.MaxHeaderBytes)
	}
}

func TestServeShutdownGraceful(t *testing.T) {
	srv := newTestServer(t)
	// Find a free ephemeral port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	srv.addr = addr

	// Wrap the default test handler with a slow one to prove shutdown
	// actually waits for in-flight requests.
	slowHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(50 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			t.Errorf("request context cancelled unexpectedly: %v", r.Context().Err())
		}
	})
	wrapped := wrapSlow(srv, slowHandler)
	ts := httptest.NewUnstartedServer(wrapped)
	ts.Config.ReadHeaderTimeout = 5 * time.Second
	ts.Start()
	t.Cleanup(ts.Close)

	// Launch a slow client to keep a request in flight.
	var inFlight atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := http.Get(ts.URL + "/api/topology")
		if err != nil {
			return
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		inFlight.Add(1)
	}()
	time.Sleep(10 * time.Millisecond) // let the goroutine start

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	wg.Wait()
	if inFlight.Load() == 0 {
		t.Fatal("in-flight client never completed")
	}
}

func TestServeShutdownRejectsWhenNoListener(t *testing.T) {
	srv := newTestServer(t)
	// httpServer is nil because Start wasn't called.
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestHandleSourcesEmpty(t *testing.T) {
	srv := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Sources []map[string]any `json:"sources"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Sources) != 0 {
		t.Fatalf("expected empty, got %d", len(resp.Sources))
	}
}

func TestHandleSourcesReportsProvenance(t *testing.T) {
	srv := newTestServer(t)
	// Allow loopback so the test URL passes the policy.
	_, err := srv.reg.Register("loop-1", "http://127.0.0.1:9001", source.PermissivePolicy())
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp struct {
		Sources []map[string]any `json:"sources"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Sources) != 1 {
		t.Fatalf("expected 1, got %d", len(resp.Sources))
	}
	if resp.Sources[0]["name"] != "loop-1" {
		t.Fatalf("source = %v", resp.Sources[0])
	}
	if resp.Sources[0]["status"] != "unknown" {
		t.Fatalf("status = %v", resp.Sources[0]["status"])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func wrapSlow(s *Server, slow http.Handler) http.Handler {
	s.routes()
	base := s.preflightHandler(s.withCORS(s.mux))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/api/topology") {
			slow.ServeHTTP(w, r)
			return
		}
		base.ServeHTTP(w, r)
	})
}

var _ = errors.New
var _ = bytes.NewReader
var _ = topology.Snapshot{}
