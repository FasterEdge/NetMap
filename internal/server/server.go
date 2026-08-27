// Package server implements the NetMap HTTP control plane. It owns a real
// *http.Server with explicit timeouts, ingests topology POSTs from
// authenticated FasterEdge nodes and serves read-only APIs to the web UI.
package server

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
	"github.com/FasterEdge/NetMap/internal/store"
)

//go:embed web
var webFS embed.FS

var indexHTML []byte

func init() {
	var err error
	indexHTML, err = webFS.ReadFile("web/index.html")
	if err != nil {
		log.Printf("warning: embedded index.html not found: %v", err)
	}
}

// Server is the HTTP control plane.
type Server struct {
	store      *store.Store
	reg        *source.Registry
	addr       string
	origin     string
	ingestTok  string
	maxIngest  int64
	mux        *http.ServeMux
	httpServer *http.Server
	startedAt  time.Time
	routesOnce sync.Once

	// reqCount is incremented atomically for /healthz.
	reqCount atomic.Uint64
}

// Config groups every dial that affects the server's runtime behaviour.
type Config struct {
	Addr              string
	AllowOrigin       string
	IngestToken       string
	MaxIngestBytes    int64
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	ShutdownTimeout   time.Duration
}

// DefaultConfig returns a Config with safe production defaults.
func DefaultConfig() Config {
	return Config{
		Addr:              ":8080",
		MaxIngestBytes:    4 << 20, // 4 MiB
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MiB
		ShutdownTimeout:   15 * time.Second,
	}
}

// New builds a Server from a Config.
func New(st *store.Store, reg *source.Registry, cfg Config) *Server {
	if cfg.MaxIngestBytes <= 0 {
		cfg.MaxIngestBytes = 4 << 20
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = 10 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 120 * time.Second
	}
	if cfg.MaxHeaderBytes <= 0 {
		cfg.MaxHeaderBytes = 1 << 20
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	return &Server{
		store:     st,
		reg:       reg,
		addr:      cfg.Addr,
		origin:    cfg.AllowOrigin,
		ingestTok: cfg.IngestToken,
		maxIngest: cfg.MaxIngestBytes,
		mux:       http.NewServeMux(),
		startedAt: time.Now(),
	}
}

// routes wires the HTTP handlers onto the mux. The mux is created in New
// and routes are only registered once across the lifetime of a Server;
// Handler/Start both call this defensively but the underlying
// http.ServeMux panics on duplicate registration.
func (s *Server) routes() {
	s.routesOnce.Do(func() {
		s.mux.HandleFunc("/api/topology", s.handleTopology)
		s.mux.HandleFunc("/api/peers", s.handlePeers)
		s.mux.HandleFunc("/api/self", s.handleSelf)
		s.mux.HandleFunc("/api/healthz", s.handleHealthz)
		s.mux.HandleFunc("/api/v1/topology", s.handleIngest)
		s.mux.HandleFunc("/api/v1/sources", s.handleSources)
		s.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" && r.URL.Path != "/index.html" {
				http.NotFound(w, r)
				return
			}
			if indexHTML == nil {
				http.Error(w, "index.html not found", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			http.ServeContent(w, r, "index.html", time.Now(), bytes.NewReader(indexHTML))
		})
	})
}

// buildHTTPServer assembles the *http.Server from current Config values.
func (s *Server) buildHTTPServer() *http.Server {
	return &http.Server{
		Addr:              s.addr,
		Handler:           s.preflightHandler(s.withCORS(s.mux)),
		ReadHeaderTimeout: s.timing("read_header_timeout", 10*time.Second),
		ReadTimeout:       s.timing("read_timeout", 30*time.Second),
		WriteTimeout:      s.timing("write_timeout", 30*time.Second),
		IdleTimeout:       s.timing("idle_timeout", 120*time.Second),
		MaxHeaderBytes:    s.intVal("max_header_bytes", 1<<20),
	}
}

// Serve binds and serves until ctx is cancelled, at which point it calls
// Shutdown with a bounded timeout and waits for in-flight requests to
// drain. The returned error is non-nil only when the listener failed to
// bind; http.ErrServerClosed on shutdown is swallowed.
func (s *Server) Serve(ctx context.Context) error {
	s.routes()
	s.httpServer = s.buildHTTPServer()
	log.Printf("NetMap listening on %s", s.addr)
	errCh := make(chan error, 1)
	go func() {
		err := s.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.timing("shutdown_timeout", 15*time.Second))
		defer cancel()
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
			return err
		}
		return <-errCh
	case err := <-errCh:
		return err
	}
}

// Shutdown stops the server using the configured timeout.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, s.timing("shutdown_timeout", 15*time.Second))
	defer cancel()
	return s.httpServer.Shutdown(shutdownCtx)
}

// ServeHTTP exposes the configured mux so tests can drive the handlers
// directly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.routes()
	s.preflightHandler(s.withCORS(s.mux)).ServeHTTP(w, r)
}

// Handler returns the wrapped handler (CORS + preflight). Useful for
// tests and embedders.
func (s *Server) Handler() http.Handler {
	s.routes()
	return s.preflightHandler(s.withCORS(s.mux))
}

// timing reads a test-overridden duration from testTimeouts, falling back
// to def when the key is absent.
func (s *Server) timing(key string, def time.Duration) time.Duration {
	if v, ok := testTimeouts.Load(key); ok {
		if d, ok := v.(time.Duration); ok {
			return d
		}
	}
	return def
}

func (s *Server) intVal(key string, def int) int {
	if v, ok := testTimeouts.Load(key); ok {
		if n, ok := v.(int); ok {
			return n
		}
	}
	return def
}

// testTimeouts lets tests tweak server timeouts without exposing them as
// public fields.
var testTimeouts = newTestTimeouts()

func newTestTimeouts() *syncMap {
	return &syncMap{}
}

// syncMap is a tiny typed wrapper around sync.Map so callers don't have
// to repeat the type assertion dance.
type syncMap struct{ m sync.Map }

func (s *syncMap) Load(key string) (any, bool) { return s.m.Load(key) }
func (s *syncMap) Store(key string, v any)     { s.m.Store(key, v) }
func (s *syncMap) Delete(key string)           { s.m.Delete(key) }

// ----------------------------------------------------------------------
// Read handlers

func (s *Server) handleTopology(w http.ResponseWriter, _ *http.Request) {
	s.reqCount.Add(1)
	s.writeJSON(w, http.StatusOK, s.store.Topology())
}

func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	s.reqCount.Add(1)
	s.writeJSON(w, http.StatusOK, s.store.AllPeers())
}

func (s *Server) handleSelf(w http.ResponseWriter, _ *http.Request) {
	s.reqCount.Add(1)
	top := s.store.Topology()
	s.writeJSON(w, http.StatusOK, top.Self)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	s.reqCount.Add(1)
	s.writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ok",
		"uptime":    time.Since(s.startedAt).String(),
		"peers":     len(s.store.AllPeers()),
		"sources":   len(s.reg.Names()),
		"requests":  s.reqCount.Load(),
		"version":   "1.0.0",
		"startedAt": s.startedAt.UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) handleSources(w http.ResponseWriter, _ *http.Request) {
	s.reqCount.Add(1)
	provenance := s.reg.Snapshot()
	out := make([]map[string]any, 0, len(provenance))
	for _, p := range provenance {
		entry := map[string]any{
			"name":                p.Name,
			"url":                 p.URL,
			"status":              p.Status().String(),
			"lastAttempt":         nullTime(p.LastAttempt),
			"lastSuccess":         nullTime(p.LastSuccess),
			"consecutiveFailures": p.ConsecutiveFailures,
			"totalSuccesses":      p.TotalSuccesses,
			"totalFailures":       p.TotalFailures,
		}
		if p.LastError != "" {
			entry["lastError"] = p.LastError
			entry["lastErrorAt"] = nullTime(p.LastErrorAt)
		}
		out = append(out, entry)
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"sources": out})
}

// writeJSON marshals v to w with the given code. It never panics; on
// encoding failure it logs.
func (s *Server) writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json: %v", err)
	}
}

// writeError emits a small structured error payload that downstream tools
// can parse deterministically.
func (s *Server) writeError(w http.ResponseWriter, code int, msg string) {
	s.writeJSON(w, code, map[string]any{
		"error": msg,
		"code":  code,
		"at":    time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// CORS middleware — only adds headers when origin is configured.
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}
		next.ServeHTTP(w, r)
	})
}

// preflightHandler short-circuits OPTIONS preflights with 204 + CORS
// headers.
func (s *Server) preflightHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			if s.origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", s.origin)
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// isAuthed returns true when the Authorization header carries a matching
// bearer token. The comparison uses constant time to avoid leaking the
// token length via timing.
func (s *Server) isAuthed(r *http.Request) bool {
	if s.ingestTok == "" {
		return false
	}
	hdr := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(hdr, prefix) {
		return false
	}
	got := strings.TrimPrefix(hdr, prefix)
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.ingestTok)) == 1
}

// m is referenced to keep the import list stable when handlers shrink.
var _ sync.Map
