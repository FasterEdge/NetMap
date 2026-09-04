package poller

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
	"github.com/FasterEdge/NetMap/internal/topology"
)

// fakeSource is a tiny in-memory Source implementation for tests.
type fakeSource struct {
	name, url string
}

func (f fakeSource) Name() string { return f.name }
func (f fakeSource) URL() string  { return f.url }

func newRegistry(t *testing.T, urls ...string) *source.Registry {
	t.Helper()
	r := source.NewRegistry()
	for i, u := range urls {
		name := string(rune('a' + i))
		if _, err := r.Register(name, u, source.PermissivePolicy()); err != nil {
			t.Fatal(err)
		}
	}
	return r
}

func TestNewRejectsMissingHTTPClient(t *testing.T) {
	_, err := New(newRegistry(t), Config{}, nil, nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	r := newRegistry(t, "http://127.0.0.1:1")
	cfg := Config{HTTPClient: &http.Client{}}
	p, err := New(r, cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.Interval <= 0 {
		t.Fatal("Interval defaulted")
	}
	if p.cfg.Jitter < 0 {
		t.Fatal("Jitter defaulted")
	}
	if p.cfg.Workers < 1 {
		t.Fatal("Workers defaulted")
	}
	if p.cfg.MaxResponseBytes <= 0 {
		t.Fatal("MaxResponseBytes defaulted")
	}
}

func TestPollerPollsEachSource(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"version":"1","self":{"nodeName":"x"},"peers":[]}`))
	}))
	defer srv.Close()

	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         50 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          2,
	}
	var received atomic.Int32
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		func(source.Source, topology.Snapshot) { received.Add(1) },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	if hits.Load() < 1 {
		t.Fatalf("hits = %d", hits.Load())
	}
	if received.Load() < 1 {
		t.Fatalf("receiver never called")
	}
}

func TestPollerMarksSourceOfflineOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         50 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          1,
	}
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	gotP, ok := r.Get("a")
	if !ok {
		t.Fatal("missing source")
	}
	if gotP.ConsecutiveFailures == 0 {
		t.Fatalf("ConsecutiveFailures = %d", gotP.ConsecutiveFailures)
	}
	if gotP.Status() != source.StatusOffline {
		t.Fatalf("status = %v", gotP.Status())
	}
}

func TestPollerNoOverlapPerSource(t *testing.T) {
	var inFlight, maxInFlight atomic.Int32
	mu := sync.Mutex{}
	peak := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inFlight.Add(1)
		mu.Lock()
		if cur > int32(maxInFlight.Load()) {
			maxInFlight.Store(cur)
			peak = int(cur)
		}
		mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		inFlight.Add(-1)
		w.Write([]byte(`{"version":"1","self":{"nodeName":"x"},"peers":[]}`))
	}))
	defer srv.Close()

	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         5 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          4,
	}
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	if peak > 1 {
		t.Fatalf("peak concurrent in-flight = %d, want <=1 per source", peak)
	}
}

func TestPollerRespectsWorkerLimit(t *testing.T) {
	var inFlight, maxInFlight atomic.Int32
	mu := sync.Mutex{}
	peak := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inFlight.Add(1)
		mu.Lock()
		if cur > int32(maxInFlight.Load()) {
			maxInFlight.Store(cur)
			peak = int(cur)
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		w.Write([]byte(`{"version":"1","self":{"nodeName":"x"},"peers":[]}`))
	}))
	defer srv.Close()

	r := newRegistry(t, srv.URL, srv.URL, srv.URL, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         5 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          2,
	}
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	if peak > 2 {
		t.Fatalf("peak = %d, want <=2", peak)
	}
}

func TestPollerCancelsOnContextDone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()
	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         10 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          1,
	}
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("Run returned %v", err)
	}
}

func TestHTTPFetcherRejectsOversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write(bytesRepeat("a", 4096))
	}))
	defer srv.Close()
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		MaxResponseBytes: 16,
	}
	f := HTTPFetcher(cfg)
	_, err := f(context.Background(), fakeSource{name: "x", url: srv.URL})
	if err == nil || !contains(err.Error(), "topology") {
		t.Fatalf("err = %v", err)
	}
}

func TestHTTPFetcherRejectsBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("not-json"))
	}))
	defer srv.Close()
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		MaxResponseBytes: 1024,
	}
	f := HTTPFetcher(cfg)
	_, err := f(context.Background(), fakeSource{name: "x", url: srv.URL})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHTTPFetcherRejectsNonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		MaxResponseBytes: 1024,
	}
	f := HTTPFetcher(cfg)
	_, err := f(context.Background(), fakeSource{name: "x", url: srv.URL})
	if err == nil || !contains(err.Error(), "502") {
		t.Fatalf("err = %v", err)
	}
}

func TestPollerEmitsErrorToSink(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         30 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          1,
	}
	var errorsSeen atomic.Int32
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		nil,
		func(_ source.Source, err error, _ time.Time) {
			if err != nil {
				errorsSeen.Add(1)
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	if errorsSeen.Load() == 0 {
		t.Fatal("ErrorSink never invoked")
	}
}

func TestBoundClamps(t *testing.T) {
	if got := Bound(5*time.Second, time.Second, 10*time.Second); got != 5*time.Second {
		t.Fatalf("got %v", got)
	}
	if got := Bound(0, time.Second, 10*time.Second); got != time.Second {
		t.Fatalf("got %v", got)
	}
	if got := Bound(20*time.Second, time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestJitterBounded(t *testing.T) {
	for i := 0; i < 64; i++ {
		d := jitter(5 * time.Millisecond)
		if d < 0 || d >= 5*time.Millisecond {
			t.Fatalf("jitter out of bounds: %v", d)
		}
	}
}

func TestPollerSkipsCancelledContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"version":"1","self":{"nodeName":"x"},"peers":[]}`))
	}))
	defer srv.Close()
	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         50 * time.Millisecond,
		MaxResponseBytes: 1024,
		Workers:          1,
	}
	var fetched atomic.Int32
	p, err := New(r, cfg,
		HTTPFetcher(cfg),
		func(_ source.Source, _ topology.Snapshot) { fetched.Add(1) },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_ = p.Run(ctx)
	// Should have completed at least one poll before the cancel landed.
	if fetched.Load() < 1 {
		t.Fatalf("fetched = %d", fetched.Load())
	}
}

// 回归: sleep 必须真正等待 Interval, 否则 Run 主循环忙轮询 (100% CPU)
// 且每轮调度所有源 — 修复前 sleep 带 default 分支永不阻塞,
// 300ms 内 hits 会达到数千次。
func TestPollerRespectsInterval(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Write([]byte(`{"version":"1","self":{"nodeName":"x"},"peers":[]}`))
	}))
	defer srv.Close()

	r := newRegistry(t, srv.URL)
	cfg := Config{
		HTTPClient:       &http.Client{Timeout: 2 * time.Second},
		Interval:         100 * time.Millisecond,
		Jitter:           0,
		MaxResponseBytes: 1024,
		Workers:          1,
	}
	p, err := New(r, cfg, HTTPFetcher(cfg), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()
	_ = p.Run(ctx)
	// 100ms 间隔 × ~350ms 窗口 → 至多 4~5 次; 忙循环会远超此数。
	if hits.Load() > 6 {
		t.Fatalf("hits = %d, want <= 6 (sleep not blocking?)", hits.Load())
	}
	if hits.Load() < 2 {
		t.Fatalf("hits = %d, want >= 2", hits.Load())
	}
}

// helpers — local to this test file so we don't pull in bytes/strings.
func bytesRepeat(s string, n int) []byte {
	out := make([]byte, 0, len(s)*n)
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return out
}

func contains(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

var _ = io.Discard
var _ = errors.New
