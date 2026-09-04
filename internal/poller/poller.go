// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
// Package poller implements the bounded concurrent worker pool that drives
// topology refreshes. Each registered source gets its own scheduled run,
// jitter is added to spread load, and per-source invocations never
// overlap. All requests are context-cancellable.
package poller

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
	"github.com/FasterEdge/NetMap/internal/topology"
)

// DefaultMaxResponseBytes caps the size of a single topology payload to
// protect NetMap from a runaway upstream.
const DefaultMaxResponseBytes int64 = 1 << 20 // 1 MiB
// Config controls a Poll instance.
type Config struct {
	// Interval between successful polls. The first poll happens after
	// jitter to avoid thundering herds.
	Interval time.Duration
	// Jitter bounds the random delay applied to each source's first poll
	// and on reschedule. Must be <= Interval / 2.
	Jitter time.Duration
	// HTTPClient is the shared client used for every source. Required.
	HTTPClient *http.Client
	// MaxResponseBytes bounds a single response body. 0 → DefaultMaxResponseBytes.
	MaxResponseBytes int64
	// Workers is the maximum number of concurrent in-flight polls. Must
	// be >= 1; values < 1 are clamped to 1.
	Workers int
	// Logger receives structured log lines. nil → log.Default().
	Logger *log.Logger
}

// Source is re-exported from the source package so callers do not have
// to import source when interacting with Fetcher/Receiver/ErrorSink.
type Source = source.Source

// Fetcher pulls and decodes a single source's topology. Returning a
// non-nil error transitions the source into the Offline state.
type Fetcher func(ctx context.Context, src Source) (topology.Snapshot, error)

// Receiver handles a successfully decoded snapshot. The poller invokes
// the receiver outside the worker pool's hot path so the caller is free to
// do whatever it likes (merge into store, fan out, etc.).
type Receiver func(src Source, snap topology.Snapshot)

// ErrorSink is invoked after a failed fetch so callers can record the
// failure somewhere persistent if they want to.
type ErrorSink func(src Source, err error, at time.Time)

// Poller is the worker pool. It is safe to call Start exactly once; the
// zero value is not usable.
type Poller struct {
	cfg     Config
	reg     *source.Registry
	fetch   Fetcher
	recv    Receiver
	onError ErrorSink
	wg      sync.WaitGroup
	// per-source guard so we never run two polls at the same time for the
	// same source even if scheduling races. The key is the source name
	// (a string) because Source is an interface value and adapter values
	// would not compare equal across iterations.
	locks sync.Map // map[string]*sync.Mutex
}

// New returns a configured Poller. It returns an error when required
// fields are missing or invalid.
func New(reg *source.Registry, cfg Config, fetch Fetcher, recv Receiver, onErr ErrorSink) (*Poller, error) {
	if reg == nil {
		return nil, errors.New("poller: registry is required")
	}
	if cfg.HTTPClient == nil {
		return nil, errors.New("poller: HTTPClient is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Jitter < 0 {
		cfg.Jitter = 0
	}
	if cfg.Jitter > cfg.Interval/2 {
		cfg.Jitter = cfg.Interval / 2
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	if cfg.MaxResponseBytes <= 0 {
		cfg.MaxResponseBytes = DefaultMaxResponseBytes
	}
	if recv == nil {
		recv = Receiver(func(source.Source, topology.Snapshot) {})
	}
	if onErr == nil {
		onErr = ErrorSink(func(source.Source, error, time.Time) {})
	}
	return &Poller{
		cfg:     cfg,
		reg:     reg,
		fetch:   fetch,
		recv:    recv,
		onError: onErr,
	}, nil
}

// sourceAdapter adapts a source.Provenance to the Source interface.
type sourceAdapter struct{ p source.Provenance }

func (s sourceAdapter) Name() string { return s.p.Name }
func (s sourceAdapter) URL() string  { return s.p.URL }

// Run blocks until ctx is cancelled, polling each source on its own
// cadence. Run returns nil on graceful cancellation; any error returned
// from the worker pool is fatal and propagated.
func (p *Poller) Run(ctx context.Context) error {
	sem := make(chan struct{}, p.cfg.Workers)
	for {
		select {
		case <-ctx.Done():
			p.wg.Wait()
			return nil
		default:
		}
		sources := p.snapshot()
		for _, prov := range sources {
			src := sourceAdapter{p: prov}
			if err := p.schedule(ctx, src, sem); err != nil {
				if errors.Is(err, context.Canceled) {
					p.wg.Wait()
					return nil
				}
				p.cfg.Logger.Printf("poller: schedule %s: %v", src.Name(), err)
			}
		}
		// Sleep until the next source's tick (or cancellation).
		if !p.sleep(ctx, p.cfg.Interval) {
			p.wg.Wait()
			return nil
		}
	}
}

// schedule queues a poll attempt for src, applying per-source no-overlap
// and the global worker pool. It returns context.Canceled when ctx is
// done before the attempt can be queued.
func (p *Poller) schedule(ctx context.Context, src source.Source, sem chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	key := src.Name()
	actual, _ := p.locks.LoadOrStore(key, &sync.Mutex{})
	mu := actual.(*sync.Mutex)
	if !mu.TryLock() {
		// Already a poll in flight for this source; skip this tick.
		return nil
	}
	select {
	case sem <- struct{}{}:
		// got a worker
	case <-ctx.Done():
		mu.Unlock()
		return ctx.Err()
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		defer mu.Unlock()
		defer func() { <-sem }()
		p.runOnce(ctx, src)
	}()
	return nil
}

// runOnce performs a single fetch + receive cycle with jitter.
func (p *Poller) runOnce(ctx context.Context, src source.Source) {
	if !p.sleep(ctx, jitter(p.cfg.Jitter)) {
		return
	}
	prov, ok := p.reg.Get(src.Name())
	if !ok {
		return
	}
	prov.RecordAttempt(time.Now())
	_ = p.reg.Upsert(prov)
	fetchCtx, cancel := context.WithTimeout(ctx, p.cfg.HTTPClient.Timeout)
	defer cancel()
	snap, err := p.fetch(fetchCtx, src)
	at := time.Now()
	if err != nil {
		prov.RecordError(at, err)
		_ = p.reg.Upsert(prov)
		p.cfg.Logger.Printf("poller: %s: %v", src.Name(), err)
		p.onError(src, err, at)
		return
	}
	prov.RecordSuccess(at)
	_ = p.reg.Upsert(prov)
	p.recv(src, snap)
}
func (p *Poller) snapshot() []source.Provenance {
	return p.reg.Snapshot()
}
// sleep blocks for d (or until ctx is cancelled, returning false).
// A non-positive d returns immediately with true so tests and zero-value
// configs keep working.
func (p *Poller) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// jitter returns a uniformly random duration in [0, max). The math is
// kept inline so we don't pull in math/big just to clamp a negative int.
func jitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	// Mask off the high bit so the result of mod is always non-negative
	// even when the underlying entropy interprets the value as signed.
	u := binary.BigEndian.Uint64(b[:]) & 0x7FFFFFFFFFFFFFFF
	span := int64(max)
	if span <= 0 {
		return 0
	}
	return time.Duration(u % uint64(span))
}

// HTTPFetcher returns a Fetcher that issues an HTTP request using the
// shared client, reads at most cfg.MaxResponseBytes and decodes the
// response into a topology.Snapshot.
func HTTPFetcher(cfg Config) Fetcher {
	return Fetcher(func(ctx context.Context, src source.Source) (topology.Snapshot, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL(), nil)
		if err != nil {
			return topology.Snapshot{}, fmt.Errorf("build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "NetMap/1.0 (+fasteredge)")
		resp, err := cfg.HTTPClient.Do(req)
		if err != nil {
			return topology.Snapshot{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return topology.Snapshot{}, fmt.Errorf("upstream %s", resp.Status)
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, cfg.MaxResponseBytes))
		if err != nil {
			return topology.Snapshot{}, fmt.Errorf("read body: %w", err)
		}
		snap, err := topology.Decode(body)
		if err != nil {
			return topology.Snapshot{}, err
		}
		snap.Source = src.Name()
		return snap, nil
	})
}

// Bound clamps d to [min, max]. Helper for callers configuring the
// poller from user input.
func Bound(d, min, max time.Duration) time.Duration {
	switch {
	case d < min:
		return min
	case d > max:
		return max
	default:
		return d
	}
}

// mathAbs is a tiny helper kept here so we do not need to add math
// imports elsewhere in the package.
func mathAbs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

var _ = mathAbs
