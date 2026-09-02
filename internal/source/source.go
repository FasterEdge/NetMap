// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
package source

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// RegistryEntry is a static registration record: the (name, url) pair
// handed to NetMap at startup. Runtime provenance is tracked in
// Provenance; RegistryEntry is the input shape used by the CLI / config
// layer.
type RegistryEntry struct {
	Name string
	URL  string
}

// Source is the minimal contract the poller needs from a registered
// upstream. It is intentionally tiny so it can be implemented by a
// Provenance value, a RegistryEntry or a mock.
type Source interface {
	Name() string
	URL() string
}

// AsSource adapts a Provenance to the Source interface.
func (p Provenance) AsSource() Source { return provenanceSource{p: p} }

type provenanceSource struct{ p Provenance }

func (s provenanceSource) Name() string { return s.p.Name }
func (s provenanceSource) URL() string  { return s.p.URL }

// AsSource adapts a RegistryEntry to the Source interface.
func (e RegistryEntry) AsSource() Source { return entrySource{e: e} }

type entrySource struct{ e RegistryEntry }

func (s entrySource) Name() string { return s.e.Name }
func (s entrySource) URL() string  { return s.e.URL }

// Status describes the high-level health of a polled source.
type Status int

const (
	// StatusUnknown is the initial value before any attempt has been made.
	StatusUnknown Status = iota
	// StatusOnline means the last poll succeeded within the freshness window.
	StatusOnline
	// StatusStale means the last poll succeeded but exceeded the freshness window.
	StatusStale
	// StatusOffline means the last poll failed; the source is considered down.
	StatusOffline
)

func (s Status) String() string {
	switch s {
	case StatusOnline:
		return "online"
	case StatusStale:
		return "stale"
	case StatusOffline:
		return "offline"
	default:
		return "unknown"
	}
}

// Provenance is the per-source metadata NetMap tracks between polls. It is
// safe for concurrent use; the registry guards it with a mutex.
type Provenance struct {
	Name string
	URL  string
	// LastAttempt is when we last started a poll attempt.
	LastAttempt time.Time
	// LastSuccess is when the last successful poll completed.
	LastSuccess time.Time
	// LastError is the most recent error string, cleared on success.
	LastError string
	// LastErrorAt is the timestamp of LastError.
	LastErrorAt time.Time
	// ConsecutiveFailures counts failed attempts since the last success.
	ConsecutiveFailures int
	// TotalSuccesses and TotalFailures are lifetime counters.
	TotalSuccesses int
	TotalFailures  int
	// status is the derived current state.
	status Status
}

// Status returns the derived status of the source.
func (p *Provenance) Status() Status {
	p.compute()
	return p.status
}
func (p *Provenance) compute() {
	if p.LastAttempt.IsZero() {
		p.status = StatusUnknown
		return
	}
	switch {
	case p.ConsecutiveFailures > 0:
		p.status = StatusOffline
	case !p.LastSuccess.IsZero() && time.Since(p.LastSuccess) > freshnessWindow():
		p.status = StatusStale
	default:
		p.status = StatusOnline
	}
}

// freshnessWindow is the duration after which a successful source
// transitions from Online to Stale. It is a var so tests can shrink it.
var freshnessWindow = func() time.Duration { return 60 * time.Second }

// SetFreshnessWindowForTests replaces the freshness window. Tests must
// restore the original via the returned function.
func SetFreshnessWindowForTests(d time.Duration) func() {
	prev := freshnessWindow
	freshnessWindow = func() time.Duration { return d }
	return func() { freshnessWindow = prev }
}

// RecordAttempt marks the start of a poll attempt.
func (p *Provenance) RecordAttempt(at time.Time) {
	p.LastAttempt = at
}

// RecordSuccess marks the end of a successful poll attempt.
func (p *Provenance) RecordSuccess(at time.Time) {
	p.LastSuccess = at
	p.LastError = ""
	p.LastErrorAt = time.Time{}
	p.ConsecutiveFailures = 0
	p.TotalSuccesses++
}

// RecordError marks the end of a failed poll attempt.
func (p *Provenance) RecordError(at time.Time, err error) {
	if err == nil {
		p.RecordSuccess(at)
		return
	}
	p.LastError = err.Error()
	p.LastErrorAt = at
	p.ConsecutiveFailures++
	p.TotalFailures++
}

// Clone returns a deep copy safe for read-only consumers.
func (p Provenance) Clone() Provenance {
	out := p
	p.compute()
	out.status = p.status
	return out
}

// Registry is a thread-safe collection of Provenance records, indexed by
// source name. Names are unique; conflicting registrations with the same
// name but different URL raise an error so callers cannot silently
// shadow a source.
type Registry struct {
	mu      sync.RWMutex
	sources map[string]Provenance
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{sources: make(map[string]Provenance)}
}

// Register inserts a new source. It returns an error when the name is
// empty, when a source with the same name already exists, or when the URL
// fails to validate against the provided policy.
func (r *Registry) Register(name, rawURL string, policy ValidationPolicy) (Provenance, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Provenance{}, &DisallowError{Reason: ReasonEmpty, Msg: "source name is empty"}
	}
	url, err := policy.Validate(rawURL)
	if err != nil {
		return Provenance{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sources[name]; exists {
		return Provenance{}, &DisallowError{Reason: ReasonBadURL, Msg: "duplicate name " + name}
	}
	prov := Provenance{Name: name, URL: url}
	r.sources[name] = prov
	return prov.Clone(), nil
}

// Upsert merges an incoming Provenance into the registry. It enforces that
// the URL matches the registered one; a mismatch indicates a provenance
// conflict and is returned as an error so the caller can decide whether
// to log, alert or refresh. The last-seen / counters are still updated so
// the fresh data wins on the non-conflicting fields.
func (r *Registry) Upsert(prov Provenance) error {
	if strings.TrimSpace(prov.Name) == "" {
		return &DisallowError{Reason: ReasonEmpty, Msg: "source name is empty"}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	existing, ok := r.sources[prov.Name]
	if ok && existing.URL != "" && prov.URL != "" && existing.URL != prov.URL {
		return &DisallowError{Reason: ReasonBadURL, Msg: "provenance conflict: existing URL " + existing.URL + " != " + prov.URL}
	}
	if !ok {
		existing = Provenance{Name: prov.Name, URL: prov.URL}
	}
	if prov.LastAttempt.IsZero() {
		prov.LastAttempt = existing.LastAttempt
	}
	if prov.LastSuccess.IsZero() {
		prov.LastSuccess = existing.LastSuccess
	}
	if prov.LastError == "" {
		prov.LastError = existing.LastError
		prov.LastErrorAt = existing.LastErrorAt
	}
	existing.LastAttempt = prov.LastAttempt
	existing.LastSuccess = prov.LastSuccess
	existing.LastError = prov.LastError
	existing.LastErrorAt = prov.LastErrorAt
	existing.ConsecutiveFailures = prov.ConsecutiveFailures
	existing.TotalSuccesses = prov.TotalSuccesses
	existing.TotalFailures = prov.TotalFailures
	if existing.URL == "" {
		existing.URL = prov.URL
	}
	existing.compute()
	r.sources[prov.Name] = existing
	return nil
}

// Get returns a snapshot of the named source's provenance.
func (r *Registry) Get(name string) (Provenance, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	p, ok := r.sources[name]
	if !ok {
		return Provenance{}, false
	}
	return p.Clone(), true
}

// Names returns the registered source names sorted lexicographically. The
// result is a copy — callers may mutate it freely.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.sources))
	for n := range r.sources {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Snapshot returns a slice with every provenance record, sorted by name.
func (r *Registry) Snapshot() []Provenance {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Provenance, 0, len(r.sources))
	for _, p := range r.sources {
		out = append(out, p.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Forget deletes a source from the registry. It returns true when a record
// was removed.
func (r *Registry) Forget(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sources[name]; !ok {
		return false
	}
	delete(r.sources, name)
	return true
}
