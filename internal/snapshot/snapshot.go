// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
// Package snapshot provides atomic, versioned JSON persistence for the
// NetMap topology store. It is pure Go (no cgo, no SQLite), writes with
// 0600 file perms, debounces frequent saves, and rejects unknown major
// schema versions with a clear error.
//
// File layout on disk:
//
//	{
//	  "version":  "1.0",
//	  "savedAt":  "2026-08-26T12:34:56Z",
//	  "topology": { "self": {...}, "peers": [...] }
//	}
//
// Atomic writes use a sibling temp file (in the same directory so the
// rename is atomic on every supported OS), fsync the file before close,
// and best-effort fsync the parent directory after the rename. Failed
// writes leave the existing file untouched.
package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Schema constants. Major version is bumped for any incompatible change.
const (
	MajorVersion = 1
	MinorVersion = 0
	Version      = "1.0"
)

// Default tuning knobs applied by New when callers pass zero values.
const (
	DefaultMaxLoadBytes int64         = 4 << 20 // 4 MiB
	DefaultDebounce     time.Duration = 500 * time.Millisecond
)

// FilePerm is the restrictive mode applied to the snapshot file. Owner
// read/write only — every other principal is denied.
const FilePerm os.FileMode = 0o600

// Topology mirrors the public store.Topology view. The fields and JSON
// tags are kept identical to the on-the-wire shape so a saved file
// round-trips without surprises.
type Topology struct {
	Self  SelfInfo `json:"self"`
	Peers []Peer   `json:"peers"`
}

// SelfInfo is the subset of the local node that the snapshot cares about.
type SelfInfo struct {
	NodeName string `json:"nodeName"`
}

// Peer is one entry of the persisted peer table.
type Peer struct {
	Name     string    `json:"name"`
	Address  string    `json:"address"`
	Role     string    `json:"role"`
	LastSeen time.Time `json:"lastSeen"`
}

// Envelope is the on-disk wrapper. Version uses "<major>.<minor>" so future
// schema changes can be negotiated by major alone.
type Envelope struct {
	Version  string    `json:"version"`
	SavedAt  time.Time `json:"savedAt"`
	Topology Topology  `json:"topology"`
}

// Provider returns the current snapshot envelope to persist. Implementations
// should take whatever locks are needed to produce a consistent view; the
// Store itself is safe for concurrent use.
type Provider func() (Envelope, error)

// Store debounces Save calls and writes atomically with 0600 perms.
// The zero value is not usable; construct via New.
type Store struct {
	path     string
	debounce time.Duration
	maxLoad  int64

	mu      sync.Mutex
	pending Provider
	timer   *time.Timer

	lastSavedAt time.Time
	lastErr     error
}

// New returns a Store configured for the given path. debounce and
// maxLoadBytes may be zero to take defaults. An empty path is rejected
// at construction so the caller can't accidentally disable persistence
// while still believing it is enabled.
func New(path string, debounce time.Duration, maxLoadBytes int64) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("snapshot: path is required")
	}
	if debounce <= 0 {
		debounce = DefaultDebounce
	}
	if maxLoadBytes <= 0 {
		maxLoadBytes = DefaultMaxLoadBytes
	}
	return &Store{
		path:     path,
		debounce: debounce,
		maxLoad:  maxLoadBytes,
	}, nil
}

// Path returns the configured file path.
func (s *Store) Path() string { return s.path }

// Debounce returns the configured debounce window.
func (s *Store) Debounce() time.Duration { return s.debounce }

// LastSavedAt returns the timestamp of the most recent successful save,
// or the zero value if nothing has been written yet.
func (s *Store) LastSavedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSavedAt
}

// LastError returns the error from the most recent failed save attempt,
// or nil if the last save succeeded or none has been attempted.
func (s *Store) LastError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Schedule requests a save to happen after the debounce window. Repeated
// calls within the window collapse to a single write. Calling Schedule
// with a nil provider is a no-op.
func (s *Store) Schedule(p Provider) {
	if p == nil {
		return
	}
	s.mu.Lock()
	s.pending = p
	if s.timer != nil {
		s.timer.Stop()
	}
	s.timer = time.AfterFunc(s.debounce, s.fire)
	s.mu.Unlock()
}

// Flush writes any pending snapshot synchronously. If no save is pending,
// it returns nil without writing.
func (s *Store) Flush() error {
	s.mu.Lock()
	p := s.pending
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.pending = nil
	s.mu.Unlock()
	if p == nil {
		return nil
	}
	return s.saveNow(p)
}

// Save writes immediately, ignoring any pending debounced save. It is
// the synchronous counterpart used by tests and any caller that wants
// a definite checkpoint.
func (s *Store) Save(p Provider) error {
	if p == nil {
		return errors.New("snapshot: nil provider")
	}
	s.mu.Lock()
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.pending = nil
	s.mu.Unlock()
	return s.saveNow(p)
}

// fire is the AfterFunc callback. It runs on its own goroutine.
func (s *Store) fire() {
	s.mu.Lock()
	p := s.pending
	s.pending = nil
	s.timer = nil
	s.mu.Unlock()
	if p == nil {
		return
	}
	if err := s.saveNow(p); err != nil {
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
	}
}

func (s *Store) saveNow(p Provider) error {
	env, err := p()
	if err != nil {
		return err
	}
	if env.Version == "" {
		env.Version = Version
	}
	env.SavedAt = time.Now().UTC()
	return s.writeAtomic(&env)
}

// writeAtomic marshals env and writes it atomically to disk with 0600 perms.
// The two-phase commit is: write+fsync temp file, rename, fsync directory.
// A failure at any step removes the temp file and leaves the existing
// target untouched.
func (s *Store) writeAtomic(env *Envelope) error {
	body, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("snapshot: marshal: %w", err)
	}
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("snapshot: mkdir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, ".snap-*.json.tmp")
	if err != nil {
		return fmt.Errorf("snapshot: temp file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	// Tighten perms before any data is written so we never expose the
	// temp file wider than the eventual target.
	if err := tmp.Chmod(FilePerm); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("snapshot: chmod: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("snapshot: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("snapshot: close: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		cleanup()
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	// Best-effort directory fsync. Failure here is non-fatal: the file
	// itself is already durable, only its directory entry metadata is at
	// risk across a crash. The next save will repair that.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	s.mu.Lock()
	s.lastSavedAt = env.SavedAt
	s.lastErr = nil
	s.mu.Unlock()
	return nil
}

// Load reads the snapshot file and returns the envelope. The reader is
// bounded to the configured maxLoadBytes. Unknown major versions are
// rejected with a clear error. A missing file is reported as a regular
// os.PathError so callers can decide whether "first run" is fatal.
func (s *Store) Load() (Envelope, error) {
	if s.path == "" {
		return Envelope{}, errors.New("snapshot: path is empty")
	}
	f, err := os.Open(s.path)
	if err != nil {
		return Envelope{}, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return Envelope{}, err
	}
	if info.Size() > s.maxLoad {
		return Envelope{}, fmt.Errorf("snapshot: file too large: %d > %d", info.Size(), s.maxLoad)
	}
	body, err := io.ReadAll(io.LimitReader(f, s.maxLoad+1))
	if err != nil {
		return Envelope{}, fmt.Errorf("snapshot: read: %w", err)
	}
	if int64(len(body)) > s.maxLoad {
		return Envelope{}, fmt.Errorf("snapshot: file exceeded limit %d bytes", s.maxLoad)
	}
	if len(body) == 0 {
		return Envelope{}, errors.New("snapshot: empty file")
	}
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return Envelope{}, fmt.Errorf("snapshot: decode: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, fmt.Errorf("snapshot: %w", err)
	}
	return env, nil
}

// Validate rejects envelopes with unknown major versions or an empty
// version string. It returns the envelope unchanged on success.
func (e *Envelope) Validate() error {
	major, err := ParseMajor(e.Version)
	if err != nil {
		return fmt.Errorf("bad version %q: %w", e.Version, err)
	}
	if major != MajorVersion {
		return fmt.Errorf("unsupported major version %d (expected %d, full=%q)", major, MajorVersion, e.Version)
	}
	return nil
}

// ParseMajor extracts the major version number from a "<major>.<minor>"
// string. Whitespace around the input is tolerated.
func ParseMajor(v string) (int, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, errors.New("empty version")
	}
	parts := strings.SplitN(v, ".", 2)
	n, err := strconv.Atoi(parts[0])
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad major %q", parts[0])
	}
	return n, nil
}

// Disabled reports whether s is a no-op stub. We never construct such a
// stub today but callers can use this to gate persistence work.
func (s *Store) Disabled() bool { return s == nil }

// CheckPlatform reports whether the runtime supports atomic rename + fsync.
// All currently supported GOOSes do, but we expose this for tests that
// exercise POSIX-only failure modes.
func CheckPlatform() bool {
	return runtime.GOOS != "js" && runtime.GOOS != "wasip1"
}
