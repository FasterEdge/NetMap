package snapshot

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// provider returns a fixed envelope. Tests can clone-and-edit when they
// want to verify mutation paths.
func sampleEnvelope() Envelope {
	return Envelope{
		Version: Version,
		SavedAt: time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC),
		Topology: Topology{
			Self: SelfInfo{NodeName: "hub-01"},
			Peers: []Peer{
				{Name: "edge-a", Address: "10.0.0.1:7000", Role: "edge", LastSeen: time.Now().UTC()},
				{Name: "edge-b", Address: "10.0.0.2:7000", Role: "edge", LastSeen: time.Now().UTC()},
			},
		},
	}
}

// TestRoundTrip saves a non-trivial envelope, reloads it, and asserts the
// decoded contents match the original field-for-field.
func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := sampleEnvelope()

	if err := s.Save(func() (Envelope, error) { return want, nil }); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Version != want.Version {
		t.Fatalf("version = %q, want %q", got.Version, want.Version)
	}
	if got.Topology.Self.NodeName != want.Topology.Self.NodeName {
		t.Fatalf("self = %q, want %q", got.Topology.Self.NodeName, want.Topology.Self.NodeName)
	}
	if len(got.Topology.Peers) != len(want.Topology.Peers) {
		t.Fatalf("peers len = %d, want %d", len(got.Topology.Peers), len(want.Topology.Peers))
	}
	for i, p := range want.Topology.Peers {
		gp := got.Topology.Peers[i]
		if gp.Name != p.Name || gp.Address != p.Address || gp.Role != p.Role {
			t.Fatalf("peer[%d] = %+v, want %+v", i, gp, p)
		}
		if !gp.LastSeen.Equal(p.LastSeen) {
			t.Fatalf("peer[%d] lastSeen = %v, want %v", i, gp.LastSeen, p.LastSeen)
		}
	}
}

// TestLoadMissingFile verifies the empty-state code path: a fresh install
// has no snapshot, so Load must return os.ErrNotExist (not a wrapped
// error that callers would have to inspect by string).
func TestLoadMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Load()
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want os.ErrNotExist", err)
	}
}

// TestLoadEmptyFile rejects zero-byte snapshots with a clear error. The
// envelope's Validate cannot detect "empty file" because there is no
// envelope at all.
func TestLoadEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	if err := os.WriteFile(path, nil, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error for empty file")
	}
}

// TestLoadUnsupportedMajorVersion verifies the schema guard. We hand-craft
// a file whose version is well-formed but unknown to this build.
func TestLoadUnsupportedMajorVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	body := []byte(`{"version":"99.0","savedAt":"2026-01-01T00:00:00Z","topology":{"self":{"nodeName":"x"},"peers":[]}}`)
	if err := os.WriteFile(path, body, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Load()
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
	if !strings.Contains(err.Error(), "unsupported major version") {
		t.Fatalf("err = %v, want unsupported major", err)
	}
	if !strings.Contains(err.Error(), "99") {
		t.Fatalf("err = %v, want major number 99 surfaced", err)
	}
}

// TestLoadMalformedVersion rejects a version field that cannot be parsed.
func TestLoadMalformedVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	body := []byte(`{"version":"not-a-number","topology":{}}`)
	if err := os.WriteFile(path, body, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error for malformed version")
	}
}

// TestLoadTruncatedFile rejects files that get cut off mid-JSON. We use a
// 1-byte body to simulate the worst-case truncation.
func TestLoadTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	body := []byte(`{"version":"1.0","topolog`) // truncated mid-key
	if err := os.WriteFile(path, body, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error for truncated file")
	}
}

// TestLoadCorruptJSON covers the "valid prefix, garbage suffix" case —
// the decode call sees a JSON value but the structure is nonsense.
func TestLoadCorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	body := []byte("not json at all")
	if err := os.WriteFile(path, body, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(); err == nil {
		t.Fatal("expected error for corrupt JSON")
	}
}

// TestLoadExceedsMaxBytes verifies the bounded reader. We write a body
// just past the configured cap and expect a clear "too large" error.
func TestLoadExceedsMaxBytes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	// 16 KiB blob; maxLoadBytes is set to 4 KiB so this trips the guard.
	big := make([]byte, 16*1024)
	for i := range big {
		big[i] = 'x'
	}
	if err := os.WriteFile(path, big, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, 0, 4*1024)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Load()
	if err == nil {
		t.Fatal("expected too-large error")
	}
	if !strings.Contains(err.Error(), "too large") && !strings.Contains(err.Error(), "exceeded") {
		t.Fatalf("err = %v, want size error", err)
	}
}

// TestRestrictivePerms verifies the snapshot file is written with 0600.
// We mask off the type bits so umask does not leak into the comparison.
func TestRestrictivePerms(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod perms are not POSIX on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(func() (Envelope, error) { return sampleEnvelope(), nil }); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != FilePerm {
		t.Fatalf("perm = %o, want %o", perm, FilePerm)
	}
}

// TestAtomicReplacePreservesOldFile writes a valid snapshot, makes the
// parent directory unwritable, attempts a second save, and asserts that
// (a) the second save fails and (b) the existing file is byte-for-byte
// unchanged. This is the durable promise of the temp-file + rename path.
func TestAtomicReplacePreservesOldFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod perms are not POSIX on Windows")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}

	first := sampleEnvelope()
	if err := s.Save(func() (Envelope, error) { return first, nil }); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Make the directory read+execute (no write). The temp file creation
	// will fail because we cannot write into the directory.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	second := first
	second.Topology.Self.NodeName = "hub-99"
	err = s.Save(func() (Envelope, error) { return second, nil })
	if err == nil {
		t.Fatal("expected write to fail on read-only directory")
	}

	// Restore perms and confirm the file on disk is still the original.
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatalf("file mutated after failed write\nbefore: %s\nafter:  %s", original, after)
	}
	// And the decoded contents must still describe "hub-01".
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Topology.Self.NodeName != "hub-01" {
		t.Fatalf("self = %q, want unchanged hub-01", got.Topology.Self.NodeName)
	}
}

// TestConcurrentMutationAndSave drives many goroutines that constantly
// schedule saves while a smaller pool performs loads. The invariants:
// (a) every Load succeeds (no torn writes), (b) LastError stays nil,
// (c) no goroutine panics on the timer race.
func TestConcurrentMutationAndSave(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, 5*time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}

	var counter atomic.Uint64
	provider := func() (Envelope, error) {
		n := counter.Add(1)
		env := sampleEnvelope()
		env.Topology.Self.NodeName = "hub-" + itoa(int(n))
		return env, nil
	}

	const writers = 32
	const readers = 8
	const iters = 200

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				s.Schedule(provider)
			}
		}()
	}
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				if _, err := s.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
					// We tolerate ErrNotExist only in the first few
					// microseconds before the first save lands.
					t.Errorf("load failed mid-run: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if err := s.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := s.LastError(); err != nil {
		t.Fatalf("last error: %v", err)
	}
	// Final file must be readable and conform to schema.
	env, err := s.Load()
	if err != nil {
		t.Fatalf("final load: %v", err)
	}
	if !strings.HasPrefix(env.Topology.Self.NodeName, "hub-") {
		t.Fatalf("self = %q, want hub-*", env.Topology.Self.NodeName)
	}
}

// TestDebounceCoalescesWrites checks that N rapid Schedule calls produce
// only one write within the debounce window. We use a counter provider
// and assert the call count is small (ideally 1, but we allow <= 3 to
// tolerate timer jitter on heavily loaded CI machines).
func TestDebounceCoalescesWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, 50*time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	provider := func() (Envelope, error) {
		writes.Add(1)
		return sampleEnvelope(), nil
	}
	for i := 0; i < 100; i++ {
		s.Schedule(provider)
	}
	// Allow the timer to fire.
	time.Sleep(150 * time.Millisecond)
	if n := writes.Load(); n < 1 || n > 3 {
		t.Fatalf("writes = %d, want 1..3 after debounce", n)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	// After a synchronous Flush, no further debounced writes should land.
	prev := writes.Load()
	time.Sleep(150 * time.Millisecond)
	if got := writes.Load(); got != prev {
		t.Fatalf("writes after flush = %d, want unchanged %d", got, prev)
	}
}

// TestFlushCompletesPending verifies the shutdown contract: a pending
// save is committed synchronously, even when the debounce window has
// not yet elapsed.
func TestFlushCompletesPending(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	// Long debounce — Flush is the only way this test's data lands.
	s, err := New(path, time.Hour, 0)
	if err != nil {
		t.Fatal(err)
	}
	env := sampleEnvelope()
	env.Topology.Self.NodeName = "hub-flush"
	s.Schedule(func() (Envelope, error) { return env, nil })

	// File does not exist yet — debounce has not fired.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no file pre-flush, stat err = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Topology.Self.NodeName != "hub-flush" {
		t.Fatalf("self = %q, want hub-flush", got.Topology.Self.NodeName)
	}
	if s.LastSavedAt().IsZero() {
		t.Fatal("LastSavedAt not updated")
	}
}

// TestFlushNoop verifies that Flush on a Store with no pending work
// returns nil and does not touch the file system.
func TestFlushNoop(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("flush with no pending: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected no file, stat err = %v", err)
	}
}

// TestNewRequiresPath guards against accidental "configure nothing"
// callers silently disabling persistence.
func TestNewRequiresPath(t *testing.T) {
	if _, err := New("", 0, 0); err == nil {
		t.Fatal("expected error for empty path")
	}
	if _, err := New("   ", 0, 0); err == nil {
		t.Fatal("expected error for whitespace path")
	}
}

// TestSaveRejectsNilProvider protects the synchronous API from a nil
// pointer dereference.
func TestSaveRejectsNilProvider(t *testing.T) {
	s, err := New(filepath.Join(t.TempDir(), "snap.json"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(nil); err == nil {
		t.Fatal("expected error for nil provider")
	}
}

// TestVersionDefaultApplied ensures Save fills in the version field if
// the caller leaves it blank — the on-disk file must always carry the
// canonical version string.
func TestVersionDefaultApplied(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	blank := sampleEnvelope()
	blank.Version = ""
	if err := s.Save(func() (Envelope, error) { return blank, nil }); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"version": "`+Version+`"`) {
		t.Fatalf("body missing version %q: %s", Version, body)
	}
}

// TestParseMajor covers the trivial cases of the version parser.
func TestParseMajor(t *testing.T) {
	cases := []struct {
		in     string
		want   int
		hasErr bool
	}{
		{"1.0", 1, false},
		{"  3.14  ", 3, false},
		{"42", 42, false},
		{"", 0, true},
		{"   ", 0, true},
		{"abc.0", 0, true},
		{"-1.0", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseMajor(tc.in)
		if (err != nil) != tc.hasErr {
			t.Fatalf("ParseMajor(%q) err=%v, wantErr=%v", tc.in, err, tc.hasErr)
		}
		if !tc.hasErr && got != tc.want {
			t.Fatalf("ParseMajor(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestWriteAtomicPreservesEmptyTopology makes sure an envelope with no
// peers round-trips with the empty slice (not "null") preserved.
func TestWriteAtomicPreservesEmptyTopology(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	env := Envelope{
		Version: Version,
		Topology: Topology{
			Self:  SelfInfo{NodeName: "only-self"},
			Peers: []Peer{},
		},
	}
	if err := s.Save(func() (Envelope, error) { return env, nil }); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), `"peers": null`) {
		t.Fatalf("body has null peers, want []\n%s", body)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Topology.Self.NodeName != "only-self" {
		t.Fatalf("self = %q", got.Topology.Self.NodeName)
	}
	if len(got.Topology.Peers) != 0 {
		t.Fatalf("peers len = %d, want 0", len(got.Topology.Peers))
	}
}

// TestDecodeAcceptsUnknownFields ensures forward compatibility: future
// versions of the snapshot may add fields the current loader does not
// know about, and we must not refuse them.
func TestDecodeAcceptsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	body := []byte(`{
	  "version": "1.0",
	  "savedAt": "2026-01-01T00:00:00Z",
	  "futureFlag": true,
	  "topology": {
	    "self": {"nodeName": "hub-x"},
	    "peers": []
	  }
	}`)
	if err := os.WriteFile(path, body, FilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := New(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Topology.Self.NodeName != "hub-x" {
		t.Fatalf("self = %q", got.Topology.Self.NodeName)
	}
	// Sanity: confirm the body is still valid JSON we can re-encode.
	if _, err := json.Marshal(got); err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
}

// TestSaveOverwritesExistingFile guards the rename semantics: writing
// twice produces a single, valid, content-replaced file.
func TestSaveOverwritesExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")
	s, err := New(path, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	first := sampleEnvelope()
	first.Topology.Self.NodeName = "first"
	if err := s.Save(func() (Envelope, error) { return first, nil }); err != nil {
		t.Fatal(err)
	}
	second := sampleEnvelope()
	second.Topology.Self.NodeName = "second"
	if err := s.Save(func() (Envelope, error) { return second, nil }); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Topology.Self.NodeName != "second" {
		t.Fatalf("self = %q, want second (overwritten)", got.Topology.Self.NodeName)
	}
	// No leftover temp files in the directory.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".snap-") {
			t.Fatalf("leftover temp file: %s", e.Name())
		}
	}
}

// itoa is a tiny helper used by TestConcurrentMutationAndSave to keep the
// counter labels short and allocation-free.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Compile-time reference so unused-import warnings stay quiet even when
// tests are filtered out.
var _ = io.Discard
