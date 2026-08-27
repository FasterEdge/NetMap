package source

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPolicyRejectsEmpty(t *testing.T) {
	if _, err := DefaultPolicy().Validate(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestPolicyRejectsBadScheme(t *testing.T) {
	if _, err := DefaultPolicy().Validate("ftp://x"); err == nil {
		t.Fatal("expected error")
	} else if reason := reasonOf(err); reason != ReasonBadScheme {
		t.Fatalf("reason = %v", reason)
	}
}

func TestPolicyRejectsLoopbackByDefault(t *testing.T) {
	_, err := DefaultPolicy().Validate("http://127.0.0.1:8080")
	if reason := reasonOf(err); reason != ReasonLoopback {
		t.Fatalf("reason = %v err=%v", reason, err)
	}
}

func TestPolicyAllowsLoopbackWhenPermissive(t *testing.T) {
	if _, err := PermissivePolicy().Validate("http://127.0.0.1:8080"); err != nil {
		t.Fatalf("expected permissive allow, got %v", err)
	}
}

func TestPolicyRejectsPrivateByDefault(t *testing.T) {
	_, err := DefaultPolicy().Validate("http://10.0.0.1")
	if reason := reasonOf(err); reason != ReasonPrivate {
		t.Fatalf("reason = %v", reason)
	}
}

func TestPolicyRejectsLinkLocalByDefault(t *testing.T) {
	_, err := DefaultPolicy().Validate("http://169.254.169.254")
	// 169.254/16 is link-local unicast → ReasonLinkLocal.
	if reason := reasonOf(err); reason != ReasonLinkLocal && reason != ReasonPrivate {
		t.Fatalf("reason = %v err=%v", reason, err)
	}
}

func TestPolicyRejectsMulticastByDefault(t *testing.T) {
	// 224.0.0.0/4 is multicast; 224.0.0.0/24 is link-local multicast.
	// 239.0.0.1 is org-local multicast — never link-local.
	_, err := DefaultPolicy().Validate("http://239.0.0.1")
	if reason := reasonOf(err); reason != ReasonMulticast && reason != ReasonPrivate {
		t.Fatalf("reason = %v err=%v", reason, err)
	}
}

func TestPolicyRejectsLinkLocalMulticast(t *testing.T) {
	_, err := DefaultPolicy().Validate("http://224.0.0.1")
	if reason := reasonOf(err); reason != ReasonLinkLocal && reason != ReasonMulticast {
		t.Fatalf("reason = %v err=%v", reason, err)
	}
}

func TestPolicyAllowsPublicHost(t *testing.T) {
	out, err := DefaultPolicy().Validate("https://example.com/api")
	if err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
	if out != "https://example.com/api" {
		t.Fatalf("normalised = %q", out)
	}
}

func TestPolicyNormalisesDefaultPort(t *testing.T) {
	out, err := DefaultPolicy().Validate("http://example.com:80/path/")
	if err != nil {
		t.Fatal(err)
	}
	if out != "http://example.com/path" {
		t.Fatalf("normalised = %q", out)
	}
}

func TestPolicyRejectsInvalidHostname(t *testing.T) {
	if _, err := DefaultPolicy().Validate("http://-bad-"); err == nil {
		t.Fatal("expected error")
	}
}

func TestPolicyAllowListOverridesDefaults(t *testing.T) {
	policy := ValidationPolicy{AllowPrivate: false, AllowedHosts: map[string]struct{}{"edge.lab": {}}}
	if _, err := policy.Validate("http://edge.lab:7000"); err != nil {
		t.Fatalf("expected allow-list accept, got %v", err)
	}
	// A different host should still be rejected for being loopback.
	if _, err := policy.Validate("http://127.0.0.1:7000"); err == nil {
		t.Fatal("expected deny for non-allowlisted loopback")
	}
}

func reasonOf(err error) DisallowReason {
	var d *DisallowError
	if !errors.As(err, &d) {
		return ""
	}
	return d.Reason
}

// ----------------------------------------------------------------------
// Registry + provenance

func TestRegistryRegisterAndGet(t *testing.T) {
	r := NewRegistry()
	p, err := r.Register("a", "https://example.com", PermissivePolicy())
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "a" {
		t.Fatalf("name = %q", p.Name)
	}
	got, ok := r.Get("a")
	if !ok {
		t.Fatal("expected entry")
	}
	if got.URL != "https://example.com" {
		t.Fatalf("url = %q", got.URL)
	}
}

func TestRegistryRejectsEmptyName(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("", "http://x", PermissivePolicy()); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegistryRejectsDuplicate(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err == nil {
		t.Fatal("expected duplicate error")
	}
}

func TestRegistryRejectsBadURL(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "ftp://x", PermissivePolicy()); err == nil {
		t.Fatal("expected scheme error")
	}
}

func TestRegistryUpsertMergesFields(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	prov := Provenance{
		Name:                "a",
		URL:                 "https://example.com",
		LastAttempt:         now,
		LastSuccess:         now,
		ConsecutiveFailures: 0,
		TotalSuccesses:      3,
	}
	if err := r.Upsert(prov); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Get("a")
	if got.TotalSuccesses != 3 {
		t.Fatalf("TotalSuccesses = %d", got.TotalSuccesses)
	}
}

func TestRegistryUpsertDetectsURLConflict(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err != nil {
		t.Fatal(err)
	}
	if err := r.Upsert(Provenance{Name: "a", URL: "https://evil.example.com"}); err == nil {
		t.Fatal("expected conflict")
	}
}

func TestRegistrySnapshotSorted(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"c", "a", "b"} {
		if _, err := r.Register(name, "https://example.com", PermissivePolicy()); err != nil {
			t.Fatal(err)
		}
	}
	names := r.Names()
	if names[0] != "a" || names[1] != "b" || names[2] != "c" {
		t.Fatalf("names = %v", names)
	}
}

func TestRegistryForget(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err != nil {
		t.Fatal(err)
	}
	if !r.Forget("a") {
		t.Fatal("forget returned false")
	}
	if r.Forget("a") {
		t.Fatal("forget returned true on missing")
	}
}

// ----------------------------------------------------------------------
// Provenance state machine

func TestProvenanceInitialStatus(t *testing.T) {
	p := Provenance{Name: "a", URL: "http://x"}
	if p.Status() != StatusUnknown {
		t.Fatalf("status = %v", p.Status())
	}
}

func TestProvenanceRecordsSuccessAndFailure(t *testing.T) {
	restore := SetFreshnessWindowForTests(time.Hour)
	defer restore()
	p := Provenance{Name: "a", URL: "http://x"}
	now := time.Now()
	p.RecordAttempt(now)
	p.RecordSuccess(now)
	if p.Status() != StatusOnline {
		t.Fatalf("status = %v", p.Status())
	}
	if p.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d", p.ConsecutiveFailures)
	}
	p.RecordError(now, errors.New("boom"))
	if p.Status() != StatusOffline {
		t.Fatalf("status = %v", p.Status())
	}
	if p.ConsecutiveFailures != 1 {
		t.Fatalf("ConsecutiveFailures = %d", p.ConsecutiveFailures)
	}
	p.RecordSuccess(now)
	if p.Status() != StatusOnline {
		t.Fatalf("status = %v", p.Status())
	}
}

func TestProvenanceStaleAfterFreshnessWindow(t *testing.T) {
	restore := SetFreshnessWindowForTests(10 * time.Millisecond)
	defer restore()
	p := Provenance{Name: "a", URL: "http://x"}
	t0 := time.Now().Add(-time.Second)
	p.RecordAttempt(t0)
	p.RecordSuccess(t0)
	if p.Status() != StatusStale {
		t.Fatalf("status = %v", p.Status())
	}
}

func TestProvenanceStatusUsesMostRecentAttempt(t *testing.T) {
	restore := SetFreshnessWindowForTests(time.Hour)
	defer restore()
	p := Provenance{Name: "a", URL: "http://x"}
	now := time.Now()
	p.RecordAttempt(now)
	p.RecordSuccess(now)
	// Now fail; status should flip to Offline.
	p.RecordAttempt(now.Add(time.Second))
	p.RecordError(now.Add(time.Second), errors.New("boom"))
	if p.Status() != StatusOffline {
		t.Fatalf("status = %v", p.Status())
	}
}

func TestProvenanceRecordErrorNilIsSuccess(t *testing.T) {
	p := Provenance{Name: "a"}
	p.RecordError(time.Now(), nil)
	if p.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d", p.ConsecutiveFailures)
	}
	if p.LastSuccess.IsZero() {
		t.Fatal("LastSuccess should be set when nil error passed")
	}
}

func TestProvenanceStatusString(t *testing.T) {
	for s, want := range map[Status]string{
		StatusOnline:  "online",
		StatusStale:   "stale",
		StatusOffline: "offline",
	} {
		if s.String() != want {
			t.Errorf("%v.String() = %q want %q", s, s.String(), want)
		}
	}
}

func TestProvenanceCloneDeepCopy(t *testing.T) {
	p := Provenance{Name: "a", URL: "http://x", ConsecutiveFailures: 3}
	clone := p.Clone()
	clone.ConsecutiveFailures = 999
	if p.ConsecutiveFailures != 3 {
		t.Fatal("clone mutated original")
	}
}

func TestRegistryConcurrentUpsert(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Register("a", "https://example.com", PermissivePolicy()); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{}, 32)
	for i := 0; i < 32; i++ {
		go func(i int) {
			now := time.Now()
			_ = r.Upsert(Provenance{
				Name:           "a",
				URL:            "https://example.com",
				LastAttempt:    now,
				LastSuccess:    now,
				TotalSuccesses: i,
			})
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 32; i++ {
		<-done
	}
}

func TestSourceInterfaces(t *testing.T) {
	p := Provenance{Name: "a", URL: "http://x"}
	var s Source = p.AsSource()
	if s.Name() != "a" || s.URL() != "http://x" {
		t.Fatalf("Source = %+v", s)
	}
	e := RegistryEntry{Name: "b", URL: "http://y"}
	var s2 Source = e.AsSource()
	if s2.Name() != "b" || s2.URL() != "http://y" {
		t.Fatalf("Source = %+v", s2)
	}
}

func TestDisallowErrorMessage(t *testing.T) {
	err := (&DisallowError{Reason: ReasonLoopback, Msg: "x"}).Error()
	if !strings.Contains(err, "loopback") {
		t.Fatalf("err = %q", err)
	}
}
