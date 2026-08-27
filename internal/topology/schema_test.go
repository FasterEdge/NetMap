package topology

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validBody() []byte {
	return []byte(`{
		"version": "1",
		"source":  "edge-1",
		"self":    {"nodeName": "edge-1", "defaultIface": "eth0"},
		"peers": [
			{"name": "peer-a", "address": "10.0.0.2", "role": "edge"},
			{"name": "peer-b", "address": "10.0.0.3", "role": "cloud"}
		],
		"issuedAt": "2026-08-26T00:00:00Z"
	}`)
}

func TestDecodeValid(t *testing.T) {
	snap, err := Decode(validBody())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Version != CurrentVersion {
		t.Fatalf("version = %q", snap.Version)
	}
	if snap.Self.NodeName != "edge-1" {
		t.Fatalf("self.NodeName = %q", snap.Self.NodeName)
	}
	if len(snap.Peers) != 2 {
		t.Fatalf("peers = %d", len(snap.Peers))
	}
	if snap.Peers[0].Name != "peer-a" {
		t.Fatalf("peer[0] = %+v", snap.Peers[0])
	}
	if snap.Issued.IsZero() {
		t.Fatal("issuedAt should be populated")
	}
}

func TestDecodeRejectsEmptyBody(t *testing.T) {
	if _, err := Decode(nil); err == nil {
		t.Fatal("expected error on empty body")
	}
	if _, err := Decode([]byte{}); err == nil {
		t.Fatal("expected error on empty body")
	}
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	body := []byte(`{"version":"42","self":{"nodeName":"x"},"peers":[]}`)
	if _, err := Decode(body); err == nil || !strings.Contains(err.Error(), "unsupported version") {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeRejectsMissingSelf(t *testing.T) {
	body := []byte(`{"version":"1","self":{"nodeName":""},"peers":[]}`)
	if _, err := Decode(body); err == nil {
		t.Fatal("expected error for missing self.nodeName")
	}
}

func TestDecodeRejectsMissingVersion(t *testing.T) {
	body := []byte(`{"self":{"nodeName":"x"},"peers":[]}`)
	if _, err := Decode(body); err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestDecodeRejectsPeerWithoutName(t *testing.T) {
	body := []byte(`{"version":"1","self":{"nodeName":"x"},"peers":[{"name":"a"},{"address":"x"}]}`)
	if _, err := Decode(body); err == nil || !strings.Contains(err.Error(), "peers[1]") {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeRejectsDuplicatePeers(t *testing.T) {
	body := []byte(`{"version":"1","self":{"nodeName":"x"},"peers":[{"name":"a"},{"name":"a"}]}`)
	if _, err := Decode(body); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("err = %v", err)
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	body := []byte(`{"version":"1","self":{"nodeName":"x"},"peers":[],"foo":"bar"}`)
	if _, err := Decode(body); err == nil {
		t.Fatal("expected error on unknown field")
	}
}

func TestDecodeRejectsGarbageJSON(t *testing.T) {
	if _, err := Decode([]byte("not-json")); err == nil {
		t.Fatal("expected error on garbage")
	}
}

func TestSnapshotIsIndependent(t *testing.T) {
	snap, err := Decode(validBody())
	if err != nil {
		t.Fatal(err)
	}
	// Mutating the returned slice should not affect subsequent decodes.
	snap.Peers[0].Name = "MUTATED"
	again, _ := Decode(validBody())
	if again.Peers[0].Name == "MUTATED" {
		t.Fatal("peers slice is shared between decodes")
	}
}

func TestValidateBaseURL(t *testing.T) {
	cases := []struct {
		in  string
		bad bool
	}{
		{"", true},
		{"not-a-url", true},
		{"ftp://x", true},
		{"http://", true},
		{"http://example.com", false},
		{"https://example.com:8080/path", false},
	}
	for _, c := range cases {
		err := ValidateBaseURL(c.in)
		if (err != nil) != c.bad {
			t.Errorf("ValidateBaseURL(%q) bad=%v err=%v", c.in, c.bad, err)
		}
	}
}

func TestEnvelopeJSONRoundTrip(t *testing.T) {
	body := validBody()
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatal(err)
	}
	if env.Version != CurrentVersion {
		t.Fatalf("version = %q", env.Version)
	}
	if env.Issued.IsZero() {
		t.Fatal("issued not parsed")
	}
	_ = time.Now() // keep time referenced if helpers shrink
}
