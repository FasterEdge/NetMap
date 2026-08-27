package config

import (
	"bytes"
	"strings"
	"testing"

	"github.com/FasterEdge/NetMap/internal/source"
)

func TestParseDefaults(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" {
		t.Fatalf("addr = %q", cfg.Addr)
	}
	if cfg.Name != "netmap-hub" {
		t.Fatalf("name = %q", cfg.Name)
	}
}

func TestParseRejectsEmptyName(t *testing.T) {
	_, err := Parse("netmap", nil, []string{"-name="}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRejectsBadNodeSpec(t *testing.T) {
	cases := []string{
		"-node=novalue",
		"-node==http://example.com",
		"-node=name=",
		"-node=name= ",
		"-node= =http://example.com",
	}
	for _, c := range cases {
		_, err := Parse("netmap", nil, []string{c}, &bytes.Buffer{})
		if err == nil {
			t.Errorf("case %q: expected error", c)
		}
	}
}

func TestParseRejectsDuplicateNames(t *testing.T) {
	args := []string{
		"-node=edge-a=http://203.0.113.1",
		"-node=edge-a=http://203.0.113.2",
	}
	_, err := Parse("netmap", nil, args, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsDuplicateURLs(t *testing.T) {
	args := []string{
		"-node=edge-a=http://203.0.113.1",
		"-node=edge-b=http://203.0.113.1",
	}
	_, err := Parse("netmap", nil, args, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseRejectsLoopbackByDefault(t *testing.T) {
	_, err := Parse("netmap", nil, []string{"-node=edge-a=http://127.0.0.1:8080"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseAllowsLoopbackWithFlag(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{
		"-allow-private-nodes",
		"-node=edge-a=http://127.0.0.1:8080",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 1 || cfg.Nodes[0].Name != "edge-a" {
		t.Fatalf("nodes = %+v", cfg.Nodes)
	}
}

func TestParsePreservesOrder(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{
		"-allow-private-nodes",
		"-node=z=http://203.0.113.1",
		"-node=a=http://203.0.113.2",
		"-node=m=http://203.0.113.3",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Nodes) != 3 {
		t.Fatalf("nodes = %+v", cfg.Nodes)
	}
	if cfg.Nodes[0].Name != "z" || cfg.Nodes[2].Name != "m" {
		t.Fatalf("order = %+v", cfg.Nodes)
	}
}

func TestParseRejectsNegativeInterval(t *testing.T) {
	_, err := Parse("netmap", nil, []string{"-poll-interval=0s"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseClampsJitter(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{
		"-poll-interval=10s",
		"-poll-jitter=10s",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PollJitter > cfg.PollInterval/2 {
		t.Fatalf("jitter = %v, interval = %v", cfg.PollJitter, cfg.PollInterval)
	}
}

func TestParseDefaultsIngestTokenEmpty(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{"-ingest-token="}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IngestToken != "" {
		t.Fatalf("IngestToken = %q", cfg.IngestToken)
	}
}

func TestParseAcceptsIngestToken(t *testing.T) {
	cfg, err := Parse("netmap", nil, []string{"-ingest-token=abc123"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.IngestToken != "abc123" {
		t.Fatalf("IngestToken = %q", cfg.IngestToken)
	}
}

func TestRawNodesSetRejectsEmpty(t *testing.T) {
	var r rawNodes
	if err := r.Set(""); err == nil {
		t.Fatal("expected error")
	}
}

func TestRawNodesSetRejectsControlChars(t *testing.T) {
	var r rawNodes
	if err := r.Set("bad,name=http://x"); err == nil {
		t.Fatal("expected error for comma in name")
	}
}

func TestResolveReturnsStableOrder(t *testing.T) {
	entries := rawNodes{
		"a=http://203.0.113.1",
		"b=http://203.0.113.2",
		"c=http://203.0.113.3",
	}
	out, err := entries.Resolve(source.DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("len = %d", len(out))
	}
	if out[0].Name != "a" {
		t.Fatalf("order = %+v", out)
	}
}

func TestSanityCheckURL(t *testing.T) {
	if err := SanityCheckURL("http://127.0.0.1", source.DefaultPolicy()); err == nil {
		t.Fatal("expected deny")
	}
	if err := SanityCheckURL("https://example.com", source.DefaultPolicy()); err != nil {
		t.Fatalf("expected accept, got %v", err)
	}
}

func TestIsAbsoluteURL(t *testing.T) {
	if !IsAbsoluteURL("http://example.com") {
		t.Fatal("expected absolute")
	}
	if IsAbsoluteURL("/local") {
		t.Fatal("expected relative")
	}
}
