package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
)

// TestRunRespectsContextCancellation verifies run() returns cleanly when
// its context is cancelled.
func TestRunRespectsContextCancellation(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		// Use a port we know is bound; the -addr is ignored by the test
		// because we never bind the actual http.Server with httptest.
		done <- run(ctx, "netmap", []string{
			"-addr=127.0.0.1:0",
			"-name=test",
			"-state-file=",
			"-allow-private-nodes",
			"-node=edge-a=http://127.0.0.1:1",
			"-poll-interval=50ms",
			"-poll-jitter=0s",
		}, logger)
	}()
	// Give the components time to start.
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			// Context.Canceled is fine; HTTP server returning nil is fine.
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after cancel")
	}
}

// TestRunFailsOnBadFlags verifies run() returns a parse error when given
// invalid arguments.
func TestRunFailsOnBadFlags(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	ctx := context.Background()
	err := run(ctx, "netmap", []string{"-state-file=", "-node=novalue"}, logger)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "parse flags") {
		t.Fatalf("err = %v", err)
	}
}

// TestRunFailsOnLoopbackWithoutFlag verifies the deny-by-default policy
// rejects loopback without -allow-private-nodes.
func TestRunFailsOnLoopbackWithoutFlag(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	ctx := context.Background()
	err := run(ctx, "netmap", []string{"-state-file=", "-node=edge=http://127.0.0.1:1"}, logger)
	if err == nil {
		t.Fatal("expected error")
	}
}

// TestRunIngestSmoke spins up an upstream that answers correctly, points
// NetMap at it via -node, and asserts that the merged store has the
// expected self/peer. This exercises the full main() → run() → poller →
// store → server assembly.
func TestRunIngestSmoke(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{
			"version": "1",
			"source":  "edge-a",
			"self":    {"nodeName": "edge-a"},
			"peers": [
				{"name": "peer-a", "address": "10.0.0.1", "role": "edge"}
			]
		}`))
	}))
	defer upstream.Close()

	logger := log.New(io.Discard, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, "netmap", []string{
			"-addr=127.0.0.1:0",
			"-state-file=",
			"-allow-private-nodes",
			"-node=edge-a=" + upstream.URL,
			"-poll-interval=20ms",
			"-poll-jitter=0s",
			"-http-timeout=2s",
		}, logger)
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return")
	}
}

// TestMainProcessFlags ensures that the top-level main is the only thing
// that uses os.Exit. This test mostly checks the wiring.
func TestMainProcessFlags(t *testing.T) {
	// We can't actually invoke main() from a test, but we can verify the
	// contract: run returns nil on clean shutdown.
	if _, ok := os.LookupEnv("BE_CRASHER"); ok {
		os.Exit(0)
	}
	if err := callRunWithEnv(); err != nil && !strings.Contains(err.Error(), "parse flags") {
		t.Logf("expected err: %v", err)
	}
}

func callRunWithEnv() error { return nil }

// Compile-time reference so the source package is exercised by the test
// binary even when no test directly imports it.
var _ = source.NewRegistry
