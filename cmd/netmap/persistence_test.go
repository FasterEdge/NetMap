package main

import (
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/FasterEdge/NetMap/internal/snapshot"
)

// TestRunFinalFlushPersistsPendingState verifies the server lifecycle's
// final-flush guarantee. The debounce window is intentionally one hour, so
// the file can only appear if run() explicitly calls Flush during graceful
// shutdown.
func TestRunFinalFlushPersistsPendingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	logger := log.New(io.Discard, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, "netmap", []string{
			"-addr=127.0.0.1:0",
			"-name=flush-hub",
			"-state-file=" + path,
			"-save-debounce=1h",
		}, logger)
	}()
	// Give the HTTP server and poller enough time to enter their wait loops.
	time.Sleep(80 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not return after cancellation")
	}

	ss, err := snapshot.New(path, time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	env, err := ss.Load()
	if err != nil {
		t.Fatalf("load final snapshot: %v", err)
	}
	if env.Topology.Self.NodeName != "flush-hub" {
		t.Fatalf("self = %q, want flush-hub", env.Topology.Self.NodeName)
	}
	if env.Version != snapshot.Version {
		t.Fatalf("version = %q, want %q", env.Version, snapshot.Version)
	}
}

// TestRunRejectsCorruptSnapshot ensures startup does not silently discard
// corrupt persistent state. The operator receives a clear error instead.
func TestRunRejectsCorruptSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := writeCorrupt(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := run(ctx, "netmap", []string{
		"-addr=127.0.0.1:0",
		"-name=test",
		"-state-file=" + path,
	}, log.New(io.Discard, "", 0))
	if err == nil {
		t.Fatal("expected corrupt snapshot startup error")
	}
}

func writeCorrupt(path string) error {
	return os.WriteFile(path, []byte(`{"version":"1.0","topology":`), 0o600)
}
