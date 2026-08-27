package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/FasterEdge/NetMap/internal/config"
	"github.com/FasterEdge/NetMap/internal/poller"
	"github.com/FasterEdge/NetMap/internal/server"
	"github.com/FasterEdge/NetMap/internal/snapshot"
	"github.com/FasterEdge/NetMap/internal/source"
	"github.com/FasterEdge/NetMap/internal/store"
	"github.com/FasterEdge/NetMap/internal/topology"
)

// run 组装所有组件并阻塞,直到 ctx 被取消或某个组件返回错误。
// 它是纯函数风格:除了 logger 之外不接受任何带副作用的输入,所有
// 状态都来自 cfg,方便测试时直接构造 cfg 验证装配。
func snapshotPeersToStore(in []snapshot.Peer) []store.Peer {
	out := make([]store.Peer, len(in))
	for i, p := range in {
		out[i] = store.Peer{
			Name: p.Name, Address: p.Address, Role: p.Role, LastSeen: p.LastSeen,
		}
	}
	return out
}

func storeTopologyToSnapshot(top store.Topology) snapshot.Envelope {
	peers := make([]snapshot.Peer, len(top.Peers))
	for i, p := range top.Peers {
		peers[i] = snapshot.Peer{
			Name: p.Name, Address: p.Address, Role: p.Role, LastSeen: p.LastSeen,
		}
	}
	return snapshot.Envelope{
		Version: snapshot.Version,
		Topology: snapshot.Topology{
			Self:  snapshot.SelfInfo{NodeName: top.Self.NodeName},
			Peers: peers,
		},
	}
}

func run(ctx context.Context, prog string, args []string, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}
	cfg, err := config.Parse(prog, nil, args, io.Discard)
	if err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	st := store.New()
	st.SetNodeName(cfg.Name)

	// Load the persisted topology before starting pollers or HTTP handlers so
	// every observer sees one coherent initial state. Missing files are the
	// normal first-run case; malformed or unsupported snapshots are rejected
	// to avoid silently starting from an empty topology.
	var snapStore *snapshot.Store
	if cfg.StateFile != "" {
		snapStore, err = snapshot.New(cfg.StateFile, cfg.SaveDebounce, cfg.MaxStateBytes)
		if err != nil {
			return fmt.Errorf("snapshot store: %w", err)
		}
		env, loadErr := snapStore.Load()
		switch {
		case loadErr == nil:
			st.ReplaceTopology(store.Topology{
				Self:  store.SelfInfo{NodeName: env.Topology.Self.NodeName},
				Peers: snapshotPeersToStore(env.Topology.Peers),
			})
			// Explicit CLI configuration wins over a persisted empty name.
			if st.NodeName() == "" {
				st.SetNodeName(cfg.Name)
			}
			logger.Printf("loaded topology snapshot: path=%s peers=%d", cfg.StateFile, len(env.Topology.Peers))
		case errors.Is(loadErr, os.ErrNotExist):
			logger.Printf("topology snapshot not found (first run): path=%s", cfg.StateFile)
		default:
			return fmt.Errorf("load topology snapshot %s: %w", cfg.StateFile, loadErr)
		}

		provider := func() (snapshot.Envelope, error) {
			top := st.Topology()
			return storeTopologyToSnapshot(top), nil
		}
		// Schedule every observed mutation through the debounce gate. The
		// callback intentionally captures the provider rather than a concrete
		// topology so the write always sees the latest state.
		st.SetOnChange(func() { snapStore.Schedule(provider) })
	}

	reg := source.NewRegistry()
	// Pre-register every statically configured source so /api/v1/sources
	// shows them before the first poll lands.
	for _, n := range cfg.Nodes {
		if _, err := reg.Register(n.Name, n.URL, cfg.Policy); err != nil {
			return fmt.Errorf("register %s: %w", n.Name, err)
		}
	}

	httpClient := &http.Client{
		Timeout: cfg.HTTPTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        32,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	pollCfg := poller.Config{
		Interval:         cfg.PollInterval,
		Jitter:           cfg.PollJitter,
		HTTPClient:       httpClient,
		MaxResponseBytes: cfg.MaxResponseBytes,
		Workers:          cfg.PollWorkers,
		Logger:           logger,
	}

	merge := func(_ source.Source, snap topology.Snapshot) {
		if snap.Self.NodeName != "" {
			st.SetNodeName(snap.Self.NodeName)
		}
		for _, p := range snap.Peers {
			if p.Name == "" {
				continue
			}
			st.UpsertPeer(store.Peer{
				Name:    p.Name,
				Address: p.Address,
				Role:    p.Role,
			})
		}
	}

	plr, err := poller.New(reg, pollCfg, poller.HTTPFetcher(pollCfg), merge, nil)
	if err != nil {
		return fmt.Errorf("poller: %w", err)
	}

	srvCfg := server.Config{
		Addr:              cfg.Addr,
		AllowOrigin:       cfg.AllowOrigin,
		IngestToken:       cfg.IngestToken,
		MaxIngestBytes:    cfg.MaxIngestBytes,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
		ShutdownTimeout:   cfg.ShutdownTimeout,
	}
	srv := server.New(st, reg, srvCfg)

	// Serve 与 poller 并行启动;任一返回错误就取消另一个并退出。
	// 退出码统一为 0(主动取消)或错误传播。
	var wg sync.WaitGroup
	errCh := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Serve(ctx); err != nil {
			errCh <- fmt.Errorf("serve: %w", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// Poller.Run blocks until ctx is cancelled; it returns nil on
		// graceful shutdown. We only forward non-nil errors.
		if err := plr.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("poller: %w", err)
		}
	}()

	logger.Printf("NetMap started: name=%s addr=%s nodes=%d ingest=%v",
		cfg.Name, cfg.Addr, len(cfg.Nodes), cfg.IngestToken != "")
	<-ctx.Done()
	logger.Println("shutdown signal received, draining...")
	wg.Wait()
	// Final synchronous checkpoint after every producer has stopped. This
	// guarantees mutations inside the debounce window are not lost during a
	// graceful shutdown.
	if snapStore != nil {
		// Always enqueue one final provider so even a server that made no
		// post-load mutations gets a cleanly versioned checkpoint.
		snapStore.Schedule(func() (snapshot.Envelope, error) {
			return storeTopologyToSnapshot(st.Topology()), nil
		})
		if err := snapStore.Flush(); err != nil {
			return fmt.Errorf("flush topology snapshot: %w", err)
		}
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	logger.Println("NetMap stopped cleanly")
	return nil
}
