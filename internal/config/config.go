// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
// Package config owns CLI flag parsing. It is split out from main so the
// arguments can be exercised directly in tests without spawning a child
// process.
package config

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/FasterEdge/NetMap/internal/source"
)

// Defaults returns a Config populated with the documented defaults. It is
// the starting point for both CLI and test invocations.
func Defaults() Config {
	return Config{
		Addr:              ":8080",
		AllowOrigin:       "",
		Name:              "netmap-hub",
		PollInterval:      30 * time.Second,
		PollJitter:        5 * time.Second,
		PollWorkers:       4,
		MaxResponseBytes:  1 << 20,
		HTTPTimeout:       3 * time.Second,
		MaxIngestBytes:    4 << 20,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
		ShutdownTimeout:   15 * time.Second,
		StateFile:         "netmap-state.json",
		SaveDebounce:      500 * time.Millisecond,
		MaxStateBytes:     4 << 20,
	}
}

// Config is the fully resolved runtime configuration.
type Config struct {
	Addr              string
	AllowOrigin       string
	Name              string
	Nodes             []source.RegistryEntry
	Policy            source.ValidationPolicy
	IngestToken       string
	PollInterval      time.Duration
	PollJitter        time.Duration
	PollWorkers       int
	MaxResponseBytes  int64
	HTTPTimeout       time.Duration
	MaxIngestBytes    int64
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
	ShutdownTimeout   time.Duration
	StateFile         string
	SaveDebounce      time.Duration
	MaxStateBytes     int64
}

// Parse builds a Config from process arguments using the supplied FlagSet.
// When fs is nil a fresh FlagSet is created. args is forwarded to
// fs.Parse; pass os.Args[1:] in main.
func Parse(name string, fs *flag.FlagSet, args []string, out io.Writer) (Config, error) {
	if fs == nil {
		fs = flag.NewFlagSet(name, flag.ContinueOnError)
		fs.SetOutput(out)
	}
	cfg := Defaults()
	var rawRemotes rawNodes
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "HTTP listen address")
	fs.StringVar(&cfg.AllowOrigin, "origin", cfg.AllowOrigin, "CORS allow-origin (empty = disabled, '*' = all)")
	fs.StringVar(&cfg.Name, "name", cfg.Name, "node name shown as self")
	fs.Var(&rawRemotes, "node", "FasterEdge node to poll: name=baseURL (repeatable)")
	fs.StringVar(&cfg.IngestToken, "ingest-token", cfg.IngestToken, "bearer token required on POST /api/v1/topology (empty disables endpoint)")
	fs.BoolVar(&cfg.Policy.AllowPrivate, "allow-private-nodes", false, "permit loopback / RFC1918 / link-local / multicast sources (dev only)")
	fs.DurationVar(&cfg.PollInterval, "poll-interval", cfg.PollInterval, "polling interval per source")
	fs.DurationVar(&cfg.PollJitter, "poll-jitter", cfg.PollJitter, "maximum random jitter added to each poll")
	fs.IntVar(&cfg.PollWorkers, "poll-workers", cfg.PollWorkers, "maximum concurrent in-flight polls")
	fs.Int64Var(&cfg.MaxResponseBytes, "max-response-bytes", cfg.MaxResponseBytes, "cap on a single upstream response body")
	fs.DurationVar(&cfg.HTTPTimeout, "http-timeout", cfg.HTTPTimeout, "per-request timeout for upstream polls")
	fs.Int64Var(&cfg.MaxIngestBytes, "max-ingest-bytes", cfg.MaxIngestBytes, "cap on a single POST /api/v1/topology body")
	fs.StringVar(&cfg.StateFile, "state-file", cfg.StateFile, "path to versioned JSON state snapshot (empty disables persistence)")
	fs.DurationVar(&cfg.SaveDebounce, "save-debounce", cfg.SaveDebounce, "minimum quiet period before saving observed changes")
	fs.Int64Var(&cfg.MaxStateBytes, "max-state-bytes", cfg.MaxStateBytes, "cap on JSON snapshot load size")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.Name = strings.TrimSpace(cfg.Name)
	if cfg.Name == "" {
		return cfg, fmt.Errorf("name must not be empty")
	}
	if cfg.PollInterval <= 0 {
		return cfg, fmt.Errorf("poll-interval must be > 0")
	}
	if cfg.PollWorkers <= 0 {
		return cfg, fmt.Errorf("poll-workers must be > 0")
	}
	if cfg.HTTPTimeout <= 0 {
		return cfg, fmt.Errorf("http-timeout must be > 0")
	}
	if cfg.StateFile != "" {
		if cfg.SaveDebounce <= 0 {
			return cfg, fmt.Errorf("save-debounce must be > 0")
		}
		if cfg.MaxStateBytes <= 0 {
			return cfg, fmt.Errorf("max-state-bytes must be > 0")
		}
	}
	if cfg.PollJitter < 0 {
		cfg.PollJitter = 0
	}
	if cfg.PollJitter > cfg.PollInterval/2 {
		cfg.PollJitter = cfg.PollInterval / 2
	}
	nodes, err := rawRemotes.Resolve(cfg.Policy)
	if err != nil {
		return cfg, err
	}
	cfg.Nodes = nodes
	return cfg, nil
}

// rawNodes is a flag.Value that collects name=baseURL pairs. Final
// validation happens in Resolve so that the policy is consulted before
// the config is accepted.
type rawNodes []string

func (r *rawNodes) String() string {
	if r == nil {
		return ""
	}
	return fmt.Sprint([]string(*r))
}

func (r *rawNodes) Set(value string) error {
	if value == "" {
		return fmt.Errorf("node spec must not be empty")
	}
	idx := strings.Index(value, "=")
	if idx <= 0 {
		return fmt.Errorf("invalid node spec %q, want name=baseURL", value)
	}
	name := strings.TrimSpace(value[:idx])
	baseURL := strings.TrimSpace(value[idx+1:])
	if name == "" {
		return fmt.Errorf("invalid node spec %q, name must not be empty", value)
	}
	if baseURL == "" {
		return fmt.Errorf("invalid node spec %q, baseURL must not be empty", value)
	}
	// Reject names with characters that would make logging & dedup messy.
	for _, c := range name {
		if c == '"' || c == '\\' || c == ',' || c == '\n' || c == '\r' || c == '\t' {
			return fmt.Errorf("invalid name %q: control characters not allowed", name)
		}
	}
	*r = append(*r, value)
	return nil
}

// Resolve turns the raw strings into validated RegistryEntry values, in
// stable order, with duplicates rejected.
func (r rawNodes) Resolve(policy source.ValidationPolicy) ([]source.RegistryEntry, error) {
	if len(r) == 0 {
		return nil, nil
	}
	type pending struct {
		entry source.RegistryEntry
		order int
	}
	seen := make(map[string]int)
	entries := make([]pending, 0, len(r))
	for idx, raw := range r {
		eq := strings.Index(raw, "=")
		if eq <= 0 {
			return nil, fmt.Errorf("invalid node spec %q", raw)
		}
		name := strings.TrimSpace(raw[:eq])
		baseURL := strings.TrimSpace(raw[eq+1:])
		normalised, err := policy.Validate(baseURL)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", name, err)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate node name %q", name)
		}
		// Belt + braces: same URL registered twice with different names is
		// almost always a config mistake.
		for _, e := range entries {
			if e.entry.URL == normalised {
				return nil, fmt.Errorf("node %q duplicates URL of %q", name, e.entry.Name)
			}
		}
		seen[name] = idx
		entries = append(entries, pending{entry: source.RegistryEntry{Name: name, URL: normalised}, order: idx})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].order == entries[j].order {
			return entries[i].entry.Name < entries[j].entry.Name
		}
		return entries[i].order < entries[j].order
	})
	out := make([]source.RegistryEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.entry)
	}
	return out, nil
}

// SanityCheckURL exposes the validation policy for callers that want to
// pre-flight a URL string.
func SanityCheckURL(raw string, policy source.ValidationPolicy) error {
	_, err := policy.Validate(raw)
	return err
}

// IsAbsoluteURL is a tiny helper kept exported for callers that want to
// assert before they ask the policy.
func IsAbsoluteURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && u.IsAbs()
}
