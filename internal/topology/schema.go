// FasterEdge 开源项目 - Github: https://github.com/FasterEdge - Gitee: https://gitee.com/FasterEdge
// Package topology defines the wire format used by FasterEdge nodes when they
// publish a network topology snapshot. The schema is intentionally
// versioned so that NetMap can negotiate an upgrade without losing older
// peers during a rolling rollout.
package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// CurrentVersion is the schema version this build understands.
const CurrentVersion = "1"

// SupportedVersions enumerates every schema version the decoder knows how
// to interpret. Keep this list in lock-step with the decode switch below.
var SupportedVersions = []string{CurrentVersion}

// AcceptVersion returns true when v is a schema version the decoder can
// handle. The check is tolerant of surrounding whitespace.
func AcceptVersion(v string) bool {
	v = strings.TrimSpace(v)
	for _, s := range SupportedVersions {
		if s == v {
			return true
		}
	}
	return false
}

// SelfInfo mirrors data.NetMapLocalInfo on the wire. Field tags are
// identical to the producer side to keep the format symmetric.
type SelfInfo struct {
	NodeName      string      `json:"nodeName"`
	DefaultIface  string      `json:"defaultIface,omitempty"`
	Interfaces    []Interface `json:"interfaces,omitempty"`
	HostAddresses []string    `json:"hostAddresses,omitempty"`
	ScannedAt     time.Time   `json:"scannedAt,omitempty"`
}

// Interface mirrors data.NetMapInterface on the wire.
type Interface struct {
	Name string   `json:"name"`
	MAC  string   `json:"mac,omitempty"`
	IPv4 []string `json:"ipv4,omitempty"`
}

// Peer mirrors ability.NetMapPeer on the wire.
type Peer struct {
	Name     string    `json:"name"`
	Address  string    `json:"address,omitempty"`
	Role     string    `json:"role,omitempty"`
	LastSeen time.Time `json:"lastSeen,omitempty"`
}

// Envelope is the versioned wrapper every producer must emit. Producers
// should set Version to topology.CurrentVersion. Unknown future fields are
// tolerated by the decoder.
type Envelope struct {
	Version string    `json:"version"`
	Source  string    `json:"source,omitempty"` // logical source name (set by NetMap when ingesting)
	Self    SelfInfo  `json:"self"`
	Peers   []Peer    `json:"peers"`
	Issued  time.Time `json:"issuedAt,omitempty"`
}

// Snapshot is the version-stripped view used internally by NetMap. All
// downstream code should consume Snapshot rather than Envelope so that
// future schema bumps do not require touching every call site.
type Snapshot struct {
	Source  string
	Self    SelfInfo
	Peers   []Peer
	Issued  time.Time
	Version string
}

// Decode parses raw bytes into a Snapshot. It rejects unknown versions and
// payloads that cannot be interpreted as a valid envelope. The body byte
// limit is checked by the caller (io.LimitReader); this function trusts
// that the input is bounded.
func Decode(body []byte) (Snapshot, error) {
	if len(body) == 0 {
		return Snapshot{}, errors.New("topology: empty body")
	}
	var env Envelope
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return Snapshot{}, fmt.Errorf("topology: decode: %w", err)
	}
	if !AcceptVersion(env.Version) {
		return Snapshot{}, fmt.Errorf("topology: unsupported version %q (supported: %v)", env.Version, SupportedVersions)
	}
	if err := env.Validate(); err != nil {
		return Snapshot{}, fmt.Errorf("topology: invalid: %w", err)
	}
	return env.Snapshot(), nil
}

// Validate enforces structural rules the JSON decoder cannot express on
// its own: every peer must carry a non-empty Name; Self.NodeName must be
// present on a snapshot.
func (e Envelope) Validate() error {
	if strings.TrimSpace(e.Version) == "" {
		return errors.New("missing version")
	}
	if strings.TrimSpace(e.Self.NodeName) == "" {
		return errors.New("self.nodeName is required")
	}
	seen := make(map[string]struct{}, len(e.Peers))
	for i, p := range e.Peers {
		if strings.TrimSpace(p.Name) == "" {
			return fmt.Errorf("peers[%d]: name is required", i)
		}
		if _, dup := seen[p.Name]; dup {
			return fmt.Errorf("peers[%d]: duplicate name %q", i, p.Name)
		}
		seen[p.Name] = struct{}{}
	}
	return nil
}

// Snapshot strips the version field and returns an internal view.
func (e Envelope) Snapshot() Snapshot {
	peers := make([]Peer, len(e.Peers))
	copy(peers, e.Peers)
	return Snapshot{
		Source:  e.Source,
		Self:    e.Self,
		Peers:   peers,
		Issued:  e.Issued,
		Version: e.Version,
	}
}

// ValidateBaseURL is a light-weight sanity check used when a producer
// dials us back. We require http or https and a non-empty host.
func ValidateBaseURL(raw string) error {
	if raw == "" {
		return errors.New("baseURL is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("baseURL parse: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("baseURL: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("baseURL: empty host")
	}
	return nil
}
