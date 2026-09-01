// ─────────────────────────────────────────────────────────────
// FasterEdge 开源项目
// Github: https://github.com/FasterEdge
// Gitee:  https://gitee.com/FasterEdge
// ─────────────────────────────────────────────────────────────
// Package store 提供拓扑数据的内存存储,等价于 FasterEdge 的 NetMapData + NetMapAbility。
package store

import (
	"sort"
	"sync"
	"time"
)

// Peer 描述一个对等节点的网络拓扑条目。
type Peer struct {
	Name     string    `json:"name"`
	Address  string    `json:"address"`
	Role     string    `json:"role"`
	LastSeen time.Time `json:"lastSeen"`
}

// Interface 描述一个网络接口。
type Interface struct {
	Name string   `json:"name"`
	MAC  string   `json:"mac"`
	IPv4 []string `json:"ipv4"`
}

// SelfInfo 描述本节点信息。
type SelfInfo struct {
	NodeName      string      `json:"nodeName"`
	DefaultIface  string      `json:"defaultIface"`
	Interfaces    []Interface `json:"interfaces"`
	HostAddresses []string    `json:"hostAddresses"`
	ScannedAt     time.Time   `json:"scannedAt"`
}

// Topology 是本节点 + 对等节点集合的快照。
type Topology struct {
	Self  SelfInfo `json:"self"`
	Peers []Peer   `json:"peers"`
}

// Store 是拓扑数据的内存存储,线程安全。
type Store struct {
	mu       sync.RWMutex
	selfName string
	peers    map[string]Peer
	onChange func()
}

func New() *Store { return &Store{peers: make(map[string]Peer)} }

// SetOnChange registers a callback invoked after every successful mutation.
// The callback runs without the store lock held, so it may safely take a
// snapshot or schedule persistence. Passing nil disables notifications.
func (s *Store) SetOnChange(fn func()) {
	s.mu.Lock()
	s.onChange = fn
	s.mu.Unlock()
}

func (s *Store) changed() {
	s.mu.RLock()
	fn := s.onChange
	s.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

func (s *Store) SetNodeName(name string) {
	s.mu.Lock()
	changed := s.selfName != name
	s.selfName = name
	s.mu.Unlock()
	if changed {
		s.changed()
	}
}
func (s *Store) NodeName() string { s.mu.RLock(); defer s.mu.RUnlock(); return s.selfName }

func (s *Store) Peer(name string) (Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.peers[name]
	return p, ok
}

func (s *Store) UpsertPeer(p Peer) {
	s.mu.Lock()
	p.LastSeen = time.Now()
	s.peers[p.Name] = p
	s.mu.Unlock()
	s.changed()
}

func (s *Store) DeletePeer(name string) bool {
	s.mu.Lock()
	_, ok := s.peers[name]
	if ok {
		delete(s.peers, name)
	}
	s.mu.Unlock()
	if ok {
		s.changed()
	}
	return ok
}

// ReplaceTopology atomically replaces all in-memory topology fields with a
// loaded snapshot. It preserves LastSeen timestamps exactly as they were
// persisted and does not emit a change notification: loading is not a new
// observed change and must not immediately rewrite the same file.
func (s *Store) ReplaceTopology(top Topology) {
	peers := make(map[string]Peer, len(top.Peers))
	for _, p := range top.Peers {
		peers[p.Name] = p
	}
	s.mu.Lock()
	s.selfName = top.Self.NodeName
	s.peers = peers
	s.mu.Unlock()
}

func (s *Store) AllPeers() []Peer {
	s.mu.RLock()
	out := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		out = append(out, p)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Store) Topology() Topology {
	s.mu.RLock()
	peers := make([]Peer, 0, len(s.peers))
	for _, p := range s.peers {
		peers = append(peers, p)
	}
	name := s.selfName
	s.mu.RUnlock()
	sort.Slice(peers, func(i, j int) bool { return peers[i].Name < peers[j].Name })
	return Topology{Self: SelfInfo{NodeName: name}, Peers: peers}
}
