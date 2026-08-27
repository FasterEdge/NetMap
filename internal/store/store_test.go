package store

import (
	"testing"
	"time"
)

func TestStorePeerCRUD(t *testing.T) {
	s := New()
	if got := s.NodeName(); got != "" {
		t.Fatalf("initial name = %q", got)
	}
	s.SetNodeName("hub-1")
	if got := s.NodeName(); got != "hub-1" {
		t.Fatalf("name after set = %q", got)
	}
	if _, ok := s.Peer("missing"); ok {
		t.Fatal("missing peer reported as present")
	}
	s.UpsertPeer(Peer{Name: "edge-1", Address: "10.0.0.1:7000", Role: "edge"})
	if p, ok := s.Peer("edge-1"); !ok || p.Name != "edge-1" {
		t.Fatalf("peer lookup failed: %+v ok=%v", p, ok)
	}
	if p, _ := s.Peer("edge-1"); p.LastSeen.IsZero() {
		t.Fatal("LastSeen not set on Upsert")
	}
	// 排序
	s.UpsertPeer(Peer{Name: "edge-2", Address: "10.0.0.2:7000"})
	s.UpsertPeer(Peer{Name: "edge-0", Address: "10.0.0.0:7000"})
	peers := s.AllPeers()
	if len(peers) != 3 {
		t.Fatalf("peers len = %d", len(peers))
	}
	if peers[0].Name != "edge-0" || peers[2].Name != "edge-2" {
		t.Fatalf("peers not sorted: %+v", peers)
	}
	if !s.DeletePeer("edge-1") {
		t.Fatal("DeletePeer returned false on existing peer")
	}
	if s.DeletePeer("ghost") {
		t.Fatal("DeletePeer returned true on missing peer")
	}
}

func TestStoreTopology(t *testing.T) {
	s := New()
	s.SetNodeName("hub")
	s.UpsertPeer(Peer{Name: "a", Address: "10.0.0.1"})
	s.UpsertPeer(Peer{Name: "b", Address: "10.0.0.2", Role: "edge"})
	top := s.Topology()
	if top.Self.NodeName != "hub" {
		t.Fatalf("self.name = %q", top.Self.NodeName)
	}
	if len(top.Peers) != 2 {
		t.Fatalf("peers len = %d", len(top.Peers))
	}
	// LastSeen 应该是合理的"刚刚"
	if time.Since(top.Peers[0].LastSeen) > 5*time.Second {
		t.Fatal("LastSeen not recent")
	}
}

func TestStoreConcurrent(t *testing.T) {
	s := New()
	const N = 64
	done := make(chan struct{}, N)
	for i := 0; i < N; i++ {
		go func(i int) {
			s.UpsertPeer(Peer{Name: string(rune('a' + i%26)), Address: "x"})
			_ = s.AllPeers()
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < N; i++ {
		<-done
	}
}
