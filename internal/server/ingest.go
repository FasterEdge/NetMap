package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/FasterEdge/NetMap/internal/store"
	"github.com/FasterEdge/NetMap/internal/topology"
)

// handleIngest accepts a POST body carrying a topology envelope. The
// handler enforces:
//
//   - HTTP method is POST (anything else → 405)
//   - Bearer token matches -ingest-token when configured (else 404 to hide
//     the endpoint from unauthenticated clients)
//   - Body is bounded by MaxIngestBytes (else 413)
//   - Content-Type contains application/json (else 415)
//   - Body decodes as a valid topology envelope (else 400)
//
// On success the peers + self are merged into the store and 202 is
// returned with a tiny ack payload.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	s.reqCount.Add(1)
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST, OPTIONS")
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if s.ingestTok == "" {
		http.NotFound(w, r)
		return
	}
	if !s.isAuthed(r) {
		s.writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	ct := r.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		s.writeError(w, http.StatusUnsupportedMediaType, "content-type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.maxIngest)
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.writeError(w, http.StatusRequestEntityTooLarge, "body exceeds max ingest bytes")
			return
		}
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("read body: %v", err))
		return
	}
	snap, err := topology.Decode(body)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := s.mergeIngested(r.Context(), snap); err != nil {
		s.writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.writeJSON(w, http.StatusAccepted, map[string]any{
		"status":     "accepted",
		"source":     snap.Source,
		"version":    snap.Version,
		"receivedAt": time.Now().UTC().Format(time.RFC3339Nano),
		"peers":      len(snap.Peers),
	})
}

// mergeIngested copies the decoded snapshot into the store. It is split
// out so tests can exercise the merge logic without going through a real
// HTTP round trip.
func (s *Server) mergeIngested(_ /*ctx*/ interface{}, snap topology.Snapshot) error {
	// The store doesn't carry provenance; we just hand it the new peers
	// and self info. The peer name is the merge key.
	for _, p := range snap.Peers {
		if strings.TrimSpace(p.Name) == "" {
			continue
		}
		s.store.UpsertPeer(store.Peer{
			Name:    p.Name,
			Address: p.Address,
			Role:    p.Role,
		})
	}
	if strings.TrimSpace(snap.Self.NodeName) != "" {
		s.store.SetNodeName(snap.Self.NodeName)
	}
	return nil
}

// compactJSON is a small helper used by tests that build payloads.
func compactJSON(v any) ([]byte, error) {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return []byte(strings.TrimRight(buf.String(), "\n")), nil
}

// _ keeps encoding/json referenced when the helpers above change.
var _ = compactJSON
