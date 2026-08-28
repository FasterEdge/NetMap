<div align="center">
  <img src="Logo.png" alt="NetMap" width="120"/>  
  <h2>NetMap</h2>
  <h3>FasterEdge Network Topology Manager</h3>
</div>

A standalone network topology manager with a web frontend.

It receives topology snapshots from FasterEdge nodes (`NetMapAbility`) and renders the network topology graph in real time in the browser.

### 1. Design

- **Backend**: Go standard library + `net/http`, using FasterEdge's `NetMapData` / `NetMapAbility` data models
- **Frontend**: pure vanilla JS + SVG self-drawn topology (no external CDN, fully offline-capable), embedded into the binary via `go:embed`
- **Transport**: REST API, periodically polling upstream nodes to fetch the topology (JSON over HTTP)
- **Storage**: in-memory topology table, supporting concurrent reads and writes

### 2. Usage

```bash
# Start a hub that periodically polls 3 FasterEdge nodes
netmap -addr=:8080 -name=hub-01 -allow-private-nodes \
       -node=edge-a=http://10.0.0.1:8080 \
       -node=edge-b=http://10.0.0.2:8080 \
       -node=cloud-c=https://ctrl.example.com

# Open http://localhost:8080/ in the browser to see the topology graph.
```

### 3. HTTP API

| Path | Method | Description |
|---|---|---|
| `/` | GET | Embedded web frontend (SVG topology graph) |
| `/api/topology` | GET | Full topology (`{self, peers[]}`) |
| `/api/peers` | GET | Peer node list |
| `/api/self` | GET | This node's information |
| `/api/healthz` | GET | Health check |
| `/api/v1/topology` | POST | Report topology using a Bearer Token; enabled only when `-ingest-token` is configured |
| `/api/v1/sources` | GET | Data source status list |
| `/api/...` | OPTIONS | CORS preflight (returns 204) |

### 4. Development

```bash
go test ./...                 # Unit tests
go build -o netmap ./cmd/netmap
./netmap -name=dev
```

### 5. Ports and CORS

- `-addr`: HTTP listening address, default `:8080`
- `-origin`: CORS-allowed origin, default empty (CORS off), `*` enables it
- `-name`: display name of this node
- `-allow-private-nodes`: allow polling loopback, RFC1918, link-local and multicast addresses; recommended for development environments only
- `-state-file`: state snapshot file, default `netmap-state.json`; set to empty to disable persistence
- `-ingest-token`: Bearer Token required to enable the topology reporting endpoint

### 6. Architecture

```
            ┌────────────────────┐
            │   FasterEdge node  │
            │ (runs NetMapAbility)│
            └──────────┬─────────┘
                       │ HTTP /api/topology (polling)
                       ▼
            ┌────────────────────┐
            │      NetMap        │
            │  ┌──────────────┐  │
            │  │  Store       │  │ ◀──── in-memory topology
            │  └──────────────┘  │
            │  ┌──────────────┐  │
            │  │  HTTP Server │  │ ────▶ browser (embedded SVG)
            │  └──────────────┘  │
            └────────────────────┘
```

### 7. Cross-Platform

No cgo, no unsafe dependencies, compiles across 6 platforms (linux/amd64, linux/arm64, windows/amd64, darwin/amd64, darwin/arm64, freebsd/amd64).

### 8. Relation to FasterEdge

NetMap is an **independent ecosystem project** of FasterEdge — it does not replace `NetMapAbility`, but plays the role of a "centralized topology panel" in multi-node deployment scenarios:

- `NetMapAbility` is responsible for **single-node** local topology
- `NetMap` is responsible for the **multi-node** global view

The two interconnect via JSON over HTTP; a FastEdge node only needs to expose `/api/topology` for NetMap to pull.