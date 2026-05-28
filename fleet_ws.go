package main

// /ws/fleet — single-stream live fleet state for the duplicacy dashboard.
//
// The controller fleet page used to poll /repos + /jobs every 15s × N
// nodes; that fan-out plus the agent's per-call /repos walk made the
// dashboard sluggish even after agent-side caching. This WS replaces the
// poll: agent pushes a fresh snapshot whenever something changes (init
// finished, job state transitioned). The router proxies the WS via the
// existing /api/duplicacy/{node}/*path catch-all (Upgrade header dispatch
// in proxyToDuplicacyNode), so no router code is needed.
//
// Pattern follows discovery-api/websocket.go's hub: register/unregister/
// broadcast channels, heartbeat ping every 30s, blocking read pump for
// disconnect detection. Single topic — frontend opens one socket per node
// and just replaces NodeStatus on every snapshot.

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type fleetEventType string

const (
	fleetEventSnapshot fleetEventType = "snapshot"
)

// HostInfo carries static facts about the host the agent runs on. Computed
// once at hub creation; identical across every snapshot. The frontend uses
// AutoThreads to render an honest "Auto (= N)" placeholder in the threads
// field of JobOptionsForm — no per-host config required to make a Pi 4 use
// 2 threads instead of 4.
type HostInfo struct {
	Hostname    string `json:"hostname"`
	NumCPU      int    `json:"num_cpu"`
	AutoThreads int    `json:"auto_threads"`
}

// autoThreads returns the agent's default thread count for backup/restore
// when the caller passes 0. max(1, NumCPU/2) — half the box reserved for
// other workloads. Pi 4 (4 CPUs) → 2; NUC 8 → 4; NAS 16 → 8.
func autoThreads() int {
	auto := runtime.NumCPU() / 2
	if auto < 1 {
		auto = 1
	}
	return auto
}

func newHostInfo() *HostInfo {
	hn, _ := os.Hostname()
	return &HostInfo{
		Hostname:    hn,
		NumCPU:      runtime.NumCPU(),
		AutoThreads: autoThreads(),
	}
}

type fleetSnapshot struct {
	Host  *HostInfo `json:"host,omitempty"`
	Repos []*Repo   `json:"repos"`
	Jobs  []jobPublic `json:"jobs"`
}

type fleetEvent struct {
	Type fleetEventType `json:"type"`
	Data fleetSnapshot  `json:"data"`
}

type fleetClient struct {
	conn *websocket.Conn
	send chan fleetEvent
	mu   sync.Mutex
}

// fleetHub broadcasts fleet snapshots to all connected /ws/fleet clients.
// Snapshot generation runs on the broadcaster goroutine — a coalescing
// trigger channel collapses bursty triggers (e.g. several job state
// transitions in the same second) into one snapshot send.
type fleetHub struct {
	app  *app
	host *HostInfo

	mu      sync.RWMutex
	clients map[*fleetClient]struct{}

	register   chan *fleetClient
	unregister chan *fleetClient
	trigger    chan struct{} // buffered cap 1 so coalescing producers never block
}

func newFleetHub(a *app) *fleetHub {
	return &fleetHub{
		app:        a,
		host:       newHostInfo(),
		clients:    map[*fleetClient]struct{}{},
		register:   make(chan *fleetClient),
		unregister: make(chan *fleetClient),
		trigger:    make(chan struct{}, 1),
	}
}

// Trigger asks the hub to broadcast a fresh snapshot to all clients. Calls
// from the same burst coalesce — the channel buffer is 1, so once a
// snapshot is queued additional Trigger() calls are dropped until that
// snapshot has been built.
func (h *fleetHub) Trigger() {
	if h == nil {
		return
	}
	select {
	case h.trigger <- struct{}{}:
	default:
	}
}

func (h *fleetHub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-h.register:
			h.mu.Lock()
			h.clients[c] = struct{}{}
			h.mu.Unlock()
			// Send initial snapshot only to the new client so the rest of
			// the fleet doesn't get a noop re-broadcast on every connect.
			if ev, ok := h.buildSnapshot(); ok {
				h.sendOne(c, ev)
			}
		case c := <-h.unregister:
			h.handleUnregister(c)
		case <-h.trigger:
			ev, ok := h.buildSnapshot()
			if !ok {
				continue
			}
			h.broadcast(ev)
		}
	}
}

// handleUnregister removes a client and closes its send channel exactly once.
// A client can reach unregister from two paths — sendOne() on a full buffer and
// the WS handler's read-pump defer on disconnect — so the close MUST be guarded
// on still-present membership. Without the guard the second unregister double-
// closes c.send → `panic: close of closed channel`, which crashed the whole
// agent under bursty job churn. Runs only on the Run() goroutine, so the
// map check + close are not racing another unregister.
func (h *fleetHub) handleUnregister(c *fleetClient) {
	h.mu.Lock()
	_, present := h.clients[c]
	delete(h.clients, c)
	h.mu.Unlock()
	if present {
		close(c.send)
	}
}

func (h *fleetHub) buildSnapshot() (fleetEvent, bool) {
	if h.app == nil {
		return fleetEvent{}, false
	}
	// Pure cache read. Earlier this called h.app.repos.scan() inline; the
	// initial cold scan on slow hosts (Pi 4 with /var/lib/rancher/k3s in
	// BACKUP_ROOTS, NAS with deep trees) takes 7–20 seconds, which exceeds
	// the frontend's WS connect timeout and causes useDuplicacyFleet to
	// mark the host offline. Subsequent mutating ops (init/bind/delete,
	// job-state transitions) call ScanForce or trigger via job hooks, so
	// the cache stays current without forcing a synchronous walk on every
	// client connect. The initial scan runs in a goroutine from newApp,
	// which fires fleet.Trigger() on completion to refresh any client
	// that connected before the cache warmed.
	return fleetEvent{
		Type: fleetEventSnapshot,
		Data: fleetSnapshot{
			Host:  h.host,
			Repos: h.app.repos.list(),
			Jobs:  h.app.jobs.list(),
		},
	}, true
}

func (h *fleetHub) broadcast(ev fleetEvent) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		h.sendOne(c, ev)
	}
}

// sendOne is non-blocking — slow client gets disconnected rather than
// stalling the broadcaster. The unregister path closes c.send so the
// per-client writer goroutine exits cleanly.
func (h *fleetHub) sendOne(c *fleetClient, ev fleetEvent) {
	select {
	case c.send <- ev:
	default:
		slog.Warn("fleet ws: client send buffer full, dropping")
		go func() { h.unregister <- c }()
	}
}

func (a *app) handleFleetWS(c *gin.Context) {
	if a.fleet == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "fleet hub not initialised"})
		return
	}
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Warn("fleet ws upgrade failed", "error", err)
		return
	}
	client := &fleetClient{conn: conn, send: make(chan fleetEvent, 16)}
	a.fleet.register <- client

	done := make(chan struct{})

	// Writer pump: serialize writes through one goroutine (gorilla/websocket
	// is not safe for concurrent writers). Heartbeat shares this pump.
	go func() {
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()
		defer conn.Close()
		for {
			select {
			case <-done:
				return
			case ev, ok := <-client.send:
				if !ok {
					return
				}
				client.mu.Lock()
				err := conn.WriteJSON(ev)
				client.mu.Unlock()
				if err != nil {
					return
				}
			case <-heartbeat.C:
				client.mu.Lock()
				err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
				client.mu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	// Read pump (blocking) — exits on disconnect, then triggers cleanup.
	defer func() {
		close(done)
		a.fleet.unregister <- client
	}()
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
