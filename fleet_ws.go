package main

// /ws/fleet — single-stream live fleet state for the duplicacy dashboard.
//
// The hub itself now comes from agent-kit-go/fleet (shared with gdrive-agent),
// so both agents use ONE correct coalescing broadcaster: guarded close, slow-
// consumer frame drop, no self-unregister. This file keeps only the duplicacy-
// specific snapshot shape (buildSnapshot deps) + the thin gin/WS handler.

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	kitfleet "github.com/Daniel-dev22/agent-kit-go/fleet"
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
// field of JobOptionsForm.
type HostInfo struct {
	Hostname    string `json:"hostname"`
	NumCPU      int    `json:"num_cpu"`
	AutoThreads int    `json:"auto_threads"`
}

// autoThreads returns the agent's default thread count for backup/restore
// when the caller passes 0. max(1, NumCPU/2).
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
	Host  *HostInfo   `json:"host,omitempty"`
	Repos []*Repo     `json:"repos"`
	Jobs  []jobPublic `json:"jobs"`
}

// fleetEvent is the wire shape the frontend's useFleet hook expects:
// {"type":"snapshot","data":<snap>}.
type fleetEvent struct {
	Type fleetEventType `json:"type"`
	Data fleetSnapshot  `json:"data"`
}

// fleetHub is the shared agent-kit-go hub specialized to this agent's event
// shape. Aliased so the rest of the agent (app.go) refers to *fleetHub.
type fleetHub = kitfleet.Hub[fleetEvent]

// newFleetHub builds the shared hub with a pure-cache-read snapshot builder.
// Trigger-only (tick 0): the agent pushes on init + every job state/progress
// transition via a.fleet.Trigger(); there is no periodic poll. The repos cache
// is warmed by an initial scan goroutine + mutating ops, so the build never
// forces a synchronous filesystem walk.
func newFleetHub(a *app) *fleetHub {
	host := newHostInfo()
	build := func(_ context.Context) (fleetEvent, error) {
		ev := fleetEvent{Type: fleetEventSnapshot, Data: fleetSnapshot{Host: host}}
		if a != nil {
			repos := a.repos.list()
			// Enrich each repo with its durable last-completed-backup time from
			// the jobs table, so the controller's freshness badge doesn't read a
			// node as "stale" merely because its backup scrolled out of the
			// 50-job fleet window behind copy/check/prune jobs. list() returns
			// value copies, so this never mutates the shared repo cache.
			if a.events != nil {
				if lb, err := a.events.lastBackupByRepo(); err != nil {
					slog.Warn("fleet: last-backup-by-repo query failed", "error", err)
				} else if len(lb) > 0 {
					for i := range repos {
						if t, ok := lb[repos[i].ID]; ok {
							tc := t
							repos[i].LastBackupAt = &tc
						}
					}
				}
			}
			ev.Data.Repos = repos
			ev.Data.Jobs = a.fleetJobs()
		}
		return ev, nil
	}
	return kitfleet.NewHub(build, 0)
}

// handleFleetWS upgrades to WebSocket and streams snapshot frames from the hub:
// one read pump (disconnect detection) + one write pump (snapshots + 30s
// heartbeat). Mirrors gdrive-agent's handler.
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
	ch, unsub := a.fleet.Subscribe()
	defer unsub()

	// Greet the new client with an initial snapshot so it doesn't wait for the
	// next trigger.
	if ev, err := a.fleet.SnapshotNow(c.Request.Context()); err == nil {
		_ = conn.WriteJSON(ev)
	}

	var writeMu sync.Mutex
	done := make(chan struct{})

	// Writer pump: serialize writes through one goroutine (gorilla/websocket is
	// not safe for concurrent writers). Heartbeat shares this pump.
	go func() {
		heartbeat := time.NewTicker(30 * time.Second)
		defer heartbeat.Stop()
		defer conn.Close()
		for {
			select {
			case <-done:
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				writeMu.Lock()
				err := conn.WriteJSON(ev)
				writeMu.Unlock()
				if err != nil {
					return
				}
			case <-heartbeat.C:
				writeMu.Lock()
				err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(10*time.Second))
				writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	// Read pump (blocking) — exits on disconnect, then triggers cleanup.
	defer close(done)
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}
