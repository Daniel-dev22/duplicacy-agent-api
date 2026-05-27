package main

// Boot-time orphan-job sweep.
//
// Pattern witnessed 2026-05-27 overnight: pi-agent crashed/restarted ~50 min
// after a backup started, leaving `pi-home-user` (job
// 6df9da80-9c4f-4529-9dbd-ce14ee5467e3) stuck in state='running' in the
// controller's duplicacy_jobs table indefinitely. JobHandle goroutines die
// with the container, but the controller row only transitions on a terminal
// event from the agent — which never came.
//
// Sweep contract:
//   1. On boot, query the controller for our own jobs in {running, pending}.
//   2. For each, POST a synthetic terminal event marking the row failed with
//      exit_code=137 and a clear "agent restart orphan" error. The existing
//      events queue (SQLite + drain loop) carries the message reliably even
//      if the controller is briefly down at boot.
//   3. Run before HTTP listener opens — keeps the agent in a clean state
//      from the moment any UI / WS client connects.
//
// Idempotent: rows already in a terminal state won't be returned by the
// controller query, so re-running the sweep on a clean boot is a no-op.
// First-boot safe: if no rows match, nothing happens.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// controllerJobRow mirrors the JSON shape the controller returns from
// `GET /api/duplicacy/jobs`. We only care about the fields needed to
// construct a synthetic terminal EventPayload.
type controllerJobRow struct {
	ID          string     `json:"id"`
	Site        string     `json:"site"`
	Node        string     `json:"node"`
	RepoID      string     `json:"repo_id"`
	RepoPath    string     `json:"repo_path"`
	Action      string     `json:"action"`
	Storage     string     `json:"storage,omitempty"`
	State       string     `json:"state"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	ScheduleID  string     `json:"schedule_id,omitempty"`
	TriggerKey  string     `json:"trigger_key,omitempty"`
}

// sweepOrphanJobs runs the boot-time sweep. ctx is the agent's startup
// context; the call is bounded by an internal 15s timeout so a slow or
// unreachable controller doesn't block boot.
func sweepOrphanJobs(ctx context.Context, cfg Config, client *http.Client, events *eventBuffer) {
	if client == nil || events == nil {
		return
	}
	swCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	jobs, err := listOrphanJobs(swCtx, cfg, client)
	if err != nil {
		slog.Warn("orphan sweep: list query failed (skipping; non-fatal)", "error", err)
		return
	}
	if len(jobs) == 0 {
		slog.Info("orphan sweep: no stuck jobs to recover")
		return
	}
	slog.Info("orphan sweep: marking stuck jobs as failed", "count", len(jobs))
	for _, j := range jobs {
		enqueueOrphanTerminal(cfg, events, j)
	}
}

func listOrphanJobs(ctx context.Context, cfg Config, client *http.Client) ([]controllerJobRow, error) {
	q := url.Values{
		"node":  []string{cfg.NodeName},
		"site":  []string{cfg.SiteID},
		"state": []string{"running,pending"},
		"limit": []string{"500"},
	}
	endpoint := cfg.ControlCenterURL + "/api/duplicacy/jobs?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build req: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Jobs []controllerJobRow `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	return out.Jobs, nil
}

// enqueueOrphanTerminal builds a synthetic terminal EventPayload and inserts
// it into the agent's pending_events queue. The drain loop will POST it on
// the next tick.
func enqueueOrphanTerminal(cfg Config, events *eventBuffer, j controllerJobRow) {
	now := time.Now().UTC()
	payload := EventPayload{
		JobID:       j.ID,
		Site:        cfg.SiteID,
		Node:        cfg.NodeName,
		RepoID:      j.RepoID,
		RepoPath:    j.RepoPath,
		Action:      JobAction(j.Action),
		StorageName: j.Storage,
		State:       JobState("failed"),
		Event:       EventFailed,
		StartedAt:   timeOrNow(j.StartedAt, now),
		CompletedAt: now,
		ExitCode:    137,
		ErrorMsg:    "agent restart orphan (boot sweep): previous job did not emit a terminal event",
		LineCount:   0,
		ScheduleID:  j.ScheduleID,
		TriggerKey:  j.TriggerKey,
		EmittedAt:   now,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Warn("orphan sweep: marshal payload failed", "job", j.ID, "error", err)
		return
	}
	res, err := events.db.Exec(
		`INSERT INTO pending_events (job_id, event, payload) VALUES (?, ?, ?)`,
		j.ID, string(EventFailed), body,
	)
	if err != nil {
		slog.Warn("orphan sweep: enqueue failed", "job", j.ID, "error", err)
		return
	}
	rowID, _ := res.LastInsertId()
	slog.Info("orphan sweep: enqueued terminal event",
		"job", j.ID, "repo", j.RepoID, "action", j.Action, "row_id", rowID)
	go events.tryPush(rowID, j.ID, body)
}

// timeOrNow returns *p when non-nil, else fallback.
func timeOrNow(p *time.Time, fallback time.Time) time.Time {
	if p != nil {
		return *p
	}
	return fallback
}
