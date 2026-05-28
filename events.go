package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, works with CGO_ENABLED=0
)

// EventPayload is the body POSTed to controller.
// Router deserializes into the same shape and writes to duplicacy_jobs / duplicacy_job_events.
type EventPayload struct {
	JobID       string    `json:"job_id"`
	Site        string    `json:"site"`
	Node        string    `json:"node"`
	RepoID      string    `json:"repo_id"`
	RepoPath    string    `json:"repo_path"`
	Action      JobAction `json:"action"`
	StorageName string    `json:"storage_name,omitempty"`
	State       JobState  `json:"state"`
	Event       JobEvent  `json:"event"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	ExitCode    int       `json:"exit_code"`
	ErrorMsg    string    `json:"error,omitempty"`
	LineCount   int       `json:"line_count"`
	ScheduleID  string    `json:"schedule_id,omitempty"`
	TriggerKey  string    `json:"trigger_key,omitempty"`
	EmittedAt   time.Time `json:"emitted_at"`
}

// eventBuffer is the durable push queue.
// Lifecycle: handleJobEvent (called on hook fire) → SQLite INSERT → immediate POST.
// On POST failure, row stays; ticker drains every 30s once POST starts succeeding again.
type eventBuffer struct {
	cfg    Config
	client *http.Client
	db     *sql.DB

	wg       sync.WaitGroup
	stopOnce sync.Once
	stop     chan struct{}
}

func newEventBuffer(cfg Config, client *http.Client) (*eventBuffer, error) {
	if err := os.MkdirAll(cfg.ConfigDir, 0700); err != nil {
		return nil, fmt.Errorf("mkdir config dir: %w", err)
	}
	dbPath := filepath.Join(cfg.ConfigDir, "events.sqlite")
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS pending_events (
			id          INTEGER PRIMARY KEY AUTOINCREMENT,
			job_id      TEXT NOT NULL,
			event       TEXT NOT NULL,
			payload     BLOB NOT NULL,
			created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			attempts    INTEGER NOT NULL DEFAULT 0,
			last_error  TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_pending_events_id ON pending_events(id);

		CREATE TABLE IF NOT EXISTS snapshot_stats (
			snapshot_id        TEXT    NOT NULL,
			revision           INTEGER NOT NULL,
			repo_id            TEXT    NOT NULL,
			storage_name       TEXT    NOT NULL,
			destination_key    TEXT    NOT NULL,
			destination_label  TEXT    NOT NULL,
			files              INTEGER,
			bytes              INTEGER,
			bytes_pretty       TEXT,
			total_chunks       INTEGER,
			total_bytes        INTEGER,
			total_bytes_pretty TEXT,
			uniq_chunks        INTEGER,
			uniq_bytes         INTEGER,
			uniq_bytes_pretty  TEXT,
			new_chunks         INTEGER,
			new_bytes          INTEGER,
			new_bytes_pretty   TEXT,
			pool_bytes         INTEGER,
			pool_chunks        INTEGER,
			captured_at        TIMESTAMP NOT NULL,
			PRIMARY KEY (snapshot_id, revision, storage_name)
		);
		CREATE INDEX IF NOT EXISTS idx_snapshot_stats_dest_time
			ON snapshot_stats (destination_key, captured_at);
		CREATE INDEX IF NOT EXISTS idx_snapshot_stats_captured
			ON snapshot_stats (captured_at);
		CREATE INDEX IF NOT EXISTS idx_snapshot_stats_repo
			ON snapshot_stats (repo_id);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	// Idempotent ADD COLUMN for snapshot_stats tables that predate the
	// pool_bytes/pool_chunks columns (1.0.69 → 1.0.70 upgrade path). SQLite
	// lacks "ADD COLUMN IF NOT EXISTS"; catch the duplicate-column error.
	for _, ddl := range []string{
		`ALTER TABLE snapshot_stats ADD COLUMN pool_bytes INTEGER`,
		`ALTER TABLE snapshot_stats ADD COLUMN pool_chunks INTEGER`,
	} {
		if _, err := db.Exec(ddl); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			return nil, fmt.Errorf("schema upgrade: %w", err)
		}
	}

	return &eventBuffer{
		cfg:    cfg,
		client: client,
		db:     db,
		stop:   make(chan struct{}),
	}, nil
}

// DB returns the shared SQLite handle so other subsystems (snapshot_stats)
// can use the same Open/WAL pair. Returns nil if the buffer hasn't been
// initialised — callers should still construct.
func (e *eventBuffer) DB() *sql.DB {
	if e == nil {
		return nil
	}
	return e.db
}

func (e *eventBuffer) close() {
	if e == nil {
		return
	}
	e.stopOnce.Do(func() { close(e.stop) })
	e.wg.Wait()
	if e.db != nil {
		_ = e.db.Close()
	}
}

// handleJobEvent is the JobEventHook registered with jobRegistry.
// It enqueues to SQLite and tries an immediate push (best-effort).
func (e *eventBuffer) handleJobEvent(j *Job, evt JobEvent) {
	snap := j.snapshot()
	payload := EventPayload{
		JobID:       snap.ID,
		Site:        e.cfg.SiteID,
		Node:        e.cfg.NodeName,
		RepoID:      snap.RepoID,
		RepoPath:    snap.RepoPath,
		Action:      snap.Action,
		StorageName: snap.StorageName,
		State:       snap.State,
		Event:       evt,
		StartedAt:   snap.StartedAt,
		CompletedAt: snap.CompletedAt,
		ExitCode:    snap.ExitCode,
		ErrorMsg:    snap.ErrorMsg,
		LineCount:   snap.LineCount,
		ScheduleID:  snap.ScheduleID,
		TriggerKey:  snap.TriggerKey,
		EmittedAt:   time.Now().UTC(),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		slog.Error("marshal event payload failed", "error", err, "job", snap.ID)
		return
	}

	res, err := e.db.Exec(
		`INSERT INTO pending_events (job_id, event, payload) VALUES (?, ?, ?)`,
		snap.ID, string(evt), body,
	)
	if err != nil {
		slog.Error("enqueue event failed; event lost", "error", err, "job", snap.ID)
		return
	}
	rowID, _ := res.LastInsertId()

	go e.tryPush(rowID, snap.ID, body)
}

// tryPush attempts a single POST. On success: DELETE row. On failure: increment attempts.
func (e *eventBuffer) tryPush(rowID int64, jobID string, body []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	url := e.cfg.ControlCenterURL + "/api/duplicacy/jobs/" + jobID + "/event"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		e.recordFailure(rowID, err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		e.recordFailure(rowID, err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if _, err := e.db.Exec(`DELETE FROM pending_events WHERE id = ?`, rowID); err != nil {
			slog.Warn("failed to delete pushed event row", "error", err, "row", rowID)
		}
		return
	}
	respBody, _ := io.ReadAll(resp.Body)
	e.recordFailure(rowID, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200)))
}

func (e *eventBuffer) recordFailure(rowID int64, msg string) {
	if _, err := e.db.Exec(
		`UPDATE pending_events SET attempts = attempts + 1, last_error = ? WHERE id = ?`,
		msg, rowID,
	); err != nil {
		slog.Warn("failed to record push failure", "error", err, "row", rowID)
	}
}

// Start runs the drain loop in a tracked goroutine. The previous pattern
// (caller `go e.drainLoop(ctx)` + Add(1) inside the goroutine) raced with
// e.wg.Wait(); wg.Go does the Add atomically before the goroutine is
// scheduled, eliminating that race.
func (e *eventBuffer) Start(ctx context.Context) {
	e.wg.Go(func() { e.drainLoop(ctx) })
}

// drainLoop runs every 30s, retrying any pending events oldest-first.
// Started via eventBuffer.Start.
func (e *eventBuffer) drainLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()

	// First-tick at startup so any leftover from a previous run flushes immediately.
	e.drainOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stop:
			return
		case <-t.C:
			e.drainOnce(ctx)
		}
	}
}

func (e *eventBuffer) drainOnce(ctx context.Context) {
	// Fetch up to 100 pending rows oldest-first; cap so a huge backlog doesn't block forever.
	rows, err := e.db.QueryContext(ctx,
		`SELECT id, job_id, payload FROM pending_events ORDER BY id ASC LIMIT 100`)
	if err != nil {
		slog.Warn("drain query failed", "error", err)
		return
	}
	type pending struct {
		id      int64
		jobID   string
		payload []byte
	}
	var batch []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.jobID, &p.payload); err != nil {
			slog.Warn("scan failed", "error", err)
			continue
		}
		batch = append(batch, p)
	}
	rows.Close()

	if len(batch) == 0 {
		return
	}

	// Push sequentially: if the first one fails, controller is probably down — bail and retry next tick.
	pushed := 0
	for _, p := range batch {
		select {
		case <-ctx.Done():
			return
		default:
		}
		ok := e.pushBlocking(ctx, p.id, p.jobID, p.payload)
		if !ok {
			break
		}
		pushed++
	}
	if pushed > 0 {
		slog.Info("drained events", "pushed", pushed, "remaining", len(batch)-pushed)
	}
}

// pushBlocking is like tryPush but returns success/failure (used by drain loop).
func (e *eventBuffer) pushBlocking(ctx context.Context, rowID int64, jobID string, body []byte) bool {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := e.cfg.ControlCenterURL + "/api/duplicacy/jobs/" + jobID + "/event"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		e.recordFailure(rowID, err.Error())
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		e.recordFailure(rowID, err.Error())
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if _, err := e.db.Exec(`DELETE FROM pending_events WHERE id = ?`, rowID); err != nil {
			slog.Warn("failed to delete pushed event row", "error", err, "row", rowID)
		}
		return true
	}
	respBody, _ := io.ReadAll(resp.Body)
	e.recordFailure(rowID, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 200)))
	return false
}
