package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	eventoutbox "github.com/Daniel-dev22/agent-kit-go/eventoutbox"
	_ "modernc.org/sqlite" // pure-Go sqlite driver, works with CGO_ENABLED=0
)

// EventPayload is the body POSTed to the controller.
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
	// Pointers, not value time.Time: `omitempty` is a no-op on a struct, so a
	// value time.Time serializes the zero value on started/progress events as
	// "0001-01-01T00:00:00Z" instead of omitting it. The controller then stores
	// that non-NULL zero and its COALESCE upsert guard pins it, discarding the
	// real completion time from the terminal event — which makes the freshness
	// rollup read every node as stale. nil ⇒ omitted ⇒ NULL on the controller.
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	ExitCode    int        `json:"exit_code"`
	ErrorMsg    string     `json:"error,omitempty"`
	LineCount   int        `json:"line_count"`
	ScheduleID  string     `json:"schedule_id,omitempty"`
	TriggerKey  string     `json:"trigger_key,omitempty"`
	EmittedAt   time.Time  `json:"emitted_at"`
}

// eventBuffer owns the agent's shared events.sqlite and wires three concerns on
// it: (1) the durable outbound event queue (the kit eventoutbox, which owns the
// pending_events table), (2) the snapshot_stats dedup table (duplicacy-specific,
// read by snapshotStatsStore), and (3) the local `jobs` table that gives the
// fleet snapshot restart-survival — the in-memory jobRegistry starts empty after
// a restart, but this table retains recent job history so the dashboard still
// shows recent backups.
type eventBuffer struct {
	cfg    Config
	db     *sql.DB
	outbox *eventoutbox.Outbox
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
	// snapshot_stats (duplicacy-specific) + jobs (restart-survival). The
	// pending_events outbox table is created by eventoutbox.New below.
	if _, err := db.Exec(`
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

		CREATE TABLE IF NOT EXISTS jobs (
			id              TEXT PRIMARY KEY,
			repo_id         TEXT,
			repo_path       TEXT,
			action          TEXT,
			storage_name    TEXT,
			state           TEXT,
			started_at_ns   INTEGER,
			completed_at_ns INTEGER,
			exit_code       INTEGER,
			error           TEXT,
			line_count      INTEGER,
			schedule_id     TEXT,
			trigger_key     TEXT,
			progress        BLOB,
			updated_at_ns   INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs (updated_at_ns);
		-- Covering index for lastBackupByRepo()'s "MAX(completed_at_ns) WHERE
		-- action='backup' AND state='completed' GROUP BY repo_id": an index scan
		-- instead of a full scan of a multi-hundred-MB jobs table, now that the
		-- fleet build runs it on the liveness tick (and per progress line).
		CREATE INDEX IF NOT EXISTS idx_jobs_action_state_repo
			ON jobs (action, state, repo_id, completed_at_ns);

		-- snapshot_files_cache: gzipped "duplicacy list -files" output, keyed by
		-- the IMMUTABLE (snapshot_id, revision, storage_name) tuple. A revision's
		-- file list never changes once created, so a hit is valid forever with
		-- zero invalidation. Eviction is only (a) size-cap LRU by last_access and
		-- (b) prune-reconcile when a revision leaves retention. See
		-- snapshot_files_cache.go.
		CREATE TABLE IF NOT EXISTS snapshot_files_cache (
			snapshot_id   TEXT    NOT NULL,
			revision      INTEGER NOT NULL,
			storage_name  TEXT    NOT NULL,
			repo_id       TEXT    NOT NULL,
			gz_output     BLOB    NOT NULL,
			raw_bytes     INTEGER NOT NULL,
			gz_bytes      INTEGER NOT NULL,
			cached_at     TIMESTAMP NOT NULL,
			last_access   TIMESTAMP NOT NULL,
			PRIMARY KEY (snapshot_id, revision, storage_name)
		);
		CREATE INDEX IF NOT EXISTS idx_sfc_repo ON snapshot_files_cache (repo_id);
		CREATE INDEX IF NOT EXISTS idx_sfc_access ON snapshot_files_cache (last_access);
		CREATE INDEX IF NOT EXISTS idx_sfc_storage ON snapshot_files_cache (storage_name);
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

	urlFor := func(jobID string) string {
		return cfg.ControlCenterURL + "/api/duplicacy/jobs/" + jobID + "/event"
	}
	outbox, err := eventoutbox.New(db, client, urlFor)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("event outbox: %w", err)
	}

	return &eventBuffer{cfg: cfg, db: db, outbox: outbox}, nil
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

// Start runs the outbox drain loop.
func (e *eventBuffer) Start(ctx context.Context) {
	if e == nil {
		return
	}
	e.outbox.Start(ctx)
}

func (e *eventBuffer) close() {
	if e == nil {
		return
	}
	e.outbox.Close()
	if e.db != nil {
		_ = e.db.Close()
	}
}

// handleJobEvent is the JobEventHook registered with jobRegistry. It persists
// the job to the local jobs table (restart-survival) and enqueues the event for
// durable delivery to the controller.
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
		StartedAt:   tPtr(snap.StartedAt),
		CompletedAt: tPtr(snap.CompletedAt),
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
	e.upsertJob(snap)
	e.outbox.Enqueue(snap.ID, string(evt), body)
}

// --- local jobs table: restart-survival for the fleet snapshot ---

const jobsTable = "jobs"

// tPtr returns nil for the zero time so the JSON `omitempty` actually omits the
// field (a value time.Time is never "empty" and would serialize as the zero
// time). Used for the wire payload to the controller.
func tPtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func tsToNs(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

func nsToTime(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

// upsertJob writes the current job state to the local jobs table. Called on
// every lifecycle event (incl. the Started event), so a job running at crash
// time is persisted as state='running' and the boot sweep can recover it.
func (e *eventBuffer) upsertJob(snap jobPublic) {
	if e == nil || e.db == nil {
		return
	}
	var progressJSON []byte
	if snap.Progress != nil {
		if b, err := json.Marshal(snap.Progress); err == nil {
			progressJSON = b
		}
	}
	_, err := e.db.Exec(`
		INSERT INTO jobs (id, repo_id, repo_path, action, storage_name, state,
			started_at_ns, completed_at_ns, exit_code, error, line_count,
			schedule_id, trigger_key, progress, updated_at_ns)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET
			repo_id=excluded.repo_id, repo_path=excluded.repo_path, action=excluded.action,
			storage_name=excluded.storage_name, state=excluded.state,
			started_at_ns=excluded.started_at_ns, completed_at_ns=excluded.completed_at_ns,
			exit_code=excluded.exit_code, error=excluded.error, line_count=excluded.line_count,
			schedule_id=excluded.schedule_id, trigger_key=excluded.trigger_key,
			progress=excluded.progress, updated_at_ns=excluded.updated_at_ns`,
		snap.ID, snap.RepoID, snap.RepoPath, string(snap.Action), snap.StorageName, string(snap.State),
		tsToNs(snap.StartedAt), tsToNs(snap.CompletedAt), snap.ExitCode, snap.ErrorMsg, snap.LineCount,
		snap.ScheduleID, snap.TriggerKey, progressJSON, time.Now().UnixNano(),
	)
	if err != nil {
		slog.Warn("persist job failed", "error", err, "job", snap.ID)
	}
}

const jobSelectCols = `id, repo_id, repo_path, action, storage_name, state,
	started_at_ns, completed_at_ns, exit_code, error, line_count,
	schedule_id, trigger_key, progress`

func scanJob(s interface{ Scan(...any) error }) (jobPublic, error) {
	var (
		jp                     jobPublic
		action, state          string
		startedNs, completedNs int64
		progress               []byte
	)
	if err := s.Scan(&jp.ID, &jp.RepoID, &jp.RepoPath, &action, &jp.StorageName, &state,
		&startedNs, &completedNs, &jp.ExitCode, &jp.ErrorMsg, &jp.LineCount,
		&jp.ScheduleID, &jp.TriggerKey, &progress); err != nil {
		return jobPublic{}, err
	}
	jp.Action = JobAction(action)
	jp.State = JobState(state)
	jp.StartedAt = nsToTime(startedNs)
	jp.CompletedAt = nsToTime(completedNs)
	if len(progress) > 0 {
		var p JobProgress
		if err := json.Unmarshal(progress, &p); err == nil {
			jp.Progress = &p
		}
	}
	return jp, nil
}

// listRecentJobs returns the most-recently-updated persisted jobs, newest first.
func (e *eventBuffer) listRecentJobs(limit int) ([]jobPublic, error) {
	if e == nil || e.db == nil {
		return nil, nil
	}
	rows, err := e.db.Query(
		`SELECT `+jobSelectCols+` FROM jobs ORDER BY updated_at_ns DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []jobPublic
	for rows.Next() {
		jp, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, jp)
	}
	return out, rows.Err()
}

// lastBackupByRepo returns, per repo_id, the wall-clock time of the most recent
// COMPLETED backup. Unlike the fleet jobs list (capped at fleetJobsCap=50 and
// sorted by recency, so a repo's last backup can fall out of the window behind
// a burst of copy/check/prune jobs), this is a direct MAX over the durable
// jobs table — the controller-side freshness badge derives node staleness from
// it so a node that backed up today never shows "stale" just because its backup
// scrolled out of the 50-job snapshot. Repos with no completed backup (e.g. a
// copy-only relay repo) are absent from the map.
func (e *eventBuffer) lastBackupByRepo() (map[string]time.Time, error) {
	if e == nil || e.db == nil {
		return nil, nil
	}
	rows, err := e.db.Query(
		`SELECT repo_id, MAX(completed_at_ns) FROM jobs
		  WHERE action = 'backup' AND state = 'completed' AND completed_at_ns > 0
		  GROUP BY repo_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var repoID string
		var ns int64
		if err := rows.Scan(&repoID, &ns); err != nil {
			return nil, err
		}
		if ns > 0 {
			out[repoID] = time.Unix(0, ns).UTC()
		}
	}
	return out, rows.Err()
}

// getJob fetches one persisted job by id.
func (e *eventBuffer) getJob(id string) (jobPublic, bool, error) {
	if e == nil || e.db == nil {
		return jobPublic{}, false, nil
	}
	jp, err := scanJob(e.db.QueryRow(`SELECT `+jobSelectCols+` FROM jobs WHERE id = ?`, id))
	if err == sql.ErrNoRows {
		return jobPublic{}, false, nil
	}
	if err != nil {
		return jobPublic{}, false, err
	}
	return jp, true, nil
}
