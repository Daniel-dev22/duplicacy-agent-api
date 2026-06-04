package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

// The wire payload must OMIT completed_at when the job isn't done yet (started /
// progress events). A value time.Time with `omitempty` would instead serialize
// the zero time as "0001-01-01T00:00:00Z"; the controller stores that non-NULL
// zero and its COALESCE upsert guard then pins it, discarding the real
// completion time from the terminal event — the false-"stale" regression. The
// *time.Time + tPtr(zero)==nil combination is what keeps it omitted.
func TestEventPayloadOmitsZeroCompletedAt(t *testing.T) {
	started := time.Now().UTC()
	running := EventPayload{State: JobRunning, StartedAt: tPtr(started), CompletedAt: tPtr(time.Time{}), EmittedAt: started}
	b, err := json.Marshal(running)
	if err != nil {
		t.Fatalf("marshal running: %v", err)
	}
	if strings.Contains(string(b), "completed_at") {
		t.Fatalf("running event must omit completed_at, got: %s", b)
	}
	if strings.Contains(string(b), "0001-01-01") {
		t.Fatalf("payload leaked the zero time: %s", b)
	}

	completed := time.Now().UTC()
	done := EventPayload{State: JobCompleted, StartedAt: tPtr(started), CompletedAt: tPtr(completed), EmittedAt: completed}
	b, err = json.Marshal(done)
	if err != nil {
		t.Fatalf("marshal completed: %v", err)
	}
	if !strings.Contains(string(b), "completed_at") {
		t.Fatalf("completed event must include completed_at, got: %s", b)
	}
}

func newTestEventBuffer(t *testing.T) *eventBuffer {
	t.Helper()
	cfg := Config{ConfigDir: t.TempDir(), ControlCenterURL: "http://127.0.0.1:0", SiteID: "kd", NodeName: "nas"}
	e, err := newEventBuffer(cfg, &http.Client{})
	if err != nil {
		t.Fatalf("newEventBuffer: %v", err)
	}
	t.Cleanup(e.close)
	return e
}

func TestJobPersistenceRoundTrip(t *testing.T) {
	e := newTestEventBuffer(t)
	started := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Nanosecond)
	completed := time.Now().UTC().Truncate(time.Nanosecond)
	snap := jobPublic{
		ID:          "job-1",
		RepoID:      "repo-a",
		RepoPath:    "/mnt/data",
		Action:      ActionBackup,
		StorageName: "default",
		State:       JobCompleted,
		StartedAt:   started,
		CompletedAt: completed,
		ExitCode:    0,
		LineCount:   42,
		TriggerKey:  "schedule",
		Progress:    &JobProgress{Percent: 100, TotalChunks: 7, NewChunks: 2},
	}
	e.upsertJob(snap)

	got, ok, err := e.getJob("job-1")
	if err != nil || !ok {
		t.Fatalf("getJob: ok=%v err=%v", ok, err)
	}
	if got.ID != snap.ID || got.RepoID != snap.RepoID || got.Action != ActionBackup ||
		got.State != JobCompleted || got.LineCount != 42 || got.TriggerKey != "schedule" {
		t.Fatalf("scalar fields mismatch: %+v", got)
	}
	if !got.StartedAt.Equal(started) || !got.CompletedAt.Equal(completed) {
		t.Fatalf("time mismatch: started %v vs %v, completed %v vs %v",
			got.StartedAt, started, got.CompletedAt, completed)
	}
	if got.Progress == nil || got.Progress.TotalChunks != 7 || got.Progress.NewChunks != 2 {
		t.Fatalf("progress not round-tripped: %+v", got.Progress)
	}

	list, err := e.listRecentJobs(10)
	if err != nil || len(list) != 1 || list[0].ID != "job-1" {
		t.Fatalf("listRecentJobs: %v err=%v", list, err)
	}
}

// fleetJobs must surface persisted jobs even when the in-memory registry is
// empty — this is the restart-survival guarantee: after a restart the registry
// comes up empty, but the fleet snapshot must still show recent backups from the
// SQLite jobs table. Also verifies live jobs are preferred over their persisted
// copy (dedup by id) so live progress isn't masked by a stale row.
func TestFleetJobsMergesPersistedAndLive(t *testing.T) {
	e := newTestEventBuffer(t)
	// Persisted-only terminal job (as if from before a restart).
	e.upsertJob(jobPublic{ID: "persisted-1", Action: ActionBackup, State: JobCompleted, CompletedAt: time.Now().Add(-time.Hour)})
	// A job that is BOTH persisted (stale) and live in-memory (running now).
	e.upsertJob(jobPublic{ID: "dual-1", Action: ActionBackup, State: JobRunning, StartedAt: time.Now()})

	reg := newJobRegistry(0, 0)
	reg.jobs["dual-1"] = &Job{jobPublic: jobPublic{ID: "dual-1", Action: ActionBackup, State: JobRunning, StartedAt: time.Now()}}

	a := &app{jobs: reg, events: e}
	got := a.fleetJobs()

	ids := map[string]int{}
	for _, j := range got {
		ids[j.ID]++
	}
	if ids["persisted-1"] != 1 {
		t.Fatalf("persisted-only job not surfaced after empty-registry merge: %v", ids)
	}
	if ids["dual-1"] != 1 {
		t.Fatalf("dual job should appear exactly once (live preferred), got %d", ids["dual-1"])
	}
}

// A running job persisted before a crash must be recoverable by the local boot
// sweep — flipped to failed with the orphan error + exit 137, with a zero
// completed_at reconstructed as the zero time.
func TestPersistedRunningJobSweptToFailed(t *testing.T) {
	e := newTestEventBuffer(t)
	e.upsertJob(jobPublic{
		ID: "job-run", RepoID: "repo-b", Action: ActionBackup, State: JobRunning,
		StartedAt: time.Now().UTC(),
	})

	ids, err := jobstore.MarkOrphanedFailed(context.Background(), e.DB(), jobstore.SweepOptions{
		Table: jobsTable, ErrorColumn: "error", ErrorMessage: orphanSweepError,
		ExitCodeColumn: "exit_code", ExitCode: 137,
	})
	if err != nil || len(ids) != 1 || ids[0] != "job-run" {
		t.Fatalf("sweep ids=%v err=%v", ids, err)
	}
	got, ok, err := e.getJob("job-run")
	if err != nil || !ok {
		t.Fatalf("getJob after sweep: ok=%v err=%v", ok, err)
	}
	if got.State != JobFailed || got.ExitCode != 137 || got.ErrorMsg != orphanSweepError {
		t.Fatalf("swept row = state %q exit %d err %q", got.State, got.ExitCode, got.ErrorMsg)
	}
	if !got.CompletedAt.IsZero() {
		t.Fatalf("expected zero completed_at, got %v", got.CompletedAt)
	}
}
