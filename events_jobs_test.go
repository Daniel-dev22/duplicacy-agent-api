package main

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

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
