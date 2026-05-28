package main

// Boot-time orphan-job sweep (LOCAL).
//
// A JobHandle goroutine dies with the container, but a row left in
// state='running'/'pending' only transitions on a terminal event — which never
// comes after a crash. Previously this swept by querying the CONTROLLER for our
// running jobs, which failed exactly when it mattered most: on 2026-05-28 02:42
// the NAS agent restarted while CNPG was momentarily refusing connections, so
// the central query errored and the sweep no-op'd.
//
// Now the sweep runs against the agent's OWN local jobs table (events.sqlite),
// which is always available, then enqueues a synthetic terminal event per
// recovered job so the controller row converges when it's reachable. The
// enqueue rides the durable outbox, so controller downtime at boot no longer
// loses the recovery. First-boot safe + idempotent: a clean table sweeps nothing.

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/jobstore"
)

const orphanSweepError = "agent restart orphan (boot sweep): previous job did not emit a terminal event"

func sweepOrphanJobs(ctx context.Context, cfg Config, events *eventBuffer) {
	if events == nil || events.DB() == nil {
		return
	}
	ids, err := jobstore.MarkOrphanedFailed(ctx, events.DB(), jobstore.SweepOptions{
		Table:          jobsTable,
		ErrorColumn:    "error",
		ErrorMessage:   orphanSweepError,
		ExitCodeColumn: "exit_code",
		ExitCode:       137,
	})
	if err != nil {
		slog.Warn("orphan sweep: local sweep failed (skipping; non-fatal)", "error", err)
		return
	}
	if len(ids) == 0 {
		slog.Info("orphan sweep: no stuck jobs to recover")
		return
	}
	slog.Info("orphan sweep: marking stuck jobs as failed", "count", len(ids))
	now := time.Now().UTC()
	for _, id := range ids {
		jp, ok, err := events.getJob(id)
		if err != nil || !ok {
			continue
		}
		payload := EventPayload{
			JobID:       id,
			Site:        cfg.SiteID,
			Node:        cfg.NodeName,
			RepoID:      jp.RepoID,
			RepoPath:    jp.RepoPath,
			Action:      jp.Action,
			StorageName: jp.StorageName,
			State:       JobState("failed"),
			Event:       EventFailed,
			StartedAt:   jp.StartedAt,
			CompletedAt: now,
			ExitCode:    137,
			ErrorMsg:    orphanSweepError,
			LineCount:   jp.LineCount,
			ScheduleID:  jp.ScheduleID,
			TriggerKey:  jp.TriggerKey,
			EmittedAt:   now,
		}
		body, err := json.Marshal(payload)
		if err != nil {
			slog.Warn("orphan sweep: marshal payload failed", "job", id, "error", err)
			continue
		}
		events.outbox.Enqueue(id, string(EventFailed), body)
		slog.Info("orphan sweep: recovered stuck job", "job", id, "repo", jp.RepoID, "action", string(jp.Action))
	}
}
