package main

// After-wave maintenance chain (duplicacy-specific wiring for the kit's
// scheduler.Chain).
//
// Backup and copy schedules are cron-fired (02:00 / 02:15). Check and prune are
// NOT cron-fired: their materialized rows carry schedule {"on":"after_wave"}
// instead of a clock time, so the kit's Matches() never fires them on the tick.
// Instead this chain watches the local job-completion stream and, once the
// night's backup+copy wave has fully drained on this host, fires the prune
// schedules; once those drain, it fires the check schedules. Check runs LAST so
// it verifies the exact post-prune retained snapshot set.
//
// Ordering rationale (backup → copy → prune → check) and why this lives in the
// agent (scheduling is agent-side) are documented in the plan/CLAUDE.md. The
// per-day idempotency + restart-survival lives in the kit (atomic marker file).

import (
	"path/filepath"

	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"
)

// afterWaveSentinel is the value of a schedule's `on` field that marks it as
// fired by the after-wave chain rather than the cron tick. Mirrors the value
// the router policy materializer writes for check/prune jobs.
const afterWaveSentinel = "after_wave"

// newDuplicacyChain builds the after-wave chain: wave = backup+copy, then prune,
// then check. Each stage fires the matching after-wave schedules and waits for
// that action's jobs to drain (gated to one-at-a-time by the maintenance
// semaphore) before the next stage fires.
func newDuplicacyChain(s *scheduler, jobs *jobRegistry, configDir string) *kitsched.Chain {
	matchStage := func(act JobAction) func(kitsched.LocalSchedule) bool {
		return func(sch kitsched.LocalSchedule) bool {
			return sch.Schedule.On == afterWaveSentinel && JobAction(sch.Action) == act
		}
	}
	activeStage := func(act JobAction) func() bool {
		return func() bool { return jobs.countActive(act) > 0 }
	}

	return kitsched.NewChain(s.Scheduler, kitsched.Spec{
		MarkerPath: filepath.Join(configDir, "chain-markers.json"),
		IsWaveJob: func(action string) bool {
			return action == string(ActionBackup) || action == string(ActionCopy)
		},
		// The wave is complete once no backup/copy job is running or pending on
		// this host. OnJobComplete only calls this on a wave-job completion, so
		// "we saw ≥1 finish" is implicit; zero active means the last one just
		// finished. A failed/cancelled wave job still drains — check will
		// surface any resulting issue.
		WaveComplete: func() bool {
			return jobs.countActive(ActionBackup, ActionCopy) == 0
		},
		Stages: []kitsched.Stage{
			{
				Name:       "prune",
				TriggerKey: "chain-prune",
				Match:      matchStage(ActionPrune),
				Active:     activeStage(ActionPrune),
			},
			{
				Name:       "check",
				TriggerKey: "chain-check",
				Match:      matchStage(ActionCheck),
				Active:     activeStage(ActionCheck),
			},
		},
	})
}
