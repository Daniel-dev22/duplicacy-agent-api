package main

// Duplicacy-side adapter for the shared per-agent scheduler in
// github.com/Daniel-dev22/agent-kit-go/scheduler. The shared kit owns:
//
//   - the 1-min fire tick + 5-min reconcile pull
//   - the on-disk schedules.json cache (atomic write)
//   - cron matching (mirrors scheduler-api/main.go)
//   - missed-run recovery on boot
//   - the /schedules and /schedules/refresh HTTP plumbing
//
// This file supplies what is duplicacy-specific:
//
//   - the FireFunc that maps a LocalSchedule → a duplicacy CLI invocation
//     (lookup repo by snapshot id, build the cliInvocation per action,
//     vend per-storage RSA keys, hand off to jobs.start).
//   - the constructor that wires repos/jobs/prepareEnv + the per-host
//     bearer-authed HTTP client into a scheduler.Config.
//   - the thin HTTP handlers that delegate to the kit's public methods.

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"
)

// fireJitter returns a deterministic delay in [0, cap) for a given (node,
// scheduleID) pair. Same pair always yields the same offset so reconcile
// pulls and event posts stay aligned with the chosen fire window across
// agent restarts — only the WHOLE-FLEET burst is smeared, not individual
// schedules' cadence.
func fireJitter(node, scheduleID string, cap time.Duration) time.Duration {
	if cap <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(node))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(scheduleID))
	return time.Duration(h.Sum64()%uint64(cap.Nanoseconds())) * time.Nanosecond
}

// prepareEnvFn returns env vars + per-alias RSA private key paths + cleanup
// func for one duplicacy invocation against the given repo. Implemented by
// app.prepareEnvForRepo. The scheduler only fires backup/check/prune (not
// restore) so the RSA priv map is unused here, but the signature stays
// consistent with the underlying implementation.
type prepareEnvFn func(ctx context.Context, repo *Repo) ([]string, map[string]string, func(), error)

// scheduler is the thin agent-side wrapper around kitsched.Scheduler.
// The embedded *kitsched.Scheduler exposes Start / Stop / Pull / Schedules /
// Cache directly — the wrapper exists only to keep the app's field type
// stable and to centralise the duplicacy-specific construction.
type scheduler struct {
	*kitsched.Scheduler
}

// newScheduler builds the duplicacy fire callback and hands it to the kit
// along with the pull URL and disk-cache path.
func newScheduler(cfg Config, client *http.Client, jobs *jobRegistry, repos *repoIndex, prepareEnv prepareEnvFn) (*scheduler, error) {
	fire := makeDuplicacyFire(cfg, jobs, repos, prepareEnv)

	s, err := kitsched.New(kitsched.Config{
		// site scopes the pull to THIS agent's site so a cross-site-synced
		// schedule for the other site's same-named host (e.g. kd-nuc vs ng-nuc)
		// isn't picked up here. Harmless before the controller filters on it
		// (unknown param ignored); required once schedules sync cross-site.
		PullURL:    fmt.Sprintf("%s/api/duplicacy/schedules?node=%s&site=%s", cfg.ControlCenterURL, cfg.NodeName, cfg.SiteID),
		NodeName:   cfg.NodeName,
		CachePath:  filepath.Join(cfg.ConfigDir, "schedules.json"),
		HTTPClient: client,
		Fire:       fire,
	})
	if err != nil {
		return nil, err
	}
	return &scheduler{Scheduler: s}, nil
}

// makeDuplicacyFire returns the FireFunc closure. Mirrors the previous
// schedules.go fire() body verbatim, just adapted to the kit's signature
// (LocalSchedule's Action is plain string; we coerce to JobAction at the
// boundary).
func makeDuplicacyFire(cfg Config, jobs *jobRegistry, repos *repoIndex, prepareEnv prepareEnvFn) kitsched.FireFunc {
	return func(ctx context.Context, sch kitsched.LocalSchedule, triggerKey string, missedRecovery bool) error {
		// Per-agent + per-schedule fire jitter: deterministically delay the
		// fire by 0–60s based on hash(node + schedule_id). Without this,
		// every agent on the fleet fires at the SAME second (02:00:00 etc.)
		// and the resulting reconcile-pull + event-ingest burst overwhelms
		// the controller's pgxpool (MaxConns=8 on the 512Mi CNPG pod).
		// Witnessed 2026-05-27 02:07 when the router returned HTTP 502 to
		// multiple agents for ~90s. Jitter spreads the load across a
		// minute; the bearer-token / vend retry handles any residual.
		//
		// Manual triggers ("manual"/"manual-*") and after-wave chain triggers
		// ("chain-*") skip the jitter. Manual runs are operator-initiated and
		// should fire immediately; chain runs are already serialised by the
		// maintenance semaphore (only one prune/check executes at a time, the
		// rest queue Pending), so they produce no controller-event herd and
		// gain nothing from smearing — while jittering them would block the
		// synchronous FireMatching loop for up to 60s per schedule.
		if !strings.HasPrefix(triggerKey, "manual") && !strings.HasPrefix(triggerKey, "chain") {
			jitter := fireJitter(cfg.NodeName, sch.ID, 60*time.Second)
			if jitter > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(jitter):
				}
			}
		}
		// sch.RepoID is the duplicacy snapshot_id (matches controller's
		// duplicacy_repos.repo_id column), NOT the agent's 12-char path-hash
		// Repo.ID. Use getBySnapshotID — the original implementation hit
		// this bug too (calling generic get() which only indexes by hash).
		repo, ok := repos.getBySnapshotID(sch.RepoID)
		if !ok {
			slog.Warn("schedule fire: repo not found, skipping", "schedule", sch.ID, "repo", sch.RepoID)
			return nil
		}

		action := JobAction(sch.Action)
		var inv cliInvocation
		switch action {
		case ActionBackup:
			threads := kitsched.ParamInt(sch.Params, "threads")
			tag := kitsched.ParamString(sch.Params, "tag")
			inv = invocationForBackup(repo, sch.Storage, tag, threads)
		case ActionCheck:
			inv = invocationForCheck(repo, sch.Storage,
				kitsched.ParamString(sch.Params, "revisions"),
				kitsched.ParamBool(sch.Params, "all"),
				resolveScheduleSnapshotID(sch))
		case ActionPrune:
			// Hard rule (hub-pool safety): prune always carries -id. The
			// materializer sets params.snapshot_id when scoping a per-source
			// prune against the relay repo; manual prune rows on a source
			// repo fall back to sch.RepoID (= that repo's own snapshot id in
			// preferences). Solo-storage repos get -id with their only id,
			// which is a no-op constraint.
			inv = invocationForPrune(repo, sch.Storage, kitsched.ParamStrings(sch.Params, "keep_rules"),
				kitsched.ParamBool(sch.Params, "exclusive"), kitsched.ParamBool(sch.Params, "exhaustive"),
				resolveScheduleSnapshotID(sch))
		case ActionCopy:
			// sch.Storage is the destination alias; params.copy_from is the
			// source (the relay's "default" / nas-primary view). params.copy_id
			// scopes to one source repo's snapshots in the shared chunk pool.
			// RSA priv key (-key) is filled in below after prepareEnv runs.
			from := kitsched.ParamString(sch.Params, "copy_from")
			snapID := kitsched.ParamString(sch.Params, "copy_id")
			threads := kitsched.ParamInt(sch.Params, "threads")
			// Unpinned copies (the common case — every auto-generated copy
			// schedule passes 0) use the low CopyThreads default, NOT
			// autoThreads, to bound per-copy RAM during the nightly fan-out.
			if threads <= 0 {
				threads = cfg.CopyThreads
			}
			inv = invocationForCopy(repo, from, sch.Storage, threads, snapID, "", "")
		default:
			slog.Warn("schedule fire: unsupported action", "action", sch.Action, "schedule", sch.ID)
			return nil
		}

		var (
			env     []string
			rsaPriv map[string]string
			cleanup func()
		)
		if prepareEnv != nil {
			var err error
			env, rsaPriv, cleanup, err = prepareEnv(ctx, repo)
			if err != nil {
				slog.Error("schedule fire: vend secrets failed; skipping", "error", err, "schedule", sch.ID)
				return err
			}
		}
		if cleanup == nil {
			cleanup = func() {}
		}
		inv.EnvAdds = append(inv.EnvAdds, env...)

		// For copy: now that prepareEnv has materialised the source's RSA
		// priv key to /dev/shm, rebuild the inv with -key so duplicacy can
		// read RSA-encrypted source snapshot indices. Without this every
		// nightly copy aborts with "An RSA private key is required to
		// decrypt the chunk".
		if action == ActionCopy && rsaPriv != nil {
			from := kitsched.ParamString(sch.Params, "copy_from")
			if path := rsaPriv[from]; path != "" {
				inv.Args = append(inv.Args, "-key", path)
			}
		}

		j, err := jobs.start(ctx, cfg.DuplicacyBinary, repo, action, sch.Storage, inv, sch.ID, triggerKey, cleanup)
		if err != nil {
			slog.Error("schedule fire failed to start job", "error", err, "schedule", sch.ID)
			return err
		}
		slog.Info("duplicacy schedule fired",
			"schedule", sch.ID,
			"job", j.ID,
			"action", sch.Action,
			"repo", sch.RepoID,
			"missed_recovery", missedRecovery)
		return nil
	}
}

// resolveScheduleSnapshotID returns the -id value to pass for a check/prune
// fire. Prefers params.snapshot_id (set by the central materializer when
// scoping a per-source prune against the relay repo); falls back to
// sch.RepoID which equals the schedule's repo's snapshot id in preferences
// — the right scope for manual one-off check/prune rows on a source repo.
// Always returns a non-empty string for any well-formed schedule row, which
// satisfies the hub-pool safety contract (prune never runs unscoped).
func resolveScheduleSnapshotID(sch kitsched.LocalSchedule) string {
	if s := kitsched.ParamString(sch.Params, "snapshot_id"); s != "" {
		return s
	}
	return sch.RepoID
}

// --- HTTP handlers (delegate to the kit) ---

func (a *app) handleListSchedules(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusOK, gin.H{"schedules": []kitsched.LocalSchedule{}})
		return
	}
	c.JSON(http.StatusOK, gin.H{"schedules": a.scheduler.Schedules()})
}

func (a *app) handleSchedulesRefresh(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduler not initialized"})
		return
	}
	if err := a.scheduler.Pull(c.Request.Context()); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"refreshed": true})
}

func (a *app) handleSchedulesCache(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusOK, gin.H{})
		return
	}
	c.JSON(http.StatusOK, a.scheduler.Cache())
}
