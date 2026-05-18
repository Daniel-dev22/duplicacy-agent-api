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
	"log/slog"
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"

	kitsched "github.com/Daniel-dev22/agent-kit-go/scheduler"
)

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
		PullURL:    fmt.Sprintf("%s/api/duplicacy/schedules?node=%s", cfg.ControlCenterURL, cfg.NodeName),
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
			inv = invocationForCheck(repo, sch.Storage, kitsched.ParamString(sch.Params, "revisions"), kitsched.ParamBool(sch.Params, "all"))
		case ActionPrune:
			inv = invocationForPrune(repo, sch.Storage, kitsched.ParamStrings(sch.Params, "keep_rules"),
				kitsched.ParamBool(sch.Params, "exclusive"), kitsched.ParamBool(sch.Params, "exhaustive"))
		default:
			slog.Warn("schedule fire: unsupported action", "action", sch.Action, "schedule", sch.ID)
			return nil
		}

		var (
			env     []string
			cleanup func()
		)
		if prepareEnv != nil {
			var err error
			env, _, cleanup, err = prepareEnv(ctx, repo)
			if err != nil {
				slog.Error("schedule fire: vend secrets failed; skipping", "error", err, "schedule", sch.ID)
				return err
			}
		}
		if cleanup == nil {
			cleanup = func() {}
		}
		inv.EnvAdds = append(inv.EnvAdds, env...)

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
