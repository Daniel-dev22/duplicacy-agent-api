package main

// Orphan reconcile loop. The router's POST /repos/delete call is best-effort
// (fire-and-forget after the central DB row is dropped). If the agent was
// offline at delete time, or the call failed for any reason, the on-disk
// `<repo_path>/.duplicacy/` is now an orphan with no central registration.
//
// reconcileOrphans queries central for the set of repos it still has
// registered for THIS node, then wipes any local repos.json entry whose UUID
// is absent from that set: `RemoveAll(<repo_path>/.duplicacy)` plus
// `mapping.delete()`. Active jobs on a candidate repo defer the wipe to the
// next tick (the same active-job guard handleDeleteRepo enforces).
//
// Safety: an empty central response is treated as "skip this tick" — a
// brand-new central or a transient outage must not trigger mass deletion.
// A path that no longer resolves inside BACKUP_ROOTS is also skipped.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// reconcileTickInterval is how often the periodic loop runs. Five minutes is
// frequent enough that orphans clear "shortly after the agent comes back
// online" but infrequent enough that the GET /api/duplicacy/repos call (a
// JOIN over every repo + storage central tracks) is not a hot path.
const reconcileTickInterval = 5 * time.Minute

// reconcileFetchTimeout bounds the GET /api/duplicacy/repos call. Comfortably
// above p99 of that endpoint while still well under the tick interval.
const reconcileFetchTimeout = 30 * time.Second

type reconcileRepoDTO struct {
	ID       string `json:"id"`
	Node     string `json:"node"`
	RepoPath string `json:"repo_path"`
}

type reconcileReposEnvelope struct {
	Repos []reconcileRepoDTO `json:"repos"`
}

func (a *app) fetchCentralRepos(ctx context.Context) ([]reconcileRepoDTO, error) {
	if a.controlCenterClient == nil {
		return nil, fmt.Errorf("controller client not initialised")
	}
	if a.cfg.ControlCenterURL == "" {
		return nil, fmt.Errorf("controller URL not configured")
	}
	url := strings.TrimRight(a.cfg.ControlCenterURL, "/") + "/api/duplicacy/repos"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := a.controlCenterClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call controller: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller returned %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}
	var env reconcileReposEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	out := env.Repos[:0]
	for _, r := range env.Repos {
		if r.Node == a.cfg.NodeName {
			out = append(out, r)
		}
	}
	return out, nil
}

func (a *app) reconcileOrphans(ctx context.Context) error {
	central, err := a.fetchCentralRepos(ctx)
	if err != nil {
		return fmt.Errorf("fetch central repos: %w", err)
	}
	if len(central) == 0 {
		log.Debug().Msg("reconcile: central returned 0 repos for this node — skipping (safety)")
		return nil
	}
	keep := make(map[string]bool, len(central))
	for _, r := range central {
		keep[r.ID] = true
	}

	wiped := 0
	for _, m := range a.mapping.list() {
		if keep[m.UUID] {
			continue
		}
		clean := filepath.Clean(m.RepoPath)
		if !pathInsideAny(clean, a.cfg.BackupRoots) {
			log.Warn().Str("repo", m.RepoPath).Msg("reconcile: orphan path outside BACKUP_ROOTS — refusing to remove")
			continue
		}
		repoID := repoIDFromPath(clean)
		skip := false
		for _, j := range a.jobs.list() {
			if j.RepoID == repoID && j.State == JobRunning {
				skip = true
				break
			}
		}
		if skip {
			log.Info().Str("repo", clean).Msg("reconcile: orphan but job running — will retry next tick")
			continue
		}
		target := filepath.Join(clean, ".duplicacy")
		if err := os.RemoveAll(target); err != nil {
			log.Error().Err(err).Str("repo", clean).Msg("reconcile: RemoveAll failed")
			continue
		}
		if err := a.mapping.delete(clean); err != nil {
			log.Error().Err(err).Str("repo", clean).Msg("reconcile: mapping.delete failed")
			continue
		}
		log.Info().Str("repo", clean).Str("uuid", m.UUID).Msg("reconcile: wiped orphan .duplicacy/")
		wiped++
	}
	if wiped > 0 {
		if err := a.repos.ScanForce(); err != nil {
			log.Warn().Err(err).Msg("reconcile: post-wipe ScanForce failed")
		}
		a.fleet.Trigger()
	}
	return nil
}

// reconcileLoop runs reconcileOrphans on startup and every reconcileTickInterval
// thereafter. Per project memory feedback_db_unavailability_pattern: log-and-
// continue, never crash the loop. Terminates when a.stop is closed.
func (a *app) reconcileLoop(ctx context.Context) {
	startCtx, startCancel := context.WithTimeout(ctx, reconcileFetchTimeout)
	if err := a.reconcileOrphans(startCtx); err != nil {
		log.Warn().Err(err).Msg("reconcile: startup pass failed (will retry on tick)")
	}
	startCancel()

	t := time.NewTicker(reconcileTickInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.stop:
			return
		case <-t.C:
			tickCtx, tickCancel := context.WithTimeout(ctx, reconcileFetchTimeout)
			if err := a.reconcileOrphans(tickCtx); err != nil {
				log.Warn().Err(err).Msg("reconcile: tick failed")
			}
			tickCancel()
		}
	}
}
