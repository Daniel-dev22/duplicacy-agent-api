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
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
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
		slog.Debug("reconcile: central returned 0 repos for this node — skipping (safety)")
		return nil
	}
	keep := make(map[string]bool, len(central))
	for _, r := range central {
		keep[r.ID] = true
	}

	wiped := 0
	for _, m := range a.mapping.list() {
		if m.UUID == "" {
			// Legacy mapping persisted before mapping.UUID was wired up.
			// Without a UUID we cannot reliably distinguish "valid but
			// un-backfilled" from "actually orphaned" — skip to avoid
			// false-positive deletion. New init/bind calls populate UUID.
			continue
		}
		if keep[m.UUID] {
			continue
		}
		clean := filepath.Clean(m.RepoPath)
		if !pathInsideAny(clean, a.cfg.BackupRoots) {
			slog.Warn("reconcile: orphan path outside BACKUP_ROOTS — refusing to remove", "repo", m.RepoPath)
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
			slog.Info("reconcile: orphan but job running — will retry next tick", "repo", clean)
			continue
		}
		target := filepath.Join(clean, ".duplicacy")
		if err := os.RemoveAll(target); err != nil {
			slog.Error("reconcile: RemoveAll failed", "error", err, "repo", clean)
			continue
		}
		if err := a.mapping.delete(clean); err != nil {
			slog.Error("reconcile: mapping.delete failed", "error", err, "repo", clean)
			continue
		}
		slog.Info("reconcile: wiped orphan .duplicacy/", "repo", clean, "uuid", m.UUID)
		wiped++
	}

	// Second pass: tombstone-driven scan-cache sweep. Catches orphans the
	// mapping pass can't see — primarily repos that were adopt'd into
	// central and then deleted before the adopt-bind path B was deployed
	// (so no mapping entry to compare against), and any future delete whose
	// RPC missed the agent altogether.
	wiped += a.reconcileTombstones(ctx)

	if wiped > 0 {
		if err := a.repos.ScanForce(); err != nil {
			slog.Warn("reconcile: post-wipe ScanForce failed", "error", err)
		}
		a.fleet.Trigger()
	}
	return nil
}

// reconcileTombstones fetches the per-node tombstone set from central and
// wipes <Repo.Path>/.duplicacy/ for any scanned repo whose SnapshotID
// appears in it. Independent of mapping state — the scan cache is the
// source of truth for "what's actually on disk", which is what we need to
// remove. Returns the number of .duplicacy/ directories wiped.
//
// Tombstone fetch failure is logged and treated as "skip this pass" — same
// safety posture as the central-repos fetch above.
func (a *app) reconcileTombstones(ctx context.Context) int {
	tombs, err := a.fetchTombstones(ctx)
	if err != nil {
		slog.Warn("reconcile-tombstone: fetch failed — skipping pass", "error", err)
		return 0
	}
	if len(tombs) == 0 {
		return 0
	}

	// Refresh the scan cache so we're comparing against what's currently on
	// disk, not a stale snapshot from before a deploy.
	if err := a.repos.scan(); err != nil {
		slog.Warn("reconcile-tombstone: pre-sweep scan failed (continuing with cached state)", "error", err)
	}

	tombSet := make(map[string]string, len(tombs))
	for _, t := range tombs {
		tombSet[t.SnapshotID] = t.RepoPath // value = central's view, for logging
	}

	wiped := 0
	for _, r := range a.repos.list() {
		if r.SnapshotID == "" {
			continue
		}
		centralPath, present := tombSet[r.SnapshotID]
		if !present {
			continue
		}
		// Active-job guard — defer wipe to next tick. The tombstone won't
		// disappear: central GCs only after 30 days.
		repoID := repoIDFromPath(r.Path)
		skip := false
		for _, j := range a.jobs.list() {
			if j.RepoID == repoID && j.State == JobRunning {
				skip = true
				break
			}
		}
		if skip {
			slog.Info("reconcile-tombstone: tombstoned but job running — will retry next tick",
				"repo", r.Path,
				"snapshot_id", r.SnapshotID)
			continue
		}
		// Belt-and-suspenders: the agent's own scan already only adds repos
		// found under a backup root, but defence in depth.
		if !pathInsideAny(r.Path, a.cfg.BackupRoots) {
			slog.Warn("reconcile-tombstone: path outside BACKUP_ROOTS — refusing", "repo", r.Path)
			continue
		}
		target := filepath.Join(r.Path, ".duplicacy")
		if err := os.RemoveAll(target); err != nil {
			slog.Error("reconcile-tombstone: RemoveAll failed", "error", err, "repo", r.Path)
			continue
		}
		// Drop any mapping entry keyed on this path (no-op when adopt
		// didn't create one — the common case for these orphans).
		if err := a.mapping.delete(r.Path); err != nil {
			slog.Warn("reconcile-tombstone: mapping.delete failed (non-fatal)", "error", err, "repo", r.Path)
		}
		slog.Info("reconcile-tombstone: wiped orphan .duplicacy/",
			"repo", r.Path,
			"snapshot_id", r.SnapshotID,
			"central_path", centralPath)
		wiped++
	}
	return wiped
}

type tombstoneDTO struct {
	SnapshotID string `json:"snapshot_id"`
	RepoPath   string `json:"repo_path"`
}

type tombstonesEnvelope struct {
	DeletedRepos []tombstoneDTO `json:"deleted_repos"`
}

func (a *app) fetchTombstones(ctx context.Context) ([]tombstoneDTO, error) {
	if a.controlCenterClient == nil {
		return nil, fmt.Errorf("controller client not initialised")
	}
	if a.cfg.ControlCenterURL == "" {
		return nil, fmt.Errorf("controller URL not configured")
	}
	url := strings.TrimRight(a.cfg.ControlCenterURL, "/") +
		"/api/duplicacy/deleted-repos?node=" + a.cfg.NodeName
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := a.controlCenterClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call controller: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		// Controller predates the tombstone endpoint — treat as empty set
		// so the agent keeps working with old controllers in mixed
		// deployments.
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("controller returned %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}
	var env tombstonesEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return env.DeletedRepos, nil
}

// reconcileLoop runs reconcileOrphans on startup and every reconcileTickInterval
// thereafter. Per project memory feedback_db_unavailability_pattern: log-and-
// continue, never crash the loop. Terminates when a.stop is closed.
func (a *app) reconcileLoop(ctx context.Context) {
	startCtx, startCancel := context.WithTimeout(ctx, reconcileFetchTimeout)
	if err := a.reconcileOrphans(startCtx); err != nil {
		slog.Warn("reconcile: startup pass failed (will retry on tick)", "error", err)
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
				slog.Warn("reconcile: tick failed", "error", err)
			}
			tickCancel()
		}
	}
}
