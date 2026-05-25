package main

// POST /repos/delete — wipes <repo_path>/.duplicacy/ and drops the mapping
// entry. Idempotent: missing .duplicacy/ is treated as success. Refuses with
// 409 if a job is currently running against this repo (operator must cancel
// first). The router calls this fire-and-forget after dropping the central
// duplicacy_repos row; if it can't reach the agent, reconcile.go cleans up
// the orphan on the next tick.
//
// Path resolution: the router may send EITHER the parent directory of the
// on-disk `.duplicacy/` (legacy) OR the source path stored by the adopt flow
// (which is what `canonicalRepoPath` produces — preferences[0].repository,
// not the cache-wrapper dir). Adopt-via-cache-wrapper repos have
// `.duplicacy/` at e.g. `/x/duplicacy/cache/localhost/3/.duplicacy/` but
// `duplicacy_repos.repo_path` = `/x`. Without resolution, the literal
// `os.Stat("/x/.duplicacy")` returns ENOENT and the real `.duplicacy/`
// survives, producing orphan re-discovery loops. When the literal target
// doesn't exist, fall back to the scan cache: any Repo whose SourcePath
// matches `clean` points us at the actual on-disk Path; wipe every match
// (cache-wrapper + ISO-root mirror can both resolve to the same source).
//
// Active-job guard checks BOTH the literal-path repo id and every scan-cache
// match before any removal — otherwise resolving multiple repos could
// silently skip the job check on the sibling rows.

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

type deleteRepoReq struct {
	RepoPath string `json:"repo_path"`
}

type deleteRepoResp struct {
	Removed     bool     `json:"removed"`
	RepoPath    string   `json:"repo_path"`
	WipedPaths  []string `json:"wiped_paths,omitempty"`  // every .duplicacy/ parent we removed
	ResolvedVia string   `json:"resolved_via,omitempty"` // "literal" | "scan-cache" | "none"
}

func (a *app) handleDeleteRepo(c *gin.Context) {
	var req deleteRepoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !strings.HasPrefix(req.RepoPath, "/") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "repo_path must be absolute"})
		return
	}
	clean := filepath.Clean(req.RepoPath)
	if clean != req.RepoPath {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("repo_path not canonical (got %q want %q)", req.RepoPath, clean)})
		return
	}
	if !pathInsideAny(clean, a.cfg.BackupRoots) {
		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("repo_path %s not inside BACKUP_ROOTS %v", clean, a.cfg.BackupRoots)})
		return
	}

	// Resolve every on-disk `.duplicacy/` parent path this request should
	// wipe. Literal path first; if that doesn't exist, fall back to the
	// scan cache by matching against SourcePath / SourceHostPath.
	literalTarget := filepath.Join(clean, ".duplicacy")
	resolvedParents := []string{}
	resolvedVia := "none"
	if _, err := os.Stat(literalTarget); err == nil {
		resolvedParents = append(resolvedParents, clean)
		resolvedVia = "literal"
	} else if !errors.Is(err, os.ErrNotExist) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "stat .duplicacy: " + err.Error()})
		return
	} else {
		// Fall back to scan-cache lookup. Ensure cache is reasonably fresh.
		if err := a.repos.scan(); err != nil {
			slog.Warn("pre-delete scan failed (continuing with cached state)", "error", err)
		}
		for _, r := range a.repos.list() {
			if r.SourcePath == clean || r.SourceHostPath == clean {
				resolvedParents = append(resolvedParents, r.Path)
			}
		}
		if len(resolvedParents) > 0 {
			resolvedVia = "scan-cache"
		}
	}

	// Active-job guard runs against EVERY repo we're about to touch, not
	// just the literal-path repo. With multi-resolution this matters: the
	// cache-wrapper and ISO-root mirror are two distinct Repo.IDs.
	checkIDs := []string{repoIDFromPath(clean)}
	for _, p := range resolvedParents {
		checkIDs = append(checkIDs, repoIDFromPath(p))
	}
	for _, j := range a.jobs.list() {
		if j.State != JobRunning {
			continue
		}
		for _, id := range checkIDs {
			if j.RepoID == id {
				c.JSON(http.StatusConflict, gin.H{
					"error": fmt.Sprintf("%s job %s is running on this repo — cancel it first", j.Action, j.ID),
				})
				return
			}
		}
	}

	wiped := []string{}
	for _, parent := range resolvedParents {
		// Re-validate the resolved parent stays inside BACKUP_ROOTS. The
		// scan cache only adds repos found under a backup root, but defence
		// in depth: a path that escapes (e.g. via symlink walk) must not
		// trigger RemoveAll.
		if !pathInsideAny(parent, a.cfg.BackupRoots) {
			slog.Warn("resolved .duplicacy/ parent outside BACKUP_ROOTS — refusing", "resolved", parent)
			continue
		}
		target := filepath.Join(parent, ".duplicacy")
		if err := os.RemoveAll(target); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "remove .duplicacy: " + err.Error()})
			return
		}
		wiped = append(wiped, parent)
		// Drop mapping entry keyed on the resolved parent (where the
		// init/bind flow would have stored it). The original `clean` may
		// not match any mapping id when we resolved via the source path.
		if err := a.mapping.delete(parent); err != nil {
			slog.Warn("mapping delete failed (non-fatal)", "error", err, "repo", parent)
		}
	}
	// Also try the literal `clean` against the mapping — covers the legacy
	// case where the mapping happened to be keyed on the source path.
	if err := a.mapping.delete(clean); err != nil {
		slog.Warn("mapping delete (literal) failed (non-fatal)", "error", err, "repo", clean)
	}
	// Background rescan (a full scan can take minutes on large hosts; don't block
	// the delete response) + notify WS subscribers.
	a.rescanAsync()

	slog.Info("repo deleted on-disk", "repo", clean, "resolved_via", resolvedVia, "wiped", wiped)
	c.JSON(http.StatusOK, deleteRepoResp{
		Removed:     len(wiped) > 0,
		RepoPath:    clean,
		WipedPaths:  wiped,
		ResolvedVia: resolvedVia,
	})
}
