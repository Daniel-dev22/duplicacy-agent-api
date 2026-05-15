package main

// POST /repos/delete — wipes <repo_path>/.duplicacy/ and drops the mapping
// entry. Idempotent: missing .duplicacy/ is treated as success. Refuses with
// 409 if a job is currently running against this repo (operator must cancel
// first). The router calls this fire-and-forget after dropping the central
// duplicacy_repos row; if it can't reach the agent, reconcile.go cleans up
// the orphan on the next tick.

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type deleteRepoReq struct {
	RepoPath string `json:"repo_path"`
}

type deleteRepoResp struct {
	Removed  bool   `json:"removed"`
	RepoPath string `json:"repo_path"`
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

	repoID := repoIDFromPath(clean)
	for _, j := range a.jobs.list() {
		if j.RepoID == repoID && j.State == JobRunning {
			c.JSON(http.StatusConflict, gin.H{
				"error": fmt.Sprintf("%s job %s is running on this repo — cancel it first", j.Action, j.ID),
			})
			return
		}
	}

	target := filepath.Join(clean, ".duplicacy")
	removed := true
	if _, err := os.Stat(target); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			removed = false
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "stat .duplicacy: " + err.Error()})
			return
		}
	}
	if removed {
		if err := os.RemoveAll(target); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "remove .duplicacy: " + err.Error()})
			return
		}
	}
	if err := a.mapping.delete(clean); err != nil {
		log.Warn().Err(err).Str("repo", clean).Msg("mapping delete failed (non-fatal)")
	}
	if err := a.repos.ScanForce(); err != nil {
		log.Warn().Err(err).Msg("post-delete repo scan failed")
	}
	a.fleet.Trigger()

	log.Info().Str("repo", clean).Bool("removed", removed).Msg("repo deleted on-disk")
	c.JSON(http.StatusOK, deleteRepoResp{Removed: removed, RepoPath: clean})
}
