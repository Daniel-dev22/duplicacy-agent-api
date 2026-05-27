package main

// Per-job log persistence.
//
// The agent's 500-line in-memory ring buffer is great for live tailing but
// vanishes the moment a job terminates beyond the recentJobsRetained
// window OR the container restarts. The 2026-05-27 first-night run
// produced 29 failed copy jobs with `exit 100` and no other diagnostic
// surface; only by reattaching to the WebSocket on a live-but-still-in-
// memory job did the underlying "Two storages are not compatible" reason
// surface. By the time we got there, agent restarts had wiped most of the
// ring buffers.
//
// Storage layout: ${CONFIG_DIR}/job-logs/<job_id>.log — plain text, one
// line per ring buffer entry. Files older than the retention window are
// pruned on agent boot by mtime. No compression (jobs are bounded at 500
// lines per the ring buffer, ~50KB worst case).
//
// Endpoints:
//   - GET /jobs/:id/log → returns the on-disk file if it exists; falls
//     back to the live ring buffer if the job is still in jobRegistry.

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// jobLogRetainN bounds the number of per-job logs kept on disk. Older
// files are pruned at agent boot. 500 lines × ~100B per line × 200 jobs
// ≈ 10 MB ceiling — well within /var/lib's budget.
const jobLogRetainN = 200

// flushJobLog writes the job's ring buffer to <dir>/<job_id>.log via
// atomic .tmp + rename. Caller MUST set jobLogDir; missing/empty dir is
// the caller's bug, not handled here. Safe to call concurrently across
// different jobs.
func flushJobLog(dir string, j *Job) error {
	if dir == "" {
		return errors.New("jobLogDir not set")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("mkdir job-logs: %w", err)
	}

	// snapshot the ring buffer under lock so we don't race the tail goroutine
	j.mu.Lock()
	lines := make([]string, len(j.ringBuffer))
	copy(lines, j.ringBuffer)
	j.mu.Unlock()

	target := filepath.Join(dir, j.ID+".log")
	tmp := target + ".tmp"

	content := strings.Join(lines, "\n")
	if !strings.HasSuffix(content, "\n") && len(lines) > 0 {
		content += "\n"
	}
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// pruneJobLogs keeps only the N newest files under dir (by mtime). Called
// at agent boot so we don't carry forever-old logs across restarts.
// Missing dir is a no-op. Best-effort: individual prune errors are logged
// but not returned.
func pruneJobLogs(dir string, keepN int) {
	if dir == "" {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("job-logs prune: readdir failed", "dir", dir, "error", err)
		}
		return
	}
	type entry struct {
		path  string
		mtime time.Time
	}
	files := make([]entry, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, entry{
			path:  filepath.Join(dir, e.Name()),
			mtime: info.ModTime(),
		})
	}
	if len(files) <= keepN {
		return
	}
	// Sort newest first
	sort.Slice(files, func(i, j int) bool { return files[i].mtime.After(files[j].mtime) })
	pruned := 0
	for _, f := range files[keepN:] {
		if err := os.Remove(f.path); err != nil {
			slog.Warn("job-logs prune: remove failed", "path", f.path, "error", err)
			continue
		}
		pruned++
	}
	if pruned > 0 {
		slog.Info("job-logs prune", "dir", dir, "kept", keepN, "removed", pruned)
	}
}

// handleGetJobLog serves GET /jobs/:id/log. Prefers the on-disk file
// (durable across container restarts); falls back to the live ring buffer
// when the job is still active.
func (a *app) handleGetJobLog(c *gin.Context) {
	id := c.Param("id")
	// Disk first — even if the job is still tracked in memory, the disk
	// copy is what survives a container exit. For live jobs, the WS
	// endpoint /ws/jobs/:id/logs is the right surface.
	if a.cfg.ConfigDir != "" {
		path := filepath.Join(a.cfg.ConfigDir, "job-logs", id+".log")
		if data, err := os.ReadFile(path); err == nil {
			c.Data(http.StatusOK, "text/plain; charset=utf-8", data)
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "read job log: " + err.Error()})
			return
		}
	}
	// In-memory fallback: still running, or terminated within
	// recentJobsRetained but pre-flush (shouldn't normally happen after
	// the registry started flushing on terminal).
	if j, ok := a.jobs.get(id); ok {
		j.mu.Lock()
		hist := strings.Join(j.ringBuffer, "\n")
		j.mu.Unlock()
		c.Data(http.StatusOK, "text/plain; charset=utf-8", []byte(hist))
		return
	}
	c.JSON(http.StatusNotFound, gin.H{"error": "no log for job " + id})
}
