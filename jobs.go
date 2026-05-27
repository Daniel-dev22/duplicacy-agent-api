package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// JobState lifecycle.
type JobState string

const (
	JobPending   JobState = "pending"
	JobRunning   JobState = "running"
	JobCompleted JobState = "completed"
	JobFailed    JobState = "failed"
	JobCancelled JobState = "cancelled"
)

// JobAction = what duplicacy subcommand we're running.
type JobAction string

const (
	ActionInit    JobAction = "init"
	ActionBackup  JobAction = "backup"
	ActionRestore JobAction = "restore"
	ActionCheck   JobAction = "check"
	ActionPrune   JobAction = "prune"
	ActionCopy    JobAction = "copy"
)

// JobEvent is what we emit to the events subsystem on lifecycle transitions.
type JobEvent string

const (
	EventStarted   JobEvent = "started"
	EventProgress  JobEvent = "progress"
	EventCompleted JobEvent = "completed"
	EventFailed    JobEvent = "failed"
	EventCancelled JobEvent = "cancelled"
)

// JobEventHook is registered by events.go to receive lifecycle notifications.
type JobEventHook func(j *Job, evt JobEvent)

// ringBufferSize bounds memory per long-running job.
const ringBufferSize = 500

// recentJobsRetained: how many *terminated* jobs to keep in the registry for status polling.
const recentJobsRetained = 50

// subscriberBuffer: lines a slow subscriber can lag before we drop oldest from its channel.
const subscriberBuffer = 256

// JobProgress is the structured form of duplicacy's backup-time
// "Uploaded chunk N size X, Y/s ETA HH:MM:SS Z%" lines. Populated as the
// agent tails stdout; cleared (nil) before backup starts and after it
// completes. Other actions (restore, check, prune) don't emit a percent
// reliably so we skip parsing them — the raw log stream remains the
// source of truth for those.
type JobProgress struct {
	Percent   float64   `json:"percent"`
	Speed     string    `json:"speed,omitempty"`      // verbatim ("7.50MB/s") — running only
	ETA       string    `json:"eta,omitempty"`        // verbatim ("03:12:54", "n/a", "0h 2m 30s")
	LastChunk int       `json:"last_chunk,omitempty"` // chunk index from the latest line
	UpdatedAt time.Time `json:"updated_at"`
	// Final-summary fields, populated from BACKUP_STATS lines on completion.
	// Frontend swaps the running view (percent + speed + ETA) for the
	// completed view (chunks + bytes + duration) using state == 'completed'.
	TotalChunks   int    `json:"total_chunks,omitempty"`   // "All chunks: <N> total, …"
	NewChunks     int    `json:"new_chunks,omitempty"`     // "; <N> new, …"
	BytesUploaded string `json:"bytes_uploaded,omitempty"` // verbatim "669K"
	Duration      string `json:"duration,omitempty"`       // verbatim "00:00:02"
}

// jobPublic carries the JSON-serializable fields of a Job. It is split out
// from Job so snapshot() can return a lock-free copy — copying the Job value
// directly would copy the sync.Mutex, which the vet copylocks analyzer
// (rightly) flags as unsafe.
type jobPublic struct {
	ID          string       `json:"id"`
	RepoID      string       `json:"repo_id"`
	RepoPath    string       `json:"repo_path"`
	Action      JobAction    `json:"action"`
	StorageName string       `json:"storage_name,omitempty"`
	Args        []string     `json:"args"`
	State       JobState     `json:"state"`
	StartedAt   time.Time    `json:"started_at"`
	CompletedAt time.Time    `json:"completed_at,omitempty"`
	ExitCode    int          `json:"exit_code"`
	ErrorMsg    string       `json:"error,omitempty"`
	LineCount   int          `json:"line_count"`
	Progress    *JobProgress `json:"progress,omitempty"`

	// Caller-set context for events
	ScheduleID string `json:"schedule_id,omitempty"`
	TriggerKey string `json:"trigger_key,omitempty"` // "manual" | "schedule" | "ui"
}

// Job is one CLI invocation tracked through its lifecycle.
type Job struct {
	jobPublic

	// Internal — never serialized, never copied.
	mu          sync.Mutex
	cancel      context.CancelFunc
	ringBuffer  []string
	subscribers map[chan string]struct{}
	cleanup     func() // unlink tmpfs cred files after cmd.Wait() returns
}

// snapshot returns a lock-free copy of the JSON-serializable fields.
func (j *Job) snapshot() jobPublic {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.jobPublic
}

func (j *Job) appendLine(line string) {
	j.mu.Lock()
	j.ringBuffer = append(j.ringBuffer, line)
	if len(j.ringBuffer) > ringBufferSize {
		j.ringBuffer = j.ringBuffer[len(j.ringBuffer)-ringBufferSize:]
	}
	j.LineCount++
	subs := make([]chan string, 0, len(j.subscribers))
	for c := range j.subscribers {
		subs = append(subs, c)
	}
	j.mu.Unlock()
	for _, c := range subs {
		select {
		case c <- line:
		default:
			// subscriber is full — drop oldest by draining one then sending
			select {
			case <-c:
			default:
			}
			select {
			case c <- line:
			default:
			}
		}
	}
}

func (j *Job) subscribe() (history []string, ch chan string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.subscribers == nil {
		j.subscribers = map[chan string]struct{}{}
	}
	ch = make(chan string, subscriberBuffer)
	j.subscribers[ch] = struct{}{}
	hist := make([]string, len(j.ringBuffer))
	copy(hist, j.ringBuffer)
	return hist, ch
}

func (j *Job) unsubscribe(ch chan string) {
	j.mu.Lock()
	delete(j.subscribers, ch)
	j.mu.Unlock()
}

// progressLineRe matches duplicacy 3.x chunk progress lines, both the
// backup variant ("Uploaded chunk …") and the restore variant
// ("Downloaded chunk …"). Source-of-truth format string in
// duplicacy_backupmanager.go:432 is `%s chunk %d size %d, %sB/s %s %.1f%%`,
// so groups are: action, chunk_idx, size_bytes, speed (e.g. "7.69MB/s"),
// eta (e.g. "0h 2m 30s" or "n/a" — may contain spaces), percent.
//
// ETA is captured non-greedily so the trailing percent token always wins
// the match — duplicacy's PrettyTime can emit space-separated tokens like
// "0h 2m 30s" that would otherwise break a strict \S+ match.
var progressLineRe = regexp.MustCompile(
	`^(Uploaded|Downloaded) chunk (\d+) size (\d+), (\S+) (.+?) ([\d.]+)%$`,
)

// parseProgressLine updates Job.Progress from a duplicacy stdout line.
// Returns true if the line was a recognised progress line. Cheap regex
// match run for every backup/restore-action line.
func (j *Job) parseProgressLine(line string) bool {
	m := progressLineRe.FindStringSubmatch(line)
	if m == nil {
		return false
	}
	chunkIdx, _ := strconv.Atoi(m[2])
	pct, err := strconv.ParseFloat(m[6], 64)
	if err != nil {
		return false
	}
	// Cap percent at 100 — duplicacy's restore counter divides by 10 and
	// can exceed 100% on incremental snapshots that re-touch chunks.
	if pct > 100 {
		pct = 100
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Progress == nil {
		j.Progress = &JobProgress{}
	}
	j.Progress.Percent = pct
	j.Progress.Speed = m[4]
	j.Progress.ETA = m[5]
	j.Progress.LastChunk = chunkIdx
	j.Progress.UpdatedAt = time.Now().UTC()
	return true
}

// statsAllChunksRe matches duplicacy's final summary line, source format
// (duplicacy_backupmanager.go:570):
//
//	"All chunks: %d total, %s bytes; %d new, %s bytes, %s bytes uploaded"
//
// Real example from /srv/containers/duplicacy/logs/backup-….log:
//
//	INFO BACKUP_STATS All chunks: 1472 total, 8,353M bytes; 5 new, 7,984K bytes, 669K bytes uploaded
//
// Captures: total_chunks, new_chunks, bytes_uploaded.
// (Total bytes and new bytes are skipped — uploaded is the operator-relevant
//
//	number for the dashboard; we can revisit if needed.)
var statsAllChunksRe = regexp.MustCompile(
	`All chunks: (\d+) total, .+? bytes; (\d+) new, .+? bytes, (\S+) bytes uploaded`,
)

// statsRunTimeRe matches the wall-clock duration line that follows the
// All chunks summary:
//
//	"Total running time: 00:00:02"
var statsRunTimeRe = regexp.MustCompile(`Total running time: (\S+)`)

// parseStatsLine populates the final-summary fields on Job.Progress when
// either the All-chunks summary or the running-time line is observed.
// Triggers a fleet snapshot on update so the dashboard sees the totals
// immediately rather than waiting for the next event hook.
func (j *Job) parseStatsLine(line string) bool {
	if m := statsAllChunksRe.FindStringSubmatch(line); m != nil {
		total, _ := strconv.Atoi(m[1])
		new_, _ := strconv.Atoi(m[2])
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		j.Progress.TotalChunks = total
		j.Progress.NewChunks = new_
		j.Progress.BytesUploaded = m[3]
		j.Progress.UpdatedAt = time.Now().UTC()
		j.mu.Unlock()
		return true
	}
	if m := statsRunTimeRe.FindStringSubmatch(line); m != nil {
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		j.Progress.Duration = m[1]
		j.Progress.UpdatedAt = time.Now().UTC()
		j.mu.Unlock()
		return true
	}
	return false
}

// errorLineRe matches duplicacy's ERROR-level lines. The agent invokes
// duplicacy without -log, so the format is bare "ERROR <TAG> <message>"
// (no timestamp/level prefix). Examples seen in the wild:
//
//	ERROR STORAGE_CREATE Failed to load the SFTP storage at sftp://…: Can't access the storage path /mnt/…
//	ERROR UPLOAD_CHUNK Failed to upload the chunk …: RequestError: send request failed
//	ERROR DOWNLOAD_OPEN Failed to open the file … for in-place writing
var errorLineRe = regexp.MustCompile(`^ERROR (\S+) (.+)$`)

// parseErrorLine checks for an ERROR-tag line and stores the LAST one as
// Job.ErrorMsg, formatted "<TAG>: <message>". Replaces the generic
// "exit status N" the cmd.Wait() path would otherwise put there. No
// rate-limiting needed — most jobs see at most a handful of ERROR lines,
// and overwriting is correct (we want the most recent failure cause).
func (j *Job) parseErrorLine(line string) {
	m := errorLineRe.FindStringSubmatch(line)
	if m == nil {
		return
	}
	j.mu.Lock()
	j.ErrorMsg = m[1] + ": " + m[2]
	j.mu.Unlock()
}

// jobRegistry holds active and recently-terminated jobs.
type jobRegistry struct {
	mu            sync.RWMutex
	jobs          map[string]*Job
	terminal      []string // ring of terminated job IDs in terminate-order; bounded by recentJobsRetained
	hooks         []JobEventHook
	progressHooks []func(*Job) // lighter-weight than JobEventHook; called on progress line parse

	// jobLogDir, when non-empty, is where ring buffers are flushed on job
	// terminal. Set via setJobLogDir() once the agent's CONFIG_DIR is known.
	// Without it, the registry just falls back to the in-memory ring buffer
	// (legacy behaviour). See flushJobLog() for the on-disk format.
	jobLogDir string
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{
		jobs:     map[string]*Job{},
		terminal: make([]string, 0, recentJobsRetained+1),
	}
}

// setJobLogDir wires the on-disk job-log directory. Idempotent.
func (r *jobRegistry) setJobLogDir(dir string) { r.jobLogDir = dir }

func (r *jobRegistry) RegisterHook(h JobEventHook) {
	r.mu.Lock()
	r.hooks = append(r.hooks, h)
	r.mu.Unlock()
}

// RegisterProgressHook subscribes to progress-line updates without going
// through the heavier event-buffer ingest path. Used by the fleet WS hub
// to push live percent/ETA to dashboards. Call site is hot (per chunk
// uploaded), so the hook should be cheap (typically just a coalescing
// channel send).
func (r *jobRegistry) RegisterProgressHook(h func(*Job)) {
	r.mu.Lock()
	r.progressHooks = append(r.progressHooks, h)
	r.mu.Unlock()
}

func (r *jobRegistry) emitProgress(j *Job) {
	r.mu.RLock()
	hooks := make([]func(*Job), len(r.progressHooks))
	copy(hooks, r.progressHooks)
	r.mu.RUnlock()
	for _, h := range hooks {
		h(j)
	}
}

func (r *jobRegistry) emit(j *Job, evt JobEvent) {
	r.mu.RLock()
	hooks := append([]JobEventHook(nil), r.hooks...)
	r.mu.RUnlock()
	for _, h := range hooks {
		go h(j, evt)
	}
}

// start spawns a CLI invocation and returns its Job. The CLI is started immediately;
// caller should subscribe to logs via WebSocket if it wants live output.
//
// cleanup, if non-nil, is invoked exactly once after cmd.Wait() returns. Used
// for unlinking /dev/shm tmpfiles that hold ephemeral cred material (SFTP
// keys, GCS service-account JSON).
func (r *jobRegistry) start(parentCtx context.Context, binary string, repo *Repo, action JobAction, storage string, inv cliInvocation, scheduleID, triggerKey string, cleanup func()) (*Job, error) {
	jobCtx, cancel := context.WithCancel(parentCtx)

	j := &Job{
		jobPublic: jobPublic{
			ID:          uuid.NewString(),
			RepoID:      repo.ID,
			RepoPath:    repo.Path,
			Action:      action,
			StorageName: storage,
			Args:        inv.Args,
			State:       JobPending,
			ScheduleID:  scheduleID,
			TriggerKey:  triggerKey,
		},
		cancel:      cancel,
		subscribers: map[chan string]struct{}{},
		cleanup:     cleanup,
	}

	r.mu.Lock()
	r.jobs[j.ID] = j
	r.mu.Unlock()

	cmd := inv.command(jobCtx, binary)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		if j.cleanup != nil {
			j.cleanup()
		}
		j.mu.Lock()
		j.State = JobFailed
		j.ErrorMsg = err.Error()
		j.CompletedAt = time.Now().UTC()
		j.mu.Unlock()
		r.markTerminated(j)
		r.emit(j, EventFailed)
		return j, err
	}

	j.mu.Lock()
	j.State = JobRunning
	j.StartedAt = time.Now().UTC()
	j.mu.Unlock()
	r.emit(j, EventStarted)

	// Tail stdout + stderr concurrently into the same ring buffer.
	go r.tail(j, stdout, "stdout")
	go r.tail(j, stderr, "stderr")

	// Wait for completion in a separate goroutine so start() returns immediately.
	go func() {
		err := cmd.Wait()
		j.mu.Lock()
		j.CompletedAt = time.Now().UTC()
		if err != nil {
			// Preserve JobCancelled when the user explicitly cancelled — the
			// cmd error is just the consequence of context cancellation, not
			// a real failure. Don't override with JobFailed.
			if j.State != JobCancelled {
				j.State = JobFailed
				// Only fall back to "exit status N" if no friendly ERROR-tag
				// line was captured during tail; that path produces messages
				// like "STORAGE_CREATE: Failed to load …" which are far more
				// useful for the operator than a bare exit code.
				if j.ErrorMsg == "" {
					j.ErrorMsg = err.Error()
				}
			} else if j.ErrorMsg == "" {
				j.ErrorMsg = "cancelled by user"
			}
			if exitErr, ok := err.(interface{ ExitCode() int }); ok {
				j.ExitCode = exitErr.ExitCode()
			}
		} else {
			j.State = JobCompleted
			j.ExitCode = 0
		}
		// close all subscriber channels so WS handlers exit cleanly
		for ch := range j.subscribers {
			close(ch)
		}
		j.subscribers = map[chan string]struct{}{}
		state := j.State
		cleanup := j.cleanup
		j.cleanup = nil
		j.mu.Unlock()

		if cleanup != nil {
			cleanup()
		}

		// Flush the ring buffer to disk so failed jobs can be diagnosed
		// post-mortem (after the agent restarts or the ring buffer rolls).
		// Best-effort: a write error is non-fatal — operator still has the
		// agent stdout + DB row to work from. See flushJobLog.
		if r.jobLogDir != "" {
			if err := flushJobLog(r.jobLogDir, j); err != nil {
				slog.Warn("job-log flush failed (non-fatal)", "job", j.ID, "error", err)
			}
		}

		r.markTerminated(j)
		switch state {
		case JobCompleted:
			r.emit(j, EventCompleted)
		case JobCancelled:
			r.emit(j, EventCancelled)
		default:
			r.emit(j, EventFailed)
		}
		slog.Info("job terminal", "job", j.ID, "state", string(state))
	}()

	return j, nil
}

func (r *jobRegistry) tail(j *Job, rdr io.Reader, source string) {
	scanner := bufio.NewScanner(rdr)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		j.appendLine(line)
		// Progress parse — backup, restore, and copy all emit "Uploaded/Downloaded chunk …".
		if j.Action == ActionBackup || j.Action == ActionRestore || j.Action == ActionCopy {
			if updated := j.parseProgressLine(line); updated {
				r.emitProgress(j)
			}
		}
		// Final-summary parse — only for backup; the BACKUP_STATS line
		// pattern is backup-specific. Triggers a fleet snapshot the same
		// way progress lines do so the dashboard sees totals on the same
		// frame the job flips to "completed".
		if j.Action == ActionBackup {
			if updated := j.parseStatsLine(line); updated {
				r.emitProgress(j)
			}
		}
		// ERROR-line capture — last "ERROR <TAG> <message>" wins. Replaces
		// the generic exit-code error string with duplicacy's friendly
		// description (e.g. "STORAGE_CREATE: Failed to load the SFTP
		// storage at …: Can't access the storage path …"). Fires for every
		// action so check/prune/restore failures also get clean messages.
		j.parseErrorLine(line)
	}
	// scanner.Err() is normal at process exit — duplicacy closes the pipe
	// from its side and our blocked Read returns fs.ErrClosed (wrapped by
	// bufio.Scanner). That's pipe end-of-life, not an error to surface.
	// Anything else still gets the WARN.
	if err := scanner.Err(); err != nil &&
		!errors.Is(err, os.ErrClosed) &&
		!errors.Is(err, fs.ErrClosed) &&
		!strings.Contains(err.Error(), "file already closed") {
		slog.Warn("tail scanner error", "error", err, "job", j.ID, "source", source)
	}
}

// markTerminated records a job as terminal and evicts the oldest if we exceed the cap.
func (r *jobRegistry) markTerminated(j *Job) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terminal = append(r.terminal, j.ID)
	for len(r.terminal) > recentJobsRetained {
		oldest := r.terminal[0]
		r.terminal = r.terminal[1:]
		delete(r.jobs, oldest)
	}
}

func (r *jobRegistry) get(id string) (*Job, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	j, ok := r.jobs[id]
	return j, ok
}

func (r *jobRegistry) list() []jobPublic {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]jobPublic, 0, len(r.jobs))
	for _, j := range r.jobs {
		out = append(out, j.snapshot())
	}
	return out
}

func (r *jobRegistry) cancel(id string) bool {
	r.mu.RLock()
	j, ok := r.jobs[id]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	j.mu.Lock()
	if j.State != JobRunning {
		j.mu.Unlock()
		return false
	}
	j.State = JobCancelled
	cancel := j.cancel
	j.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return true
}

// --- HTTP / WebSocket handlers (override placeholders) ---

func (a *app) handleListJobs(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"jobs": a.jobs.list()})
}

func (a *app) handleGetJob(c *gin.Context) {
	j, ok := a.jobs.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	c.JSON(http.StatusOK, j.snapshot())
}

// handleCancelJob signals SIGTERM-equivalent (context cancellation) to a
// running job. Returns 200 if cancellation was sent, 404 if no such job,
// 409 if the job was already in a terminal state. The fleet snapshot push
// triggered by the eventual EventCancelled emission carries the new state
// to the UI within ~1s.
func (a *app) handleCancelJob(c *gin.Context) {
	id := c.Param("id")
	j, ok := a.jobs.get(id)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	if !a.jobs.cancel(id) {
		c.JSON(http.StatusConflict, gin.H{"error": "job not in a cancellable state", "state": j.snapshot().State})
		return
	}
	c.JSON(http.StatusOK, gin.H{"cancelled": true, "job_id": id})
}

var wsUpgrader = websocket.Upgrader{
	CheckOrigin:     func(_ *http.Request) bool { return true }, // we're behind Traefik with auth
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

func (a *app) handleJobLogsWS(c *gin.Context) {
	j, ok := a.jobs.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
		return
	}
	ws, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		slog.Warn("ws upgrade failed", "error", err)
		return
	}
	defer ws.Close()

	history, ch := j.subscribe()
	defer j.unsubscribe(ch)

	// Read pump: detect client disconnect.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()

	// Send history as one batch (newline-joined).
	if len(history) > 0 {
		if err := ws.WriteMessage(websocket.TextMessage, []byte(strings.Join(history, "\n")+"\n")); err != nil {
			return
		}
	}

	// Stream live lines with batching + heartbeat.
	const maxBatch = 20
	const maxBufferTime = 500 * time.Millisecond
	heartbeat := time.NewTicker(30 * time.Second)
	flush := time.NewTicker(maxBufferTime)
	defer heartbeat.Stop()
	defer flush.Stop()

	var buf []string
	send := func() error {
		if len(buf) == 0 {
			return nil
		}
		err := ws.WriteMessage(websocket.TextMessage, []byte(strings.Join(buf, "\n")+"\n"))
		buf = buf[:0]
		return err
	}

	for {
		select {
		case <-done:
			_ = send()
			return
		case <-heartbeat.C:
			deadline := time.Now().Add(10 * time.Second)
			if err := ws.WriteControl(websocket.PingMessage, nil, deadline); err != nil {
				return
			}
		case <-flush.C:
			if err := send(); err != nil {
				return
			}
		case line, ok := <-ch:
			if !ok {
				// job terminated; flush any remaining
				_ = send()
				// send a terminal sentinel so the UI can show "completed"/"failed"
				ss := j.snapshot()
				_ = ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("[duplicacy-agent-api] job %s state=%s exit=%d\n", ss.ID, ss.State, ss.ExitCode)))
				return
			}
			buf = append(buf, line)
			if len(buf) >= maxBatch {
				if err := send(); err != nil {
					return
				}
			}
		}
	}
}

// --- action handlers (use the invocation builders from duplicacy.go) ---

type backupRequest struct {
	Storage    string `json:"storage"`
	Tag        string `json:"tag"`
	Threads    int    `json:"threads"`
	ScheduleID string `json:"schedule_id"`
	TriggerKey string `json:"trigger_key"` // manual | schedule | ui
}

func (a *app) handleBackup(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	var req backupRequest
	_ = c.ShouldBindJSON(&req) // all fields optional
	if req.TriggerKey == "" {
		req.TriggerKey = "manual"
	}
	env, _, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	inv := invocationForBackup(repo, req.Storage, req.Tag, req.Threads)
	inv.EnvAdds = append(inv.EnvAdds, env...)
	// Detached context — the spawned duplicacy process must outlive the
	// HTTP request, otherwise gin cancels c.Request.Context() on return
	// and the job dies with "signal: killed" before the first chunk runs.
	// jobRegistry.start wraps this in its own WithCancel so the registry
	// can still abort the job.
	j, err := a.jobs.start(context.Background(), a.cfg.DuplicacyBinary, repo, ActionBackup, req.Storage, inv, req.ScheduleID, req.TriggerKey, cleanup)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

type restoreRequest struct {
	Storage   string   `json:"storage"`
	Revision  int      `json:"revision"`
	Paths     []string `json:"paths"`
	Overwrite bool     `json:"overwrite"`
	// Target controls where files land. Safe-by-default semantics — an unset
	// or empty Target is treated as "scratch" so restores never overwrite
	// the source unless the operator explicitly opts in:
	//   ""         → "scratch" (default; lands in RestoreScratchRoot)
	//   "scratch"  → <RestoreScratchRoot>/<snapshot_id>-r<rev>/
	//   "original" → restore in-place over the original repo root
	//   "/abs/dir" → arbitrary absolute path (custom target)
	// For non-original targets, the agent symlinks .duplicacy into the
	// target so duplicacy resolves storage config the same way it would in-
	// place.
	Target     string `json:"target"`
	TriggerKey string `json:"trigger_key"`
}

func (a *app) handleRestore(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	var req restoreRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if req.Revision <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "revision required and must be > 0"})
		return
	}
	if req.TriggerKey == "" {
		req.TriggerKey = "manual"
	}

	// Resolve restore target. Scratch sentinel + arbitrary absolute path both
	// require .duplicacy to be visible from the target dir; agent prepares
	// that here and tracks teardown via prepCleanup so the registry's cleanup
	// chain runs it after cmd.Wait().
	targetRoot, prepCleanup, err := a.prepareRestoreTarget(repo, req.Target, req.Revision)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "restore target: " + err.Error()})
		return
	}

	env, rsaPriv, credCleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		if prepCleanup != nil {
			prepCleanup()
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	// Restore reads from a single storage; for RSA-encrypted storages we must
	// pass -key <priv-path>. The targeted storage alias defaults to "default"
	// when the caller leaves req.Storage empty.
	storageAlias := req.Storage
	if storageAlias == "" {
		storageAlias = "default"
	}
	inv := invocationForRestore(repo, req.Storage, req.Revision, req.Paths, req.Overwrite, rsaPriv[storageAlias], targetRoot)
	inv.EnvAdds = append(inv.EnvAdds, env...)
	cleanup := credCleanup
	if prepCleanup != nil {
		cleanup = func() {
			if credCleanup != nil {
				credCleanup()
			}
			prepCleanup()
		}
	}
	// Detached context — see handleBackup comment.
	j, err := a.jobs.start(context.Background(), a.cfg.DuplicacyBinary, repo, ActionRestore, req.Storage, inv, "", req.TriggerKey, cleanup)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

// prepareRestoreTarget resolves the request's Target into an absolute path
// for duplicacy to chdir into, and returns a teardown closure that removes
// the .duplicacy symlink we plant there. Safe-by-default — an empty Target
// is treated as "scratch" so restores never overwrite source unless the
// operator explicitly passes Target="original". Returns ("", nil, nil) only
// for Target=="original", meaning duplicacy chdir's into the repo root
// directly (the legacy in-place behaviour).
func (a *app) prepareRestoreTarget(repo *Repo, target string, revision int) (string, func(), error) {
	// Map the safe default and the explicit in-place sentinel.
	if target == "" {
		target = "scratch"
	}
	if target == "original" {
		return "", nil, nil
	}
	var dir string
	if target == "scratch" {
		root := a.cfg.RestoreScratchRoot
		if root == "" {
			return "", nil, fmt.Errorf("scratch target requested but RESTORE_SCRATCH_ROOT is empty")
		}
		dir = filepath.Join(root, fmt.Sprintf("%s-r%d", repo.SnapshotID, revision))
	} else {
		if !filepath.IsAbs(target) {
			return "", nil, fmt.Errorf("custom target must be an absolute path")
		}
		dir = filepath.Clean(target)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create target dir %q: %w", dir, err)
	}
	// duplicacy resolves storage config from <cwd>/.duplicacy/preferences.
	// Symlink it from the original repo so the restore knows where to read
	// chunks from. If a .duplicacy already exists at the target (e.g. from
	// a previous restore), leave it alone.
	link := filepath.Join(dir, ".duplicacy")
	src := filepath.Join(repo.Path, ".duplicacy")
	if _, err := os.Lstat(link); os.IsNotExist(err) {
		if err := os.Symlink(src, link); err != nil {
			return "", nil, fmt.Errorf("symlink .duplicacy into target: %w", err)
		}
	} else if err != nil {
		return "", nil, fmt.Errorf("stat existing .duplicacy at target: %w", err)
	}
	cleanup := func() {
		// Best-effort: only remove the symlink (not real directories) so we
		// never accidentally rm an operator's pre-existing folder.
		if fi, err := os.Lstat(link); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			_ = os.Remove(link)
		}
	}
	return dir, cleanup, nil
}

type checkRequest struct {
	Storage    string `json:"storage"`
	Revisions  string `json:"revisions"`   // optional: e.g., "1,3,5" or "1-10"
	All        bool   `json:"all"`
	SnapshotID string `json:"snapshot_id"` // optional: scope to one snapshot id via -id (hub relay)
	TriggerKey string `json:"trigger_key"`
}

func (a *app) handleCheck(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	var req checkRequest
	_ = c.ShouldBindJSON(&req)
	if req.TriggerKey == "" {
		req.TriggerKey = "manual"
	}
	env, _, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	inv := invocationForCheck(repo, req.Storage, req.Revisions, req.All, req.SnapshotID)
	inv.EnvAdds = append(inv.EnvAdds, env...)
	// Detached context — see handleBackup comment.
	j, err := a.jobs.start(context.Background(), a.cfg.DuplicacyBinary, repo, ActionCheck, req.Storage, inv, "", req.TriggerKey, cleanup)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

type pruneRequest struct {
	Storage    string   `json:"storage"`
	KeepRules  []string `json:"keep_rules"`  // e.g., ["1:7", "7:30", "30:180"]
	Exclusive  bool     `json:"exclusive"`
	Exhaustive bool     `json:"exhaustive"`
	SnapshotID string   `json:"snapshot_id"` // optional: scope to one snapshot id via -id (hub relay)
	TriggerKey string   `json:"trigger_key"`
}

func (a *app) handlePrune(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	var req pruneRequest
	_ = c.ShouldBindJSON(&req)
	if req.TriggerKey == "" {
		req.TriggerKey = "manual"
	}
	// Hard rule: prune MUST carry a snapshot_id. The hub topology has many
	// snapshot_ids in one shared NAS chunk pool; an unscoped prune would
	// consider snapshots from every host when computing chunk reachability
	// and could orphan another host's chunks (especially with -exclusive).
	// The materializer always emits snapshot_id; manual prune callers must
	// pass it explicitly. Solo-storage repos lose nothing — passing the
	// repo's only snapshot_id is a no-op constraint there.
	if strings.TrimSpace(req.SnapshotID) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "snapshot_id is required (hub-pool safety: unscoped prune can orphan another host's chunks)"})
		return
	}
	env, _, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	inv := invocationForPrune(repo, req.Storage, req.KeepRules, req.Exclusive, req.Exhaustive, req.SnapshotID)
	inv.EnvAdds = append(inv.EnvAdds, env...)
	// Detached context — see handleBackup comment.
	j, err := a.jobs.start(context.Background(), a.cfg.DuplicacyBinary, repo, ActionPrune, req.Storage, inv, "", req.TriggerKey, cleanup)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

// copyRequest drives `duplicacy copy -from <a> -to <b> -id <snap>`. From and To
// are the storage aliases inside the relay repo's .duplicacy/preferences (e.g.
// From="default" To="b2"). SnapshotID scopes the copy to a single source repo's
// snapshots in the shared chunk pool — required when many source repos share
// one storage.
type copyRequest struct {
	From       string `json:"from"`
	To         string `json:"to"`
	Threads    int    `json:"threads"`
	SnapshotID string `json:"snapshot_id"`
	TriggerKey string `json:"trigger_key"`
}

func (a *app) handleCopy(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	var req copyRequest
	_ = c.ShouldBindJSON(&req)
	if req.TriggerKey == "" {
		req.TriggerKey = "manual"
	}
	if req.From == "" || req.To == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "from and to storage aliases are required"})
		return
	}
	// prepareEnvForRepo vends env for ALL of the repo's storages, so both
	// -from and -to credentials are available without per-call selection.
	// rsaPriv carries per-alias RSA priv key paths; pass the SOURCE's to
	// `duplicacy copy -key` so RSA-encrypted source snapshot indices can be
	// read. The matching passphrase (if any) reaches duplicacy via the
	// DUPLICACY_RSA_PASSPHRASE env var already in `env`.
	env, rsaPriv, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	inv := invocationForCopy(repo, req.From, req.To, req.Threads, req.SnapshotID, rsaPriv[req.From], "")
	inv.EnvAdds = append(inv.EnvAdds, env...)
	// Detached context — see handleBackup comment. StorageName on the job is
	// the destination alias so the fleet WS shows "copy to b2" rather than the
	// source.
	j, err := a.jobs.start(context.Background(), a.cfg.DuplicacyBinary, repo, ActionCopy, req.To, inv, "", req.TriggerKey, cleanup)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}
