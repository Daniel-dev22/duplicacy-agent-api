package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
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
)

// JobEvent is what we emit to the events subsystem on lifecycle transitions.
type JobEvent string

const (
	EventStarted   JobEvent = "started"
	EventProgress  JobEvent = "progress"
	EventCompleted JobEvent = "completed"
	EventFailed    JobEvent = "failed"
)

// JobEventHook is registered by events.go to receive lifecycle notifications.
type JobEventHook func(j *Job, evt JobEvent)

// ringBufferSize bounds memory per long-running job.
const ringBufferSize = 500

// recentJobsRetained: how many *terminated* jobs to keep in the registry for status polling.
const recentJobsRetained = 50

// subscriberBuffer: lines a slow subscriber can lag before we drop oldest from its channel.
const subscriberBuffer = 256

// Job is one CLI invocation tracked through its lifecycle.
type Job struct {
	ID          string    `json:"id"`
	RepoID      string    `json:"repo_id"`
	RepoPath    string    `json:"repo_path"`
	Action      JobAction `json:"action"`
	StorageName string    `json:"storage_name,omitempty"`
	Args        []string  `json:"args"`
	State       JobState  `json:"state"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	ExitCode    int       `json:"exit_code"`
	ErrorMsg    string    `json:"error,omitempty"`
	LineCount   int       `json:"line_count"`

	// Caller-set context for events
	ScheduleID string `json:"schedule_id,omitempty"`
	TriggerKey string `json:"trigger_key,omitempty"` // "manual" | "schedule" | "ui"

	// Internal
	mu          sync.Mutex
	cancel      context.CancelFunc
	ringBuffer  []string
	subscribers map[chan string]struct{}
}

// snapshot returns a goroutine-safe copy of the JSON-serializable fields.
func (j *Job) snapshot() Job {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := *j
	out.ringBuffer = nil
	out.subscribers = nil
	out.cancel = nil
	return out
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

// jobRegistry holds active and recently-terminated jobs.
type jobRegistry struct {
	mu       sync.RWMutex
	jobs     map[string]*Job
	terminal []string // ring of terminated job IDs in terminate-order; bounded by recentJobsRetained
	hooks    []JobEventHook
}

func newJobRegistry() *jobRegistry {
	return &jobRegistry{
		jobs:     map[string]*Job{},
		terminal: make([]string, 0, recentJobsRetained+1),
	}
}

func (r *jobRegistry) RegisterHook(h JobEventHook) {
	r.mu.Lock()
	r.hooks = append(r.hooks, h)
	r.mu.Unlock()
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
func (r *jobRegistry) start(parentCtx context.Context, binary string, repo *Repo, action JobAction, storage string, inv cliInvocation, scheduleID, triggerKey string) (*Job, error) {
	jobCtx, cancel := context.WithCancel(parentCtx)

	j := &Job{
		ID:          uuid.NewString(),
		RepoID:      repo.ID,
		RepoPath:    repo.Path,
		Action:      action,
		StorageName: storage,
		Args:        inv.Args,
		State:       JobPending,
		ScheduleID:  scheduleID,
		TriggerKey:  triggerKey,
		cancel:      cancel,
		subscribers: map[chan string]struct{}{},
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
			j.State = JobFailed
			j.ErrorMsg = err.Error()
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
		j.mu.Unlock()

		r.markTerminated(j)
		if state == JobCompleted {
			r.emit(j, EventCompleted)
		} else {
			r.emit(j, EventFailed)
		}
		log.Info().Str("job", j.ID).Str("state", string(state)).Msg("job terminal")
	}()

	return j, nil
}

func (r *jobRegistry) tail(j *Job, rdr io.Reader, source string) {
	scanner := bufio.NewScanner(rdr)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	for scanner.Scan() {
		j.appendLine(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		log.Warn().Err(err).Str("job", j.ID).Str("source", source).Msg("tail scanner error")
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

func (r *jobRegistry) list() []Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.jobs))
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
		log.Warn().Err(err).Msg("ws upgrade failed")
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
				_ = ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("[duplicacy-api] job %s state=%s exit=%d\n", ss.ID, ss.State, ss.ExitCode)))
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
	inv := invocationForBackup(repo, req.Storage, req.Tag, req.Threads)
	j, err := a.jobs.start(c.Request.Context(), a.cfg.DuplicacyBinary, repo, ActionBackup, req.Storage, inv, req.ScheduleID, req.TriggerKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

type restoreRequest struct {
	Storage    string   `json:"storage"`
	Revision   int      `json:"revision"`
	Paths      []string `json:"paths"`
	Overwrite  bool     `json:"overwrite"`
	TriggerKey string   `json:"trigger_key"`
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
	inv := invocationForRestore(repo, req.Storage, req.Revision, req.Paths, req.Overwrite)
	j, err := a.jobs.start(c.Request.Context(), a.cfg.DuplicacyBinary, repo, ActionRestore, req.Storage, inv, "", req.TriggerKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

type checkRequest struct {
	Storage    string `json:"storage"`
	Revisions  string `json:"revisions"` // optional: e.g., "1,3,5" or "1-10"
	All        bool   `json:"all"`
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
	inv := invocationForCheck(repo, req.Storage, req.Revisions, req.All)
	j, err := a.jobs.start(c.Request.Context(), a.cfg.DuplicacyBinary, repo, ActionCheck, req.Storage, inv, "", req.TriggerKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}

type pruneRequest struct {
	Storage    string   `json:"storage"`
	KeepRules  []string `json:"keep_rules"` // e.g., ["1:7", "7:30", "30:180"]
	Exclusive  bool     `json:"exclusive"`
	Exhaustive bool     `json:"exhaustive"`
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
	inv := invocationForPrune(repo, req.Storage, req.KeepRules, req.Exclusive, req.Exhaustive)
	j, err := a.jobs.start(c.Request.Context(), a.cfg.DuplicacyBinary, repo, ActionPrune, req.Storage, inv, "", req.TriggerKey)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "job_id": j.ID})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"job_id": j.ID})
}
