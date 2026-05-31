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

// JobProgress is the structured form of duplicacy progress lines, populated
// as the agent tails stdout. Field set varies by action:
//
//   - backup / restore / copy: Percent + Speed + ETA + LastChunk (chunk lines).
//     Backup also fills the BACKUP_STATS summary group (TotalChunks, NewChunks,
//     BytesUploaded, Duration) on completion.
//   - check: Percent is derived as CheckRevisionsVerified/CheckRevisionsTotal*100
//     (no native percent emitted; counters drive the bar).
//   - prune: no Percent (total work unknown up-front); counters only.
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

	// Check counters — Percent is derived as verified/total*100.
	CheckRevisionsTotal    int `json:"check_revisions_total,omitempty"`
	CheckRevisionsVerified int `json:"check_revisions_verified,omitempty"`

	// Destination pool size, parsed from "INFO SNAPSHOT_CHECK Total chunk size
	// is X in N chunks" — emitted once per check run and represents the
	// destination's actual deduplicated disk usage. Stays in JobProgress so the
	// live job card can surface it as soon as the line lands.
	CheckPoolBytes       int64  `json:"check_pool_bytes,omitempty"`
	CheckPoolBytesPretty string `json:"check_pool_bytes_pretty,omitempty"`
	CheckPoolChunks      int    `json:"check_pool_chunks,omitempty"`

	// Prune counters — no percent; total work is unknown until done.
	PruneSnapshotsRemoved int `json:"prune_snapshots_removed,omitempty"`
	PruneChunksDeleted    int `json:"prune_chunks_deleted,omitempty"`
	PruneFossilsProcessed int `json:"prune_fossils_processed,omitempty"`
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

	// tabular parser state (check jobs only). Populated under j.mu by tail()
	// when Action == ActionCheck; drained by takeTabularRows() on the
	// completion hook path, which UPSERTs into snapshot_stats.
	tabular     *checkTabularParser
	tabularRows []*snapshotStatRow
}

// takeTabularRows returns and clears the rows collected during a check job's
// tabular parse. Returns nil if no rows were collected (e.g. for non-check
// actions, or check that failed before emitting the table). Safe to call
// from the completion-hook goroutine — it locks j.mu.
func (j *Job) takeTabularRows() []*snapshotStatRow {
	j.mu.Lock()
	defer j.mu.Unlock()
	rows := j.tabularRows
	j.tabularRows = nil
	j.tabular = nil
	return rows
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

// copyProgressLineRe matches the COPY action's progress lines, which use a
// different shape than backup/restore — see
// reference_duplicacy_copy_progress_format memory entry. Format
// (duplicacy 3.2.5):
//
//	Copied chunk <hash> (<idx>/<total>) <speed>MB/s <ETA> <pct>%
//
// e.g.:
//	Copied chunk 4ac96c2e...d3 (2/1274) 4.15MB/s 00:19:32 0.2%
//
// Groups: chunk_hash, idx, total, speed, eta, percent.
//
// ETA is captured non-greedily for the same reason as the backup regex
// (PrettyTime can emit space-separated tokens like "0h 2m 30s").
var copyProgressLineRe = regexp.MustCompile(
	`^Copied chunk (\S+) \((\d+)/(\d+)\) (\S+) (.+?) ([\d.]+)%$`,
)

// parseProgressLine updates Job.Progress from a duplicacy stdout line.
// Returns true if the line was a recognised progress line. Cheap regex
// match run for every backup/restore/copy-action line.
func (j *Job) parseProgressLine(line string) bool {
	if m := progressLineRe.FindStringSubmatch(line); m != nil {
		return j.applyProgress(m[4], m[5], m[6], m[2], "")
	}
	if m := copyProgressLineRe.FindStringSubmatch(line); m != nil {
		// Copy lines: m[2]=idx, m[3]=total, m[4]=speed, m[5]=eta, m[6]=pct
		return j.applyProgress(m[4], m[5], m[6], m[2], m[3])
	}
	return false
}

// applyProgress updates Job.Progress from parsed regex groups. totalStr
// is empty for backup/restore (no per-line total) and "<N>" for copy.
func (j *Job) applyProgress(speed, eta, pctStr, idxStr, totalStr string) bool {
	pct, err := strconv.ParseFloat(pctStr, 64)
	if err != nil {
		return false
	}
	if pct > 100 {
		pct = 100
	}
	chunkIdx, _ := strconv.Atoi(idxStr)
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Progress == nil {
		j.Progress = &JobProgress{}
	}
	j.Progress.Percent = pct
	j.Progress.Speed = speed
	j.Progress.ETA = eta
	j.Progress.LastChunk = chunkIdx
	if totalStr != "" {
		if n, err := strconv.Atoi(totalStr); err == nil {
			j.Progress.TotalChunks = n
		}
	}
	j.Progress.UpdatedAt = time.Now().UTC()
	return true
}

// `INFO SNAPSHOT_CHECK ` prefix is OPTIONAL on every check parser regex:
// duplicacy only prepends "INFO <TAG> " when invoked with `-log`. The agent
// runs without `-log`, so the lines arrive bare. Pre-fix witness 2026-05-28:
// regexes required the prefix → check counters never populated and
// pool_bytes stayed 0 across the fleet (1.0.70 deploy day).

// checkRevisionsTotalRe captures the "<N> snapshots and <M> revisions" line
// duplicacy emits early in `check`. M is the work-total we divide against
// when deriving Percent.
var checkRevisionsTotalRe = regexp.MustCompile(
	`^(?:INFO SNAPSHOT_CHECK )?\d+ snapshots and (\d+) revisions$`,
)

// checkRevisionDoneRe fires once per verified revision; increments the
// verified-counter on every match.
var checkRevisionDoneRe = regexp.MustCompile(
	`^(?:INFO SNAPSHOT_CHECK )?All chunks referenced by snapshot \S+ at revision \d+ exist$`,
)

// checkPoolSizeRe captures the "Total chunk size is X in N chunks" line —
// the destination's actual deduplicated disk usage. Reported once per check
// run near the start. duplicacy's PrettyBytes form (e.g. "1.2G", "350.4M"),
// converted to int64 via parsePrettyBytes.
var checkPoolSizeRe = regexp.MustCompile(
	`^(?:INFO SNAPSHOT_CHECK )?Total chunk size is (\S+) in (\d+) chunks$`,
)

// parseCheckLine bumps CheckRevisions{Total,Verified} from `check` stdout
// and recomputes Percent. Returns true on any match so the caller can fire
// the progress hook.
func (j *Job) parseCheckLine(line string) bool {
	if m := checkRevisionsTotalRe.FindStringSubmatch(line); m != nil {
		total, err := strconv.Atoi(m[1])
		if err != nil {
			return false
		}
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		j.Progress.CheckRevisionsTotal = total
		if total > 0 {
			j.Progress.Percent = float64(j.Progress.CheckRevisionsVerified) / float64(total) * 100
		}
		j.Progress.UpdatedAt = time.Now().UTC()
		j.mu.Unlock()
		return true
	}
	if checkRevisionDoneRe.MatchString(line) {
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		j.Progress.CheckRevisionsVerified++
		if j.Progress.CheckRevisionsTotal > 0 {
			pct := float64(j.Progress.CheckRevisionsVerified) / float64(j.Progress.CheckRevisionsTotal) * 100
			if pct > 100 {
				pct = 100
			}
			j.Progress.Percent = pct
		}
		j.Progress.UpdatedAt = time.Now().UTC()
		j.mu.Unlock()
		return true
	}
	if m := checkPoolSizeRe.FindStringSubmatch(line); m != nil {
		chunks, _ := strconv.Atoi(m[2])
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		j.Progress.CheckPoolBytesPretty = m[1]
		j.Progress.CheckPoolBytes = parsePrettyBytes(m[1])
		j.Progress.CheckPoolChunks = chunks
		j.Progress.UpdatedAt = time.Now().UTC()
		j.mu.Unlock()
		return true
	}
	return false
}

// pruneSnapshotRemovedRe / pruneChunkDeletedRe / pruneFossilCollectedRe
// match duplicacy's per-action log lines during `prune`. We have no total to
// divide against, so Percent stays at 0 and the UI shows an indeterminate
// bar + the live counters. `INFO <TAG>` prefix is optional (agent runs
// without -log; same reason as the check parsers above).
var (
	pruneSnapshotRemovedRe = regexp.MustCompile(
		`^(?:INFO SNAPSHOT_DELETE )?The snapshot \S+ at revision \d+ has been removed$`,
	)
	pruneChunkDeletedRe = regexp.MustCompile(
		`^(?:INFO CHUNK_DELETE )?The chunk \S+ has been permanently removed$`,
	)
	pruneFossilCollectedRe = regexp.MustCompile(
		`^(?:INFO FOSSIL_COLLECT )?Fossil collection \d+ saved$`,
	)
)

// parsePruneLine bumps one of the three prune counters per matching line.
// Returns true on any match.
func (j *Job) parsePruneLine(line string) bool {
	var field *int
	switch {
	case pruneSnapshotRemovedRe.MatchString(line):
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		field = &j.Progress.PruneSnapshotsRemoved
	case pruneChunkDeletedRe.MatchString(line):
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		field = &j.Progress.PruneChunksDeleted
	case pruneFossilCollectedRe.MatchString(line):
		j.mu.Lock()
		if j.Progress == nil {
			j.Progress = &JobProgress{}
		}
		field = &j.Progress.PruneFossilsProcessed
	default:
		return false
	}
	*field++
	j.Progress.UpdatedAt = time.Now().UTC()
	j.mu.Unlock()
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

	// copySem caps concurrent `duplicacy copy` processes on this host. The
	// relay fans every source repo out to several destinations on the same
	// cron minute; running them all at once spikes RAM and OOM-kills the NAS.
	// Buffered to MaxConcurrentCopies; nil means unbounded (cap disabled).
	// Only copy jobs acquire it — backup/restore are unaffected.
	copySem chan struct{}

	// maintSem caps concurrent maintenance jobs (check + prune). After the
	// nightly wave the chain fires ~30 prune then ~30 check schedules at once;
	// running them all in parallel would spike NAS RAM the same way unbounded
	// copies did. Buffered to MaxConcurrentMaint; nil means unbounded.
	// Only check/prune acquire it — backup/copy/restore are unaffected.
	maintSem chan struct{}
}

func newJobRegistry(maxConcurrentCopies, maxConcurrentMaint int) *jobRegistry {
	var copySem chan struct{}
	if maxConcurrentCopies > 0 {
		copySem = make(chan struct{}, maxConcurrentCopies)
	}
	var maintSem chan struct{}
	if maxConcurrentMaint > 0 {
		maintSem = make(chan struct{}, maxConcurrentMaint)
	}
	return &jobRegistry{
		jobs:     map[string]*Job{},
		terminal: make([]string, 0, recentJobsRetained+1),
		copySem:  copySem,
		maintSem: maintSem,
	}
}

// semFor returns the concurrency semaphore that gates this action, or nil if
// the action runs ungated. Copy is gated by copySem; check/prune by maintSem;
// everything else (backup/restore/init) runs immediately.
func (r *jobRegistry) semFor(action JobAction) chan struct{} {
	switch action {
	case ActionCopy:
		return r.copySem
	case ActionCheck, ActionPrune:
		return r.maintSem
	default:
		return nil
	}
}

// countActive returns how many jobs matching any of `actions` are currently
// running or pending (i.e. in flight, including queued-on-semaphore). Used by
// the after-wave chain to tell when the wave (backup/copy) has drained and when
// a maintenance stage (prune, then check) has finished. Terminal jobs in the
// registry ring are not counted.
func (r *jobRegistry) countActive(actions ...JobAction) int {
	want := make(map[JobAction]struct{}, len(actions))
	for _, a := range actions {
		want[a] = struct{}{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := 0
	for _, j := range r.jobs {
		s := j.snapshot()
		if _, ok := want[s.Action]; !ok {
			continue
		}
		if s.State == JobRunning || s.State == JobPending {
			n++
		}
	}
	return n
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

	// Copy and maintenance (check/prune) jobs are gated by a per-host
	// semaphore so the nightly destination fan-out — and the after-wave
	// prune/check fan-out — don't spawn N processes at once and OOM the box.
	// The slot is acquired inside a goroutine (NOT here) so neither the gin
	// handler nor the kit's single, serial fireLoop blocks while a job waits
	// its turn — the queued job simply stays Pending until a slot frees.
	if sem := r.semFor(action); sem != nil {
		go r.spawnGated(jobCtx, cancel, j, binary, inv, sem)
		return j, nil
	}

	// All other actions (and copies when the cap is disabled) spawn
	// synchronously so start() can still surface a cmd.Start() error to the
	// caller — unchanged legacy behaviour.
	if err := r.spawn(jobCtx, cancel, j, binary, inv, nil); err != nil {
		return j, err
	}
	return j, nil
}

// spawnGated waits for a copy slot, then spawns the job. Runs in its own
// goroutine; the job is already registered as Pending. If the job is cancelled
// while queued, it never starts. The acquired slot is released exactly once
// when the job reaches a terminal state (handled inside spawn via the release
// callback, which also covers the cmd.Start()-failure path).
func (r *jobRegistry) spawnGated(jobCtx context.Context, cancel context.CancelFunc, j *Job, binary string, inv cliInvocation, sem chan struct{}) {
	select {
	case <-jobCtx.Done():
		// Cancelled (or parent context done) before we ever got a slot —
		// finalise as cancelled without spawning anything.
		r.finalizeUnstarted(j, cancel)
		return
	case sem <- struct{}{}:
	}
	released := false
	release := func() {
		if !released {
			released = true
			<-sem
		}
	}
	if err := r.spawn(jobCtx, cancel, j, binary, inv, release); err != nil {
		// spawn already finalised the job + invoked release on the
		// Start-failure path; nothing more to do.
		slog.Warn("gated copy failed to start", "job", j.ID, "error", err)
	}
}

// finalizeUnstarted marks a job that never spawned (e.g. cancelled while queued
// on the copy semaphore) as terminal and runs its cleanup.
func (r *jobRegistry) finalizeUnstarted(j *Job, cancel context.CancelFunc) {
	cancel()
	j.mu.Lock()
	if j.State != JobCancelled {
		j.State = JobCancelled
	}
	if j.ErrorMsg == "" {
		j.ErrorMsg = "cancelled before start"
	}
	j.CompletedAt = time.Now().UTC()
	for ch := range j.subscribers {
		close(ch)
	}
	j.subscribers = map[chan string]struct{}{}
	cleanup := j.cleanup
	j.cleanup = nil
	j.mu.Unlock()
	if cleanup != nil {
		cleanup()
	}
	r.markTerminated(j)
	r.emit(j, EventCancelled)
	slog.Info("job terminal", "job", j.ID, "state", string(JobCancelled))
}

// spawn starts the CLI process, tails its output, and waits for completion in a
// background goroutine. release, if non-nil, is invoked exactly once when the
// job reaches a terminal state (including the Start-failure path) — used to
// free the copy semaphore slot. Returns a non-nil error only when the process
// could not be started.
func (r *jobRegistry) spawn(jobCtx context.Context, cancel context.CancelFunc, j *Job, binary string, inv cliInvocation, release func()) error {
	if release == nil {
		release = func() {}
	}
	cmd := inv.command(jobCtx, binary)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		release()
		return fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		release()
		return fmt.Errorf("stderr pipe: %w", err)
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
		release()
		return err
	}

	j.mu.Lock()
	j.State = JobRunning
	j.StartedAt = time.Now().UTC()
	j.mu.Unlock()
	r.emit(j, EventStarted)

	// Tail stdout + stderr concurrently into the same ring buffer.
	go r.tail(j, stdout, "stdout")
	go r.tail(j, stderr, "stderr")

	// Wait for completion in a separate goroutine so spawn() returns immediately.
	go func() {
		defer release()
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

	return nil
}

func (r *jobRegistry) tail(j *Job, rdr io.Reader, source string) {
	scanner := bufio.NewScanner(rdr)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		j.appendLine(line)
		// Progress parse — shape depends on action. Backup/restore/copy use the
		// chunk progress lines; check derives percent from revision counters;
		// prune just counts (no percent). One match → one fleet snapshot push.
		switch j.Action {
		case ActionBackup, ActionRestore, ActionCopy:
			if j.parseProgressLine(line) {
				r.emitProgress(j)
			}
		case ActionCheck:
			if j.parseCheckLine(line) {
				r.emitProgress(j)
			}
			// Tabular table parse runs in parallel with the per-revision
			// counter parse — the table comes from `check -tabular` (always-on),
			// the counters come from the SNAPSHOT_CHECK summary lines that
			// appear regardless. Both fire for the same job.
			j.mu.Lock()
			if j.tabular == nil {
				j.tabular = &checkTabularParser{}
			}
			parser := j.tabular
			j.mu.Unlock()
			if row := parser.feed(line); row != nil {
				j.mu.Lock()
				j.tabularRows = append(j.tabularRows, row)
				j.mu.Unlock()
			}
		case ActionPrune:
			if j.parsePruneLine(line) {
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
	// Running jobs are cancelled by killing the process; Pending jobs are
	// copies still queued on the copy semaphore — cancelling their context
	// trips the jobCtx.Done() path in spawnGated so they finalise as cancelled
	// without ever spawning. Both set State first so the terminal-handler
	// preserves JobCancelled instead of overwriting with JobFailed.
	if j.State != JobRunning && j.State != JobPending {
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
	copyThreads := req.Threads
	if copyThreads <= 0 {
		copyThreads = a.cfg.CopyThreads
	}
	inv := invocationForCopy(repo, req.From, req.To, copyThreads, req.SnapshotID, rsaPriv[req.From], "")
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
