package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

// fleetJobsCap bounds how many jobs the fleet snapshot carries.
const fleetJobsCap = 50

// fleetJobs returns the jobs for the fleet snapshot: live in-memory jobs (with
// up-to-date progress) plus recent persisted jobs from the local SQLite store
// that aren't currently in memory. The persisted overlay is what gives the
// dashboard restart-survival — the in-memory registry comes up empty after an
// agent restart, but events.sqlite still holds recent history, so the node keeps
// showing its recent backups instead of going blank until the next run.
func (a *app) fleetJobs() []jobPublic {
	live := a.jobs.list()
	seen := make(map[string]struct{}, len(live))
	for _, j := range live {
		seen[j.ID] = struct{}{}
	}
	out := make([]jobPublic, 0, len(live)+fleetJobsCap)
	out = append(out, live...)
	if a.events != nil {
		persisted, err := a.events.listRecentJobs(fleetJobsCap)
		if err != nil {
			slog.Warn("fleet: list persisted jobs failed", "error", err)
		} else {
			for _, jp := range persisted {
				if _, ok := seen[jp.ID]; !ok {
					out = append(out, jp)
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return jobRecency(out[i]).After(jobRecency(out[j]))
	})
	if len(out) > fleetJobsCap {
		out = out[:fleetJobsCap]
	}
	return out
}

func jobRecency(j jobPublic) time.Time {
	if !j.CompletedAt.IsZero() {
		return j.CompletedAt
	}
	return j.StartedAt
}

// app holds wiring for all subsystems. Constructed once in main(), passed to handlers.
type app struct {
	cfg                 Config
	controlCenterClient *http.Client
	jobs                *jobRegistry
	repos               *repoIndex
	scheduler           *scheduler
	filters             *filterCache
	events              *eventBuffer
	snapshotStats       *snapshotStatsStore // per-snapshot dedup rows from `check -tabular`
	mapping             *repoMappingStore   // controller-managed repo↔credential mapping
	secrets             *secretCache      // 60s TTL cache of vended bundles
	compose             *composeIndex     // bounded scan of mounted COMPOSE_SCAN_ROOTS for compose project dirs
	fleet               *fleetHub         // /ws/fleet broadcaster — pushes snapshot on init / job state change
	trees               *treeWalker       // 5-min push of repo + node filesystem trees to controller
	sizes               *dirSizeCache     // persisted per-directory subtree byte totals (read by the tree push)
	sizeGatherer        *sizeGatherer     // self-paced background loop that fills `sizes`
	stop                chan struct{}     // closed in close(); subsystems range on it for shutdown
}

func newApp(ctx context.Context, cfg Config) (*app, error) {
	cc, err := buildControlCenterClient(cfg)
	if err != nil {
		return nil, err
	}

	events, err := newEventBuffer(cfg, cc)
	if err != nil {
		return nil, fmt.Errorf("event buffer: %w", err)
	}

	jobs := newJobRegistry()
	jobs.RegisterHook(events.handleJobEvent)

	// Persist per-job ring buffers under ${CONFIG_DIR}/job-logs on terminal.
	// Prune oldest beyond jobLogRetainN at boot so we don't carry history
	// forever across restarts.
	jobLogDir := filepath.Join(cfg.ConfigDir, "job-logs")
	jobs.setJobLogDir(jobLogDir)
	pruneJobLogs(jobLogDir, jobLogRetainN)

	repos := newRepoIndex(cfg.BackupRoots, cfg.DuplicacyBinary, cfg.LegacyBackuprootMap, cfg.BackupExcludePaths)

	mapping := newRepoMappingStore(cfg.ConfigDir)
	if err := mapping.load(); err != nil {
		return nil, fmt.Errorf("load repo mapping: %w", err)
	}

	// Ensure /root/.ssh/known_hosts symlinks into the persistent state mount
	// before any SFTP work runs. Idempotent — a no-op once set up. Don't
	// fail-fast on error: an SFTP-less agent (b2/s3/gcs only) shouldn't be
	// blocked from starting just because we couldn't prep a file it'll
	// never need.
	if err := ensureKnownHostsSetup(cfg); err != nil {
		slog.Warn("known_hosts setup failed (SFTP storages will fail until resolved)", "error", err)
	}

	filters, err := newFilterCache(cfg, cc, repos)
	if err != nil {
		return nil, fmt.Errorf("filter cache: %w", err)
	}

	// Construct app first so the scheduler can borrow a.prepareEnvForRepo via a
	// bound method literal. This avoids leaking the *app pointer into scheduler;
	// only the function value travels.
	a := &app{
		cfg:                 cfg,
		controlCenterClient: cc,
		jobs:                jobs,
		repos:               repos,
		events:              events,
		snapshotStats:       newSnapshotStatsStore(events.DB()),
		filters:             filters,
		mapping:             mapping,
		secrets:             newSecretCache(),
		compose:             newComposeIndex(cfg.ComposeScanRoots),
		stop:                make(chan struct{}),
	}
	// Flush per-snapshot dedup rows captured during a `check -tabular` run
	// into snapshot_stats on terminal state. Runs in a goroutine because the
	// completion hook fires from cmd.Wait()'s tail goroutine — we don't want
	// to block job teardown on a slow SQLite write.
	jobs.RegisterHook(func(j *Job, evt JobEvent) {
		if evt != EventCompleted || j.snapshot().Action != ActionCheck {
			return
		}
		go a.flushSnapshotStats(j)
	})
	a.fleet = newFleetHub(a)
	// Trigger a fleet snapshot on every job lifecycle event so connected
	// dashboards see state transitions in real time.
	jobs.RegisterHook(func(_ *Job, _ JobEvent) { a.fleet.Trigger() })
	// Also trigger on progress-line updates during a backup. Fires per
	// chunk-uploaded line; the hub's coalescing channel collapses bursts
	// into one snapshot per broadcast cycle, so this is cheap.
	jobs.RegisterProgressHook(func(_ *Job) { a.fleet.Trigger() })

	sched, err := newScheduler(cfg, cc, jobs, repos, a.prepareEnvForRepo)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %w", err)
	}
	a.scheduler = sched

	a.trees = newTreeWalker(cfg, repos, a)

	// Per-directory size cache + its self-paced background gatherer. The cache
	// is read by the tree push (annotateSizes); the gatherer fills it on its own
	// cadence, fully decoupled from the 5-min push. See tree_sizes.go.
	a.sizes = newDirSizeCache(cfg.ConfigDir)
	a.sizeGatherer = newSizeGatherer(cfg, a.sizes, a)

	// Best-effort initial repo scan so /repos returns something on first call.
	// Run asynchronously so it doesn't block newApp returning — otherwise
	// startup is gated on walking every backup root (on pi-class hosts with
	// /var/lib/rancher/k3s bind-mounted, that has overrun the ansible
	// /health/ready readiness probe). The HTTP listener is up immediately;
	// any /repos call landing before the scan finishes triggers its own.
	//
	// On completion we trigger the fleet hub so any WS clients that
	// connected during the cold scan (and got an empty snapshot from the
	// pre-warm cache) receive a refreshed broadcast with the real repo
	// list. Without this, slow-disk hosts looked offline until the next
	// job event happened to land.
	go func() {
		if err := a.repos.scan(); err != nil {
			slog.Warn("initial repo scan failed (will retry on first /repos call)", "error", err)
		}
		a.fleet.Trigger()
	}()
	return a, nil
}

func (a *app) close() {
	if a.stop != nil {
		select {
		case <-a.stop:
			// already closed
		default:
			close(a.stop)
		}
	}
	if a.scheduler != nil {
		a.scheduler.Stop()
	}
	if a.events != nil {
		a.events.close()
	}
}

func (a *app) startBackgroundWorkers(ctx context.Context) {
	a.events.Start(ctx)
	a.scheduler.Start(ctx)
	go a.filters.reconcileLoop(ctx, a.stop)
	go a.fleet.Run(ctx)
	go a.reconcileLoop(ctx)
	a.trees.Start(ctx)
	a.sizeGatherer.Start(ctx)
	slog.Info("background workers started")
}

// --- placeholder handlers; real implementations replace these in subsequent tasks ---

func (a *app) handleLiveness(c *gin.Context)  { c.JSON(http.StatusOK, gin.H{"status": "alive"}) }
func (a *app) handleReadiness(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ready"}) }

func notImplemented(c *gin.Context) {
	c.JSON(http.StatusNotImplemented, gin.H{"error": "not implemented yet"})
}

// handleListRepos / handleListSnapshots / handleSnapshotFiles / handleGetPreferences:
//   in repos.go and duplicacy.go.
// handleBackup / handleRestore / handleCheck / handlePrune / handleListJobs / handleGetJob
//   / handleJobLogsWS: in jobs.go.

// handleListSchedules / handleSchedulesRefresh / handleSchedulesCache: in schedules.go.
// handleGetFilters / handlePutFilters / handleRenderFilters / handleGlobalFiltersCache /
//   handleGlobalFiltersRefresh: in filters.go.

func (a *app) handleInitRepo(c *gin.Context)       { notImplemented(c) }
func (a *app) handlePutPreferences(c *gin.Context) { notImplemented(c) }
func (a *app) handleListStorages(c *gin.Context)   { notImplemented(c) }
func (a *app) handleAddStorage(c *gin.Context)     { notImplemented(c) }
func (a *app) handleDeleteStorage(c *gin.Context)  { notImplemented(c) }
