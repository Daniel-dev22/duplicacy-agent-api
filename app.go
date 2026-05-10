package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// app holds wiring for all subsystems. Constructed once in main(), passed to handlers.
type app struct {
	cfg                 Config
	controlCenterClient *http.Client
	jobs                *jobRegistry
	repos               *repoIndex
	scheduler           *scheduler
	filters             *filterCache
	events              *eventBuffer
	mapping             *repoMappingStore // controller-managed repo↔credential mapping
	secrets             *secretCache      // 60s TTL cache of vended bundles
	compose             *composeIndex     // bounded scan of mounted COMPOSE_SCAN_ROOTS for compose project dirs
	fleet               *fleetHub         // /ws/fleet broadcaster — pushes snapshot on init / job state change
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

	repos := newRepoIndex(cfg.BackupRoots, cfg.DuplicacyBinary, cfg.HostToContainer)

	mapping := newRepoMappingStore(cfg.ConfigDir)
	if err := mapping.load(); err != nil {
		return nil, fmt.Errorf("load repo mapping: %w", err)
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
		filters:             filters,
		mapping:             mapping,
		secrets:             newSecretCache(),
		compose:             newComposeIndex(cfg.ComposeScanRoots),
		stop:                make(chan struct{}),
	}
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

	// Best-effort initial repo scan so /repos returns something on first call.
	if err := a.repos.scan(); err != nil {
		log.Warn().Err(err).Msg("initial repo scan failed (will retry on first /repos call)")
	}
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
	go a.events.drainLoop(ctx)
	a.scheduler.Start(ctx)
	go a.filters.reconcileLoop(ctx, a.stop)
	go a.fleet.Run(ctx)
	log.Info().Msg("background workers started")
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
