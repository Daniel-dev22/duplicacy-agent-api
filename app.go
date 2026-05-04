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
	stop                chan struct{} // closed in close(); subsystems range on it for shutdown
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

	repos := newRepoIndex(cfg.BackupRoots, cfg.DuplicacyBinary)

	sched, err := newScheduler(cfg, cc, jobs, repos)
	if err != nil {
		return nil, fmt.Errorf("scheduler: %w", err)
	}

	filters, err := newFilterCache(cfg, cc, repos)
	if err != nil {
		return nil, fmt.Errorf("filter cache: %w", err)
	}

	a := &app{
		cfg:                 cfg,
		controlCenterClient: cc,
		jobs:                jobs,
		repos:               repos,
		events:              events,
		scheduler:           sched,
		filters:             filters,
		stop:                make(chan struct{}),
	}

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
