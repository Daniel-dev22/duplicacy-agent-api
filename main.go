package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/logging"
	"github.com/gin-gonic/gin"
)

func main() {
	logging.Setup(os.Getenv("LOG_LEVEL"))
	// When LOG_DIR is set, augment with a rotating file sink (lumberjack)
	// alongside stderr. See logging_setup.go for the rotation policy. No-op
	// otherwise — stderr-only path is preserved for legacy deploys.
	attachRotatingLogFile(os.Getenv("LOG_LEVEL"))

	cfg := loadConfig()
	slog.Info("duplicacy-agent-api starting",
		"node", cfg.NodeName,
		"site", cfg.SiteID,
		"backup_roots", cfg.BackupRoots)

	// Report how long the previous instance was offline. Logs WARN on a
	// >2 min gap (likely crash/OOM/restart). Must run BEFORE newApp opens
	// any DB so a "first boot" / "clean shutdown" / "long downtime" line
	// is the very first record we emit for this boot.
	reportBootGap(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := newApp(ctx, cfg)
	if err != nil {
		slog.Error("failed to initialize app", "error", err)
		os.Exit(1)
	}
	defer app.close()

	// Persistent heartbeat — 30s tick writes a Unix-nano timestamp to
	// ${CONFIG_DIR}/heartbeat.last. Clean shutdown writes "0" so the
	// next boot's reportBootGap can distinguish stop vs crash.
	go heartbeatLoop(ctx, cfg, 30*time.Second)

	// Orphan-job sweep — recover controller rows stuck in running/pending
	// from a prior container exit before the HTTP listener opens. Bounded
	// 15s timeout inside; non-fatal on failure so a slow controller doesn't
	// block boot.
	sweepOrphanJobs(ctx, cfg, app.controlCenterClient, app.events)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(ginZerologMiddleware())

	registerRoutes(r, app)

	srv := &http.Server{Addr: ":8080", Handler: r}

	go func() {
		slog.Info("HTTP server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	app.startBackgroundWorkers(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP shutdown error", "error", err)
	}
}

func registerRoutes(r *gin.Engine, app *app) {
	r.GET("/health/live", app.handleLiveness)
	r.GET("/health/ready", app.handleReadiness)

	r.GET("/repos", app.handleListRepos)
	r.GET("/compose-projects", app.handleListComposeProjects)
	r.POST("/repos", app.handleInitRepo)
	r.POST("/repos/init", app.handleInitRepoNew)
	r.POST("/repos/bind", app.handleBindRepo)
	r.POST("/repos/delete", app.handleDeleteRepo)
	r.GET("/repos/:id/snapshots", app.handleListSnapshots)
	r.GET("/repos/:id/snapshots/:rev/files", app.handleSnapshotFiles)
	r.GET("/repos/:id/snapshot-stats", app.handleListSnapshotStats)
	r.GET("/repos/:id/storage-rollup", app.handleRepoStorageRollup)
	r.GET("/storage-rollup", app.handleNodeStorageRollup)
	r.GET("/storage-rollup/repos", app.handleStorageReposBreakdown)
	r.POST("/repos/:id/backup", app.handleBackup)
	r.POST("/repos/:id/restore", app.handleRestore)
	r.POST("/repos/:id/check", app.handleCheck)
	r.POST("/repos/:id/prune", app.handlePrune)
	r.POST("/repos/:id/copy", app.handleCopy)

	r.GET("/repos/:id/preferences", app.handleGetPreferences)
	r.PUT("/repos/:id/preferences", app.handlePutPreferences)
	r.GET("/repos/:id/filters", app.handleGetFilters)
	r.PUT("/repos/:id/filters", app.handlePutFilters)
	r.POST("/repos/:id/filters/render", app.handleRenderFilters)

	r.GET("/storages", app.handleListStorages)
	r.POST("/storages", app.handleAddStorage)
	r.DELETE("/storages/:name", app.handleDeleteStorage)

	r.GET("/jobs", app.handleListJobs)
	r.GET("/jobs/:id", app.handleGetJob)
	// Durable per-job log retrieval. Prefers ${CONFIG_DIR}/job-logs/<id>.log
	// (survives container restart); falls back to the in-memory ring
	// buffer for currently-running jobs. See job_logs.go.
	r.GET("/jobs/:id/log", app.handleGetJobLog)
	r.POST("/jobs/:id/cancel", app.handleCancelJob)
	r.GET("/ws/jobs/:id/logs", app.handleJobLogsWS)
	r.GET("/ws/fleet", app.handleFleetWS)

	r.GET("/global-filters/cache", app.handleGlobalFiltersCache)
	r.POST("/global-filters/refresh", app.handleGlobalFiltersRefresh)

	// Internal: controller invalidates the agent's secrets cache when a
	// credential is updated/deleted. Authenticated by the controller's mTLS
	// client cert at the traefik-internal hop.
	r.POST("/internal/credentials/:id/invalidate", app.handleInvalidateCredential)

	r.GET("/schedules", app.handleListSchedules)
	r.POST("/schedules/refresh", app.handleSchedulesRefresh)
	r.GET("/schedules/cache", app.handleSchedulesCache)
}

func ginZerologMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		slog.Info("http",
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"dur", time.Since(start))
	}
}
