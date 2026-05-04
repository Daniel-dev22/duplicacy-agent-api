package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	cfg := loadConfig()
	log.Info().
		Str("node", cfg.NodeName).
		Str("site", cfg.SiteID).
		Strs("backup_roots", cfg.BackupRoots).
		Msg("duplicacy-agent-api starting")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := newApp(ctx, cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to initialize app")
	}
	defer app.close()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(ginZerologMiddleware())

	registerRoutes(r, app)

	srv := &http.Server{Addr: ":8080", Handler: r}

	go func() {
		log.Info().Msg("HTTP server listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server error")
		}
	}()

	app.startBackgroundWorkers(ctx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info().Msg("shutting down")
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error().Err(err).Msg("HTTP shutdown error")
	}
}

func registerRoutes(r *gin.Engine, app *app) {
	r.GET("/health/live", app.handleLiveness)
	r.GET("/health/ready", app.handleReadiness)

	r.GET("/repos", app.handleListRepos)
	r.POST("/repos", app.handleInitRepo)
	r.GET("/repos/:id/snapshots", app.handleListSnapshots)
	r.GET("/repos/:id/snapshots/:rev/files", app.handleSnapshotFiles)
	r.POST("/repos/:id/backup", app.handleBackup)
	r.POST("/repos/:id/restore", app.handleRestore)
	r.POST("/repos/:id/check", app.handleCheck)
	r.POST("/repos/:id/prune", app.handlePrune)

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
	r.GET("/ws/jobs/:id/logs", app.handleJobLogsWS)

	r.GET("/global-filters/cache", app.handleGlobalFiltersCache)
	r.POST("/global-filters/refresh", app.handleGlobalFiltersRefresh)

	r.GET("/schedules", app.handleListSchedules)
	r.POST("/schedules/refresh", app.handleSchedulesRefresh)
	r.GET("/schedules/cache", app.handleSchedulesCache)
}

func ginZerologMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("dur", time.Since(start)).
			Msg("http")
	}
}
