package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// Schedule mirrors scheduler-api/main.go:46-52 exactly so the JSON shape and
// matching semantics are identical between scheduler-api and the agent's local cron.
type Schedule struct {
	Days            []string `json:"days"`
	Hours           []string `json:"hours"`
	Minute          string   `json:"minute"`
	IntervalMinutes *int     `json:"interval_minutes,omitempty"`
}

// MissPolicy controls catch-up behaviour when an agent boots after missing fire windows.
type MissPolicy string

const (
	MissSkip            MissPolicy = "skip"
	MissRunOnceWithin24 MissPolicy = "run-once-within-24h"
	MissRunAll          MissPolicy = "run-all" // rarely used; e.g., low-frequency prune
)

// LocalSchedule is what the agent stores: the Schedule plus what to fire and how.
// Pulled from controller, persisted in CONFIG_DIR/schedules.json.
type LocalSchedule struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	RepoID     string                 `json:"repo_id"`
	Action     JobAction              `json:"action"` // backup | check | prune
	Storage    string                 `json:"storage,omitempty"`
	Schedule   Schedule               `json:"schedule"`
	MissPolicy MissPolicy             `json:"miss_policy"`
	Enabled    bool                   `json:"enabled"`
	Params     map[string]interface{} `json:"params,omitempty"`
	Version    int64                  `json:"version"`
}

// scheduleCache is what we serialize to disk.
type scheduleCache struct {
	Schedules     []LocalSchedule  `json:"schedules"`
	LastFiredAt   map[string]time.Time `json:"last_fired_at"` // keyed by schedule ID
	LastFetchedAt time.Time            `json:"last_fetched_at"`
}

// prepareEnvFn returns env vars + per-alias RSA private key paths + cleanup
// func for one duplicacy invocation against the given repo. Implemented by
// app.prepareEnvForRepo. Defining it as a function-value here keeps the
// scheduler decoupled from the app struct. The scheduler only fires backup /
// check / prune (not restore) so the RSA priv map is unused here, but the
// signature stays consistent with the underlying implementation.
type prepareEnvFn func(ctx context.Context, repo *Repo) ([]string, map[string]string, func(), error)

type scheduler struct {
	cfg        Config
	client     *http.Client
	jobs       *jobRegistry
	repos      *repoIndex
	prepareEnv prepareEnvFn
	easternTZ  *time.Location

	mu       sync.RWMutex
	cache    scheduleCache
	cachePath string

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func newScheduler(cfg Config, client *http.Client, jobs *jobRegistry, repos *repoIndex, prepareEnv prepareEnvFn) (*scheduler, error) {
	tz, err := time.LoadLocation("America/New_York")
	if err != nil {
		return nil, fmt.Errorf("load eastern tz: %w", err)
	}
	s := &scheduler{
		cfg:        cfg,
		client:     client,
		jobs:       jobs,
		repos:      repos,
		prepareEnv: prepareEnv,
		easternTZ:  tz,
		cachePath:  filepath.Join(cfg.ConfigDir, "schedules.json"),
		stop:       make(chan struct{}),
	}
	s.cache = scheduleCache{LastFiredAt: map[string]time.Time{}}
	if err := s.loadCache(); err != nil {
		log.Warn().Err(err).Msg("schedule cache load failed (starting empty)")
	}
	return s, nil
}

// loadCache reads the on-disk cache. Missing file is not an error.
func (s *scheduler) loadCache() error {
	data, err := os.ReadFile(s.cachePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var c scheduleCache
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("parse cache: %w", err)
	}
	if c.LastFiredAt == nil {
		c.LastFiredAt = map[string]time.Time{}
	}
	s.mu.Lock()
	s.cache = c
	s.mu.Unlock()
	log.Info().Int("count", len(c.Schedules)).Msg("loaded schedule cache from disk")
	return nil
}

// saveCache atomically writes the in-memory cache to disk.
func (s *scheduler) saveCache() error {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.cache, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	tmp := s.cachePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, s.cachePath)
}

// Start runs the 1-minute fire loop and the 5-minute reconciliation loop.
func (s *scheduler) Start(ctx context.Context) {
	s.wg.Add(2)
	go s.fireLoop(ctx)
	go s.reconcileLoop(ctx)
	// Initial pull is best-effort; if it fails we keep the on-disk cache.
	go func() {
		_ = s.pull(ctx)
		s.MissedRunCheck(ctx)
	}()
}

func (s *scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.wg.Wait()
}

func (s *scheduler) fireLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case now := <-t.C:
			s.tick(ctx, now)
		}
	}
}

func (s *scheduler) reconcileLoop(ctx context.Context) {
	defer s.wg.Done()
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stop:
			return
		case <-t.C:
			if err := s.pull(ctx); err != nil {
				log.Warn().Err(err).Msg("schedule reconcile pull failed")
			}
		}
	}
}

// pull fetches /api/duplicacy/schedules?node=<self> and replaces the local cache.
// Preserves LastFiredAt for schedules that survive the diff.
func (s *scheduler) pull(ctx context.Context) error {
	url := fmt.Sprintf("%s/api/duplicacy/schedules?node=%s", s.cfg.ControlCenterURL, s.cfg.NodeName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	var body struct {
		Schedules []LocalSchedule `json:"schedules"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode: %w", err)
	}

	s.mu.Lock()
	preservedFired := map[string]time.Time{}
	for _, sch := range body.Schedules {
		if t, ok := s.cache.LastFiredAt[sch.ID]; ok {
			preservedFired[sch.ID] = t
		}
	}
	s.cache.Schedules = body.Schedules
	s.cache.LastFiredAt = preservedFired
	s.cache.LastFetchedAt = time.Now().UTC()
	s.mu.Unlock()

	if err := s.saveCache(); err != nil {
		log.Warn().Err(err).Msg("save cache after pull failed")
	}
	log.Info().Int("count", len(body.Schedules)).Msg("schedule pull complete")
	return nil
}

// tick checks every enabled schedule against now and fires any matching ones.
func (s *scheduler) tick(ctx context.Context, now time.Time) {
	s.mu.RLock()
	schedules := append([]LocalSchedule(nil), s.cache.Schedules...)
	s.mu.RUnlock()

	for _, sch := range schedules {
		if !sch.Enabled {
			continue
		}
		if !scheduleMatches(sch.Schedule, now, s.easternTZ) {
			continue
		}
		s.fire(ctx, sch, "schedule", false)
	}
}

// scheduleMatches is the core fire-window check. Mirrors scheduler-api/main.go:908-938 verbatim.
func scheduleMatches(schedule Schedule, now time.Time, tz *time.Location) bool {
	now = now.In(tz)
	if schedule.IntervalMinutes != nil && *schedule.IntervalMinutes > 0 {
		minutesSinceMidnight := now.Hour()*60 + now.Minute()
		return minutesSinceMidnight%*schedule.IntervalMinutes == 0
	}
	dayName := strings.ToLower(now.Weekday().String()[:3])
	if !contains(schedule.Days, "*") && !contains(schedule.Days, dayName) {
		return false
	}
	hourStr := strconv.Itoa(now.Hour())
	if !contains(schedule.Hours, "*") && !contains(schedule.Hours, hourStr) {
		return false
	}
	minuteInt, _ := strconv.Atoi(schedule.Minute)
	return now.Minute() == minuteInt
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

// fire dispatches a job for the given schedule.
func (s *scheduler) fire(ctx context.Context, sch LocalSchedule, triggerKey string, isMissedRecovery bool) {
	repo, ok := s.repos.get(sch.RepoID)
	if !ok {
		log.Warn().Str("schedule", sch.ID).Str("repo", sch.RepoID).Msg("schedule fire: repo not found, skipping")
		return
	}
	var inv cliInvocation
	switch sch.Action {
	case ActionBackup:
		threads := paramInt(sch.Params, "threads")
		tag := paramString(sch.Params, "tag")
		inv = invocationForBackup(repo, sch.Storage, tag, threads)
	case ActionCheck:
		inv = invocationForCheck(repo, sch.Storage, paramString(sch.Params, "revisions"), paramBool(sch.Params, "all"))
	case ActionPrune:
		inv = invocationForPrune(repo, sch.Storage, paramStrings(sch.Params, "keep_rules"),
			paramBool(sch.Params, "exclusive"), paramBool(sch.Params, "exhaustive"))
	default:
		log.Warn().Str("action", string(sch.Action)).Str("schedule", sch.ID).Msg("schedule fire: unsupported action")
		return
	}

	var (
		env     []string
		cleanup func()
	)
	if s.prepareEnv != nil {
		var err error
		env, _, cleanup, err = s.prepareEnv(ctx, repo)
		if err != nil {
			log.Error().Err(err).Str("schedule", sch.ID).Msg("schedule fire: vend secrets failed; skipping")
			return
		}
	}
	if cleanup == nil {
		cleanup = func() {}
	}
	inv.EnvAdds = append(inv.EnvAdds, env...)

	j, err := s.jobs.start(ctx, s.cfg.DuplicacyBinary, repo, sch.Action, sch.Storage, inv, sch.ID, triggerKey, cleanup)
	if err != nil {
		log.Error().Err(err).Str("schedule", sch.ID).Msg("schedule fire failed to start job")
		return
	}

	s.mu.Lock()
	s.cache.LastFiredAt[sch.ID] = time.Now().UTC()
	s.mu.Unlock()
	if err := s.saveCache(); err != nil {
		log.Warn().Err(err).Msg("save cache after fire failed")
	}

	log.Info().
		Str("schedule", sch.ID).
		Str("job", j.ID).
		Str("action", string(sch.Action)).
		Str("repo", sch.RepoID).
		Bool("missed_recovery", isMissedRecovery).
		Msg("schedule fired")
}

// MissedRunCheck walks each schedule and fires once per miss policy if a window was missed.
// Called on boot after the first successful pull.
func (s *scheduler) MissedRunCheck(ctx context.Context) {
	s.mu.RLock()
	schedules := append([]LocalSchedule(nil), s.cache.Schedules...)
	lastFired := map[string]time.Time{}
	for k, v := range s.cache.LastFiredAt {
		lastFired[k] = v
	}
	s.mu.RUnlock()

	now := time.Now()

	for _, sch := range schedules {
		if !sch.Enabled || sch.MissPolicy == MissSkip {
			continue
		}
		// run-all is supported only for very low-frequency schedules; v1 treats it the
		// same as run-once to avoid a missed-prune storm. We log the truncation.
		if sch.MissPolicy == MissRunAll {
			log.Warn().Str("schedule", sch.ID).Msg("run-all miss policy treated as run-once in v1")
		}

		windowEnd := now
		windowStart := now.Add(-24 * time.Hour)
		lastWindow, ok := findLastFireWindow(sch.Schedule, windowStart, windowEnd, s.easternTZ)
		if !ok {
			continue // no fire window in last 24h
		}
		fired := lastFired[sch.ID]
		if fired.After(lastWindow) || fired.Equal(lastWindow) {
			continue // we already fired since the last window
		}

		// Fire once now as catch-up.
		log.Info().
			Str("schedule", sch.ID).
			Time("missed_window", lastWindow).
			Time("last_fired", fired).
			Msg("missed-run recovery: firing catch-up")
		s.fire(ctx, sch, "schedule-recovery", true)
	}
}

// findLastFireWindow scans backward minute-by-minute looking for the most recent
// minute (in [start, end]) where scheduleMatches returns true. Returns false if none.
func findLastFireWindow(schedule Schedule, start, end time.Time, tz *time.Location) (time.Time, bool) {
	t := end.Truncate(time.Minute)
	startBound := start.Truncate(time.Minute)
	for t.After(startBound) || t.Equal(startBound) {
		if scheduleMatches(schedule, t, tz) {
			return t, true
		}
		t = t.Add(-1 * time.Minute)
	}
	return time.Time{}, false
}

// --- helpers for params ---

func paramString(m map[string]interface{}, k string) string {
	if v, ok := m[k]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func paramInt(m map[string]interface{}, k string) int {
	if v, ok := m[k]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		}
	}
	return 0
}

func paramBool(m map[string]interface{}, k string) bool {
	if v, ok := m[k]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func paramStrings(m map[string]interface{}, k string) []string {
	if v, ok := m[k]; ok {
		if arr, ok := v.([]interface{}); ok {
			out := make([]string, 0, len(arr))
			for _, e := range arr {
				if s, ok := e.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
	}
	return nil
}

// --- HTTP handlers (override placeholders) ---

func (a *app) handleListSchedules(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusOK, gin.H{"schedules": []LocalSchedule{}})
		return
	}
	a.scheduler.mu.RLock()
	out := append([]LocalSchedule(nil), a.scheduler.cache.Schedules...)
	a.scheduler.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	c.JSON(http.StatusOK, gin.H{"schedules": out})
}

func (a *app) handleSchedulesRefresh(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "scheduler not initialized"})
		return
	}
	if err := a.scheduler.pull(c.Request.Context()); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"refreshed": true})
}

func (a *app) handleSchedulesCache(c *gin.Context) {
	if a.scheduler == nil {
		c.JSON(http.StatusOK, gin.H{})
		return
	}
	a.scheduler.mu.RLock()
	defer a.scheduler.mu.RUnlock()
	c.JSON(http.StatusOK, a.scheduler.cache)
}
