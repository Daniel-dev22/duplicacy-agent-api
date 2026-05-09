package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

// Storage describes one configured backup destination on a repo.
// Mirrors duplicacy_preference.go::Preference but redacts the actual key values
// (we only return which keys are configured, never the secrets).
type Storage struct {
	Name           string   `json:"name"`
	URL            string   `json:"url"`
	Encrypted      bool     `json:"encrypted"`
	NoBackup       bool     `json:"no_backup"`
	NoRestore      bool     `json:"no_restore"`
	FiltersFile    string   `json:"filters_file,omitempty"`
	ConfiguredKeys []string `json:"configured_keys"` // names only, no values
}

// Repo is one duplicacy-initialised directory (one .duplicacy/preferences file).
// Each Repo can have multiple Storages (e.g. NAS + S3 for the same source tree).
type Repo struct {
	ID            string    `json:"id"`            // stable hash of Path
	Path          string    `json:"path"`          // absolute path to repo root
	SnapshotID    string    `json:"snapshot_id"`   // duplicacy snapshot id (typically same across storages)
	Storages      []Storage `json:"storages"`
	HasFilters    bool      `json:"has_filters"`
	LastScannedAt time.Time `json:"last_scanned_at"`
}

// rawPreference is the on-disk shape from duplicacy_preference.go::Preference.
type rawPreference struct {
	Name              string            `json:"name"`
	SnapshotID        string            `json:"id"`
	RepositoryPath    string            `json:"repository"`
	StorageURL        string            `json:"storage"`
	Encrypted         bool              `json:"encrypted"`
	BackupProhibited  bool              `json:"no_backup"`
	RestoreProhibited bool              `json:"no_restore"`
	DoNotSavePassword bool              `json:"no_save_password"`
	NobackupFile      string            `json:"nobackup_file"`
	Keys              map[string]string `json:"keys"`
	FiltersFile       string            `json:"filters"`
	ExcludeByAttr     bool              `json:"exclude_by_attribute"`
}

// repoIndex caches scanned repos. The HTTP layer reads from the cache; a refresh
// is triggered by handleListRepos and by anything that mutates a repo (init, storage add).
//
// scanTTL bounds how often the on-disk walk runs in response to /repos polls.
// The fleet-page polls every 15s × N nodes; without a cache each call walks
// ALL backup roots to depth 3 (~1.5–2s on busy hosts), making the dashboard
// feel sluggish. Mutating ops (init, storage add, delete) call ScanForce()
// to invalidate the cache and re-walk immediately so the next list reflects
// fresh state.
type repoIndex struct {
	roots  []string
	binary string

	mu          sync.RWMutex
	repos       map[string]*Repo // keyed by Repo.ID
	lastScanned time.Time
}

const repoScanTTL = 30 * time.Second

func newRepoIndex(roots []string, binary string) *repoIndex {
	return &repoIndex{
		roots:  roots,
		binary: binary,
		repos:  map[string]*Repo{},
	}
}

// scan walks each backup root looking for .duplicacy/preferences files,
// honouring the cache TTL. Use ScanForce after init/storage-add/delete.
func (r *repoIndex) scan() error {
	r.mu.RLock()
	fresh := !r.lastScanned.IsZero() && time.Since(r.lastScanned) < repoScanTTL
	r.mu.RUnlock()
	if fresh {
		return nil
	}
	return r.scanLocked()
}

// ScanForce always rewalks regardless of cache state.
func (r *repoIndex) ScanForce() error { return r.scanLocked() }

// scanLocked walks each backup root looking for .duplicacy/preferences files.
// Cap depth at 3 so we cover both "root IS a repo" and "root contains a few repos"
// without descending into the file tree we're meant to back up.
func (r *repoIndex) scanLocked() error {
	const maxDepth = 3

	found := map[string]*Repo{}

	for _, root := range r.roots {
		rootClean := filepath.Clean(root)
		err := filepath.WalkDir(rootClean, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				log.Warn().Err(walkErr).Str("path", path).Msg("walk error (skipping)")
				return nil
			}
			depth := walkDepth(rootClean, path)
			if depth > maxDepth {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.IsDir() || d.Name() != ".duplicacy" {
				return nil
			}
			repoRoot := filepath.Dir(path)
			repo, err := loadRepo(repoRoot)
			if err != nil {
				log.Warn().Err(err).Str("repo", repoRoot).Msg("failed to load repo (skipping)")
				return filepath.SkipDir
			}
			found[repo.ID] = repo
			return filepath.SkipDir // do not descend into the chunk store
		})
		if err != nil {
			return fmt.Errorf("walk %s: %w", rootClean, err)
		}
	}

	r.mu.Lock()
	r.repos = found
	r.lastScanned = time.Now()
	r.mu.Unlock()

	log.Info().Int("count", len(found)).Msg("repo scan complete")
	return nil
}

func walkDepth(root, path string) int {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." {
		return 0
	}
	return len(filepath.SplitList(rel)) // not perfect on win; we're linux-only
}

func loadRepo(repoRoot string) (*Repo, error) {
	prefsPath := filepath.Join(repoRoot, ".duplicacy", "preferences")
	data, err := os.ReadFile(prefsPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", prefsPath, err)
	}
	var raws []rawPreference
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, fmt.Errorf("parse %s: %w", prefsPath, err)
	}
	if len(raws) == 0 {
		return nil, fmt.Errorf("preferences in %s contained no entries", repoRoot)
	}

	storages := make([]Storage, 0, len(raws))
	for _, p := range raws {
		keys := make([]string, 0, len(p.Keys))
		for k := range p.Keys {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		storages = append(storages, Storage{
			Name:           p.Name,
			URL:            p.StorageURL,
			Encrypted:      p.Encrypted,
			NoBackup:       p.BackupProhibited,
			NoRestore:      p.RestoreProhibited,
			FiltersFile:    p.FiltersFile,
			ConfiguredKeys: keys,
		})
	}

	id := repoIDFromPath(repoRoot)
	hasFilters := fileExists(filepath.Join(repoRoot, ".duplicacy", "filters"))

	return &Repo{
		ID:            id,
		Path:          repoRoot,
		SnapshotID:    raws[0].SnapshotID,
		Storages:      storages,
		HasFilters:    hasFilters,
		LastScannedAt: time.Now().UTC(),
	}, nil
}

func repoIDFromPath(p string) string {
	sum := sha1.Sum([]byte(filepath.Clean(p)))
	return hex.EncodeToString(sum[:])[:12]
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

// list returns the cached repos sorted by path.
func (r *repoIndex) list() []*Repo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Repo, 0, len(r.repos))
	for _, rp := range r.repos {
		out = append(out, rp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

func (r *repoIndex) get(id string) (*Repo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rp, ok := r.repos[id]
	return rp, ok
}

// --- HTTP handlers (override the placeholders in app.go) ---

func (a *app) handleListRepos(c *gin.Context) {
	if err := a.repos.scan(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"repos": a.repos.list()})
}

func (a *app) handleGetPreferences(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	prefsPath := filepath.Join(repo.Path, ".duplicacy", "preferences")
	data, err := os.ReadFile(prefsPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var raws []rawPreference
	if err := json.Unmarshal(data, &raws); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Redact key values; expose names only.
	for i := range raws {
		redacted := make(map[string]string, len(raws[i].Keys))
		for k := range raws[i].Keys {
			redacted[k] = "<configured>"
		}
		raws[i].Keys = redacted
	}
	c.JSON(http.StatusOK, gin.H{"preferences": raws})
}
