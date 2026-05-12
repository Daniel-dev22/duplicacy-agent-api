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
	"strings"
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
	ID         string `json:"id"`   // stable hash of Path
	Path       string `json:"path"` // absolute path to repo root (container-side)
	// HostPath is the host-side path that the operator originally typed when
	// initing the repo. Reverse-translated from Path via BACKUP_ROOT_MOUNTS
	// when a mapping exists (e.g. container /backuproot/path1 → host
	// /home/user). Empty when the agent has no mount mapping for the repo
	// or when running on a host where Path == HostPath. The Adopt tab uses
	// this to match agent-discovered repos against centrally-registered ones
	// (which always store the host path).
	HostPath      string    `json:"host_path,omitempty"`
	SnapshotID    string    `json:"snapshot_id"` // duplicacy snapshot id (typically same across storages)
	Storages      []Storage `json:"storages"`
	HasFilters    bool      `json:"has_filters"`
	LastScannedAt time.Time `json:"last_scanned_at"`
	// SourcePath is the container-side path the agent should walk when
	// building this repo's filter-picker tree. Derived from
	// preferences[0].repository: if it names a host path that maps via
	// HostToContainer, the container equivalent is stored here; if it's the
	// agent's synthetic umbrella (/backuproot) we keep that — walking it
	// surfaces every mounted backup root. Empty means walk Repo.Path as a
	// fallback (covers the case where preferences has no usable repository).
	SourcePath string `json:"source_path,omitempty"`
	// SourceHostPath is the host-side display version of SourcePath. Used as
	// the source_path field in /api/duplicacy/repo-trees so the UI can show
	// the operator-meaningful path even though the agent walked a synthetic
	// container path.
	SourceHostPath string `json:"source_host_path,omitempty"`
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
	roots           []string
	binary          string
	hostToContainer map[string]string // for reverse container→host lookup on Repo.HostPath

	mu          sync.RWMutex
	repos       map[string]*Repo // keyed by Repo.ID
	lastScanned time.Time
}

const repoScanTTL = 30 * time.Second

func newRepoIndex(roots []string, binary string, hostToContainer map[string]string) *repoIndex {
	return &repoIndex{
		roots:           roots,
		binary:          binary,
		hostToContainer: hostToContainer,
		repos:           map[string]*Repo{},
	}
}

// containerToHost is the reverse of init_handler.translateHostPath. Given a
// container path like "/backuproot/path1/foo" and the host:container mount
// map, returns the host path "/home/user/foo" if the longest container
// prefix matches; otherwise the original path and false.
func containerToHost(path string, hostToContainer map[string]string) (string, bool) {
	if len(hostToContainer) == 0 {
		return path, false
	}
	bestContainer, bestHost := "", ""
	for host, container := range hostToContainer {
		cc := filepath.Clean(container)
		if path == cc || strings.HasPrefix(path, cc+string(filepath.Separator)) {
			if len(cc) > len(bestContainer) {
				bestContainer, bestHost = cc, filepath.Clean(host)
			}
		}
	}
	if bestContainer == "" {
		return path, false
	}
	suffix := strings.TrimPrefix(path, bestContainer)
	return bestHost + suffix, true
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
			repo, err := r.loadRepo(repoRoot)
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
	// filepath.SplitList splits on the OS PATH-list separator (':' on Linux,
	// ';' on Windows) — it does NOT split a single path into components.
	// Previous use of SplitList always returned [rel], so this function
	// reported depth=1 for every entry and the maxDepth cap in scanLocked
	// was never enforced. Split on filepath.Separator instead.
	return strings.Count(rel, string(filepath.Separator)) + 1
}

func (r *repoIndex) loadRepo(repoRoot string) (*Repo, error) {
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
	// Reverse-translate container path → host path so the controller's
	// Adopt walkthrough can match agent-discovered repos against centrally-
	// registered ones (which always store the host path the operator
	// originally typed). Empty when no mount mapping covers this path.
	hostPath := ""
	if hp, ok := containerToHost(repoRoot, r.hostToContainer); ok {
		hostPath = hp
	}

	// Resolve the source path the filter-picker tree should walk. duplicacy's
	// preferences[0].repository is the duplicacy-side view of the source —
	// for repos init'd inside the duplicacy-web container it's the synthetic
	// "/backuproot" umbrella, for repos init'd via this agent's CLI it's
	// already a container path under /backuproot/pathN, and for ones init'd
	// directly on a host it may be a real host path like /home/user/photos.
	// We translate host → container when we can; otherwise we keep the raw
	// value (it's already container-accessible for the synthetic case since
	// docker creates /backuproot as the bind-mount parent). Empty / "/" /
	// missing falls back to repoRoot so the picker degrades to walking the
	// repo's own dir rather than the entire filesystem.
	sourcePath := strings.TrimRight(raws[0].RepositoryPath, "/")
	if sourcePath == "" {
		sourcePath = repoRoot
	} else if translated, ok := translateHostPath(filepath.Clean(sourcePath), r.hostToContainer); ok {
		sourcePath = translated
	}
	sourceHostPath := sourcePath
	if hp, ok := containerToHost(sourcePath, r.hostToContainer); ok {
		sourceHostPath = hp
	}

	return &Repo{
		ID:             id,
		Path:           repoRoot,
		HostPath:       hostPath,
		SnapshotID:     raws[0].SnapshotID,
		Storages:       storages,
		HasFilters:     hasFilters,
		LastScannedAt:  time.Now().UTC(),
		SourcePath:     sourcePath,
		SourceHostPath: sourceHostPath,
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
