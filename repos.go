package main

import (
	"cmp"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
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
//
// Mounts are mirrored — host path == container path. HostPath is therefore
// always equal to Path, and SourceHostPath always equals SourcePath; both
// host_* JSON fields are kept for backward compatibility with the controller's
// Adopt flow (which used to match agent-discovered repos against centrally-
// stored host paths). With mirrored mounts that match is trivial because the
// paths are identical, but the field stays so older controllers don't break.
type Repo struct {
	ID            string    `json:"id"`                  // stable hash of Path
	Path          string    `json:"path"`                // absolute path to repo root (host == container)
	HostPath      string    `json:"host_path,omitempty"` // always == Path on mirrored mounts
	SnapshotID    string    `json:"snapshot_id"`         // duplicacy snapshot id (typically same across storages)
	Storages      []Storage `json:"storages"`
	HasFilters    bool      `json:"has_filters"`
	LastScannedAt time.Time `json:"last_scanned_at"`
	// SourcePath is the path the agent walks when building this repo's
	// filter-picker tree. Comes from preferences[0].repository, with any
	// legacy /backuproot/pathN prefix rewritten to the matching host path via
	// the LegacyBackuprootMap config. Empty when preferences has no usable
	// repository value (falls back to walking Repo.Path).
	SourcePath     string `json:"source_path,omitempty"`
	SourceHostPath string `json:"source_host_path,omitempty"` // always == SourcePath on mirrored mounts
	// LastBackupAt is this repo's most recent completed backup, from the durable
	// jobs table (NOT the 50-job fleet window). Set only when enriching the
	// /ws/fleet snapshot (fleet_ws.go); the filesystem scanner leaves it nil.
	// Absent (nil) for copy-only repos that never run a local backup. Drives the
	// controller's freshness badge so a node that backed up today is never
	// flagged "stale" merely because its backup scrolled out of the job window.
	LastBackupAt *time.Time `json:"last_backup_at,omitempty"`
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
	// legacyBackuprootMap rewrites stale on-disk preferences whose
	// `repository` field still references the old synthetic /backuproot/pathN
	// paths. Empty in fresh deploys; populated for hosts that have repos
	// migrated from duplicacy-web. Used only by rewriteLegacyBackuproot at
	// preferences-load time — no other code path consults it.
	legacyBackuprootMap map[string]string

	// excludePaths are operator-configured path prefixes the scan skips
	// entirely (from BACKUP_EXCLUDE_PATHS). The duplicacy-web cache layout is
	// excluded automatically via isDuplicacyWebCache regardless of this list.
	excludePaths []string

	mu          sync.RWMutex
	repos       map[string]*Repo // keyed by Repo.ID
	lastScanned time.Time

	// loggedStaleRepos tracks repoRoots already logged as missing
	// .duplicacy/preferences so we don't spam the agent log every 5 min on
	// the scan tick. INFO-once-per-session is enough — the operator
	// either fixes the stale directory or accepts it.
	loggedStaleRepos sync.Map // map[string]struct{}
}

const repoScanTTL = 30 * time.Second

// markStalePreferences logs once per repoRoot per agent lifetime when a
// .duplicacy dir is found without a preferences file. Quiets the every-5-min
// "failed to load repo (skipping)" WARN that was specifically witnessed for
// frigate's docker-managed .duplicacy/cache sibling on 2026-05-27.
func (r *repoIndex) markStalePreferences(repoRoot string) {
	if _, loaded := r.loggedStaleRepos.LoadOrStore(repoRoot, struct{}{}); loaded {
		return
	}
	slog.Info("repo has .duplicacy dir but no preferences (skipping — likely stale or third-party cache)",
		"repo", repoRoot)
}

func newRepoIndex(roots []string, binary string, legacyBackuprootMap map[string]string, excludePaths []string) *repoIndex {
	return &repoIndex{
		roots:               roots,
		binary:              binary,
		legacyBackuprootMap: legacyBackuprootMap,
		excludePaths:        excludePaths,
		repos:               map[string]*Repo{},
	}
}

// rewriteLegacyBackuproot turns "/backuproot/pathN/X" into the matching host
// path "/home/user/X" (etc) using the legacyMap. Longest-prefix wins so a
// more specific synthetic key overrides a less specific one. Returns the input
// unchanged when no key matches — the path is then either a real mirrored host
// path already or genuinely unknown (lstat at walk time decides).
func rewriteLegacyBackuproot(path string, legacyMap map[string]string) string {
	if len(legacyMap) == 0 || path == "" {
		return path
	}
	bestSynthetic, bestHost := "", ""
	for synth, host := range legacyMap {
		sc := filepath.Clean(synth)
		if path == sc || strings.HasPrefix(path, sc+string(filepath.Separator)) {
			if len(sc) > len(bestSynthetic) {
				bestSynthetic, bestHost = sc, filepath.Clean(host)
			}
		}
	}
	if bestSynthetic == "" {
		return path
	}
	return bestHost + strings.TrimPrefix(path, bestSynthetic)
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
// maxDepth=8 bounds the wasted walk in deep trees that contain no repo at all
// (most of /var/lib/rancher/k3s, big media trees, etc.); we SkipDir as soon as
// a .duplicacy is found so real repos at any reachable depth still register.
//
// Exclusions (the pruning the old comment claimed but never actually applied):
//   - excludeBasenames (shared with trees.go) — .git, node_modules, caches, …
//   - r.excludePaths — operator-configured prefixes (BACKUP_EXCLUDE_PATHS)
//   - isDuplicacyWebCache — Duplicacy Web Edition's working/cache tree
//     (…/cache/localhost/N/.duplicacy). Those carry a preferences file but are
//     NOT user-managed repos: duplicacy-web regenerates them continuously, so
//     surfacing them produced undeletable "phantom" repos (a delete just gets
//     recreated). We skip the whole subtree so they never appear.
func (r *repoIndex) scanLocked() error {
	const maxDepth = 8

	found := map[string]*Repo{}

	for _, root := range r.roots {
		rootClean := filepath.Clean(root)
		err := filepath.WalkDir(rootClean, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				slog.Warn("walk error (skipping)", "error", walkErr, "path", path)
				return nil
			}
			depth := walkDepth(rootClean, path)
			if depth > maxDepth {
				if d.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if !d.IsDir() {
				return nil
			}
			// Prune excluded directories before treating them — or anything
			// beneath them — as repos. This catches both the duplicacy-web
			// cache tree and operator-excluded prefixes (and the .duplicacy
			// inside them, since its path inherits the excluded ancestor).
			if r.shouldSkipDir(path, d.Name()) {
				return filepath.SkipDir
			}
			if d.Name() != ".duplicacy" {
				return nil
			}
			repoRoot := filepath.Dir(path)
			repo, err := r.loadRepo(repoRoot)
			if err != nil {
				// ENOENT on .duplicacy/preferences = the directory exists but
				// the marker file's gone (uninitialised partial duplicacy
				// dir, e.g. the operator removed the repo but left the
				// chunks/cache tree, OR a third-party app dropped a
				// .duplicacy/cache sibling without preferences). Log once at
				// INFO with the path; quiet thereafter — this is the noise
				// witnessed every 5 min for /mnt/storage/srv/containers/frigate
				// on 2026-05-27.
				if errors.Is(err, os.ErrNotExist) {
					r.markStalePreferences(repoRoot)
				} else {
					slog.Warn("failed to load repo (skipping)", "error", err, "repo", repoRoot)
				}
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

	slog.Info("repo scan complete", "count", len(found))
	return nil
}

// shouldSkipDir reports whether the walker must skip (not descend into, and not
// treat as a repo) the directory at path with basename name.
func (r *repoIndex) shouldSkipDir(path, name string) bool {
	if _, ok := excludeBasenames[name]; ok {
		return true
	}
	if isDuplicacyWebCache(path) {
		return true
	}
	return pathUnderAny(path, r.excludePaths)
}

// isDuplicacyWebCache reports whether path is inside Duplicacy Web Edition's
// working/cache tree (…/cache/localhost/…). Those dirs hold a
// .duplicacy/preferences that is app machinery, not a user-managed repo, and is
// regenerated continuously — surfacing it creates undeletable phantom repos.
func isDuplicacyWebCache(path string) bool {
	sep := string(filepath.Separator)
	marker := sep + "cache" + sep + "localhost"
	return strings.HasSuffix(path, marker) || strings.Contains(path, marker+sep)
}

// pathUnderAny reports whether path equals or is nested under any prefix.
func pathUnderAny(path string, prefixes []string) bool {
	for _, p := range prefixes {
		pc := filepath.Clean(p)
		if path == pc || strings.HasPrefix(path, pc+string(filepath.Separator)) {
			return true
		}
	}
	return false
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
		slices.Sort(keys)
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

	// Resolve the source path the filter-picker tree should walk. Mirrored
	// mounts mean preferences[0].repository is usable as-is — UNLESS the
	// preferences file is a legacy duplicacy-web migrated one that still
	// names the synthetic /backuproot/pathN umbrella. rewriteLegacyBackuproot
	// turns those substrings into the matching host path; on fresh deploys
	// the map is empty and the function is a passthrough. Empty / "/" falls
	// back to repoRoot so the picker degrades to the repo's own dir rather
	// than the whole filesystem.
	sourcePath := strings.TrimRight(raws[0].RepositoryPath, "/")
	if sourcePath == "" || sourcePath == "/" {
		sourcePath = repoRoot
	} else {
		sourcePath = rewriteLegacyBackuproot(filepath.Clean(sourcePath), r.legacyBackuprootMap)
	}

	return &Repo{
		ID:             id,
		Path:           repoRoot,
		HostPath:       repoRoot, // mirrored mounts — host == container
		SnapshotID:     raws[0].SnapshotID,
		Storages:       storages,
		HasFilters:     hasFilters,
		LastScannedAt:  time.Now().UTC(),
		SourcePath:     sourcePath,
		SourceHostPath: sourcePath, // mirrored mounts — host == container
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
	slices.SortFunc(out, func(a, b *Repo) int { return cmp.Compare(a.Path, b.Path) })
	return out
}

func (r *repoIndex) get(id string) (*Repo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rp, ok := r.repos[id]
	return rp, ok
}

// getBySnapshotID looks up a repo by its duplicacy snapshot_id (the value
// stored in .duplicacy/preferences.name and persisted to the controller's
// duplicacy_repos.repo_id). Used by callers — like the scheduler — that
// only carry the snapshot_id and don't know the agent's local 12-char
// path-hash ID. Linear scan over the indexed map; the map is small (a
// few dozen repos at most per agent) so O(n) is fine. Returns the first
// match if multiple repos share the same snapshot_id (a misconfiguration,
// but at least we pick one deterministically by path order).
func (r *repoIndex) getBySnapshotID(snapID string) (*Repo, bool) {
	if snapID == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var bestPath string
	var best *Repo
	for _, rp := range r.repos {
		if rp.SnapshotID != snapID {
			continue
		}
		if best == nil || rp.Path < bestPath {
			best = rp
			bestPath = rp.Path
		}
	}
	if best == nil {
		return nil, false
	}
	return best, true
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
