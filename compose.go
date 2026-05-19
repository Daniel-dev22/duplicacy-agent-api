package main

import (
	"cmp"
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

// ComposeProject is one docker-compose project root discovered under a
// COMPOSE_SCAN_ROOTS entry. WorkingDir is the absolute path of the directory
// containing the compose file — same path the host sees because the agent
// mounts compose roots same-path host:container.
type ComposeProject struct {
	Name        string `json:"name"`
	WorkingDir  string `json:"working_dir"`
	ComposeFile string `json:"compose_file"`
}

// composeIndex caches the result of the most recent scan. The handler refreshes
// before serving — the walk is depth-bounded and read-only, so it's cheap.
// Cached so the index can later be reused (e.g. by /repos enrichment) without
// re-walking.
type composeIndex struct {
	roots []string

	mu        sync.RWMutex
	projects  []ComposeProject
	scannedAt time.Time
}

func newComposeIndex(roots []string) *composeIndex {
	return &composeIndex{roots: roots}
}

// composeFileNames is the set of filenames that mark a directory as a compose
// project root. Compose v2+ accepts compose.yaml; legacy and tooling-generated
// stacks use docker-compose.yml.
var composeFileNames = []string{
	"docker-compose.yml",
	"docker-compose.yaml",
	"compose.yml",
	"compose.yaml",
}

// scan walks each compose root with the same depth cap as repoIndex.scan (3)
// so behaviour is consistent and the worst-case fan-out is bounded. Hidden
// directories (.git, .duplicacy, etc.) are pruned to keep the walk tight.
func (ci *composeIndex) scan() {
	const maxDepth = 3

	var found []ComposeProject
	seen := map[string]struct{}{}

	for _, root := range ci.roots {
		rootClean := filepath.Clean(root)
		err := filepath.WalkDir(rootClean, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				slog.Warn("compose walk error (skipping)", "error", walkErr, "path", path)
				return nil
			}
			if !d.IsDir() {
				return nil
			}
			depth := walkDepth(rootClean, path)
			if depth > maxDepth {
				return filepath.SkipDir
			}
			// Prune hidden dirs (".git", ".duplicacy", ".cache", etc.). Keep the
			// root itself even if its name happens to start with a dot.
			if depth > 0 && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			if hit := matchComposeFile(path); hit != "" {
				if _, dup := seen[path]; !dup {
					seen[path] = struct{}{}
					found = append(found, ComposeProject{
						Name:        filepath.Base(path),
						WorkingDir:  path,
						ComposeFile: hit,
					})
				}
				// A compose project root can't itself be nested inside another
				// compose project root in any meaningful sense for our use case
				// (repo path picker), so stop descending.
				return filepath.SkipDir
			}
			return nil
		})
		if err != nil {
			slog.Warn("compose root walk failed", "error", err, "root", rootClean)
		}
	}

	slices.SortFunc(found, func(a, b ComposeProject) int { return cmp.Compare(a.WorkingDir, b.WorkingDir) })

	ci.mu.Lock()
	ci.projects = found
	ci.scannedAt = time.Now().UTC()
	ci.mu.Unlock()

	slog.Info("compose scan complete", "count", len(found))
}

// matchComposeFile returns the matching filename if the directory contains one
// of the recognised compose files, else "". Stat is per-candidate so we touch
// at most len(composeFileNames) inodes per directory.
func matchComposeFile(dir string) string {
	for _, name := range composeFileNames {
		st, err := os.Stat(filepath.Join(dir, name))
		if err == nil && !st.IsDir() {
			return name
		}
	}
	return ""
}

func (ci *composeIndex) list() []ComposeProject {
	ci.mu.RLock()
	defer ci.mu.RUnlock()
	out := make([]ComposeProject, len(ci.projects))
	copy(out, ci.projects)
	return out
}

// handleListComposeProjects rescans on each request — the walk is bounded
// (depth 3, read-only mounts) and the data lives outside the agent's control,
// so a stale cache would surprise the user.
func (a *app) handleListComposeProjects(c *gin.Context) {
	if a.compose == nil || len(a.cfg.ComposeScanRoots) == 0 {
		c.JSON(http.StatusOK, gin.H{"projects": []ComposeProject{}})
		return
	}
	a.compose.scan()
	c.JSON(http.StatusOK, gin.H{"projects": a.compose.list()})
}
