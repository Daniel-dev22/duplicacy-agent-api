package main

// Periodic tree push from the duplicacy agent to the controller.
//
// Two payload shapes:
//   - repo trees   → one per loaded *Repo, rooted at the repo's source path
//                    so filter patterns built in the UI line up with duplicacy's
//                    own evaluation. POST /api/duplicacy/repo-trees.
//   - node trees   → one per backup root (the operator-meaningful paths like
//                    /home/user, /srv/containers, or /mnt on a NAS),
//                    so RepoPathPicker's adopt flow can browse the disk
//                    without depending on server_metrics' filesystem tree.
//                    POST /api/duplicacy/node-trees.
//
// Performance choices:
//   - Single-pass filepath.WalkDir per root (one getdents per dir, no extra
//     Lstat for the DirEntry's type).
//   - Basename-keyed exclude set, O(1) membership tests.
//   - Hard depth + per-dir file caps as early-exit; oversized dirs emit a
//     single "truncated" marker child rather than sampling.
//   - Symlinks are not followed (avoids cycles and double-walks of mounts).
//   - Per-directory mtime cache: on the next 5-min tick, dirs whose Lstat
//     mtime hasn't changed return their cached subtree without descending.
//     This is the cpu-restricted-pi safeguard — typical idle ticks become
//     ~one Lstat per dir from the previous walk, no recursion underneath.
//   - All paths in emitted TreeNodes are the host-visible paths (reverse-
//     translated from the container backup-root mount), so what the UI shows
//     matches what the operator sees in a shell.
//
// Wire protocol — manifest first, then only what the controller lacks:
//   1. POST <lane>/manifest with {key…, sha256} per tree. The controller
//      touches every row it already holds byte for byte and answers `need`.
//   2. POST <lane> with only the needed trees, gzip-encoded.
// Before this every tree went up every 5 minutes: ~48 MB per tick from one
// host (a 24 MB tree pushed as both a repo and a node tree) when one tree in
// thirteen had changed. The hash is over the exact bytes sent in step 2 — the
// controller stores the SHA-256 of what it receives, so any other encoding
// would mismatch and re-send every tree forever.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// -----------------------------------------------------------------------------
// Tuning
// -----------------------------------------------------------------------------

const (
	treeMaxDepth       = 5
	treeMaxFilesPerDir = 100
	treePushInterval   = 5 * time.Minute
	treePushTimeout    = 30 * time.Second
)

// excludeBasenames is the set of directory basenames the walker skips entirely.
// Matches the set the post-revert server_metrics task uses plus a handful of
// noise dirs we never want to surface in a filter picker.
var excludeBasenames = map[string]struct{}{
	"__pycache__":     {},
	"node_modules":    {},
	".git":            {},
	".cache":          {},
	".npm":            {},
	".venv":           {},
	"venv":            {},
	"logs":            {},
	"tmp":             {},
	".zfs":            {},
	"ix-applications": {},
}

// -----------------------------------------------------------------------------
// Wire shape — matches discovery's TreeNode so the existing TreeView renderer
// reads agent-pushed trees with no extra parsing.
// -----------------------------------------------------------------------------

type treeNode struct {
	Name     string      `json:"name"`
	Path     string      `json:"path"`
	Type     string      `json:"type"` // "directory" | "file" | "truncated"
	Children []*treeNode `json:"children,omitempty"`
	// FileCount is only set on "truncated" markers so the UI can show
	// "<N> files (too many to list)" without sampling.
	FileCount int `json:"file_count,omitempty"`
	// Size is the recursive subtree byte total for a directory node, filled in
	// from the dirSizeCache by annotateSizes at push time (NOT computed during
	// the structural walk). Omitted when the gatherer hasn't sized this dir yet.
	Size int64 `json:"size,omitempty"`
}

// -----------------------------------------------------------------------------
// Walker — long-lived; owns the mtime cache across ticks.
// -----------------------------------------------------------------------------

// cachedDir stores the previous walk's snapshot of a directory keyed by its
// container-side path. mtime is the dir's own Lstat ModTime at walk time;
// children is the materialised subtree (already host-translated). On the next
// tick, an unchanged mtime lets the walker reuse children without descending.
type cachedDir struct {
	mtime    time.Time
	children []*treeNode
}

type treeWalker struct {
	cfg   Config
	repos *repoIndex
	app   *app // for controlCenterClient + stop chan access

	mu    sync.Mutex
	cache map[string]*cachedDir // keyed by container-side path
}

func newTreeWalker(cfg Config, repos *repoIndex, a *app) *treeWalker {
	return &treeWalker{
		cfg:   cfg,
		repos: repos,
		app:   a,
		cache: map[string]*cachedDir{},
	}
}

// Start launches the periodic push goroutine. Returns immediately; the
// goroutine respects ctx and the app's stop channel.
func (w *treeWalker) Start(ctx context.Context) {
	go w.loop(ctx)
}

func (w *treeWalker) loop(ctx context.Context) {
	// First push: do it eagerly so the UI has data within ~seconds of start,
	// not at the end of the first 5-min window.
	w.tick(ctx)

	t := time.NewTicker(treePushInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.app.stop:
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

func (w *treeWalker) tick(ctx context.Context) {
	if err := w.pushRepoTrees(ctx); err != nil {
		slog.Warn("repo trees push failed", "error", err)
	}
	if err := w.pushNodeTrees(ctx); err != nil {
		slog.Warn("node trees push failed", "error", err)
	}
}

// -----------------------------------------------------------------------------
// Walk + cache
// -----------------------------------------------------------------------------

// walkRoot returns the tree rooted at root. nil if the root is unreadable.
func (w *treeWalker) walkRoot(root string) *treeNode {
	st, err := os.Lstat(root)
	if err != nil {
		slog.Warn("tree walk: root lstat failed", "error", err, "root", root)
		return nil
	}
	if !st.IsDir() {
		return nil
	}

	return &treeNode{
		Name:     filepath.Base(root),
		Path:     root,
		Type:     "directory",
		Children: w.walkDir(root, st.ModTime(), 0),
	}
}

// walkBackupRootsUmbrella builds a synthetic tree rooted at the agent's
// BackupRoots union. Used as a fallback when a repo's source_path can't be
// resolved to a real on-disk path — e.g. duplicacy-web migrated repos whose
// preferences say `repository=/backuproot` (the umbrella covering every bind
// mount in duplicacy-web's container). With mirrored mounts there is no
// /backuproot dir, so we emit each backup root as a child of a virtual root.
// The picker shows real files; the filter patterns the operator builds are
// absolute host paths under one of those roots.
func (w *treeWalker) walkBackupRootsUmbrella() *treeNode {
	if len(w.cfg.BackupRoots) == 0 {
		return nil
	}
	children := make([]*treeNode, 0, len(w.cfg.BackupRoots))
	for _, r := range w.cfg.BackupRoots {
		child := w.walkRoot(r)
		if child != nil {
			children = append(children, child)
		}
	}
	if len(children) == 0 {
		return nil
	}
	return &treeNode{
		Name:     "(all backup roots)",
		Path:     "/",
		Type:     "directory",
		Children: children,
	}
}

// walkDir returns the children of dir (one level), recursing depth-first up to
// treeMaxDepth. Uses the mtime cache to short-circuit unchanged subtrees.
// Mounts are mirrored so paths are emitted as-is (no host/container split).
func (w *treeWalker) walkDir(containerDir string, mtime time.Time, depth int) []*treeNode {
	if depth >= treeMaxDepth {
		return nil
	}

	// Cache short-circuit: if mtime unchanged since last walk, reuse children.
	w.mu.Lock()
	if c, ok := w.cache[containerDir]; ok && c.mtime.Equal(mtime) {
		out := c.children
		w.mu.Unlock()
		return out
	}
	w.mu.Unlock()

	entries, err := os.ReadDir(containerDir)
	if err != nil {
		// Common on permission-denied subtrees inside backup roots; warn once.
		return nil
	}

	dirs := make([]*treeNode, 0, len(entries))
	fileEntries := make([]fs.DirEntry, 0, len(entries))

	for _, e := range entries {
		name := e.Name()
		if _, skip := excludeBasenames[name]; skip {
			continue
		}
		typ := e.Type()
		// Skip symlinks entirely — duplicacy itself doesn't follow them by
		// default, and following them risks cycles and double-walking
		// bind-mounted subtrees.
		if typ&fs.ModeSymlink != 0 {
			continue
		}

		childPath := filepath.Join(containerDir, name)

		// Skip the Duplicacy Web cache tree and operator-excluded prefixes —
		// the same rules the repo scanner applies (repos.go), so the tree push
		// never surfaces duplicacy-web app machinery either.
		if isDuplicacyWebCache(childPath) || pathUnderAny(childPath, w.cfg.BackupExcludePaths) {
			continue
		}

		if e.IsDir() {
			// Lstat for mtime — one extra syscall but lets the cache work.
			info, err := e.Info()
			if err != nil {
				continue
			}
			dirs = append(dirs, &treeNode{
				Name:     name,
				Path:     childPath,
				Type:     "directory",
				Children: w.walkDir(childPath, info.ModTime(), depth+1),
			})
		} else if typ.IsRegular() {
			// Defer the size stat until after the file cap is known so a
			// truncated directory stats nothing.
			fileEntries = append(fileEntries, e)
		}
		// Anything else (devices, sockets, pipes) is silently dropped.
	}

	// File cap with truncation marker. The marker counts ALL filtered files
	// so the UI can show "N files (too many to list)" rather than imply
	// sampling. Subdirectory walks are unaffected.
	var children []*treeNode
	if len(fileEntries) > treeMaxFilesPerDir {
		children = make([]*treeNode, 0, len(dirs)+1)
		children = append(children, dirs...)
		children = append(children, &treeNode{
			Name:      fmt.Sprintf("%d files (too many to list)", len(fileEntries)),
			Path:      containerDir,
			Type:      "truncated",
			FileCount: len(fileEntries),
		})
	} else {
		children = make([]*treeNode, 0, len(dirs)+len(fileEntries))
		children = append(children, dirs...)
		// Per-file size: one Lstat per file, ONLY in the non-truncated branch,
		// so this adds at most treeMaxFilesPerDir stats per directory and is
		// memoised by the mtime cache exactly like the rest of the subtree.
		for _, fe := range fileEntries {
			var size int64
			if info, err := fe.Info(); err == nil {
				size = info.Size()
			}
			children = append(children, &treeNode{
				Name: fe.Name(),
				Path: filepath.Join(containerDir, fe.Name()),
				Type: "file",
				Size: size,
			})
		}
	}

	w.mu.Lock()
	w.cache[containerDir] = &cachedDir{mtime: mtime, children: children}
	w.mu.Unlock()

	return children
}

// annotateSizes walks an in-memory tree (depth-capped, small) and stamps each
// directory node's Size from the dirSizeCache. Decoupled from the structural
// walk so it stays correct even when walkDir serves a node from its mtime cache:
// sizes are applied fresh on every push, picking up whatever the background
// gatherer has filled in since. A pure map lookup per node — no I/O. Directories
// the gatherer hasn't sized yet keep Size==0 (omitted by json:omitempty).
func (w *treeWalker) annotateSizes(n *treeNode) {
	if n == nil || w.app.sizes == nil {
		return
	}
	if n.Type == "directory" {
		if b, ok := w.app.sizes.Size(n.Path); ok {
			n.Size = b
		}
	}
	for _, c := range n.Children {
		w.annotateSizes(c)
	}
}

// toHost is intentionally removed. Mirrored mounts mean container path
// equals host path; the walker emits paths as-is.

// -----------------------------------------------------------------------------
// Push — repo trees (one per loaded repo, rooted at repo source path)
// -----------------------------------------------------------------------------

type repoTreeOut struct {
	Node       string          `json:"node"`
	Site       string          `json:"site"`
	RepoID     string          `json:"repo_id"`
	SourcePath string          `json:"source_path"`
	SHA256     string          `json:"sha256,omitempty"`
	Tree       json.RawMessage `json:"tree,omitempty"`
}

func (w *treeWalker) pushRepoTrees(ctx context.Context) error {
	repos := w.repos.list()
	if len(repos) == 0 {
		return nil
	}

	out := make([]repoTreeOut, 0, len(repos))
	for _, r := range repos {
		// Walk the duplicacy source (preferences[0].repository), not the
		// repo's cache dir. duplicacy-web migrated repos either contain a
		// real host subpath under /backuproot (rewritten in loadRepo via
		// LegacyBackuprootMap) OR the bare umbrella `/backuproot` which
		// covered every bind mount in duplicacy-web's container. With
		// mirrored mounts the umbrella doesn't exist as a real dir, so
		// fall back to walking the agent's BackupRoots as a synthetic
		// tree — the picker still surfaces real files.
		source := r.SourcePath
		if source == "" {
			source = r.Path
		}
		var root *treeNode
		if source == "/backuproot" {
			root = w.walkBackupRootsUmbrella()
			source = "(all backup roots)"
		} else {
			root = w.walkRoot(source)
		}
		if root == nil {
			continue
		}
		w.annotateSizes(root)
		raw, err := json.Marshal(root)
		if err != nil {
			return fmt.Errorf("encode repo tree %s: %w", source, err)
		}
		// Repo identity on the central side is the snapshot id (HostPath-derived
		// resource), not the agent's short hash — match what the controller's
		// duplicacy_repos table stores in repo_id.
		repoID := r.SnapshotID
		if repoID == "" {
			repoID = r.ID
		}
		out = append(out, repoTreeOut{
			Node:       w.cfg.NodeName,
			Site:       w.cfg.SiteID,
			RepoID:     repoID,
			SourcePath: source,
			SHA256:     sha256Hex(raw),
			Tree:       raw,
		})
	}
	return pushTreeLane(ctx, w.post, "/api/duplicacy/repo-trees", out,
		func(t repoTreeOut) string { return t.RepoID },
		func(t repoTreeOut) repoTreeOut { t.Tree = nil; return t })
}

// -----------------------------------------------------------------------------
// Push — node trees (one per backup root)
// -----------------------------------------------------------------------------

type nodeTreeOut struct {
	Node     string          `json:"node"`
	Site     string          `json:"site"`
	RootPath string          `json:"root_path"`
	SHA256   string          `json:"sha256,omitempty"`
	Tree     json.RawMessage `json:"tree,omitempty"`
}

func (w *treeWalker) pushNodeTrees(ctx context.Context) error {
	if len(w.cfg.BackupRoots) == 0 {
		return nil
	}

	out := make([]nodeTreeOut, 0, len(w.cfg.BackupRoots))
	for _, hostRoot := range w.cfg.BackupRoots {
		root := w.walkRoot(hostRoot)
		if root == nil {
			continue
		}
		w.annotateSizes(root)
		raw, err := json.Marshal(root)
		if err != nil {
			return fmt.Errorf("encode node tree %s: %w", hostRoot, err)
		}
		out = append(out, nodeTreeOut{
			Node:     w.cfg.NodeName,
			Site:     w.cfg.SiteID,
			RootPath: hostRoot,
			SHA256:   sha256Hex(raw),
			Tree:     raw,
		})
	}
	return pushTreeLane(ctx, w.post, "/api/duplicacy/node-trees", out,
		func(t nodeTreeOut) string { return t.RootPath },
		func(t nodeTreeOut) nodeTreeOut { t.Tree = nil; return t })
}

// -----------------------------------------------------------------------------
// Manifest-first lane push
// -----------------------------------------------------------------------------

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// postFunc is treeWalker.post's shape, so the lane logic is testable against
// a stub controller.
type postFunc func(ctx context.Context, path string, body any, out any, gz bool) error

// pushTreeLane sends the manifest (every item, hashes only), then only the
// items the controller answered it needs, trees included, gzip-encoded.
//
// Every item stays in the manifest even when unchanged: that call is what
// moves the row's last_seen_at, and the controller's reaper deletes a tree
// nobody has reported for 30 minutes.
//
// manifestOf returns an item's manifest form: the same row with its tree
// dropped (omitempty), so the manifest is a few hundred bytes.
func pushTreeLane[T any](ctx context.Context, post postFunc, base string, items []T,
	key func(T) string, manifestOf func(T) T) error {
	if len(items) == 0 {
		return nil
	}
	manifest := make([]T, len(items))
	for i, it := range items {
		manifest[i] = manifestOf(it)
	}
	var reply struct {
		Need []json.RawMessage `json:"need"`
	}
	if err := post(ctx, base+"/manifest", map[string]any{"trees": manifest}, &reply, false); err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	if len(reply.Need) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(reply.Need))
	for _, n := range reply.Need {
		var e T
		if err := json.Unmarshal(n, &e); err != nil {
			return fmt.Errorf("manifest reply: %w", err)
		}
		want[key(e)] = struct{}{}
	}
	send := make([]T, 0, len(want))
	for _, it := range items {
		if _, ok := want[key(it)]; ok {
			send = append(send, it)
		}
	}
	if len(send) == 0 {
		return nil
	}
	return post(ctx, base, map[string]any{"trees": send}, nil, true)
}

// -----------------------------------------------------------------------------
// HTTP push
// -----------------------------------------------------------------------------

// post sends body as JSON (gzip-encoded when gz) and, when out is non-nil,
// decodes the JSON reply into it.
func (w *treeWalker) post(ctx context.Context, path string, body any, out any, gz bool) error {
	buf := &bytes.Buffer{}
	var enc io.Writer = buf
	var zw *gzip.Writer
	if gz {
		zw = gzip.NewWriter(buf)
		enc = zw
	}
	if err := json.NewEncoder(enc).Encode(body); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if zw != nil {
		if err := zw.Close(); err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
	}
	url := strings.TrimRight(w.cfg.ControlCenterURL, "/") + path

	ctx, cancel := context.WithTimeout(ctx, treePushTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if gz {
		req.Header.Set("Content-Encoding", "gzip")
	}

	resp, err := w.app.controlCenterClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, path)
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(out); err != nil {
		return fmt.Errorf("decode reply from %s: %w", path, err)
	}
	return nil
}
