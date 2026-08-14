package main

// Per-directory recursive size gathering for the filter file picker.
//
// Design (fully decoupled from the 5-min tree push in trees.go):
//
//   - sizeGatherer is its own goroutine — a SCHEDULER THAT IDLES, not a
//     perpetual walker. Each cycle it walks the due backup roots, sleeps until
//     the next root is due, and repeats. If a pass finishes in 20 min and the
//     cadence is 6h, it idles ~5h40m before the next pass.
//   - The tree push only ever READS this cache (trees.go annotateSizes): it
//     attaches a `size` to directory nodes that already have a value and omits
//     it otherwise. The push never computes, enqueues, or blocks on the gatherer.
//
// Cadence (the "4×/day, large 1×/day" rule):
//   - A directory refreshes every TreeSizeSmallRefresh (~6h → 4×/day).
//   - A directory whose subtree file count exceeds TreeSizeLargeFileThreshold,
//     or whose last walk took longer than TreeSizeSlowWalk, drops to
//     TreeSizeLargeRefresh (~24h → 1×/day).
//
// Efficiency (cost of a pass scales with what's actually due, not the tree):
//   - Bottom-up single pass per root: one traversal accumulates every dir's
//     recursive Bytes/FileCount as the recursion unwinds.
//   - Reuse cached totals for subtrees that aren't due — a 6h parent pass folds
//     in a giant 24h-cadence child's cached total instead of re-descending it.
//   - Allocation-free per-file stat: os.Open + ReadDir + unix.Fstatat into a
//     stack Stat_t; file paths are never materialised, only directory paths
//     (the cache keys). See sizeOfRegularFile.
//
// Persistence: the cache is written atomically to CONFIG_DIR/dir_sizes.json so
// a restart resumes the cadence instead of re-walking every root from scratch.
// First run: an empty cache means the push simply reports no sizes until the
// gatherer fills them — never a synchronous walk storm at startup.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// dirSize is one cached directory total. Keyed by absolute (container == host)
// path. Bytes is the sum of regular-file apparent sizes in the subtree with the
// same exclusions the tree walk applies, so it approximates duplicacy's backup
// payload rather than raw on-disk (block-rounded) usage.
type dirSize struct {
	Bytes      int64     `json:"bytes"`
	FileCount  int64     `json:"file_count"`
	ComputedAt time.Time `json:"computed_at"`
	LastWalkMS int64     `json:"last_walk_ms"` // wall-clock of the last recompute → slow-walk demotion
}

// -----------------------------------------------------------------------------
// dirSizeCache — concurrent map + atomic disk persistence
// -----------------------------------------------------------------------------

type dirSizeCache struct {
	path string

	mu sync.RWMutex
	m  map[string]dirSize

	persistMu sync.Mutex // serialises the tmp+rename pair, like repoMappingStore
}

func newDirSizeCache(configDir string) *dirSizeCache {
	return &dirSizeCache{
		path: filepath.Join(configDir, "dir_sizes.json"),
		m:    map[string]dirSize{},
	}
}

// Size returns the cached subtree bytes for a path. The bool distinguishes
// "not computed yet" from a genuine zero so the push can omit the field.
func (c *dirSizeCache) Size(path string) (int64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[path]
	return e.Bytes, ok
}

func (c *dirSizeCache) get(path string) (dirSize, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.m[path]
	return e, ok
}

func (c *dirSizeCache) put(path string, e dirSize) {
	c.mu.Lock()
	c.m[path] = e
	c.mu.Unlock()
}

// prune removes cache entries at or under any of the given path prefixes.
// Returns the count removed. Used to drop size-excluded mounts left over in the
// cache from before they were excluded (e.g. /var/lib/rancher/k3s).
func (c *dirSizeCache) prune(excludes []string) int {
	if len(excludes) == 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for p := range c.m {
		if pathUnderAny(p, excludes) {
			delete(c.m, p)
			n++
		}
	}
	return n
}

// load reads the on-disk cache. Missing/empty/corrupt file is non-fatal — we
// just start cold and the gatherer refills it (a corrupt cache is never worse
// than no cache here, unlike repos.json which is authoritative).
func (c *dirSizeCache) load() {
	data, err := os.ReadFile(c.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			slog.Warn("dir size cache unreadable, starting cold", "error", err, "path", c.path)
		}
		return
	}
	if len(data) == 0 {
		return
	}
	var m map[string]dirSize
	if err := json.Unmarshal(data, &m); err != nil {
		slog.Warn("dir size cache corrupt, starting cold", "error", err, "path", c.path)
		return
	}
	c.mu.Lock()
	c.m = m
	c.mu.Unlock()
}

// save writes the cache atomically (.tmp + rename).
func (c *dirSizeCache) save() error {
	c.persistMu.Lock()
	defer c.persistMu.Unlock()

	c.mu.RLock()
	data, err := json.Marshal(c.m)
	c.mu.RUnlock()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.path), 0700); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(c.path), err)
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", c.path, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// sizeGatherer — the idle-between-cycles scheduler
// -----------------------------------------------------------------------------

type sizeGatherer struct {
	cfg   Config
	cache *dirSizeCache
	app   *app // for the stop channel

	// lastAttempt records when each root was last walked (success OR timeout),
	// so a root that can't finish within TreeSizeWalkTimeout still backs off to
	// its cadence between passes instead of hot-looping. In-memory only — on
	// restart we simply re-attempt, which is correct.
	lastAttempt map[string]time.Time
	firstPass   bool

	// guard classifies foreign mounts (pseudo filesystems, mounted disk images,
	// orphaned dm targets) so the walk never descends into one. Rebuilt at the
	// start of every pass because the mount table changes between passes — that is
	// the whole point, an image build can leak a mount at any time. Nil is safe and
	// means "no mount is skipped"; see runPass for why that degradation is correct.
	guard *mountGuard
	// lastGuardSig is the previous pass's skip set, so the summary logs on change
	// rather than every pass.
	lastGuardSig string
}

func newSizeGatherer(cfg Config, cache *dirSizeCache, a *app) *sizeGatherer {
	return &sizeGatherer{
		cfg:         cfg,
		cache:       cache,
		app:         a,
		lastAttempt: map[string]time.Time{},
		firstPass:   true,
	}
}

// roots returns the deduplicated set of paths to size. BackupRoots cover both
// the node-tree picker and (via the mirrored mount layout) every repo whose
// source lives under a backup root. Sizes are keyed by absolute path, so a
// repo's sub-directories populated during a backup-root walk are readable by
// the repo-tree push regardless of their depth.
func (g *sizeGatherer) roots() []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(g.cfg.BackupRoots))
	for _, r := range g.cfg.BackupRoots {
		r = filepath.Clean(r)
		if _, ok := seen[r]; ok {
			continue
		}
		if pathUnderAny(r, g.excludePaths()) {
			continue // size-excluded (env var, or a controller-pushed filter rule)
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	return out
}

// excludePaths is the single exclusion list the gatherer honours: the two env-var
// lists plus the absolute exclude rules from the controller-pushed filter sets.
//
// Merging them here rather than at each call site is the point. There were three
// separate exclusion checks in walk() and two more in roots()/Start(), all reading
// only the env vars — which is how a rule the operator set in the UI ended up
// applying to backups but not to this walker.
func (g *sizeGatherer) excludePaths() []string {
	env := len(g.cfg.BackupExcludePaths) + len(g.cfg.TreeSizeExcludePaths)
	var pushed []string
	if g.app != nil {
		pushed = g.app.filters.AbsoluteExcludePrefixes()
	}
	out := make([]string, 0, env+len(pushed))
	out = append(out, g.cfg.BackupExcludePaths...)
	out = append(out, g.cfg.TreeSizeExcludePaths...)
	return append(out, pushed...)
}

// isDeadMountErr reports whether err means the backing store is gone rather than
// the file being absent — a detached loop device, a dead NFS/CIFS server. These are
// leaks worth naming: ENOENT is routine, EIO is not.
func isDeadMountErr(err error) bool {
	return errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.ESTALE) ||
		errors.Is(err, unix.ENOTCONN) ||
		errors.Is(err, unix.ENODEV)
}

// cadence returns the refresh interval for a cached entry: the daily interval
// when the subtree is large (by file count) or was slow to walk, else 4×/day.
func (g *sizeGatherer) cadence(e dirSize) time.Duration {
	if e.FileCount > g.cfg.TreeSizeLargeFileThreshold ||
		time.Duration(e.LastWalkMS)*time.Millisecond > g.cfg.TreeSizeSlowWalk {
		return g.cfg.TreeSizeLargeRefresh
	}
	return g.cfg.TreeSizeSmallRefresh
}

// due reports whether a cached directory should be recomputed now.
func (g *sizeGatherer) due(path string, now time.Time) bool {
	e, ok := g.cache.get(path)
	if !ok {
		return true // never computed
	}
	return now.Sub(e.ComputedAt) >= g.cadence(e)
}

func (g *sizeGatherer) Start(ctx context.Context) {
	if !g.cfg.TreeSizeEnabled {
		slog.Info("dir size gatherer disabled")
		return
	}
	g.cache.load()
	// Drop any cached entries for now-excluded paths (e.g. k3s sized before the
	// exclude was added) so the picker stops showing their stale sizes.
	//
	// This now also covers the controller-pushed rules, which makes it self-healing:
	// the 142,066 phantom keys under the leaked image chroot on ng-pi were already
	// covered by an org exclude, so a boot after this change drops them without an
	// operator deleting dir_sizes.json by hand.
	excludes := g.excludePaths()
	if pruned := g.cache.prune(excludes); pruned > 0 {
		slog.Info("dir size cache: pruned size-excluded entries", "count", pruned, "excludes", excludes)
		if err := g.cache.save(); err != nil {
			slog.Warn("dir size cache save after prune failed", "error", err)
		}
	}
	go g.loop(ctx)
}

func (g *sizeGatherer) loop(ctx context.Context) {
	slog.Info("dir size gatherer started",
		"roots", g.roots(),
		"exclude_paths", g.cfg.TreeSizeExcludePaths,
		"small_refresh", g.cfg.TreeSizeSmallRefresh,
		"large_refresh", g.cfg.TreeSizeLargeRefresh,
		"large_file_threshold", g.cfg.TreeSizeLargeFileThreshold)

	for {
		g.runPass(ctx)
		select {
		case <-ctx.Done():
			return
		case <-g.app.stop:
			return
		case <-time.After(g.untilNextDue(time.Now())):
		}
	}
}

// untilNextDue returns how long to sleep before the earliest root is due again.
// Clamped so we never hot-loop (min 1m) nor oversleep past the small cadence.
func (g *sizeGatherer) untilNextDue(now time.Time) time.Duration {
	const minSleep = time.Minute
	next := now.Add(g.cfg.TreeSizeSmallRefresh)
	for _, root := range g.roots() {
		at, attempted := g.lastAttempt[root]
		if !attempted {
			return minSleep // due now
		}
		cad := g.cfg.TreeSizeSmallRefresh
		if e, ok := g.cache.get(root); ok {
			cad = g.cadence(e)
		}
		if d := at.Add(cad); d.Before(next) {
			next = d
		}
	}
	d := time.Until(next)
	if d < minSleep {
		return minSleep
	}
	return d
}

// runPass walks each due root once, bottom-up, refreshing due directories and
// reusing cached totals for not-due subtrees. Persists after each root.
func (g *sizeGatherer) runPass(ctx context.Context) {
	now := time.Now()

	// Rebuild the mount view for this pass. A failure here must NOT stop the pass:
	// sizes are advisory (the tree push omits what it lacks), so degrading to
	// "skip nothing" keeps the picker populated. It is logged at WARN because the
	// walk is then unguarded, which is the state that produced the EXT4 errors.
	guard, err := newMountGuard()
	if err != nil {
		slog.Warn("dir size gatherer: mount guard unavailable, walking unguarded", "error", err)
	}
	g.guard = guard
	// Log on the first pass and on every CHANGE thereafter, never every pass: a new
	// leaked image mount must be visible in the agent log the night it appears, but
	// a steady state must not reprint hourly.
	if sig := guard.signature(); g.firstPass || sig != g.lastGuardSig {
		guard.logSummary()
		g.lastGuardSig = sig
	}

	var walked int
	for _, root := range g.roots() {
		select {
		case <-ctx.Done():
			return
		case <-g.app.stop:
			return
		default:
		}
		if !g.due(root, now) {
			continue
		}
		walked++
		wctx, cancel := context.WithTimeout(ctx, g.cfg.TreeSizeWalkTimeout)
		_, _, err := g.walk(wctx, root)
		cancel()
		g.lastAttempt[root] = time.Now()
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("dir size walk failed", "root", root, "error", err)
		} else if errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("dir size walk hit timeout (will resume next cadence)", "root", root, "timeout", g.cfg.TreeSizeWalkTimeout)
		}
		if err := g.cache.save(); err != nil {
			slog.Warn("dir size cache save failed", "error", err)
		}
		// Honour shutdown promptly if the app-level ctx was cancelled.
		if ctx.Err() != nil {
			return
		}
	}
	if g.firstPass {
		root, files := g.summary()
		slog.Info("dir size gatherer first pass complete", "roots_walked", walked, "dirs_cached", root, "files_counted", files)
		g.firstPass = false
	}
}

// summary returns (dirs cached, total files counted) for the first-pass log.
func (g *sizeGatherer) summary() (int, int64) {
	g.cache.mu.RLock()
	defer g.cache.mu.RUnlock()
	var files int64
	for _, e := range g.cache.m {
		files += e.FileCount
	}
	return len(g.cache.m), files
}

// walk returns the recursive (bytes, fileCount) for dir, refreshing the cache
// for every directory it recomputes. A not-due cached directory is reused
// without descending. ctx cancellation/timeout propagates up so in-progress
// parents are NOT cached partial — already-cached children remain valid and the
// next pass resumes by reusing them.
func (g *sizeGatherer) walk(ctx context.Context, dir string) (int64, int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	now := time.Now()
	if e, ok := g.cache.get(dir); ok && now.Sub(e.ComputedAt) < g.cadence(e) {
		return e.Bytes, e.FileCount, nil // reuse, no descent
	}

	t0 := time.Now()
	f, err := os.Open(dir)
	if err != nil {
		// Unreadable (permissions, vanished). Record an empty entry so the
		// scheduler backs off to cadence rather than retrying every minute.
		//
		// EIO/ESTALE/ENOTCONN mean the BACKING STORE is gone, not that a file was
		// removed — a detached loop device, a dead NFS server. That is a leak worth
		// naming: it is how the orphaned kpartx maps announced themselves, and it
		// previously surfaced only as a kernel EXT4 error with no agent-side trace.
		if isDeadMountErr(err) {
			slog.Warn("dir size: unreadable mount, skipping subtree",
				"dir", dir, "error", err)
		}
		g.cache.put(dir, dirSize{ComputedAt: now, LastWalkMS: time.Since(t0).Milliseconds()})
		return 0, 0, nil
	}
	entries, readErr := f.ReadDir(-1)
	fd := int(f.Fd())

	var bytes, count int64
	for _, e := range entries {
		name := e.Name()
		if _, skip := excludeBasenames[name]; skip {
			continue
		}
		if e.Type()&os.ModeSymlink != 0 {
			continue // never follow symlinks (cycles / double-walks), like the tree walk
		}
		child := filepath.Join(dir, name)
		if isDuplicacyWebCache(child) || pathUnderAny(child, g.excludePaths()) {
			continue
		}
		if e.IsDir() {
			// A foreign mount: pseudo filesystem, mounted disk image, or an
			// orphaned dm target whose backing device is gone. Never descend.
			if reason, skip := g.guard.SkipReason(child); skip {
				slog.Debug("dir size: skipping mount", "dir", child, "reason", reason)
				continue
			}
			cb, cc, cerr := g.walk(ctx, child)
			if cerr != nil {
				f.Close()
				return 0, 0, cerr // ctx cancelled — don't cache this parent
			}
			bytes += cb
			count += cc
		} else if sz, ok := sizeOfRegularFile(fd, name); ok {
			bytes += sz
			count++
		}
	}
	f.Close()

	if g.cfg.TreeSizeStepSleep > 0 {
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-time.After(g.cfg.TreeSizeStepSleep):
		}
	}

	if readErr != nil {
		// Partial listing — cache what we summed but don't treat as authoritative
		// for long; small cadence will recompute. Still record to enable backoff.
		slog.Debug("dir size: partial readdir", "dir", dir, "error", readErr)
	}
	g.cache.put(dir, dirSize{
		Bytes:      bytes,
		FileCount:  count,
		ComputedAt: now,
		LastWalkMS: time.Since(t0).Milliseconds(),
	})
	return bytes, count, nil
}

// sizeOfRegularFile stats a directory entry relative to its parent fd without
// allocating a fileInfo or materialising the file's path. Returns the apparent
// size and whether the entry is a regular file (symlinks/devices/etc. → false).
// unix.Fstatat with AT_SYMLINK_NOFOLLOW stats the entry itself, never a target.
func sizeOfRegularFile(dirFd int, name string) (int64, bool) {
	var st unix.Stat_t
	if err := unix.Fstatat(dirFd, name, &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return 0, false
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return 0, false
	}
	return st.Size, true
}
