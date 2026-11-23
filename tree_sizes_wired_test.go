package main

// Wiring harness for the directory-size gatherer. The gatherer itself lives in
// agent-kit-go (dirsize); these tests pin the duplicacy-specific side of the seam
// (config fields, excludeBasenames, web-cache skip, env + pushed excludes, repo anchors,
// the stop channel) so it cannot change unnoticed. The goldens below were taken on the
// pre-hoist code and must not move.
//
//   - TestWiredPassMatchesPinnedDigest drives one REAL pass (RunPass) over a tree that
//     contains every duplicacy-specific rule and pins the digest of the resulting cache.
//   - BenchmarkWiredPass is the Pi-class gate: a full pass with production-like excludes
//     (the kit-shaped walk benchmark has none, so it cannot see the per-child cost of
//     merging env and pushed exclude lists).
//   - TestWiredHostDigest prints the digest of a real tree (DIRSIZE_HOST_ROOT) so the
//     pre- and post-migration builds can be compared back to back on one host.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Daniel-dev22/agent-kit-go/dirsize"
	"github.com/Daniel-dev22/agent-kit-go/dirsize/dirsizetest"
)

func testSizeConfig(dir string) Config {
	return Config{
		ConfigDir:                  dir,
		TreeSizeEnabled:            true,
		TreeSizeLargeFileThreshold: 50000,
		TreeSizeSlowWalk:           30 * time.Second,
		TreeSizeLargeRefresh:       24 * time.Hour,
		TreeSizeSmallRefresh:       6 * time.Hour,
		TreeSizeWalkTimeout:        30 * time.Minute,
		TreeSizeStepSleep:          0,
	}
}

// baselineConfig keeps every directory so the whole tally is observable.
func baselineConfig(dir string) Config {
	c := testSizeConfig(dir)
	c.TreeSizeKeepDepth = 1 << 20
	return c
}

func writeFileN(t testing.TB, path string, n int) {
	t.Helper()
	if err := os.WriteFile(path, make([]byte, n), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func cacheRows(t testing.TB, cache *dirsize.Cache, root string) []dirsizetest.Row {
	t.Helper()
	return dirsizetest.RowsFrom(t, root, func(add func(string, int64, int64)) {
		cache.Range(func(p string, e dirsize.Entry) { add(p, e.Bytes, e.FileCount) })
	})
}

// wiredApp is the minimal app the gatherer reads: a stop channel, the pushed filter
// sets and the repo index.
func wiredApp(t testing.TB, pushed []string, repos ...*Repo) *app {
	t.Helper()
	rules := make([]FilterRule, 0, len(pushed))
	for i, p := range pushed {
		rules = append(rules, FilterRule{Position: i, Action: "exclude", Pattern: p})
	}
	ri := newRepoIndex("", nil, nil)
	for i, r := range repos {
		ri.repos[fmt.Sprintf("r%d", i)] = r
	}
	return &app{
		stop:    make(chan struct{}),
		repos:   ri,
		filters: &filterCache{sets: []FilterSet{{ID: "org", Scope: "org", Rules: rules}}},
	}
}

// runWiredPass is the production wiring (newSizeCache + sizeOptions + NewGatherer)
// followed by exactly one pass.
func runWiredPass(cfg Config, a *app) *dirsize.Cache {
	cache := newSizeCache(cfg)
	dirsize.NewGatherer(sizeOptions(cfg, a), cache).RunPass(context.Background())
	return cache
}

// quietLogs discards slog output for the test: a real pass prints one line per mount
// the guard skips, which is hundreds on a k3s host.
func quietLogs(t testing.TB) {
	t.Helper()
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
}

func mkdirAll(t testing.TB, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if err := os.MkdirAll(p, 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func TestWiredPassMatchesPinnedDigest(t *testing.T) {
	quietLogs(t)
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	repoA := filepath.Join(root, "deep", "l1", "l2", "l3", "repoA")
	srcB := filepath.Join(root, "deep", "l1", "l2", "l3", "srcB")
	envX := filepath.Join(root, "envx")
	bkX := filepath.Join(root, "bkx")
	pushedX := filepath.Join(root, "pushed")
	mkdirAll(t,
		filepath.Join(root, "keep"),
		filepath.Join(root, "node_modules"), filepath.Join(root, ".git"), filepath.Join(root, "logs"),
		filepath.Join(root, "cache", "localhost", "x"), // Duplicacy Web cache layout
		envX, bkX, pushedX,
		filepath.Join(repoA, "c1", "c2"), filepath.Join(srcB, "d1", "d2"),
	)
	writeFileN(t, filepath.Join(root, "a.txt"), 10)
	writeFileN(t, filepath.Join(root, "keep", "k"), 20)
	for _, junk := range []string{"node_modules", ".git", "logs", "envx", "bkx", "pushed"} {
		writeFileN(t, filepath.Join(root, junk, "junk"), 999) // every one of these must NOT count
	}
	writeFileN(t, filepath.Join(root, "cache", "localhost", "x", "junk"), 999)
	writeFileN(t, filepath.Join(root, "cache", "ok"), 3)
	writeFileN(t, filepath.Join(repoA, "c1", "c2", "f"), 5)
	writeFileN(t, filepath.Join(srcB, "d1", "d2", "f"), 7)

	cfg := testSizeConfig(dir)
	cfg.BackupRoots = []string{root}
	cfg.BackupExcludePaths = []string{bkX}
	cfg.TreeSizeExcludePaths = []string{envX}
	// A tiny display budget with no size-based rescue: only the repo anchors keep the
	// directories below repoA and srcB cached, so a dropped Path or SourcePath mapping
	// changes the digest.
	cfg.TreeSizeKeepDepth = 2
	cfg.TreeSizeKeepMinFiles = 1_000_000
	cfg.TreeSizeSlowWalk = 1 << 60

	a := wiredApp(t, []string{pushedX}, &Repo{Path: repoA}, &Repo{Path: filepath.Join(dir, "elsewhere"), SourcePath: srcB})
	cache := runWiredPass(cfg, a)

	rows := cacheRows(t, cache, root)
	for _, rel := range []string{"deep/l1/l2/l3/repoA/c1", "deep/l1/l2/l3/repoA/c1/c2", "deep/l1/l2/l3/srcB/d1", "deep/l1/l2/l3/srcB/d1/d2"} {
		found := false
		for _, r := range rows {
			if r.Rel == rel {
				found = true
			}
		}
		if !found {
			t.Errorf("%s not cached: a repo Path or SourcePath anchor is not wired", rel)
		}
	}
	if e, ok := cache.Get(root); !ok || e.Bytes != 10+20+3+5+7 || e.FileCount != 5 {
		t.Fatalf("root = %+v ok=%v, want 45 bytes in 5 files: a basename, web-cache, env, backup or pushed exclude is not wired", e, ok)
	}
	if d := dirsizetest.DigestRows(rows); d != wiredGolden {
		t.Fatalf("wired digest changed: %s (golden %s)", d, wiredGolden)
	}
}

const wiredGolden = "ff9b3ed99a5d6297af46c4a77699c8747078db802909914ec010c8b8df5ce924"

// productionLikeAppAndConfig mirrors a real host's exclude load: a few env excludes and
// a pushed org set with a dozen absolute rules (none of them present on disk, so the
// walk is identical to the kit-shaped benchmark and only the per-child exclude merge
// differs).
func productionLikeAppAndConfig(t testing.TB, dir, root string) (Config, *app) {
	cfg := baselineConfig(dir)
	cfg.BackupRoots = []string{root}
	cfg.BackupExcludePaths = []string{"/var/lib/rancher/k3s", "/home/daniel/custom_os_isos"}
	cfg.TreeSizeExcludePaths = []string{"/var/lib/docker", "/mnt/raid_array/scratch"}
	pushed := make([]string, 0, 12)
	for i := 0; i < 12; i++ {
		pushed = append(pushed, fmt.Sprintf("/srv/not-here-%02d", i))
	}
	return cfg, wiredApp(t, pushed)
}

func BenchmarkWiredPass(b *testing.B) {
	quietLogs(b)
	root, want := dirsizetest.EnsureBenchTree(b, excludeBasenames)
	b.ReportAllocs()
	b.ResetTimer()
	var dirs int
	for i := 0; i < b.N; i++ {
		cfgDir := b.TempDir()
		cfg, a := productionLikeAppAndConfig(b, cfgDir, root)
		dirs = runWiredPass(cfg, a).Len()
	}
	b.StopTimer()
	cfgDir := b.TempDir()
	cfg, a := productionLikeAppAndConfig(b, cfgDir, root)
	cache := runWiredPass(cfg, a)
	if d := dirsizetest.DigestRows(cacheRows(b, cache, root)); d != want {
		b.Fatalf("wired pass digest %s != oracle digest %s", d, want)
	}
	b.ReportMetric(float64(dirs), "dirs")
}

// TestWiredHostDigest prints the digest of a real tree for a before/after comparison
// on one host. Skipped unless DIRSIZE_HOST_ROOT is set.
func TestWiredHostDigest(t *testing.T) {
	quietLogs(t)
	root := os.Getenv("DIRSIZE_HOST_ROOT")
	if root == "" {
		t.Skip("DIRSIZE_HOST_ROOT not set")
	}
	dir := t.TempDir()
	cfg := baselineConfig(dir)
	cfg.BackupRoots = []string{root}
	cache := runWiredPass(cfg, wiredApp(t, nil))
	rows := cacheRows(t, cache, root)
	fmt.Printf("HOST_DIGEST root=%s dirs=%d digest=%s\n", root, len(rows), dirsizetest.DigestRows(rows))
}
