package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func writeFileN(t *testing.T, path string, n int) {
	t.Helper()
	if err := os.WriteFile(path, make([]byte, n), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// TestDirSizeWalkAccumulates verifies bottom-up accumulation, exclusion of
// excludeBasenames dirs, symlink skipping, and per-directory cache population.
func TestDirSizeWalkAccumulates(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	sub := filepath.Join(root, "sub")
	deeper := filepath.Join(sub, "deeper")
	excluded := filepath.Join(root, "node_modules") // in excludeBasenames
	for _, d := range []string{root, sub, deeper, excluded} {
		if err := os.MkdirAll(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeFileN(t, filepath.Join(root, "a.txt"), 10)
	writeFileN(t, filepath.Join(sub, "b.txt"), 20)
	writeFileN(t, filepath.Join(sub, "c.txt"), 5)
	writeFileN(t, filepath.Join(deeper, "d.txt"), 100)
	writeFileN(t, filepath.Join(excluded, "junk.txt"), 999) // must NOT count
	// Symlink to a file — must be skipped, not followed.
	if err := os.Symlink(filepath.Join(root, "a.txt"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}

	cache := newDirSizeCache(dir)
	g := newSizeGatherer(testSizeConfig(dir), cache, nil)

	gotB, gotC, err := g.walk(context.Background(), root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if wantB := int64(135); gotB != wantB { // 10+20+5+100
		t.Errorf("root bytes = %d, want %d", gotB, wantB)
	}
	if wantC := int64(4); gotC != wantC {
		t.Errorf("root file count = %d, want %d", gotC, wantC)
	}

	cases := []struct {
		path  string
		bytes int64
		count int64
	}{
		{root, 135, 4},
		{sub, 125, 3},
		{deeper, 100, 1},
	}
	for _, tc := range cases {
		e, ok := cache.get(tc.path)
		if !ok {
			t.Errorf("cache missing entry for %s", tc.path)
			continue
		}
		if e.Bytes != tc.bytes || e.FileCount != tc.count {
			t.Errorf("%s = (%d bytes, %d files), want (%d, %d)", tc.path, e.Bytes, e.FileCount, tc.bytes, tc.count)
		}
	}
	// Excluded directory must never be cached/descended.
	if _, ok := cache.get(excluded); ok {
		t.Errorf("excluded dir %s should not be cached", excluded)
	}
}

// TestDirSizeWalkReusesNotDue verifies a not-due child is folded in from cache
// without re-descending (its on-disk content is changed but the cached value is
// kept because the entry is within its cadence window).
func TestDirSizeWalkReusesNotDue(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "root")
	sub := filepath.Join(root, "sub")
	if err := os.MkdirAll(sub, 0700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(sub, "b.txt"), 20)

	cache := newDirSizeCache(dir)
	g := newSizeGatherer(testSizeConfig(dir), cache, nil)

	// Seed `sub` with a fresh, deliberately-wrong cached total. Because it's
	// within the small cadence window, the parent walk must reuse it verbatim.
	cache.put(sub, dirSize{Bytes: 999, FileCount: 7, ComputedAt: time.Now()})

	gotB, gotC, err := g.walk(context.Background(), root)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if gotB != 999 || gotC != 7 {
		t.Errorf("reuse failed: got (%d,%d), want (999,7) from cached child", gotB, gotC)
	}
}

func TestDirSizeCadence(t *testing.T) {
	g := newSizeGatherer(testSizeConfig(t.TempDir()), nil, nil)
	cases := []struct {
		name string
		e    dirSize
		want time.Duration
	}{
		{"small", dirSize{FileCount: 100}, 6 * time.Hour},
		{"large by count", dirSize{FileCount: 60000}, 24 * time.Hour},
		{"large by slow walk", dirSize{FileCount: 100, LastWalkMS: 40000}, 24 * time.Hour},
	}
	for _, tc := range cases {
		if got := g.cadence(tc.e); got != tc.want {
			t.Errorf("%s: cadence = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestDirSizeDue(t *testing.T) {
	dir := t.TempDir()
	cache := newDirSizeCache(dir)
	g := newSizeGatherer(testSizeConfig(dir), cache, nil)
	now := time.Now()

	if !g.due("/never/computed", now) {
		t.Error("uncached path should be due")
	}
	cache.put("/fresh", dirSize{FileCount: 10, ComputedAt: now})
	if g.due("/fresh", now) {
		t.Error("freshly computed small dir should not be due")
	}
	cache.put("/stale", dirSize{FileCount: 10, ComputedAt: now.Add(-7 * time.Hour)})
	if !g.due("/stale", now) {
		t.Error("small dir older than 6h should be due")
	}
	cache.put("/bigrecent", dirSize{FileCount: 60000, ComputedAt: now.Add(-7 * time.Hour)})
	if g.due("/bigrecent", now) {
		t.Error("large dir within 24h should not be due")
	}
}

func TestDirSizeCachePersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	c := newDirSizeCache(dir)
	c.put("/a", dirSize{Bytes: 100, FileCount: 3, ComputedAt: time.Now().Truncate(time.Second)})
	c.put("/b", dirSize{Bytes: 200, FileCount: 9, ComputedAt: time.Now().Truncate(time.Second)})
	if err := c.save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	c2 := newDirSizeCache(dir)
	c2.load()
	for _, p := range []string{"/a", "/b"} {
		want, _ := c.get(p)
		got, ok := c2.get(p)
		if !ok {
			t.Errorf("reloaded cache missing %s", p)
			continue
		}
		if got.Bytes != want.Bytes || got.FileCount != want.FileCount || !got.ComputedAt.Equal(want.ComputedAt) {
			t.Errorf("%s round-trip mismatch: got %+v want %+v", p, got, want)
		}
	}
}

func TestSizeGathererExcludePaths(t *testing.T) {
	dir := t.TempDir()
	keep := filepath.Join(dir, "keep")
	excl := filepath.Join(dir, "excluded")
	if err := os.MkdirAll(filepath.Join(keep, "k"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(excl, "e"), 0700); err != nil {
		t.Fatal(err)
	}
	writeFileN(t, filepath.Join(keep, "kf"), 10)
	writeFileN(t, filepath.Join(excl, "ef"), 999) // must be excluded from the sum

	cfg := testSizeConfig(dir)
	cfg.BackupRoots = []string{keep, excl}
	cfg.TreeSizeExcludePaths = []string{excl}
	cache := newDirSizeCache(dir)
	g := newSizeGatherer(cfg, cache, nil)

	// roots() drops the excluded backup root entirely.
	roots := g.roots()
	if len(roots) != 1 || roots[0] != keep {
		t.Fatalf("roots() = %v, want [%s]", roots, keep)
	}

	// Walking the parent must skip the excluded subtree (size + count).
	b, c, err := g.walk(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	if b != 10 || c != 1 {
		t.Errorf("walk = (%d bytes, %d files), want (10,1) — excluded subtree must be skipped", b, c)
	}
	if _, ok := cache.get(excl); ok {
		t.Error("excluded path should not be cached")
	}
}

func TestDirSizeCachePrune(t *testing.T) {
	c := newDirSizeCache(t.TempDir())
	c.put("/keep/a", dirSize{Bytes: 1})
	c.put("/var/lib/rancher/k3s", dirSize{Bytes: 2})
	c.put("/var/lib/rancher/k3s/agent/x", dirSize{Bytes: 3})

	if n := c.prune([]string{"/var/lib/rancher/k3s"}); n != 2 {
		t.Errorf("prune removed %d, want 2", n)
	}
	if _, ok := c.get("/keep/a"); !ok {
		t.Error("/keep/a should remain")
	}
	if _, ok := c.get("/var/lib/rancher/k3s"); ok {
		t.Error("k3s root should be pruned")
	}
	if _, ok := c.get("/var/lib/rancher/k3s/agent/x"); ok {
		t.Error("k3s child should be pruned")
	}
	if n := c.prune(nil); n != 0 {
		t.Errorf("prune(nil) = %d, want 0", n)
	}
}

func TestWalkDirFileSizes(t *testing.T) {
	dir := t.TempDir()
	writeFileN(t, filepath.Join(dir, "a.txt"), 10)
	writeFileN(t, filepath.Join(dir, "b.bin"), 250)
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0700); err != nil {
		t.Fatal(err)
	}

	w := newTreeWalker(Config{}, nil, nil)
	st, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	children := w.walkDir(dir, st.ModTime(), 0)

	got := map[string]*treeNode{}
	for _, c := range children {
		got[c.Name] = c
	}
	if got["a.txt"] == nil || got["a.txt"].Size != 10 {
		t.Errorf("a.txt size = %v, want 10", got["a.txt"])
	}
	if got["b.bin"] == nil || got["b.bin"].Size != 250 {
		t.Errorf("b.bin size = %v, want 250", got["b.bin"])
	}
	// Directory nodes get Size from annotateSizes (the gatherer cache), NOT from
	// walkDir — so the structural walk leaves dir Size at 0.
	if got["sub"] == nil || got["sub"].Type != "directory" || got["sub"].Size != 0 {
		t.Errorf("sub dir from walkDir = %v, want directory with Size 0", got["sub"])
	}
}

func TestWalkDirTruncatedSkipsFileStats(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < treeMaxFilesPerDir+5; i++ {
		writeFileN(t, filepath.Join(dir, fmt.Sprintf("f%04d", i)), 1)
	}
	w := newTreeWalker(Config{}, nil, nil)
	st, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	children := w.walkDir(dir, st.ModTime(), 0)
	if len(children) != 1 || children[0].Type != "truncated" {
		t.Fatalf("want single truncated marker, got %d children", len(children))
	}
	if children[0].FileCount != treeMaxFilesPerDir+5 {
		t.Errorf("truncated FileCount = %d, want %d", children[0].FileCount, treeMaxFilesPerDir+5)
	}
}

func TestAnnotateSizes(t *testing.T) {
	cache := newDirSizeCache(t.TempDir())
	cache.put("/x", dirSize{Bytes: 100})
	cache.put("/x/d", dirSize{Bytes: 40})
	// /x/d2 intentionally not cached → Size stays 0 (omitted).

	w := &treeWalker{app: &app{sizes: cache}}
	tree := &treeNode{
		Name: "x", Path: "/x", Type: "directory",
		Children: []*treeNode{
			{Name: "f", Path: "/x/f", Type: "file"},
			{Name: "d", Path: "/x/d", Type: "directory"},
			{Name: "d2", Path: "/x/d2", Type: "directory"},
		},
	}
	w.annotateSizes(tree)

	if tree.Size != 100 {
		t.Errorf("root size = %d, want 100", tree.Size)
	}
	if tree.Children[0].Size != 0 {
		t.Errorf("file node should not be sized, got %d", tree.Children[0].Size)
	}
	if tree.Children[1].Size != 40 {
		t.Errorf("/x/d size = %d, want 40", tree.Children[1].Size)
	}
	if tree.Children[2].Size != 0 {
		t.Errorf("uncached dir should stay 0, got %d", tree.Children[2].Size)
	}
}
