package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// boundCfg is a Config with the bounding knobs set explicitly, so a default
// change cannot silently alter what these tests assert.
func boundCfg(keepDepth int, keepMinFiles int64, maxEntries int) Config {
	return Config{
		TreeSizeSmallRefresh: 6 * time.Hour,
		TreeSizeLargeRefresh: 24 * time.Hour,
		TreeSizeSlowWalk:     30 * time.Second,
		TreeSizeKeepDepth:    keepDepth,
		TreeSizeKeepMinFiles: keepMinFiles,
		TreeSizeMaxEntries:   maxEntries,
		TreeSizeStepEvery:    16,
	}
}

// -----------------------------------------------------------------------------
// THE CORRECTNESS KEYSTONE.
//
// The whole bound rests on one claim: not caching a directory changes only how
// much work a later pass does, never a reported number. If that is false, the
// filter silently corrupts every size on the dashboard — the failure mode that
// looks exactly like a correct answer.
// -----------------------------------------------------------------------------

func TestWorthinessFilterDoesNotChangeAnyTotal(t *testing.T) {
	root := t.TempDir()
	// A tree deep enough to fall outside KeepDepth and small enough to fall under
	// KeepMinFiles, so the filter is genuinely active on most of it.
	// Four DISTINCT branches (an earlier version reused one chain, so each pass
	// silently overwrote the last and the hand-computed expectation was wrong).
	var wantBytes int64
	var wantFiles int64
	for i := 0; i < 4; i++ {
		d := filepath.Join(root, "branch"+string(rune('a'+i)))
		for j := 0; j <= i+4; j++ {
			d = filepath.Join(d, "lvl")
			if err := os.MkdirAll(d, 0o755); err != nil {
				t.Fatal(err)
			}
			content := strings.Repeat("x", 10+j)
			if err := os.WriteFile(filepath.Join(d, "f"), []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			wantBytes += int64(len(content))
			wantFiles++
		}
	}

	run := func(cfg Config) (int64, int64, int) {
		cache := newDirSizeCache(t.TempDir())
		g := &sizeGatherer{cfg: cfg, cache: cache, app: &app{stop: make(chan struct{})}, lastAttempt: map[string]time.Time{}}
		b, c, err := g.walk(context.Background(), root, 0)
		if err != nil {
			t.Fatal(err)
		}
		return b, c, cache.len()
	}

	// Unbounded: keep everything.
	ub, uc, uEntries := run(boundCfg(1000, 0, 0))
	// Bounded: keep almost nothing.
	bb, bc, bEntries := run(boundCfg(1, 1_000_000, 0))

	if ub != bb || uc != bc {
		t.Fatalf("filter changed the totals: unbounded=(%d,%d) bounded=(%d,%d)", ub, uc, bb, bc)
	}
	if ub != wantBytes || uc != wantFiles {
		t.Fatalf("totals wrong: got (%d,%d) want (%d,%d)", ub, uc, wantBytes, wantFiles)
	}
	if bEntries >= uEntries {
		t.Fatalf("bounding did not shrink the cache: %d entries vs %d unbounded", bEntries, uEntries)
	}
	t.Logf("identical totals (%d bytes, %d files); entries %d -> %d", ub, uc, uEntries, bEntries)
}

// -----------------------------------------------------------------------------
// Worthiness rules
// -----------------------------------------------------------------------------

func TestWorthCaching(t *testing.T) {
	g := &sizeGatherer{cfg: boundCfg(6, 100, 0)}
	tests := []struct {
		name  string
		depth int
		e     dirSize
		want  bool
	}{
		{"shallow tiny dir is kept (UI can read it)", 2, dirSize{FileCount: 1}, true},
		{"at the depth boundary", 6, dirSize{FileCount: 0}, true},
		{"one past the boundary and tiny", 7, dirSize{FileCount: 99}, false},
		{"deep but big enough to matter", 12, dirSize{FileCount: 100}, true},
		{"deep, tiny, but SLOW keeps its demotion", 12, dirSize{FileCount: 1, LastWalkMS: 40_000}, true},
		{"deep, tiny, fast", 12, dirSize{FileCount: 1, LastWalkMS: 5}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := g.worthCaching(tc.depth, tc.e); got != tc.want {
				t.Errorf("worthCaching(depth=%d, files=%d, walk=%dms) = %v, want %v",
					tc.depth, tc.e.FileCount, tc.e.LastWalkMS, got, tc.want)
			}
		})
	}
}

// An entry already on disk from before the filter existed must be REMOVED when it
// is recomputed and found unworthy — otherwise a 295 MB cache reloads and
// re-serialises forever and never actually shrinks.
func TestUnworthyEntryIsDroppedNotJustSkipped(t *testing.T) {
	root := t.TempDir()
	deep := filepath.Join(root, "a", "b", "c", "d", "e", "f", "g")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "f"), []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}

	cache := newDirSizeCache(t.TempDir())
	// Pre-seed it the way an old cache file would.
	cache.put(deep, dirSize{Bytes: 999, FileCount: 1, ComputedAtS: 1})
	if _, ok := cache.get(deep); !ok {
		t.Fatal("precondition: entry should be seeded")
	}

	g := &sizeGatherer{cfg: boundCfg(2, 100, 0), cache: cache, app: &app{stop: make(chan struct{})}, lastAttempt: map[string]time.Time{}}
	if _, _, err := g.walk(context.Background(), root, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.get(deep); ok {
		t.Fatal("stale unworthy entry survived the walk — the cache can never shrink")
	}
}

// -----------------------------------------------------------------------------
// Hard cap
// -----------------------------------------------------------------------------

func TestEnforceCapKeepsTheLargestSubtrees(t *testing.T) {
	c := newDirSizeCache(t.TempDir())
	for i := 0; i < 100; i++ {
		c.put(filepath.Join("/d", string(rune('a'+i%26)), string(rune('0'+i/26))),
			dirSize{FileCount: int64(i)})
	}
	before := c.len()
	dropped := c.enforceCap(10)
	if dropped != before-10 {
		t.Fatalf("dropped %d, want %d", dropped, before-10)
	}
	if c.len() != 10 {
		t.Fatalf("kept %d, want 10", c.len())
	}
	// Everything kept must be at least as large as everything dropped.
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, e := range c.m {
		if e.FileCount < 90 {
			t.Errorf("kept a small entry %s (files=%d) while dropping bigger ones", k, e.FileCount)
		}
	}
}

func TestEnforceCapIsANoOpUnderTheCap(t *testing.T) {
	c := newDirSizeCache(t.TempDir())
	c.put("/a", dirSize{FileCount: 1})
	if dropped := c.enforceCap(10); dropped != 0 {
		t.Errorf("dropped %d under the cap", dropped)
	}
	if dropped := c.enforceCap(0); dropped != 0 {
		t.Errorf("cap=0 must disable the backstop, dropped %d", dropped)
	}
}

// -----------------------------------------------------------------------------
// The streaming encoder. save() hand-builds the JSON object rather than calling
// json.Marshal on the map, so the object framing and key escaping are OUR bugs to
// have. A path with a quote or a backslash is legal on Linux.
// -----------------------------------------------------------------------------

func TestStreamingSaveRoundTripsThroughLoad(t *testing.T) {
	dir := t.TempDir()
	c := newDirSizeCache(dir)
	paths := []string{
		"/plain/path",
		`/weird/with"quote`,
		`/weird/with\backslash`,
		"/weird/with space",
		"/weird/with\tnewline-ish",
		"/weird/ünïcødé/日本語",
		"/",
	}
	for i, p := range paths {
		c.put(p, dirSize{Bytes: int64(i * 1000), FileCount: int64(i), ComputedAtS: int64(1700000000 + i), LastWalkMS: int64(i)})
	}
	if err := c.save(); err != nil {
		t.Fatalf("save: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, dirSizeCacheFile))
	if err != nil {
		t.Fatal(err)
	}
	// Must be valid JSON by a strict parser, not just by our own loader.
	var generic map[string]json.RawMessage
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("streamed output is not valid JSON: %v\n%s", err, truncateForLog(raw))
	}
	if len(generic) != len(paths) {
		t.Fatalf("got %d keys, want %d", len(generic), len(paths))
	}

	c2 := newDirSizeCache(dir)
	c2.load()
	if c2.len() != len(paths) {
		t.Fatalf("loaded %d entries, want %d", c2.len(), len(paths))
	}
	for i, p := range paths {
		got, ok := c2.get(p)
		if !ok {
			t.Errorf("path %q did not round-trip", p)
			continue
		}
		if got.Bytes != int64(i*1000) || got.FileCount != int64(i) || got.ComputedAtS != int64(1700000000+i) {
			t.Errorf("path %q: got %+v", p, got)
		}
	}
}

func TestStreamingSaveOfEmptyCacheIsValidJSON(t *testing.T) {
	dir := t.TempDir()
	c := newDirSizeCache(dir)
	if err := c.save(); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, dirSizeCacheFile))
	var m map[string]dirSize
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("empty cache produced invalid JSON %q: %v", raw, err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty map, got %d", len(m))
	}
}

// The v1 file must never be read into the v2 struct. encoding/json does NOT
// reject it — unknown fields are ignored and missing ones zeroed — so every entry
// would load as {Bytes:0, ComputedAtS:0} and Size() would answer (0, true),
// painting a confident "0 B" on every directory in the picker. Versioning is by
// FILENAME precisely because the decoder cannot be trusted here.
func TestLegacyCacheIsNotReadAndIsReclaimed(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, dirSizeCacheLegacyFile)
	old := `{"/a":{"bytes":12345,"file_count":9,"computed_at":"2026-08-13T21:26:11.006582794-04:00","last_walk_ms":3}}`
	if err := os.WriteFile(legacy, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}

	// A VALID v2 file must coexist and win. Without this the test passes for the
	// wrong reason: if `path` were (wrongly) the legacy name, load() would delete
	// that file and then fail to read it, producing the same cold start and the
	// same "legacy gone" — verifying the deletion rather than the versioning. A
	// negative control caught exactly that.
	v2 := filepath.Join(dir, dirSizeCacheFile)
	if err := os.WriteFile(v2, []byte(`{"/b":{"b":777,"f":5,"t":1700000000}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newDirSizeCache(dir)
	c.load()

	if _, ok := c.get("/a"); ok {
		t.Error("v1 entry was loaded: Size() would report ok=true with bytes=0")
	}
	got, ok := c.get("/b")
	if !ok {
		t.Fatal("the v2 file was not loaded — the cache is reading the wrong filename")
	}
	if got.Bytes != 777 || got.FileCount != 5 {
		t.Errorf("v2 entry decoded wrong: %+v", got)
	}
	if c.len() != 1 {
		t.Errorf("expected exactly the v2 entry, got %d", c.len())
	}
	if _, err := os.Stat(legacy); !os.IsNotExist(err) {
		t.Error("legacy cache file was not reclaimed (295 MB on kd-nas01)")
	}
}

// Guard the underlying decoder assumption directly, so that if someone later
// "simplifies" back to a single filename, this states why they must not.
func TestV1ShapeSilentlyDecodesToZeros(t *testing.T) {
	var m map[string]dirSize
	old := `{"/a":{"bytes":12345,"file_count":9,"computed_at":"2026-08-13T21:26:11Z","last_walk_ms":3}}`
	if err := json.Unmarshal([]byte(old), &m); err != nil {
		t.Skip("decoder rejected v1 outright; the filename versioning is then belt-and-braces")
	}
	e := m["/a"]
	if e.Bytes != 0 || e.FileCount != 0 || e.ComputedAtS != 0 {
		t.Fatalf("expected v1 to decode to zeros, got %+v — revisit the versioning rationale", e)
	}
	t.Log("confirmed: v1 decodes silently to a zeroed entry, hence filename versioning")
}

func truncateForLog(b []byte) string {
	if len(b) > 400 {
		return string(b[:400]) + "…"
	}
	return string(b)
}

// -----------------------------------------------------------------------------
// Display depth is measured from the nearest ANCHOR (a backup root or a repo),
// not from the walk root.
//
// trees.go caps a pushed tree at treeMaxDepth (5) below whatever it is rooted at,
// and repos are not at the top. Measured on kd-nas01: backup root /mnt, repos at
// /mnt/raid_array/kdhome_backup/duplicacy-relay (depth 4) and deeper — so a repo
// tree spans absolute depths 5..9. Measuring from the backup root would drop the
// sizes for the bottom half of that picker, and the UI renders a directory with
// no size at all rather than an obviously wrong one, so nobody would notice.
// -----------------------------------------------------------------------------

func TestAnchorResetsTheDisplayDepthBudget(t *testing.T) {
	root := t.TempDir()
	// A repo four levels below the walk root, mirroring the kd-nas01 layout.
	repo := filepath.Join(root, "raid_array", "kdhome_backup", "duplicacy-relay")
	// One level below the repo: inside the picker's reach, outside a
	// walk-root-relative budget of 2.
	inside := filepath.Join(repo, "chunks")
	if err := os.MkdirAll(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inside, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := boundCfg(2, 1_000_000, 0) // tiny depth budget, no size-based rescue
	cfg.BackupRoots = []string{root}
	cache := newDirSizeCache(t.TempDir())
	g := &sizeGatherer{
		cfg: cfg, cache: cache,
		app:         &app{stop: make(chan struct{})},
		lastAttempt: map[string]time.Time{},
		anchors:     map[string]struct{}{root: {}, repo: {}}, // as rebuildAnchors would
	}
	if _, _, err := g.walk(context.Background(), root, 0); err != nil {
		t.Fatal(err)
	}

	if _, ok := cache.get(repo); !ok {
		t.Error("the repo root itself must be cached")
	}
	if _, ok := cache.get(inside); !ok {
		t.Fatal("a directory one level inside a repo was not cached — the repo " +
			"picker would show it with no size")
	}
}

// Without the anchor the same tree must NOT be cached, or the test above proves
// nothing (it would pass on depth budget alone).
func TestWithoutAnchorTheDeepRepoDirIsDropped(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "raid_array", "kdhome_backup", "duplicacy-relay")
	inside := filepath.Join(repo, "chunks")
	if err := os.MkdirAll(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inside, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg := boundCfg(2, 1_000_000, 0)
	cfg.BackupRoots = []string{root}
	cache := newDirSizeCache(t.TempDir())
	g := &sizeGatherer{
		cfg: cfg, cache: cache,
		app:         &app{stop: make(chan struct{})},
		lastAttempt: map[string]time.Time{},
		anchors:     map[string]struct{}{root: {}}, // repo NOT an anchor
	}
	if _, _, err := g.walk(context.Background(), root, 0); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.get(inside); ok {
		t.Fatal("without the repo anchor this dir should fall outside the depth " +
			"budget; the anchor test would otherwise prove nothing")
	}
}

// rebuildAnchors must at minimum promote every backup root, and tolerate a nil
// repo index (tests and early boot) rather than panicking.
func TestRebuildAnchorsIncludesRootsAndSurvivesNilRepos(t *testing.T) {
	cfg := boundCfg(6, 100, 0)
	cfg.BackupRoots = []string{"/home/daniel", "/docker_container_volumes/"}
	g := &sizeGatherer{cfg: cfg, app: &app{stop: make(chan struct{})}}
	g.rebuildAnchors()

	for _, want := range []string{"/home/daniel", "/docker_container_volumes"} {
		if !g.isAnchor(want) {
			t.Errorf("%s should be an anchor (got %v)", want, g.anchors)
		}
	}
	if g.isAnchor("/somewhere/else") {
		t.Error("unrelated path must not be an anchor")
	}
}
