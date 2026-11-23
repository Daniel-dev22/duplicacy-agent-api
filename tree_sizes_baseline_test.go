package main

// Baseline harness for the directory-size gatherer, committed BEFORE the hoist into
// agent-kit-go so the same assertions and benchmark run on both sides of it.
//
//   - TestDirSizeWalkMatchesIndependentOracle: every cached directory equals an
//     independent filepath.WalkDir tally (differential, so a walk that visits less
//     cannot pass), and the digest of the sorted (relative path, bytes, files) tuples
//     equals a pinned golden. ComputedAtS and LastWalkMS are deliberately NOT hashed:
//     they change on every run, so hashing them would make the gate meaningless.
//   - BenchmarkDirSizeWalk: walk time, allocations and (via -benchmem) bytes per walk
//     over a fixed synthetic tree. BASELINE_TREE_FANOUT/DEPTH/FILES/DIR override the
//     shape and location so the same binary can be pinned on a Pi under taskset.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// baselineShape is a deterministic tree: fanout^level directories per level down to
// depth, filesPerDir regular files each, plus one excluded node_modules directory and
// one symlink per directory that must never be counted.
type baselineShape struct {
	fanout, depth, filesPerDir int
}

func buildBaselineTree(tb testing.TB, root string, s baselineShape) {
	tb.Helper()
	var build func(dir string, level int)
	build = func(dir string, level int) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			tb.Fatal(err)
		}
		for i := 0; i < s.filesPerDir; i++ {
			// Sizes depend only on position, never on the run.
			n := 1 + (level*131+i*17)%997
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("f%03d", i)), make([]byte, n), 0o600); err != nil {
				tb.Fatal(err)
			}
		}
		if err := os.Symlink("f000", filepath.Join(dir, "link")); err != nil {
			tb.Fatal(err)
		}
		if level == 0 {
			nm := filepath.Join(dir, "node_modules")
			if err := os.MkdirAll(nm, 0o700); err != nil {
				tb.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(nm, "junk"), make([]byte, 4096), 0o600); err != nil {
				tb.Fatal(err)
			}
		}
		if level == s.depth {
			return
		}
		for c := 0; c < s.fanout; c++ {
			build(filepath.Join(dir, fmt.Sprintf("d%02d", c)), level+1)
		}
	}
	build(root, 0)
}

func baselineConfig(dir string) Config {
	c := testSizeConfig(dir)
	c.TreeSizeKeepDepth = 1 << 20 // keep every directory so the whole tally is observable
	return c
}

type baselineRow struct {
	rel          string
	bytes, files int64
}

// oracleTally computes per-directory recursive totals with plain filepath.WalkDir and
// the same exclusion rules (excludeBasenames, no symlinks), independent of the walker.
func oracleTally(tb testing.TB, root string) map[string]baselineRow {
	tb.Helper()
	rows := map[string]baselineRow{}
	var rec func(dir string) (int64, int64)
	rec = func(dir string) (int64, int64) {
		ents, err := os.ReadDir(dir)
		if err != nil {
			tb.Fatal(err)
		}
		var b, f int64
		for _, e := range ents {
			if _, skip := excludeBasenames[e.Name()]; skip {
				continue
			}
			if e.Type()&fs.ModeSymlink != 0 {
				continue
			}
			p := filepath.Join(dir, e.Name())
			if e.IsDir() {
				cb, cf := rec(p)
				b += cb
				f += cf
				continue
			}
			info, err := e.Info()
			if err != nil {
				tb.Fatal(err)
			}
			if info.Mode().IsRegular() {
				b += info.Size()
				f++
			}
		}
		rel, _ := filepath.Rel(root, dir)
		rows[rel] = baselineRow{rel: rel, bytes: b, files: f}
		return b, f
	}
	rec(root)
	return rows
}

// digestRows hashes sorted (relative path, bytes, files) tuples. No timestamps.
func digestRows(rows []baselineRow) string {
	sort.Slice(rows, func(i, j int) bool { return rows[i].rel < rows[j].rel })
	h := sha256.New()
	for _, r := range rows {
		fmt.Fprintf(h, "%s\x00%d\x00%d\n", r.rel, r.bytes, r.files)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cacheRows(tb testing.TB, cache *dirSizeCache, root string) []baselineRow {
	tb.Helper()
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	rows := make([]baselineRow, 0, len(cache.m))
	for p, e := range cache.m {
		rel, err := filepath.Rel(root, p)
		if err != nil || strings.HasPrefix(rel, "..") {
			tb.Fatalf("cache key %q outside root %q", p, root)
		}
		rows = append(rows, baselineRow{rel: rel, bytes: e.Bytes, files: e.FileCount})
	}
	return rows
}

// baselineGolden pins the digest of the small shape below. It must not change in the
// hoist: the walker's arithmetic is the gate.
const baselineGolden = "31ca124a90e9c27cf5f530ade7ad8c00a2c10b21f33a9a871e97adf6a32727ba"

var smallShape = baselineShape{fanout: 4, depth: 3, filesPerDir: 7}

func TestDirSizeWalkMatchesIndependentOracle(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "tree")
	buildBaselineTree(t, root, smallShape)

	cache := newDirSizeCache(dir)
	g := newSizeGatherer(baselineConfig(dir), cache, nil)
	if _, _, err := g.walk(context.Background(), root, 0); err != nil {
		t.Fatal(err)
	}

	want := oracleTally(t, root)
	got := cacheRows(t, cache, root)
	if len(got) != len(want) {
		t.Fatalf("cached %d dirs, oracle has %d", len(got), len(want))
	}
	for _, r := range got {
		w, ok := want[r.rel]
		if !ok || w.bytes != r.bytes || w.files != r.files {
			t.Fatalf("%q = (%d B, %d files), oracle (%d, %d) present=%v", r.rel, r.bytes, r.files, w.bytes, w.files, ok)
		}
	}
	// The pushed form reads Size() from the same cache; pin it for every directory.
	for _, r := range got {
		sz, ok := cache.Size(filepath.Join(root, r.rel))
		if !ok || sz != r.bytes {
			t.Fatalf("Size(%q) = %d,%v want %d", r.rel, sz, ok, r.bytes)
		}
	}
	if d := digestRows(got); d != baselineGolden {
		t.Fatalf("digest changed: %s (golden %s)", d, baselineGolden)
	}
}

var (
	benchOnce sync.Once
	benchRoot string
	benchWant string
)

func benchShape() baselineShape {
	atoi := func(k string, d int) int {
		if v, err := strconv.Atoi(os.Getenv(k)); err == nil && v > 0 {
			return v
		}
		return d
	}
	return baselineShape{
		fanout:      atoi("BASELINE_TREE_FANOUT", 8),
		depth:       atoi("BASELINE_TREE_DEPTH", 4),
		filesPerDir: atoi("BASELINE_TREE_FILES", 12),
	}
}

func BenchmarkDirSizeWalk(b *testing.B) {
	benchOnce.Do(func() {
		base := os.Getenv("BASELINE_TREE_DIR")
		if base == "" {
			base = b.TempDir()
		}
		benchRoot = filepath.Join(base, "tree")
		if _, err := os.Stat(benchRoot); err != nil {
			buildBaselineTree(b, benchRoot, benchShape())
		}
		benchWant = digestRows(rowsFromOracle(oracleTally(b, benchRoot)))
	})
	cfgDir := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	var dirs int
	for i := 0; i < b.N; i++ {
		cache := newDirSizeCache(cfgDir)
		g := newSizeGatherer(baselineConfig(cfgDir), cache, nil)
		if _, _, err := g.walk(context.Background(), benchRoot, 0); err != nil {
			b.Fatal(err)
		}
		dirs = cache.len()
	}
	b.StopTimer()
	// Falsifiable control: a walk that visits less cannot report the oracle's digest.
	cache := newDirSizeCache(cfgDir)
	g := newSizeGatherer(baselineConfig(cfgDir), cache, nil)
	if _, _, err := g.walk(context.Background(), benchRoot, 0); err != nil {
		b.Fatal(err)
	}
	if d := digestRows(cacheRows(b, cache, benchRoot)); d != benchWant {
		b.Fatalf("walk digest %s != oracle digest %s", d, benchWant)
	}
	b.ReportMetric(float64(dirs), "dirs")
}

func rowsFromOracle(m map[string]baselineRow) []baselineRow {
	out := make([]baselineRow, 0, len(m))
	for _, r := range m {
		out = append(out, r)
	}
	return out
}
