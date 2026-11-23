package main

import (
	"path/filepath"

	"github.com/Daniel-dev22/agent-kit-go/dirsize"
)

// The directory-size gatherer and its persisted cache live in agent-kit-go (dirsize).
// This file is the whole duplicacy-specific side of that seam: file names and the
// mapping from Config, the filter cache and the repo index onto dirsize.Options.
//
// The format is versioned VIA THE FILENAME (dir_sizes.v2.json). The v1 file reached
// 309,170,812 bytes on kd-nas01 and nothing reads it again, so it is deleted on first
// load.
const (
	dirSizeCacheFile       = "dir_sizes.v2.json"
	dirSizeCacheLegacyFile = "dir_sizes.json"
)

func newSizeCache(cfg Config) *dirsize.Cache {
	return dirsize.NewCache(
		filepath.Join(cfg.ConfigDir, dirSizeCacheFile),
		filepath.Join(cfg.ConfigDir, dirSizeCacheLegacyFile),
	)
}

// sizeOptions maps every TreeSize* setting and every duplicacy-specific rule onto the
// kit's Options. Hooks are evaluated by the gatherer on its own goroutine; they read
// the live filter cache and repo index, so a pushed filter or a newly registered repo
// takes effect without a restart.
func sizeOptions(cfg Config, a *app) dirsize.Options {
	return dirsize.Options{
		Enabled:            cfg.TreeSizeEnabled,
		SmallRefresh:       cfg.TreeSizeSmallRefresh,
		LargeRefresh:       cfg.TreeSizeLargeRefresh,
		LargeFileThreshold: cfg.TreeSizeLargeFileThreshold,
		SlowWalk:           cfg.TreeSizeSlowWalk,
		WalkTimeout:        cfg.TreeSizeWalkTimeout,
		StepSleep:          cfg.TreeSizeStepSleep,
		StepEvery:          cfg.TreeSizeStepEvery,
		KeepDepth:          cfg.TreeSizeKeepDepth,
		KeepMinFiles:       cfg.TreeSizeKeepMinFiles,
		MaxEntries:         cfg.TreeSizeMaxEntries,

		Roots:            func() []string { return cfg.BackupRoots },
		ExcludePaths:     func() []string { return sizeExcludePaths(cfg, a) },
		ExcludeBasenames: excludeBasenames,
		SkipPath:         isDuplicacyWebCache,
		Anchors:          func() []string { return sizeAnchors(a) },
		Stop:             a.stop,
	}
}

// sizeExcludePaths is the single exclusion list the gatherer honours: the two env-var
// lists plus the absolute exclude rules from the controller-pushed filter sets.
// Merging them here rather than at each call site is the point: a rule the operator
// set in the UI must apply to this walker as well as to backups.
func sizeExcludePaths(cfg Config, a *app) []string {
	env := len(cfg.BackupExcludePaths) + len(cfg.TreeSizeExcludePaths)
	var pushed []string
	if a != nil {
		pushed = a.filters.AbsoluteExcludePrefixes()
	}
	out := make([]string, 0, env+len(pushed))
	out = append(out, cfg.BackupExcludePaths...)
	out = append(out, cfg.TreeSizeExcludePaths...)
	return append(out, pushed...)
}

// sizeAnchors returns each registered repo's Path and SourcePath: the extra paths a
// displayed tree can be rooted at, so every node of a repo picker keeps a cached size
// however deep it sits under a backup root.
func sizeAnchors(a *app) []string {
	if a == nil || a.repos == nil {
		return nil
	}
	var out []string
	for _, r := range a.repos.list() {
		if r.Path != "" {
			out = append(out, filepath.Clean(r.Path))
		}
		if r.SourcePath != "" {
			out = append(out, filepath.Clean(r.SourcePath))
		}
	}
	return out
}
