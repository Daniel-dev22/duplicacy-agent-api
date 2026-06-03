package main

import (
	"context"
	"log/slog"
	"sort"
	"strconv"
	"time"
)

const (
	// listWarmDebounce collapses a burst of backup/copy/prune completions (the
	// nightly wave) into a single warm sweep that runs once activity settles.
	listWarmDebounce = 2 * time.Minute
	// maxWarmListsPerSweep bounds `list -files` invocations in one sweep so a
	// cold cache (first run after deploy) can't pin the maintenance lane for
	// hours. Anything skipped this sweep is warmed on a later sweep or lazily
	// on first open. We log when the cap bites (no silent truncation).
	maxWarmListsPerSweep = 200
)

// runListCacheWarmer is the debounced driver behind the list-files warm cache.
// It waits for a warmDirty nudge, lets the burst settle (listWarmDebounce),
// then runs one warmListCacheSweep. A single goroutine ⇒ sweeps never overlap
// and the `list` subprocesses never fan out across the host.
func (a *app) runListCacheWarmer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.warmDirty:
		}
		// Debounce: keep extending the window while nudges keep arriving.
		timer := time.NewTimer(listWarmDebounce)
	settle:
		for {
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-a.warmDirty:
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(listWarmDebounce)
			case <-timer.C:
				break settle
			}
		}
		a.warmListCacheSweep(ctx)
	}
}

// warmListCacheSweep pre-lists the newest-N revisions per (snapshot_id,
// storage) for every repo on this agent and reconciles pruned revisions out of
// the cache. Because cache keys are immutable, already-cached revisions are
// skipped — so after the first sweep each run only lists genuinely new
// revisions. Serial by construction (one warmer goroutine); the slow
// cross-site/SFTP secondaries are pulled here, off the restore hot path.
func (a *app) warmListCacheSweep(ctx context.Context) {
	if a.filesCache == nil {
		return
	}
	keepN := a.cfg.ListCacheWarmN
	if keepN <= 0 {
		return
	}
	var warmed, reconciled int
	capped := false

	for _, repo := range a.repos.list() {
		if ctx.Err() != nil {
			return
		}
		env, rsaPriv, cleanup, err := a.prepareEnvForRepo(ctx, repo)
		if err != nil {
			slog.Warn("list-cache warm: vend secrets failed", "repo", repo.ID, "error", err)
			continue
		}
		for _, st := range repo.Storages {
			if ctx.Err() != nil {
				cleanup()
				return
			}
			storage := st.Name
			if storage == "" {
				storage = "default"
			}
			keyPath := rsaPriv[storage]

			// 1. List live revisions across every snapshot id on this storage
			//    (cheap — downloads only the snapshot index). Used both to
			//    reconcile prunes and to choose the newest-N to warm.
			live, err := a.warmListSnapshots(ctx, repo, storage, keyPath, env)
			if err != nil {
				slog.Warn("list-cache warm: list failed", "repo", repo.ID, "storage", storage, "error", err)
				continue
			}
			if n, rerr := a.filesCache.reconcile(ctx, storage, live); rerr != nil {
				slog.Warn("list-cache reconcile failed", "repo", repo.ID, "storage", storage, "error", rerr)
			} else {
				reconciled += n
			}

			// 2. Warm the newest-N revisions per snapshot id that aren't cached.
			for _, rev := range newestRevisionsPerSnapshot(live, keepN) {
				if warmed >= maxWarmListsPerSweep {
					capped = true
					break
				}
				has, herr := a.filesCache.has(ctx, rev.SnapshotID, rev.Revision, storage)
				if herr != nil {
					slog.Warn("list-cache warm: has check failed", "error", herr)
					continue
				}
				if has {
					continue // immutable ⇒ already correct
				}
				if werr := a.warmOneListing(ctx, repo, storage, keyPath, rev.SnapshotID, rev.Revision, env); werr != nil {
					slog.Warn("list-cache warm: pre-list failed",
						"repo", repo.ID, "storage", storage,
						"snapshot_id", rev.SnapshotID, "rev", rev.Revision, "error", werr)
					continue
				}
				warmed++
			}
		}
		cleanup()
	}

	if err := a.filesCache.evictBySize(ctx); err != nil {
		slog.Warn("list-cache evict failed", "error", err)
	}
	rows, gzBytes, _ := a.filesCache.stats(ctx)
	slog.Info("list-files cache warm sweep done",
		"warmed", warmed, "reconciled", reconciled, "capped", capped,
		"rows", rows, "gz_bytes", gzBytes)
}

// warmListSnapshots runs `duplicacy list -all -storage <storage>` and returns
// the parsed revisions across ALL snapshot ids on that storage. -all is
// required so a relay/hub repo enumerates every pooled source id (without it
// duplicacy lists only the repo's own snapshot id).
func (a *app) warmListSnapshots(ctx context.Context, repo *Repo, storage, keyPath string, env []string) ([]Snapshot, error) {
	args := []string{"list", "-all"}
	if keyPath != "" {
		args = append(args, "-key", keyPath)
	}
	if storage != "" && storage != "default" {
		args = append(args, "-storage", storage)
	}
	out, err := runSync(ctx, a.cfg.DuplicacyBinary, cliInvocation{
		RepoRoot: repo.Path, Args: args, EnvAdds: env,
	}, 60*time.Second)
	if err != nil {
		return nil, err
	}
	return parseListOutput(string(out)), nil
}

// warmOneListing runs `duplicacy list -files -r <rev> -id <snapshot_id>
// -storage <storage>` and stores the output under the immutable key. -id is
// always set so -r resolves even on a relay/hub repo that pools many ids.
func (a *app) warmOneListing(ctx context.Context, repo *Repo, storage, keyPath, snapshotID string, rev int, env []string) error {
	args := []string{"list", "-files", "-r", strconv.Itoa(rev)}
	if keyPath != "" {
		args = append(args, "-key", keyPath)
	}
	if snapshotID != "" {
		args = append(args, "-id", snapshotID)
	}
	if storage != "" && storage != "default" {
		args = append(args, "-storage", storage)
	}
	out, err := runSync(ctx, a.cfg.DuplicacyBinary, cliInvocation{
		RepoRoot: repo.Path, Args: args, EnvAdds: env,
	}, 5*time.Minute)
	if err != nil {
		return err
	}
	return a.filesCache.put(ctx, snapshotID, rev, storage, repo.ID, out)
}

// newestRevisionsPerSnapshot returns the newest keepN revisions for each
// distinct snapshot id in snaps.
func newestRevisionsPerSnapshot(snaps []Snapshot, keepN int) []Snapshot {
	bySnap := map[string][]Snapshot{}
	for _, s := range snaps {
		bySnap[s.SnapshotID] = append(bySnap[s.SnapshotID], s)
	}
	out := make([]Snapshot, 0, len(bySnap)*keepN)
	for _, list := range bySnap {
		sort.Slice(list, func(i, j int) bool { return list[i].Revision > list[j].Revision })
		if len(list) > keepN {
			list = list[:keepN]
		}
		out = append(out, list...)
	}
	return out
}
