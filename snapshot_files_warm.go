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
	// listWarmStartupDelay defers the post-restart safety-net sweep so the
	// initial repo scan can settle first. The persisted SQLite cache survives
	// restarts, so this sweep is usually cheap (most entries already present);
	// it exists to re-warm anything created/evicted/missed while the agent was
	// down.
	listWarmStartupDelay = 2 * time.Minute
	// maxWarmListsPerSweep bounds `list -files` invocations in one sweep so a
	// cold cache (first run after deploy) can't pin the maintenance lane for
	// hours. Anything skipped this sweep is warmed on a later sweep or lazily
	// on first open. We log when the cap bites (no silent truncation).
	maxWarmListsPerSweep = 200
)

// runListCacheWarmer drives the list-files warm cache from three triggers:
//   - edge: a warmDirty nudge on backup/copy/prune completion (debounced so the
//     nightly burst collapses to one sweep that runs after the wave settles);
//   - level: a periodic ticker (ListCacheWarmInterval) — the SLA safety net that
//     re-warms newest-N even when a completion event is missed; and
//   - startup: one sweep shortly after boot to recover anything created/evicted
//     while the agent was down.
//
// A single goroutine ⇒ sweeps never overlap and the `list` subprocesses never
// fan out across the host; each `list` additionally takes a maintenance-semaphore
// slot (see runListGated) so warming queues behind/beside check+prune rather
// than piling onto the wave.
func (a *app) runListCacheWarmer(ctx context.Context) {
	startup := time.NewTimer(listWarmStartupDelay)
	defer startup.Stop()

	var tickC <-chan time.Time
	if a.cfg.ListCacheWarmInterval > 0 {
		ticker := time.NewTicker(a.cfg.ListCacheWarmInterval)
		defer ticker.Stop()
		tickC = ticker.C
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-startup.C:
			a.warmListCacheSweep(ctx)
		case <-tickC:
			a.warmListCacheSweep(ctx)
		case <-a.warmDirty:
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
}

// runListGated runs a warm `list`/`list -files` invocation while holding a
// maintenance-semaphore slot, so background warming shares the same concurrency
// budget as check+prune and never competes with the nightly wave on the
// RAM-constrained NAS. Returns ctx.Err() if cancelled while queued.
func (a *app) runListGated(ctx context.Context, inv cliInvocation, timeout time.Duration) ([]byte, error) {
	release, ok := a.jobs.acquireMaint(ctx)
	if !ok {
		return nil, ctx.Err()
	}
	defer release()
	return runSync(ctx, a.cfg.DuplicacyBinary, inv, timeout)
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
	var warmed int
	capped := false
	// Accumulated across the whole sweep, then reconciled ONCE at the end (see
	// reconcileAgainstKeep): swept = storages we successfully listed; keep =
	// every currently-existing (storage, snapshot_id, revision) discovered.
	swept := map[string]struct{}{}
	keep := map[string]struct{}{}

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

			// 1. List live revisions on this storage (cheap — downloads only the
			//    snapshot index). Scoped per warmListArgs: own id for default, all
			//    ids for relay secondaries.
			live, err := a.warmListSnapshots(ctx, repo, storage, keyPath, env)
			if err != nil {
				slog.Warn("list-cache warm: list failed", "repo", repo.ID, "storage", storage, "error", err)
				continue
			}
			// Record what exists so the end-of-sweep reconcile keeps it. Only a
			// successfully-listed storage is marked swept (a transient list error
			// never purges that storage's cache).
			swept[storage] = struct{}{}
			for _, sn := range live {
				keep[storageSnapRevKey(storage, sn.SnapshotID, sn.Revision)] = struct{}{}
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

	// One reconcile against the whole sweep's keep-set: removes pruned revisions
	// AND any snapshot this node no longer warms (e.g. foreign snapshots an edge
	// node used to over-warm via -all), without multi-repo thrashing.
	reconciled, rerr := a.filesCache.reconcileAgainstKeep(ctx, swept, keep)
	if rerr != nil {
		slog.Warn("list-cache reconcile failed", "error", rerr)
	}
	if err := a.filesCache.evictBySize(ctx); err != nil {
		slog.Warn("list-cache evict failed", "error", err)
	}
	rows, gzBytes, _ := a.filesCache.stats(ctx)
	slog.Info("list-files cache warm sweep done",
		"warmed", warmed, "reconciled", reconciled, "capped", capped,
		"rows", rows, "gz_bytes", gzBytes)
}

// warmListArgs builds the discovery `duplicacy list` args that decide WHICH
// snapshots a sweep warms on a given storage:
//
//   - default (the shared local pool every host SFTPs into): scope to the
//     repo's OWN snapshot id. `list -all` here would return the entire fleet's
//     snapshots, but a node only ever serves routine-restore listings for its
//     OWN repos (default listings are served by the source node, not the relay),
//     so warming the whole pool just wastes cache + warm-time on every node
//     (notably the SD-card pi). Fall back to -all only if the repo has no id.
//   - a relay secondary (remote-nas / storj): keep -all. These storages exist
//     only on the NAS relay, which serves relay-files for EVERY pooled source,
//     so all ids must be enumerated.
func warmListArgs(storage, keyPath, ownSnapshotID string) []string {
	args := []string{"list"}
	if keyPath != "" {
		args = append(args, "-key", keyPath)
	}
	if storage == "" || storage == "default" {
		if ownSnapshotID != "" {
			args = append(args, "-id", ownSnapshotID)
		} else {
			args = append(args, "-all")
		}
	} else {
		args = append(args, "-all")
	}
	if storage != "" && storage != "default" {
		args = append(args, "-storage", storage)
	}
	return args
}

// warmListSnapshots runs the discovery `duplicacy list` for a (repo, storage)
// and returns the parsed revisions to consider for warming. See warmListArgs
// for the own-id vs -all scoping.
func (a *app) warmListSnapshots(ctx context.Context, repo *Repo, storage, keyPath string, env []string) ([]Snapshot, error) {
	out, err := a.runListGated(ctx, cliInvocation{
		RepoRoot: repo.Path, Args: warmListArgs(storage, keyPath, repo.SnapshotID), EnvAdds: env,
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
	out, err := a.runListGated(ctx, cliInvocation{
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
