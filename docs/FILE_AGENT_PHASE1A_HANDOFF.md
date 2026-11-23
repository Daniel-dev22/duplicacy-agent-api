# File-agent Phase 1a handoff: size gatherer and mount guard hoisted into agent-kit-go

Status: built, tested, Pi-measured, reviewed (see Review). **Not merged, not tagged, not deployed**: that waits for a relayed go from the orchestrator.

## What changed

| Repo | Branch | SHA | Content |
|---|---|---|---|
| agent-kit-go | `feat/fileagent-1a` | `58a3743` | new package `dirsize` (Gatherer, Cache, Options, mount guard, PathUnderAny), `Entry.Incomplete` (json `i,omitempty`), exported `Gatherer.RunPass`, `Cache.Put/Get/Range/Len`, test-support package `dirsize/dirsizetest` |
| duplicacy-agent-api | `feat/fileagent-1a` | `9b8da50` | `tree_sizes.go`, `mountguard.go` and their tests deleted; `size_options.go` is the whole duplicacy seam (`sizeOptions`, `sizeExcludePaths`, `sizeAnchors`, `newSizeCache`) |
| gdrive-agent | none | none | has no size/tree code, so it is unaffected and is not bumped (still on kit v0.5.1) |

Duplicacy was first moved from kit v0.5.1 to v0.9.2 on its own (`cf55bae`, an advisor-required prerequisite) so the hash and Pi gates attribute cleanly, then pinned to the kit pseudo-version for `58a3743`. **No `replace` is committed.** At release the pseudo-version is swapped for the real tag (a new package is a minor bump, `v0.10.0`).

The three `trees.go` tests (`TestWalkDirFileSizes`, `TestWalkDirTruncatedSkipsFileStats`, `TestAnnotateSizes`) stay in duplicacy (`trees_sizes_test.go`), because they exercise `trees.go`, not the gatherer.

## Gates

**Hashes unchanged.** Pinned goldens did not move: kit oracle `31ca124a...a32727ba`, duplicacy wired pass `ff9b3ed9...ce924`. Real-tree digest on the Pi for `/usr/lib/python3` is identical before and after: `dirs=1528 digest=85c70f33c82d61547d19fbfe22c1947a8cadad90ab99e294a83b04f41668d12f`.

**Pi cost unchanged** (kd Pi 4 Model B 4c/8GB, arm64 `go test -c`, `taskset -c 3`, tree 8x4x12 = 4681 dirs, ~56k files; old and new binaries interleaved to cancel load drift; raw lines in `pi-after.out`, baseline in `baseline-numbers.md`, both under the session scratch dir):

| | BEFORE (old, dd44daf) | AFTER (new, 9b8da50) |
|---|---|---|
| wired pass ns/op | 1.360 to 1.569 s (median 1.414 s) | 1.382 to 1.628 s |
| allocs/op | 731,788 | 731,790 to 731,794 |
| B/op | 65.97 MB | 66.21 MB (+0.37 %) |
| cpu user / sys | 5.9-6.4 / 3.1-3.6 s | 5.95-6.43 / 3.14-3.54 s |
| RSS | 20.7-21.5 MB | 20.9-22.2 MB |
| kit-shaped walk | 656.7-827 ms, 290,870 allocs | 591.8-801.7 ms, 290,866 allocs |

The ranges overlap run for run and the allocation count is the same to within 6, so the hoist is cost-neutral; the +0.24 MB per pass is the new `Entry` field and is inside the noise of the per-run RSS.

## Contract impact (API consumer check)

`annotateSizes` does not surface `Incomplete`, so the pushed tree payload is byte-identical: **Ansible, system_monitor and the Control Center CLI are unaffected** (evidence: wired golden and the real-tree digest above). No control-center API, route or WebSocket change.

Cache compatibility: no filename bump. `Entry` field `i` is `omitempty`, so an old `dir_sizes.v2.json` loads as complete and an old agent ignores `i`.

## Phase 2 pointers

- Chunk/transfer code that must be REUSED, never reimplemented: `/docker_container_volumes/agent-repos/filemesh-agent/internal/content/{service.go,routes.go}` and `internal/files/{service.go,routes.go}`, plus the kit's `storage/chunkstage.go` (`ChunkStage`, bitmap sidecar).

## Deferred (each its own measured commit, not in 1a)

1. `ExcludePaths` is merged per child (`sizeExcludePaths` is called for every directory entry): about +0.72 s per pass and +441k allocs on the Pi (wired 1.41 s against kit-shaped 0.68 s). Fix is to evaluate once per pass; trap: a pushed filter then takes effect on the next pass, not mid-pass.
2. `filepath.Join` runs before the `IsDir` check, so a path is built for every file (290k allocs). Trap: moving it after `IsDir` would silently drop file-level excludes and the web-cache skip, because `PathUnderAny` and `SkipPath` also run on files.

## Test support worth knowing

`dirsize/dirsizetest` imports only std and nothing from `dirsize`, so the kit's internal tests and any consumer's wiring test share one tree builder, one independent oracle and one digest. A new consumer wires `sizeOptions`-style options and reuses it; do not copy the helpers. `size_options_test.go` fails when the kit adds a scalar `Options` field that the seam does not populate.

## Review (Opus, diff-only, one round, frozen SHAs kit `58a3743` / duplicacy `9b8da50`)

Verdict: **no HIGH, 2 MED, 9 LOW**; the hoist is faithful (mount guard rename-only, cache and walker logic-identical, seam fully mapped, every deleted test ported, no new third-party import). Full report: `review-r1.md` in the session scratch dir. No second round: round 1 found no HIGH.

Fixed in kit `1f026d6` and duplicacy (re-pinned to it, `go mod tidy` run):

| Finding | Fix |
|---|---|
| MED-2 `EnsureBenchTree` cached a `b.TempDir()` in a process-wide `sync.Once`, so the second benchmark run measured a deleted tree | no process state; rebuilt per call; `-bench DirSizeWalk -count=2` completes |
| LOW-1 exported `RunPass` could run alongside `Start`'s loop | pass mutex; `TestRunPassIsSingleFlight` (clean under `-race`) |
| LOW-2 `Put` before `Start` is discarded by the load | documented on `Put` and `RunPass` |
| LOW-3 a cache file holding `null` panicked on the first write | loads cold; `TestNullCacheFileLoadsCold` |
| LOW-5 stale duplicacy-specific doc text in the kit | corrected |
| LOW-6 `sizeOptions(cfg, nil)` panicked | nil-safe Stop; `TestSizeOptionsNilAppDoesNotPanic` |
| LOW-8 `Incomplete` controls too narrow | `TestDeliberateSkipsNeverSetIncomplete` (prefix, SkipPath, basename), `TestReadableSiblingStaysComplete` |
| LOW-9 stale `go.sum` entries | `go mod tidy` |

Recorded, not code changes:

- **MED-1 (MERGE GATE): duplicacy pins a pseudo-version of an untagged kit branch commit.** If the kit branch is rebased, squash-merged or deleted, that commit is unreachable and a clean build fails with "unknown revision". **Merge order is: kit branch to kit `main`, tag it (`v0.10.0`, a new package is a minor bump), move duplicacy's pin to that tag, THEN merge duplicacy.** Tagging waits for the relayed go.
- **LOW-4** the startup log key `exclude_paths` now prints the merged env + backup + pushed list instead of the env list only. Log-only, and the merged list is what the walker honours.
- **LOW-7** the goldens hash (relative path, bytes, files), so they do not see the `Incomplete` flag or the on-disk bytes; the flag is guarded by the kit's `incomplete_test.go` and `review_fixes_test.go`.
- **Release scope of the pin jump** (found by the review): moving duplicacy from kit v0.5.1 to the new pin brings about 6.7k lines of unrelated, almost entirely additive kit code (`eventoutbox`, `reconcile`, `intentq`, `sdnotify`, `unixhttp`) into the build; three deletions repo-wide. Verified by duplicacy's full `go test ./...`. The release reviewer should not assume "only dirsize moved".
- Out-of-scope observation confirmed by the review: the per-entry `ExcludePaths` hook call is most of the 731,788 allocations per pass; see Deferred.

The Pi numbers above were taken at kit `58a3743` / duplicacy `9b8da50`. The follow-up changes only add a mutex acquisition per pass, a nil guard and tests, so the per-pass cost is not expected to move; re-take the wired benchmark with the rig in `pi-bench2.sh` if the release reviewer wants numbers at the final SHAs.
