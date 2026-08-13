# duplicacy-agent-api

A single-host sidecar daemon that owns [duplicacy](https://github.com/gilbertchen/duplicacy)
backups for one machine.

It runs the duplicacy CLI as subprocesses, parses its human-readable stdout into structured
progress and summary stats, persists jobs and their logs to a local SQLite database, fires its
own cron schedules, and reports upward to a central controller over HTTP.

The design goal is that **the machine keeps backing up correctly while the controller is down.**
Schedules are cached to disk and fire from a local ticker; job lifecycle events queue in a
durable outbox and drain when the controller returns; the repo registry, filter sets, snapshot
statistics and per-job logs all live on local disk. The controller is where you *manage* backups
from, not something the backups depend on.

One agent per host. A fleet is N independent agents plus a controller that aggregates them.

---

## ⚠ Security

Read this before exposing the agent to anything.

**There is no inbound authentication or authorization in this process.** `main.go` installs
exactly two pieces of middleware — `gin.Recovery()` and a request logger — and no auth handler.
Every one of the 40 routes below, including the mutating ones and `/internal/*`, is
**unauthenticated at the application layer**. Anyone who can open a TCP connection to port 8080
can start a restore, delete a repo's duplicacy state, cancel a running backup, or read the job
logs.

The deployment this was written for puts the agent behind a reverse proxy that terminates
**mTLS** and only forwards requests carrying a valid client certificate. That proxy is the entire
access control story. If you run this, you must provide the equivalent.

Two more things worth stating plainly:

- **The bearer token is outbound-only.** `BEARER_TOKEN_FILE` is read once at startup and attached
  as `Authorization: Bearer …` to requests the agent makes *to the controller*, so the controller
  can identify which node is asking for credentials. It is never checked on inbound requests.
- **The process runs as root and reaches the whole filesystem under `BACKUP_ROOTS`.** It needs to
  read arbitrary files to back them up and write arbitrary files to restore them. A restore
  request chooses its own destination path. There is no sandbox.

Credentials are handled carefully in one respect: they are never written to the container's
persistent filesystem. See [Secret vending](#secret-vending).

---

## Architecture

Single flat `package main`. One file per concern.

**Entrypoint and wiring**

| File | Role |
|---|---|
| `main.go` | Process entrypoint: logging, config, boot-gap report, `newApp`, orphan sweep, route registration, `:8080` listener, graceful shutdown on SIGINT/SIGTERM. |
| `app.go` | The `app` struct that owns every subsystem; registers job hooks (event push, stats flush, cache warm nudge, fleet broadcast) and starts the 5-minute job-state reconcile. |
| `config.go` | `Config` + `loadConfig()` — every env var, and the startup read of the bearer token file. |
| `logging_setup.go` | Optional rotating file sink at `$LOG_DIR/agent.log` (20 MB × 5, 14 days, gzipped) teed alongside stderr. |
| `heartbeat.go` | 30s liveness stamp to `heartbeat.last`; on boot, reports how long the previous instance was down. |

**Scheduling**

| File | Role |
|---|---|
| `schedules.go` | Adapter over the shared scheduler: deterministic per-(node, schedule) fire jitter and the callback that turns a schedule into a duplicacy job. |
| `chain.go` | "After-wave" maintenance chain — once the nightly backup+copy wave drains on this host, fire prune schedules, then check schedules. Per-day markers on disk so a restart mid-chain resumes rather than re-runs. |

**Job store and execution**

| File | Role |
|---|---|
| `jobs.go` | The job engine: spawn, stream, cancel, concurrency semaphores for copy and maintenance, every stdout parser, and the action HTTP handlers. |
| `duplicacy.go` | The CLI layer — argument builders for backup/restore/check/prune/copy/init/add, output parsing, and `scrubPreferences`. |
| `job_logs.go` | Durable per-job logs: flush the ring buffer to `job-logs/<id>.log` on terminal state, prune to the newest 200 at boot. |
| `check_tabular.go` | Line parser for `duplicacy check -tabular`, producing per-revision dedup rows. |
| `orphan_sweep.go` | At boot, mark jobs the previous container left in running/pending as terminal and emit synthetic terminal events. |
| `reconcile.go` | Every 5 min: fetch this node's repos from the controller, wipe local `.duplicacy/` for entries it no longer knows, and repopulate the local registry. |

**Event outbox**

| File | Role |
|---|---|
| `events.go` | Owns `events.sqlite` — schema creation, the durable outbox that POSTs job lifecycle events, and local job persistence/queries. |

**Fleet WebSocket hub**

| File | Role |
|---|---|
| `fleet_ws.go` | `/ws/fleet` — builds the node's snapshot (host info, repos with last-backup times, recent jobs) and pushes it on change, with a 30s ping. |

**Secret vending**

| File | Role |
|---|---|
| `secrets.go` | `SecretsBundle`, the `DUPLICACY_*` env construction, `/dev/shm` materialisation of key material, PKCS#8→PKCS#1 normalisation, and the TTL cache. |
| `secrets_runtime.go` | `prepareEnvForRepo` — resolve every storage of a repo, vend each credential, build env + key paths + a cleanup closure. Fails loudly; never falls back to on-disk credentials. |
| `network.go` | The process-global pooled controller HTTP client (direct or proxy-rewrite dial mode) and the vend call with its retry/permanence classification. |

**Repo registry**

| File | Role |
|---|---|
| `repos.go` | `Repo`/`Storage` types and the registry-backed index; loads `.duplicacy/preferences` and computes the stable repo ID. |
| `repos_mapping.go` | The `repos.json` store — repo path → repo id, UUID, and storage-alias → credential id. The source of truth for which repos exist. |
| `init_handler.go` | `POST /repos/init` (create) and `POST /repos/bind` (adopt an existing repo), plus credential-cache invalidation. |
| `delete_handler.go` | `POST /repos/delete` — remove `.duplicacy/` and drop the registry entry; 409 while a job is running. |
| `compose.go` | Bounded read-only scan of `COMPOSE_SCAN_ROOTS` for docker-compose project directories. |

**Filters**

| File | Role |
|---|---|
| `filters.go` | Org/site/repo filter sets: pull, cache, merge by scope precedence, anchor absolute patterns to the repo root, and render `.duplicacy/filters`. |

**Trees and sizes**

| File | Role |
|---|---|
| `trees.go` | The 5-minute filesystem tree push — mtime-cached, depth- and file-capped walks of repo source paths and backup roots. |
| `tree_sizes.go` | Persisted per-directory recursive size cache and the self-paced gatherer that fills it, decoupled from the push. |

**Snapshot stats and caches**

| File | Role |
|---|---|
| `snapshot_stats.go` | Read/write over the `snapshot_stats` table; powers the per-repo and per-node storage rollups. |
| `snapshot_files_cache.go` | SQLite cache of gzipped `duplicacy list -files` output keyed by the immutable (snapshot id, revision, storage), with size-capped LRU eviction that pins the warm set. |
| `snapshot_files_warm.go` | The warm driver: debounced edge sweeps after backup/copy/prune, a periodic level sweep, and one at startup. |
| `destination.go` | Reduces a storage URL to a stable "same physical destination" key plus an operator-facing label, so repos sharing a bucket aggregate into one series. |

**Storage URL handling**

| File | Role |
|---|---|
| `template.go` | Placeholder expansion at init/add time, and SFTP absolute-path normalisation. |
| `storage_url_validator.go` | Per-backend URL shape checks applied on every vend. |
| `s3region.go` | Detects an S3 URL with a custom endpoint but no region. |

**Plumbing**

| File | Role |
|---|---|
| `known_hosts.go` | Agent-managed SSH `known_hosts` for SFTP storages — trust on first contact, persisted across restarts. |
| `sftp_mkdir.go` | Creates the remote storage directory over SFTP before `duplicacy init`, because the storage user is usually shell-less. |

---

## HTTP API

The agent listens on **`:8080`**, plain HTTP, inside its container. 40 routes.

Every route is unauthenticated — see [Security](#-security).

### Health (2)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/health/live` | Always 200 while the process is up. |
| `GET` | `/health/ready` | Always 200. Used by the container `HEALTHCHECK`. |

### Repos and registry (6)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/repos` | List repos from the local registry. |
| `GET` | `/compose-projects` | Docker-compose project directories found under `COMPOSE_SCAN_ROOTS`. |
| `POST` | `/repos` | **501 Not Implemented.** Use `/repos/init`. |
| `POST` | `/repos/init` | Create a repo: validate the path, `duplicacy init`/`add`, scrub preferences, persist the mapping. |
| `POST` | `/repos/bind` | Adopt an existing on-disk duplicacy repo into the registry. |
| `POST` | `/repos/delete` | Remove `.duplicacy/` and drop the registry entry. 409 while a job is running on it. |

### Preferences (2)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/repos/:id/preferences` | The repo's `.duplicacy/preferences`, secrets scrubbed. |
| `PUT` | `/repos/:id/preferences` | **501 Not Implemented.** |

### Snapshots and stats (6)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/repos/:id/snapshots` | `duplicacy list` for the repo. |
| `GET` | `/repos/:id/snapshots/:rev/files` | `duplicacy list -files`, served from the persistent cache when warm. |
| `GET` | `/repos/:id/snapshot-stats` | Per-revision dedup rows collected from `check -tabular`. |
| `GET` | `/repos/:id/storage-rollup` | This repo's usage rolled up per destination. |
| `GET` | `/storage-rollup` | The whole node's usage rolled up per destination. |
| `GET` | `/storage-rollup/repos` | Per-repo breakdown behind the node rollup. |

### Actions (6)

All six are **asynchronous**: they validate, start a job, and return **202** with a `job_id`.
Follow progress on `/jobs/:id` or `/ws/jobs/:id/logs`.

| Method | Path | Notes |
|---|---|---|
| `POST` | `/repos/:id/backup` | |
| `POST` | `/repos/:id/restore` | Destination is `scratch` (under `RESTORE_SCRATCH_ROOT`) unless overridden. |
| `POST` | `/repos/:id/check` | |
| `POST` | `/repos/:id/prune` | |
| `POST` | `/repos/:id/prune/preview` | Same body as `/prune` plus `-dry-run`: reports the revisions it would delete and the chunks that would become unreferenced, without touching storage. |
| `POST` | `/repos/:id/copy` | |

### Filters (5)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/repos/:id/filters` | This repo's own rules. |
| `PUT` | `/repos/:id/filters` | Replace this repo's rules and re-render `.duplicacy/filters`. |
| `POST` | `/repos/:id/filters/render` | Preview the merged, anchored filter file without writing it. |
| `GET` | `/global-filters/cache` | The cached org+site filter sets. |
| `POST` | `/global-filters/refresh` | Force a pull from the controller. |

### Storages (3)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/storages` | **501 Not Implemented.** |
| `POST` | `/storages` | **501 Not Implemented.** |
| `DELETE` | `/storages/:name` | **501 Not Implemented.** |

Storages are configured through the controller and arrive with the credential bundle; there is
no local storage CRUD.

### Jobs (4 + 2 WebSocket)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/jobs` | Live jobs plus recent persisted ones. |
| `GET` | `/jobs/:id` | One job with its current progress. |
| `GET` | `/jobs/:id/log` | Durable log from `job-logs/<id>.log`; falls back to the in-memory ring for a running job. |
| `POST` | `/jobs/:id/cancel` | Signal the subprocess. |
| `GET` | `/ws/jobs/:id/logs` | WebSocket: stream a job's output live. |
| `GET` | `/ws/fleet` | WebSocket: this node's snapshot, pushed on change, 30s ping. |

### Schedules (3)

| Method | Path | Notes |
|---|---|---|
| `GET` | `/schedules` | Schedules currently loaded. |
| `POST` | `/schedules/refresh` | Nudge an immediate pull from the controller. |
| `GET` | `/schedules/cache` | The raw on-disk schedule cache. |

### Internal (1)

| Method | Path | Notes |
|---|---|---|
| `POST` | `/internal/credentials/:id/invalidate` | Drop one credential from the secret cache. 204. Called by the controller when a credential changes, so the agent does not serve a stale one for up to the cache TTL. |

---

## Storage URL templating

A credential's `storage_url` may contain placeholders, expanded once at `duplicacy init`/`add`
time using this node's identity. The resolved URL is what gets written into
`.duplicacy/preferences`; later backup/check/restore/prune runs never re-resolve.

| Placeholder | Expands to | Example |
|---|---|---|
| `{server}` | `NODE_NAME`, or a per-storage `server_override` when set | `nuc01` |
| `{server_type}` | `NODE_NAME` with trailing digits stripped | `nuc` |
| `{site}` | `SITE_ID` | `site-a` |
| `{home}` | `SITE_ID` + `"home"` | `site-ahome` |
| `{remote_home}` | `REMOTE_SITE` + `"home"`. **Unset ⇒ a template using this placeholder is rejected**, rather than silently resolving to the local site | `site-bhome` |
| `{repo_id}` | The repo's snapshot id, filled in per init | `nuc01-data` |

```
s3://US1@gateway.storjshare.io/{home}/{site}-{server_type}/duplicacy
  → s3://US1@gateway.storjshare.io/site-ahome/site-a-nuc/duplicacy
```

**An unknown placeholder is a hard error, not a pass-through.** `expandStorageURL` rejects any
remaining `{lowercase_identifier}` so a typo cannot ship a literal `{srvr}` directory to the
storage backend and silently start a second, wrong repo there.

One related normalisation: an `sftp://` URL whose path starts with a well-known system root
(`/mnt`, `/home`, `/var`, `/opt`, `/srv`, `/data`, `/backup`) is rewritten to the double-slash
form `sftp://host//mnt/…`, because a single slash is interpreted by SFTP servers as relative to
the SSH user's home directory. A path that already uses `//`, or one that looks deliberately
user-relative, is left alone.

---

## Secret vending

The agent holds no credentials on disk. It asks the controller for them at the moment it needs
to run duplicacy.

- **Fetch.** `GET {CONTROL_CENTER_URL}/api/duplicacy/credentials/{credential_id}/secrets-for-node/{node}`
  returns a bundle: `storage_url`, `storage_type`, `encryption_password`, and a backend-specific
  key map. Four attempts with 0 / 500 ms / 1 s / 2 s backoff; 401/403/404 are permanent failures,
  5xx are retried. Every returned URL goes through `ValidateStorageURL` before any caller sees it.
- **Cache.** In-memory, keyed by `credential_id`, TTL **60 s**. A `put` will not overwrite a newer
  entry with an older one. `POST /internal/credentials/:id/invalidate` drops a single entry, so a
  credential rotation takes effect immediately rather than after the TTL.
- **Env construction.** Each storage becomes `DUPLICACY_<VAR>` for the primary, or
  `DUPLICACY_<ALIAS>_<VAR>` for a secondary (alias uppercased). The primary always uses the bare
  prefix, even when its alias is not `default`, because `duplicacy init` names the primary
  preference `default` and looks its env vars up by preference name. Per backend:
  `PASSWORD` always; `B2_ID`/`B2_KEY`; `S3_ID`/`S3_SECRET`/`S3_TOKEN`;
  `SSH_KEY_FILE`/`SSH_KEY_PASSPHRASE`/`SSH_PASSWORD`; `GCS_TOKEN_FILE`;
  `AZURE_ACCOUNT`/`AZURE_KEY`. An unrecognised key for a storage type is a hard error, not a
  warning — the schema is closed on purpose.
- **Key material.** Anything that duplicacy wants as a *file* — SSH private keys, GCS service
  account JSON, RSA public/private keys — is written to a `0600` temp file under **`/dev/shm`**
  (tmpfs, RAM-backed, so the bytes never touch disk) and the path is passed in the env. Every job
  carries a cleanup closure that unlinks its temp files after the subprocess exits, including on
  the never-started path.
- **No on-disk fallback.** Every duplicacy invocation runs with `-no-save-password`, and
  `.duplicacy/preferences` is post-scrubbed (`keys` emptied, `no_save_password: true`). If
  vending fails, the job fails; the agent does not fall back to whatever might be on disk.
- **PKCS#8 → PKCS#1.** duplicacy 3.2.5 only parses PKCS#1 (`-----BEGIN RSA PRIVATE KEY-----`),
  but most tooling emits PKCS#8 (`-----BEGIN PRIVATE KEY-----`) by default, which fails at
  runtime with `Unsupported private key type PRIVATE KEY`. So an RSA private key is re-encoded to
  PKCS#1 on the way to its tmpfile. Input that is already PKCS#1, or that is encrypted, is passed
  through untouched.

---

## Configuration

All configuration is environment variables. `NODE_NAME`, `SITE_ID`, `BACKUP_ROOTS` and
`CONTROL_CENTER_URL` are **required** — the process logs and exits if any is unset. A readable,
non-empty bearer token file is also required at startup.

### Required

| Var | Meaning |
|---|---|
| `NODE_NAME` | Short host name this agent speaks for, e.g. `nuc01`. Used in every controller call and in `{server}`. |
| `SITE_ID` | Short id of the site this node belongs to, e.g. `site-a`. Scopes schedule and filter pulls. |
| `BACKUP_ROOTS` | Comma-separated absolute paths the agent may manage repos under. See [Mounts](#mounts-and-state) — path identity matters. |
| `CONTROL_CENTER_URL` | Base URL of the controller, e.g. `https://controller.example.com:1443`. |

### Controller connection

| Var | Default | Meaning |
|---|---|---|
| `BEARER_TOKEN_FILE` | `/etc/duplicacy-agent-api/bearer-token` | File holding this node's bearer token. Read once at startup; missing or empty is fatal. Trailing whitespace stripped. |
| `REMOTE_SITE` | *(empty)* | Peer site id, used only to expand `{remote_home}`. Leave unset for a single-site deployment — but if any `storage_url` uses `{remote_home}`, this is **required** and init fails loudly without it. |
| `TRAEFIK_DOCKER_DNS` | *(empty)* | When set, the dialer ignores the URL's host and connects to this DNS name instead, so a host-local reverse proxy can attach the client certificate. Leave empty for direct dialing. |
| `TRAEFIK_DIAL_PORT` | `1443` | Port used with `TRAEFIK_DOCKER_DNS`. |

### Paths

| Var | Default | Meaning |
|---|---|---|
| `CONFIG_DIR` | `/var/lib/duplicacy-agent-api` | Persistent state directory. See [Mounts](#mounts-and-state). |
| `DUPLICACY_BINARY` | `/usr/local/bin/duplicacy` | Path to the duplicacy CLI. |
| `RESTORE_SCRATCH_ROOT` | `/tmp/duplicacy-restore` | Where `target=scratch` restores land, as `<root>/<snapshot_id>-r<rev>/`, created `0700` at restore time. |
| `BACKUP_EXCLUDE_PATHS` | *(empty)* | Comma-separated path prefixes the repo scanner and tree walker skip entirely. For directories that contain a `.duplicacy/` but are not user-managed repos. duplicacy-web's own cache layout is excluded automatically. |
| `COMPOSE_SCAN_ROOTS` | *(empty)* | Comma-separated read-only paths scanned (depth ≤ 3) for docker-compose project directories. |
| `LEGACY_BACKUPROOT_MAP` | *(empty)* | `synthetic:real,…` rewrites of stale `repository=` values in on-disk preferences, applied in memory at load. Migration aid; empty once every repo has been re-inited. |

### Concurrency

| Var | Default | Meaning |
|---|---|---|
| `DUPLICACY_COPY_THREADS` | `2` | `copy -threads N` when the request or schedule does not pin its own. Deliberately low: each thread holds an in-flight chunk buffer, and a nightly fan-out runs many copies at once. |
| `DUPLICACY_MAX_CONCURRENT_COPIES` | `1` | Cap on simultaneous `duplicacy copy` processes. Excess queue as Pending. `≤0` disables the cap. |
| `DUPLICACY_MAX_CONCURRENT_MAINT` | `1` | Same, for maintenance (check + prune). `≤0` disables the cap. |

### `list -files` cache

| Var | Default | Meaning |
|---|---|---|
| `LIST_FILES_CACHE_ENABLED` | `true` | Master switch. Disabled ⇒ every call site no-ops. |
| `LIST_FILES_CACHE_WARM_N` | `5` | Newest-N revisions pre-listed per (snapshot, storage), and pinned against size eviction. |
| `LIST_FILES_CACHE_MAX_BYTES` | `1073741824` (1 GiB) | Ceiling on gzipped cache bytes. `≤0` disables size eviction. |
| `LIST_FILES_CACHE_WARM_INTERVAL` | `30m` | Periodic level-triggered warm sweep, in addition to the event-triggered one, so the cache self-heals after a missed completion event. `≤0` disables the tick; the startup sweep still runs. |

### Directory-size gatherer

| Var | Default | Meaning |
|---|---|---|
| `TREE_SIZE_ENABLED` | `true` | Master switch. |
| `TREE_SIZE_LARGE_FILE_THRESHOLD` | `50000` | Subtree file count above which a directory moves to the slow cadence. |
| `TREE_SIZE_SLOW_WALK` | `30s` | Last-walk duration above which a directory auto-demotes to the slow cadence. |
| `TREE_SIZE_LARGE_REFRESH` | `24h` | Cadence for large or slow directories. |
| `TREE_SIZE_SMALL_REFRESH` | `6h` | Cadence for normal directories. |
| `TREE_SIZE_WALK_TIMEOUT` | `30m` | Per-root walk ceiling; the walk resumes progressively on the next cadence. |
| `TREE_SIZE_STEP_SLEEP` | `2ms` | Pause per directory, to avoid hammering slow disks. |
| `TREE_SIZE_EXCLUDE_PATHS` | *(empty)* | Path prefixes never walked for size. Size-only: they still appear in the tree and are still backed up. |

### Logging

| Var | Default | Meaning |
|---|---|---|
| `LOG_LEVEL` | *(handler default)* | `debug` / `info` / `warn` / `error`. |
| `LOGUTIL_FORMAT` | `json` | Set to `text` for a human-readable handler. |
| `LOG_DIR` | *(empty)* | When set, tee logs to `<LOG_DIR>/agent.log` with rotation (20 MB × 5 backups, 14 days, gzipped) *in addition to* stderr, so `docker logs` keeps working. |

---

## Mounts and state

**Path identity is assumed throughout.** Every `BACKUP_ROOTS` entry must be bind-mounted at the
**same path inside the container as on the host**. duplicacy stores the repository path in
`.duplicacy/preferences`, the agent reports paths to the controller, and restores write back to
them; a mount that renames the path breaks all three. `-v /srv/data:/srv/data`, not
`-v /srv/data:/data`.

**`CONFIG_DIR`** must be a persistent volume. It holds everything that must survive a restart:

| Entry | Contents |
|---|---|
| `events.sqlite` (+ WAL/SHM) | Pending outbound events, jobs, snapshot stats, the `list -files` cache. |
| `repos.json` | The repo registry — path → repo id, UUID, storage alias → credential id. Credential-free. |
| `schedules.json` | Cached schedules, so the agent keeps firing while the controller is unreachable. |
| `filter-sets.json` | Cached org+site filter sets. |
| `per-repo-filters/` | One JSON file of rules per repo. |
| `job-logs/` | `<job_id>.log`, flushed on terminal state, pruned to the newest 200 at boot. |
| `known_hosts` | SSH host keys for SFTP storages, trusted on first contact. |
| `dir_sizes.json` | Per-directory recursive size cache. |
| `heartbeat.last` | Unix-nano liveness stamp; `0` on clean shutdown, so the next boot can tell a stop from a crash. |
| `chain-markers.json` | Per-day after-wave chain stage markers. |

Also required:

- **`/dev/shm` must exist and be writable.** Credential key material is materialised there and
  nowhere else; if it is unavailable, every job that needs a key file fails.
- **`RESTORE_SCRATCH_ROOT`** — bind-mount it if you want to inspect scratch restores from the
  host.
- **`/root/.ssh/known_hosts`** is created *inside the container* as a symlink into
  `CONFIG_DIR/known_hosts` at startup. Do not mount over it; mount `CONFIG_DIR` instead.
- **`LOG_DIR`**, if set, should be a mounted host directory — that is the point of it.

---

## Controller contract

The agent is not standalone: to be scheduled and to obtain credentials, it needs a controller
implementing these nine endpoints. If you adopt this agent, you are adopting the job of
providing them.

Every call goes through one pooled HTTP client and carries `Authorization: Bearer <token>`.
Base is `CONTROL_CENTER_URL`.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/duplicacy/credentials/{credential_id}/secrets-for-node/{node}` | Vend one credential bundle. |
| `POST` | `/api/duplicacy/jobs/{job_id}/event` | One job lifecycle event, from the durable outbox. The row is deleted on 2xx and retried otherwise. |
| `POST` | `/api/duplicacy/reconcile` | Every 5 min and at startup: this node's full authoritative job set (non-terminal plus last-24h terminals), so the controller can converge rows a lost event stranded in `running`. |
| `GET` | `/api/duplicacy/schedules?node=&site=` | This node's schedules. Cached to disk. |
| `GET` | `/api/duplicacy/filter-sets?node=&site=` | Org and site filter sets. Cached to disk. |
| `GET` | `/api/duplicacy/repos` | All repos the controller knows; the agent filters to its own node for orphan detection and registry repopulation. |
| `GET` | `/api/duplicacy/deleted-repos?node=` | Tombstones for the orphan sweep. A 404 is tolerated and read as "none". |
| `POST` | `/api/duplicacy/repo-trees` | One filesystem tree per repo, rooted at its source path — for building filter patterns in a UI. |
| `POST` | `/api/duplicacy/node-trees` | One tree per backup root — for a path picker when adopting a repo. |

The agent tolerates all of these failing. Events queue, schedules fire from cache, tree pushes
are dropped, and reconcile retries on its next tick.

---

## Build and run

Plain Go build and test work with no special setup:

```bash
go build ./...
go test ./...
```

**The container build needs a token**, because `github.com/Daniel-dev22/agent-kit-go` is a
private module. It is passed as a BuildKit secret named `ghtoken`, read by an inline credential
helper at `go mod download` time, so it never lands in `.gitconfig` or in any image layer — which
matters because the build cache is pushed to a registry:

```bash
docker buildx build --secret id=ghtoken,env=GITHUB_TOKEN -t duplicacy-agent-api .
```

The builder stage also **downloads the duplicacy CLI and asserts its architecture**, which is
worth knowing about if you touch the Dockerfile. The fetch happens in the *builder* stage, not
the runtime stage, because BuildKit's `TARGETARCH` resolved to `amd64` even for arm64 builds in
the setup this was written for — shipping an x86 duplicacy into an arm64 image, which fails at
runtime with exec-format-error on a real ARM host. The builder runs under the target's emulation,
so `go env GOARCH` reports the target authoritatively. Having downloaded the matching release,
the build then reads **byte 18 of the ELF header** (`e_machine`: `0x3e` for x86-64, `0xb7` for
aarch64) and fails if it disagrees. Reading the header rather than executing the binary is the
point — under emulation, running it would succeed either way and prove nothing.

Runtime image is `alpine:3.21` with a `HEALTHCHECK` on `/health/ready`.

---

## Testing

```bash
go test ./...
```

22 test files, 72 top-level tests (203 including subtests). **Fully hermetic** — no live
services, no network, no
environment-gated tests, nothing to skip. Everything either operates on in-process data
structures, on captured CLI output fixtures, or on a `t.TempDir()`.

Coverage concentrates on the parts where a silent wrong answer is expensive:

- **CLI output parsing** — backup/copy progress lines, check and prune counters, the
  `check -tabular` table, `list` output, error-line extraction, and the summary-stats regex,
  including the with- and without-`-log` prefix forms.
- **Argument construction** — that each action builds the duplicacy invocation it is supposed to,
  including `-dry-run` for prune preview and the thread flags.
- **Filter anchoring** — that absolute stored patterns are rewritten to repo-relative form,
  that a sibling-prefix path does not match, and that patterns for other repos are dropped rather
  than mis-anchored. This is regression coverage for a bug in which every exclusion rule was
  silently inert.
- **Storage URLs** — placeholder expansion including the unknown-placeholder rejection, SFTP
  double-slash normalisation, per-backend shape validation, S3 region detection, and destination
  key/label derivation (in particular that two repos pointing at one bucket produce one key).
- **Secrets** — the `DUPLICACY_*` prefix rules for primary vs aliased storages, and PKCS#8 →
  PKCS#1 conversion.
- **Concurrency** — that the copy and maintenance semaphores actually queue rather than run.
- **Persistence** — job round-trips through SQLite, job-log flush and retention, and the
  `list -files` cache's eviction and warm-set pinning.
- **Scheduling** — fire-jitter determinism and bounds.

---

## Related projects

- [agent-kit-go](https://github.com/Daniel-dev22/agent-kit-go) — the shared Go module these
  agents are built on: scheduler, fleet hub, job store, durable event outbox, reconcile loop,
  logging, and the router HTTP client.

Sibling agents built the same way:

- [build-agent](https://github.com/Daniel-dev22/build-agent)
- [docker-agent](https://github.com/Daniel-dev22/docker-agent)
- [gdrive-agent](https://github.com/Daniel-dev22/gdrive-agent)
- [filemesh-agent](https://github.com/Daniel-dev22/filemesh-agent)

---

## License

MIT — see [LICENSE](LICENSE).
