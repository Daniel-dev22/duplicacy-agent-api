# duplicacy-agent-api

Go REST + WebSocket wrapper around the open-source [duplicacy](https://github.com/gilbertchen/duplicacy) CLI. Runs as a docker-compose container on every Docker host and is fronted by a per-host Traefik route. The controller frontend's `/duplicacy` page fans out to every agent through the controller router.

## Architecture summary

- **One agent per Docker host** (kd-nuc01, kd-nuc02, ng-nuc01, ng-pi01, ...). Bind-mounts existing duplicacy backup roots and the persistent config dir.
- **Per-node Traefik label** at `duplicacy-agent-api.{hostname-short}.{domain}` — same pattern as `portainer-agent.{node}.{domain}`. K8s side has one ExternalName Service per node so the central router can address agents by stable name.
- **Push/pull split**:
  - Job lifecycle events → push to controller (mTLS, with SQLite buffer fallback when controller is unreachable).
  - Live operational queries (running jobs, repos, snapshots) → pulled on demand from the agent.
- **Agent owns scheduling**. Schedules originate in controller (Postgres), are pushed to the affected agent, and fire from a 1-min ticker inside the agent. Backups continue running through cluster outages.
- **Global filter layer**. Org-wide and site-wide filter sets fetched from controller are merged with per-repo filters at backup time — define `node_modules/`, `.git/`, `*.tmp` once for the whole fleet.

Full design: `/home/user/.claude/plans/yes-i-like-it-fluttering-patterson.md`.

## Files

| File | Purpose |
|------|---------|
| `main.go` | HTTP entrypoint, Gin server, route registration, graceful shutdown |
| `config.go` | Env-var driven `Config` |
| `app.go` | Wires together all subsystems; placeholder handlers |
| `network.go` | Process-global pooled `controlCenterClient` (mTLS, no per-request clients) |
| `repos.go` | Scans bind-mounted backup roots for `.duplicacy/preferences` |
| `duplicacy.go` | `exec.Cmd` wrapper around the duplicacy CLI |
| `jobs.go` | In-memory job registry + WebSocket log streaming |
| `events.go` | Push events to controller with SQLite buffer fallback |
| `schedules.go` | Local scheduler mirroring `scheduler-api`'s `scheduleMatches()` |
| `filters.go` | Org/site/per-repo filter cache + merge at backup time |

## Required env vars

| Var | Example | Purpose |
|-----|---------|---------|
| `NODE_NAME` | `nuc02` | Hostname short form (matches Traefik label) |
| `SITE_ID` | `kd` \| `ng` | Site identifier |
| `BACKUP_ROOTS` | `/backuproot/path1,/backuproot/path2` | Comma-separated bind-mount paths to scan for repos |
| `CONFIG_DIR` | `/var/lib/duplicacy-agent-api` | Persistent dir (events.sqlite, schedule cache, filter cache) |
| `CONTROL_CENTER_URL` | `https://controller-api-internal.example.com:1443` | Push target for events / pull source for schedules + filters |
| `CONTROL_CENTER_CA` | `/etc/duplicacy-mtls/ca.crt` | CA bundle for verifying controller's server cert |
| `CLIENT_CERT` | `/etc/duplicacy-mtls/tls.crt` | mTLS client cert for event push |
| `CLIENT_KEY` | `/etc/duplicacy-mtls/tls.key` | mTLS client key |
| `DUPLICACY_BINARY` | `/usr/local/bin/duplicacy` | Override CLI path (defaults to baked-in binary) |

## Build

```bash
ansible-playbook /home/user/ansible/kubernetes/build-deploy.yml \
  -e project=duplicacy-agent-api -e target_servers=kd-nas01,ng-nas01
```

Local development:

```bash
cd /srv/containers/duplicacy-agent-api
go build ./...
```
