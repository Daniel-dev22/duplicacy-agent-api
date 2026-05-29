package main

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeName    string   // hostname-short this agent runs on (e.g. "nuc02")
	SiteID      string   // "kd" | "ng"
	BackupRoots []string // host paths to scan for .duplicacy repos (mirrored: host == container)
	// BackupExcludePaths are path prefixes the repo scanner and tree walker
	// skip entirely. Use to suppress dirs that contain a .duplicacy/ but are
	// not user-managed repos — e.g. the Duplicacy Web Edition data dir
	// (/srv/containers/duplicacy). The duplicacy-web cache layout
	// (…/cache/localhost/N/.duplicacy) is excluded automatically regardless,
	// so this is only needed to prune additional host-specific paths.
	BackupExcludePaths []string
	// LegacyBackuprootMap rewrites stale on-disk preferences that still say
	// `repository=/backuproot/pathN/...` in memory at preferences-load time.
	// Populated from LEGACY_BACKUPROOT_MAP env var (e.g.
	// "/backuproot/path1:/home/user,/backuproot/path2:/srv/containers").
	// Empty when every repo has been re-init'd against the mirrored layout —
	// at which point this field, the env var, and rewriteLegacyBackuproot can
	// be deleted. No bearing on any other code path.
	LegacyBackuprootMap map[string]string
	ComposeScanRoots    []string // bind-mounted (read-only) paths to scan for docker-compose project dirs
	ConfigDir           string   // persistent state dir (events.sqlite, schedule cache, filter cache)
	ControlCenterURL    string   // The agent always reaches controller-api via the host's
	// Traefik (or k3s Traefik on cluster nodes). Traefik handles
	// mTLS at the boundary; this code does not load or present any
	// client certificate. Per-node identity is conveyed via the
	// BearerToken loaded below.
	TraefikDockerDNS string // Docker DNS name to resolve to local Traefik (default "traefik")
	TraefikDialPort  string // port to dial on the resolved Traefik IP (default "1443")
	DuplicacyBinary  string // path to duplicacy CLI (default /usr/local/bin/duplicacy)
	BearerTokenFile  string // path to file containing this node's bearer token (per-host)
	BearerToken      string // populated at startup from BearerTokenFile contents
	// RestoreScratchRoot is where target=scratch restores land — agent expands
	// the sentinel to <root>/<snapshot_id>-r<rev>/ at handleRestore time. The
	// directory is created at restore time (mode 0700) inside the agent
	// container; operator may bind-mount this path read-write to a host dir
	// to inspect restored files outside the container.
	RestoreScratchRoot string

	// CopyThreads is the duplicacy `copy -threads N` default used when a copy
	// schedule/request does not pin its own thread count (the common case —
	// every auto-generated copy schedule passes 0). Kept deliberately low
	// (default 2, vs backup/restore's autoThreads = NumCPU/2) because each copy
	// thread holds an in-flight chunk buffer (≤ max-chunk 64M) plus codec
	// scratch, and the nightly fan-out runs many copies; a high per-copy thread
	// count multiplied across destinations is what OOM-killed the NAS.
	// Override via DUPLICACY_COPY_THREADS. An explicit per-schedule/request
	// `threads` still wins.
	CopyThreads int
	// MaxConcurrentCopies caps how many `duplicacy copy` processes run at once
	// on this host. The relay fans every source repo out to several
	// destinations on the same cron minute; without a cap all of them spike RAM
	// simultaneously and trip the kernel OOM-killer (witnessed 2026-05-29:
	// nightly NAS copies dying with "signal: killed" / exit 137). Excess copies
	// queue as Pending and run as slots free. Override via
	// DUPLICACY_MAX_CONCURRENT_COPIES; <=0 disables the cap (unbounded).
	MaxConcurrentCopies int

	// --- Directory-size gatherer (tree_sizes.go) ---
	// The gatherer is a self-paced background loop, fully decoupled from the
	// 5-min tree push: it fills a persisted per-directory size cache "as it
	// can", and the push only reads whatever sizes already exist (omitting the
	// rest). See tree_sizes.go for the scheduler/cadence semantics.
	TreeSizeEnabled            bool          // master switch
	TreeSizeLargeFileThreshold int64         // subtree file count above which a dir uses the daily cadence
	TreeSizeSlowWalk           time.Duration // last-walk duration above which a dir auto-demotes to daily
	TreeSizeLargeRefresh       time.Duration // cadence for large/slow dirs (≈1×/day)
	TreeSizeSmallRefresh       time.Duration // cadence for normal dirs (≈4×/day)
	TreeSizeWalkTimeout        time.Duration // per-root walk ceiling (resumes progressively next cadence)
	TreeSizeStepSleep          time.Duration // gentle pause per directory to avoid hammering slow disks
	// TreeSizeExcludePaths are path prefixes the size gatherer skips entirely.
	// Size-only: these still appear in the tree picker and are still backed up;
	// they're just not walked for a recursive size. Use for large/churny mounts
	// where sizing is wasteful — e.g. /var/lib/rancher/k3s on k3s nodes.
	TreeSizeExcludePaths []string
}

func loadConfig() Config {
	cfg := Config{
		NodeName:            requireEnv("NODE_NAME"),
		SiteID:              requireEnv("SITE_ID"),
		BackupRoots:         splitCSV(requireEnv("BACKUP_ROOTS")),
		BackupExcludePaths:  splitCSV(getEnv("BACKUP_EXCLUDE_PATHS", "")),
		LegacyBackuprootMap: parseMountMap(getEnv("LEGACY_BACKUPROOT_MAP", "")),
		ComposeScanRoots:    splitCSV(getEnv("COMPOSE_SCAN_ROOTS", "")),
		ConfigDir:           getEnv("CONFIG_DIR", "/var/lib/duplicacy-agent-api"),
		ControlCenterURL:    requireEnv("CONTROL_CENTER_URL"),
		// Empty default — k3s nodes leave this unset and use direct mode
		// (URL host resolved normally via Docker DNS, kube-proxy DNATs the
		// cluster ClusterIP). NAS hosts must explicitly set this to
		// "traefik" in their docker-compose env to opt into rewrite mode
		// where the dialer ignores the URL and connects to the local
		// docker Traefik so the host's mTLS ServersTransport can attach.
		// Earlier default of "traefik" silently broke k3s nodes — every
		// agent → controller call landed on the local Traefik (which
		// has no route for the cluster service hostname) and returned 404.
		TraefikDockerDNS:   getEnv("TRAEFIK_DOCKER_DNS", ""),
		TraefikDialPort:    getEnv("TRAEFIK_DIAL_PORT", "1443"),
		DuplicacyBinary:    getEnv("DUPLICACY_BINARY", "/usr/local/bin/duplicacy"),
		BearerTokenFile:    getEnv("BEARER_TOKEN_FILE", "/etc/duplicacy-agent-api/bearer-token"),
		RestoreScratchRoot: getEnv("RESTORE_SCRATCH_ROOT", "/tmp/duplicacy-restore"),

		CopyThreads:         getEnvInt("DUPLICACY_COPY_THREADS", 2),
		MaxConcurrentCopies: getEnvInt("DUPLICACY_MAX_CONCURRENT_COPIES", 1),

		TreeSizeEnabled:            getEnvBool("TREE_SIZE_ENABLED", true),
		TreeSizeLargeFileThreshold: int64(getEnvInt("TREE_SIZE_LARGE_FILE_THRESHOLD", 50000)),
		TreeSizeSlowWalk:           getEnvDuration("TREE_SIZE_SLOW_WALK", 30*time.Second),
		TreeSizeLargeRefresh:       getEnvDuration("TREE_SIZE_LARGE_REFRESH", 24*time.Hour),
		TreeSizeSmallRefresh:       getEnvDuration("TREE_SIZE_SMALL_REFRESH", 6*time.Hour),
		TreeSizeWalkTimeout:        getEnvDuration("TREE_SIZE_WALK_TIMEOUT", 30*time.Minute),
		TreeSizeStepSleep:          getEnvDuration("TREE_SIZE_STEP_SLEEP", 2*time.Millisecond),
		TreeSizeExcludePaths:       splitCSV(getEnv("TREE_SIZE_EXCLUDE_PATHS", "")),
	}
	// Load bearer token from file once at startup. Missing/empty token is
	// fatal — without it the controller will reject every credential vend.
	tok, err := readBearerToken(cfg.BearerTokenFile)
	if err != nil {
		slog.Error("bearer token unreadable — cannot authenticate to controller for credential vending",
			"error", err,
			"path", cfg.BearerTokenFile)
		os.Exit(1)
	}
	cfg.BearerToken = tok
	return cfg
}

// readBearerToken reads the per-host bearer token from disk. The file is
// provisioned by the duplicacy-agent-api ansible role at deploy time, mode
// 0600, and bind-mounted into the agent container. Trailing whitespace is
// stripped so a trailing newline from `printf` or `tee` doesn't break the
// HTTP header.
func readBearerToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", &emptyTokenErr{path: path}
	}
	return tok, nil
}

type emptyTokenErr struct{ path string }

func (e *emptyTokenErr) Error() string { return "bearer token file " + e.path + " is empty" }

func requireEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("required env var not set", "var", k)
		os.Exit(1)
	}
	return v
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getEnvInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		slog.Warn("invalid int env var, using default", "var", k, "value", v, "default", def)
		return def
	}
	return n
}

func getEnvBool(k string, def bool) bool {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		slog.Warn("invalid bool env var, using default", "var", k, "value", v, "default", def)
		return def
	}
	return b
}

func getEnvDuration(k string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		slog.Warn("invalid duration env var, using default", "var", k, "value", v, "default", def)
		return def
	}
	return d
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseMountMap parses "host1:container1,host2:container2,..." into a map
// keyed by host path. Bad entries are skipped with a warning rather than
// failing startup — the agent still functions for repos that picked the
// already-canonical container path.
func parseMountMap(s string) map[string]string {
	out := map[string]string{}
	for _, p := range splitCSV(s) {
		host, container, ok := strings.Cut(p, ":")
		if !ok || host == "" || container == "" {
			slog.Warn("BACKUP_ROOT_MOUNTS: skipping malformed entry (want host:container)", "entry", p)
			continue
		}
		out[host] = container
	}
	return out
}
