package main

import (
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

type Config struct {
	NodeName         string   // hostname-short this agent runs on (e.g. "nuc02")
	SiteID           string   // "kd" | "ng"
	BackupRoots      []string // bind-mounted paths to scan for .duplicacy repos
	ComposeScanRoots []string // bind-mounted (read-only) paths to scan for docker-compose project dirs
	ConfigDir        string   // persistent state dir (events.sqlite, schedule cache, filter cache)
	ControlCenterURL string   // The agent always reaches controller-api via the host's
	                          // Traefik (or k3s Traefik on cluster nodes). Traefik handles
	                          // mTLS at the boundary; this code does not load or present any
	                          // client certificate. Per-node identity is conveyed via the
	                          // BearerToken loaded below.
	TraefikDockerDNS string   // Docker DNS name to resolve to local Traefik (default "traefik")
	TraefikDialPort  string   // port to dial on the resolved Traefik IP (default "1443")
	DuplicacyBinary  string   // path to duplicacy CLI (default /usr/local/bin/duplicacy)
	BearerTokenFile  string   // path to file containing this node's bearer token (per-host)
	BearerToken      string   // populated at startup from BearerTokenFile contents
}

func loadConfig() Config {
	cfg := Config{
		NodeName:         requireEnv("NODE_NAME"),
		SiteID:           requireEnv("SITE_ID"),
		BackupRoots:      splitCSV(requireEnv("BACKUP_ROOTS")),
		ComposeScanRoots: splitCSV(getEnv("COMPOSE_SCAN_ROOTS", "")),
		ConfigDir:        getEnv("CONFIG_DIR", "/var/lib/duplicacy-agent-api"),
		ControlCenterURL: requireEnv("CONTROL_CENTER_URL"),
		TraefikDockerDNS: getEnv("TRAEFIK_DOCKER_DNS", "traefik"),
		TraefikDialPort:  getEnv("TRAEFIK_DIAL_PORT", "1443"),
		DuplicacyBinary:  getEnv("DUPLICACY_BINARY", "/usr/local/bin/duplicacy"),
		BearerTokenFile:  getEnv("BEARER_TOKEN_FILE", "/etc/duplicacy-agent-api/bearer-token"),
	}
	// Load bearer token from file once at startup. Missing/empty token is
	// fatal — without it the controller will reject every credential vend.
	tok, err := readBearerToken(cfg.BearerTokenFile)
	if err != nil {
		log.Fatal().Err(err).Str("path", cfg.BearerTokenFile).Msg("bearer token unreadable — cannot authenticate to controller for credential vending")
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
		log.Fatal().Str("var", k).Msg("required env var not set")
	}
	return v
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
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
