package main

import (
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

type Config struct {
	NodeName            string   // hostname this agent runs on (e.g. "nuc02")
	SiteID              string   // "kd" | "ng"
	BackupRoots         []string // bind-mounted paths to scan for .duplicacy repos
	ConfigDir           string   // persistent state dir (events.sqlite, schedule cache, filter cache)
	ControlCenterURL    string   // base URL for push events / pull schedules / pull filters
	ControlCenterCAFile string   // path to CA cert for verifying controller server
	ClientCertFile      string   // path to mTLS client cert for pushing events
	ClientKeyFile       string   // path to mTLS client key
	DuplicacyBinary     string   // path to duplicacy CLI (default /usr/local/bin/duplicacy)
}

func loadConfig() Config {
	cfg := Config{
		NodeName:            requireEnv("NODE_NAME"),
		SiteID:              requireEnv("SITE_ID"),
		BackupRoots:         splitCSV(requireEnv("BACKUP_ROOTS")),
		ConfigDir:           getEnv("CONFIG_DIR", "/var/lib/duplicacy-api"),
		ControlCenterURL:    requireEnv("CONTROL_CENTER_URL"),
		ControlCenterCAFile: getEnv("CONTROL_CENTER_CA", ""),
		ClientCertFile:      getEnv("CLIENT_CERT", ""),
		ClientKeyFile:       getEnv("CLIENT_KEY", ""),
		DuplicacyBinary:     getEnv("DUPLICACY_BINARY", "/usr/local/bin/duplicacy"),
	}
	return cfg
}

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
