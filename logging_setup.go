package main

// Agent-driven log rotation.
//
// The kit's logging.Setup() writes slog to stderr (Docker captures it for
// `docker logs`). That's fine for live tailing but the host's container log
// file is unbounded by default — at one progress line per chunk during
// backup/copy this fills /var/lib/docker quickly. Asking operators to
// configure Docker's logging driver per host is fragile (one missed
// daemon.json = unbounded growth again).
//
// Better: agent writes its own log file alongside stderr. The host
// bind-mounts a logs directory (LOG_DIR), the agent uses lumberjack to cap
// file size + roll, and the host always has the same predictable path
// regardless of Docker logging driver config. Operators can `tail -F
// <LOG_DIR>/agent.log` directly.
//
// stderr is preserved — both sinks get the same records. So `docker logs`
// still works for live debugging.

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

// attachRotatingLogFile, when LOG_DIR is set, augments the default slog
// logger with a second sink writing to <LOG_DIR>/agent.log via lumberjack.
// Format matches the kit's stderr handler (JSON unless LOGUTIL_FORMAT=text).
//
// Rotation policy (chosen for a backup-agent's traffic profile):
//   - max-size: 20 MB per file
//   - max-backups: 5 rotated files
//   - max-age: 14 days
//   - compress: rotated files gzipped
//
// Net ceiling: ~120 MB on disk after compression amortises. Plenty for two
// weeks of operational history; small enough to live in /var/log forever.
//
// No-op when LOG_DIR is empty (legacy stderr-only path).
func attachRotatingLogFile(level string) {
	dir := strings.TrimSpace(os.Getenv("LOG_DIR"))
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		slog.Warn("LOG_DIR mkdir failed; falling back to stderr-only", "dir", dir, "error", err)
		return
	}

	rot := &lumberjack.Logger{
		Filename:   filepath.Join(dir, "agent.log"),
		MaxSize:    20, // megabytes
		MaxBackups: 5,
		MaxAge:     14, // days
		Compress:   true,
	}

	// Tee to both stderr and the rotating file so `docker logs` still works.
	combined := io.MultiWriter(os.Stderr, rot)

	lvl := parseLogLevel(level)
	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOGUTIL_FORMAT")) == "text" {
		handler = slog.NewTextHandler(combined, &slog.HandlerOptions{Level: lvl})
	} else {
		handler = slog.NewJSONHandler(combined, &slog.HandlerOptions{Level: lvl})
	}
	slog.SetDefault(slog.New(handler))
	slog.Info("agent log file attached",
		"path", rot.Filename, "max_mb", rot.MaxSize, "max_backups", rot.MaxBackups, "max_age_days", rot.MaxAge)
}

// parseLogLevel mirrors agent-kit-go/logging's level parser so the rotated
// sink uses the same level the kit configured for stderr.
func parseLogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
