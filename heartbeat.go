package main

// Liveness heartbeat + boot-gap detection.
//
// On agent boot, log how long the previous instance was offline (gap
// between last-known-alive timestamp and now). If the gap is >120s the
// previous container likely crashed or was killed (Docker restart of an
// unresponsive container, OOM kill, panic). The persistent gap signal
// gives operators something to grep for after an unexpected restart —
// we can't read /var/lib/docker container exit codes from inside the
// container, so a self-reported "I was down for X seconds" is the
// closest the agent can get to a restart-cause indicator.
//
// Witness: an agent restarted ~50 min into a backup, leaving that repo's
// job orphaned in 'running' state. There was no log of WHY the agent
// restarted; without an existing heartbeat file there is no baseline to
// compare against.

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const heartbeatFilename = "heartbeat.last"

// heartbeatLoop ticks every interval, writing the current Unix-nano
// timestamp to ${CONFIG_DIR}/heartbeat.last via atomic .tmp + rename.
// Cleaned up on ctx.Done(). Cheap (~30 bytes write per tick).
//
// On clean shutdown (ctx cancelled), the loop writes one final empty
// "0" so the next boot can distinguish "agent stopped cleanly" from
// "agent crashed".
func heartbeatLoop(ctx context.Context, cfg Config, interval time.Duration) {
	if cfg.ConfigDir == "" {
		return
	}
	if interval <= 0 {
		interval = 30 * time.Second
	}
	path := filepath.Join(cfg.ConfigDir, heartbeatFilename)
	t := time.NewTicker(interval)
	defer t.Stop()

	write := func(unixNano int64) {
		data := strconv.FormatInt(unixNano, 10)
		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, []byte(data), 0o600); err != nil {
			slog.Warn("heartbeat write failed", "error", err)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			slog.Warn("heartbeat rename failed", "error", err)
			_ = os.Remove(tmp)
		}
	}

	// Immediate tick so the file exists right away.
	write(time.Now().UnixNano())

	for {
		select {
		case <-ctx.Done():
			// Clean-shutdown marker. Next boot reads "0" and reports
			// "previous shutdown was clean" instead of a phantom gap.
			write(0)
			return
		case <-t.C:
			write(time.Now().UnixNano())
		}
	}
}

// reportBootGap reads the previous heartbeat (if any) and logs how long
// the agent was offline. Three outcomes:
//   - file missing → "first boot" (no comparison possible)
//   - file == "0" → "clean shutdown" (no warning)
//   - file > 0 → compute now - prev; log INFO if <120s, WARN if larger
//
// Called once at agent boot before the heartbeat loop starts.
func reportBootGap(cfg Config) {
	if cfg.ConfigDir == "" {
		return
	}
	path := filepath.Join(cfg.ConfigDir, heartbeatFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("agent boot: no previous heartbeat found (first boot or wiped state dir)")
			return
		}
		slog.Warn("agent boot: heartbeat read failed", "error", err)
		return
	}
	raw := strings.TrimSpace(string(data))
	prevNano, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		slog.Warn("agent boot: heartbeat parse failed", "error", err, "raw", raw)
		return
	}
	if prevNano == 0 {
		slog.Info("agent boot: previous shutdown was clean")
		return
	}
	gap := time.Since(time.Unix(0, prevNano))
	switch {
	case gap < 0:
		// Clock skew (heartbeat stamp is in the future). Either system clock
		// changed or NTP corrected.
		slog.Warn("agent boot: heartbeat timestamp is in the future — clock skew?",
			"prev", time.Unix(0, prevNano), "now", time.Now())
	case gap < 2*time.Minute:
		slog.Info("agent boot: short downtime", "gap", gap.Round(time.Second).String())
	default:
		slog.Warn("agent boot: long unexpected downtime (likely crash/OOM/external restart)",
			"gap", gap.Round(time.Second).String(),
			"hint", fmt.Sprintf("inspect docker logs / system logs around %s", time.Unix(0, prevNano).Format(time.RFC3339)))
	}
}
