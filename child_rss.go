package main

import (
	"log/slog"
	"os"
	"sync"
	"syscall"
)

// Every duplicacy child lives in the agent's container cgroup, so its memory
// counts against the container limit while the Go soft limit cannot see it.
// Record each child's peak RSS (rusage Maxrss, KiB on Linux) when it ends so
// the cap can be sized from measurement; /health/ready exposes the max seen.
var childRSS = &childRSSStats{max: map[string]int64{}}

type childRSSStats struct {
	mu  sync.Mutex
	max map[string]int64 // kind -> max peak RSS KiB seen since start
}

// Caveat: Linux seeds a new process's ru_maxrss with the parent's RSS at fork, so a
// child's figure is never below the agent's own RSS at spawn. Treat it as an upper
// bound; the container's anonymous-memory peak (anon_peak_bytes on /health/ready)
// is the number to size the cgroup cap from.
//
// childMaxRSSKB returns the child's peak RSS in KiB, or 0 if unknown.
func childMaxRSSKB(ps *os.ProcessState) int64 {
	if ps == nil {
		return 0
	}
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok || ru == nil {
		return 0
	}
	return int64(ru.Maxrss)
}

// record notes one finished child and returns its peak RSS (KiB). It logs at
// Info when always is set (a job's end) or when the kind's maximum grows, and
// at Debug otherwise, so periodic discovery lists add no steady noise.
func (s *childRSSStats) record(kind, repo string, ps *os.ProcessState, always bool) int64 {
	rss := childMaxRSSKB(ps)
	if rss <= 0 {
		return 0
	}
	s.mu.Lock()
	grew := rss > s.max[kind]
	if grew {
		s.max[kind] = rss
	}
	s.mu.Unlock()
	if always || grew {
		slog.Info("duplicacy child peak rss", "kind", kind, "repo", repo, "max_rss_kb", rss, "new_max", grew)
	} else {
		slog.Debug("duplicacy child peak rss", "kind", kind, "repo", repo, "max_rss_kb", rss)
	}
	return rss
}

func (s *childRSSStats) snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.max))
	for k, v := range s.max {
		out[k] = v
	}
	return out
}
