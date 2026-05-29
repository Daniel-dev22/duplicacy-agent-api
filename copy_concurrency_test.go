package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sleepInv returns a cliInvocation that runs `sleep <dur>` so we can exercise
// the job runner with a real, harmless child process.
func sleepInv(dur string) cliInvocation {
	return cliInvocation{RepoRoot: "/", Args: []string{dur}}
}

// waitForState polls a job until it reaches one of the wanted states or the
// deadline elapses. Returns the observed state.
func waitForState(t *testing.T, r *jobRegistry, id string, deadline time.Duration, wanted ...JobState) JobState {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if j, ok := r.get(id); ok {
			st := j.snapshot().State
			for _, w := range wanted {
				if st == w {
					return st
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if j, ok := r.get(id); ok {
		return j.snapshot().State
	}
	return ""
}

// With MaxConcurrentCopies=1, a second copy must stay Pending until the first
// finishes — proving the per-host copy semaphore serialises the nightly
// destination fan-out instead of spawning all copies at once.
func TestCopySemaphoreSerialises(t *testing.T) {
	r := newJobRegistry(1)
	repo := &Repo{ID: "r1", Path: "/"}

	j1, err := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "remote-nas", sleepInv("0.4"), "", "schedule", nil)
	if err != nil {
		t.Fatalf("start j1: %v", err)
	}
	j2, err := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "storj", sleepInv("0.1"), "", "schedule", nil)
	if err != nil {
		t.Fatalf("start j2: %v", err)
	}

	// j1 should be running quickly; j2 should still be queued (Pending).
	if st := waitForState(t, r, j1.ID, time.Second, JobRunning, JobCompleted); st != JobRunning && st != JobCompleted {
		t.Fatalf("j1 never ran, state=%q", st)
	}
	// Give the scheduler a beat; j2 must NOT be running while j1 holds the slot.
	time.Sleep(50 * time.Millisecond)
	if st := func() JobState { j, _ := r.get(j2.ID); return j.snapshot().State }(); st == JobRunning {
		t.Fatalf("j2 ran concurrently with j1 — semaphore did not serialise (state=%q)", st)
	}

	// Both should complete eventually.
	if st := waitForState(t, r, j1.ID, 3*time.Second, JobCompleted, JobFailed); st != JobCompleted {
		t.Fatalf("j1 final state=%q want completed", st)
	}
	if st := waitForState(t, r, j2.ID, 3*time.Second, JobCompleted, JobFailed); st != JobCompleted {
		t.Fatalf("j2 final state=%q want completed", st)
	}
}

// Non-copy actions are never gated by the copy semaphore: many backups can run
// at once even with MaxConcurrentCopies=1.
func TestNonCopyNotGated(t *testing.T) {
	r := newJobRegistry(1)
	repo := &Repo{ID: "r1", Path: "/"}

	const n = 4
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		j, err := r.start(context.Background(), "/bin/sleep", repo, ActionBackup, "remote-nas", sleepInv("0.3"), "", "schedule", nil)
		if err != nil {
			t.Fatalf("start backup %d: %v", i, err)
		}
		ids = append(ids, j.ID)
	}

	// All backups should be Running concurrently shortly after start.
	deadline := time.Now().Add(time.Second)
	for {
		running := 0
		for _, id := range ids {
			if j, ok := r.get(id); ok && j.snapshot().State == JobRunning {
				running++
			}
		}
		if running == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d backups running concurrently — they were gated", running, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// A copy cancelled while queued behind the semaphore must finalise as
// cancelled and never spawn a process.
func TestQueuedCopyCancelBeforeStart(t *testing.T) {
	r := newJobRegistry(1)
	repo := &Repo{ID: "r1", Path: "/"}

	// Hold the only slot with a long-running copy.
	blocker, err := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "remote-nas", sleepInv("2"), "", "schedule", nil)
	if err != nil {
		t.Fatalf("start blocker: %v", err)
	}
	waitForState(t, r, blocker.ID, time.Second, JobRunning)

	queued, err := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "storj", sleepInv("2"), "", "schedule", nil)
	if err != nil {
		t.Fatalf("start queued: %v", err)
	}
	// It should be Pending (waiting for the slot).
	time.Sleep(30 * time.Millisecond)
	if st := func() JobState { j, _ := r.get(queued.ID); return j.snapshot().State }(); st != JobPending {
		t.Fatalf("queued copy state=%q want pending", st)
	}

	// Cancel it while queued; it must become Cancelled without ever running.
	if !r.cancel(queued.ID) {
		t.Fatalf("cancel returned false")
	}
	if st := waitForState(t, r, queued.ID, time.Second, JobCancelled); st != JobCancelled {
		t.Fatalf("queued copy state=%q want cancelled", st)
	}
}

// Sanity: concurrent starts don't race the semaphore (run under -race).
func TestCopySemaphoreNoRace(t *testing.T) {
	r := newJobRegistry(2)
	repo := &Repo{ID: "r1", Path: "/"}
	var wg sync.WaitGroup
	var started int32
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if j, err := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "remote-nas", sleepInv("0.05"), "", "schedule", nil); err == nil && j != nil {
				atomic.AddInt32(&started, 1)
			}
		}()
	}
	wg.Wait()
	if started != 8 {
		t.Fatalf("registered %d/8 copy jobs", started)
	}
	// Let them drain.
	time.Sleep(800 * time.Millisecond)
}
