package main

import (
	"context"
	"testing"
	"time"
)

// With MaxConcurrentMaint=1, a second maintenance job (check/prune) must stay
// Pending until the first finishes — the after-wave fan-out fires ~30 prune
// then ~30 check schedules at once, and this semaphore is what keeps them from
// spiking NAS RAM by running in parallel.
func TestMaintSemaphoreSerialises(t *testing.T) {
	r := newJobRegistry(0, 1) // copies unbounded, maint capped at 1
	repo := &Repo{ID: "r1", Path: "/"}

	j1, err := r.start(context.Background(), "/bin/sleep", repo, ActionPrune, "default", sleepInv("0.4"), "", "chain-prune", nil)
	if err != nil {
		t.Fatalf("start j1: %v", err)
	}
	j2, err := r.start(context.Background(), "/bin/sleep", repo, ActionCheck, "default", sleepInv("0.1"), "", "chain-check", nil)
	if err != nil {
		t.Fatalf("start j2: %v", err)
	}

	if st := waitForState(t, r, j1.ID, time.Second, JobRunning, JobCompleted); st != JobRunning && st != JobCompleted {
		t.Fatalf("j1 never ran, state=%q", st)
	}
	// j2 must NOT run while j1 holds the only maint slot.
	time.Sleep(50 * time.Millisecond)
	if st := func() JobState { j, _ := r.get(j2.ID); return j.snapshot().State }(); st == JobRunning {
		t.Fatalf("j2 ran concurrently with j1 — maint semaphore did not serialise (state=%q)", st)
	}

	if st := waitForState(t, r, j1.ID, 3*time.Second, JobCompleted, JobFailed); st != JobCompleted {
		t.Fatalf("j1 final state=%q want completed", st)
	}
	if st := waitForState(t, r, j2.ID, 3*time.Second, JobCompleted, JobFailed); st != JobCompleted {
		t.Fatalf("j2 final state=%q want completed", st)
	}
}

// Maintenance and copy semaphores are independent: a maint cap of 1 does not
// gate copies, and vice-versa. Also exercises countActive across actions.
func TestMaintAndCopyIndependentAndCountActive(t *testing.T) {
	r := newJobRegistry(0, 1) // copies unbounded, maint=1
	repo := &Repo{ID: "r1", Path: "/"}

	// Two copies run concurrently (unbounded), one prune runs, one prune queues.
	c1, _ := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "storj", sleepInv("0.5"), "", "schedule", nil)
	c2, _ := r.start(context.Background(), "/bin/sleep", repo, ActionCopy, "remote-nas", sleepInv("0.5"), "", "schedule", nil)
	p1, _ := r.start(context.Background(), "/bin/sleep", repo, ActionPrune, "default", sleepInv("0.5"), "", "chain-prune", nil)
	p2, _ := r.start(context.Background(), "/bin/sleep", repo, ActionPrune, "storj", sleepInv("0.5"), "", "chain-prune", nil)
	_ = c1
	_ = c2
	_ = p1
	_ = p2

	// Let states settle: both copies running, one prune running, one prune pending.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.countActive(ActionCopy) == 2 && r.countActive(ActionPrune) == 2 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := r.countActive(ActionCopy); got != 2 {
		t.Fatalf("countActive(copy) = %d, want 2 (copies must not be gated by maint cap)", got)
	}
	if got := r.countActive(ActionPrune); got != 2 {
		t.Fatalf("countActive(prune) = %d, want 2 (one running + one pending both count as active)", got)
	}
	if got := r.countActive(ActionBackup); got != 0 {
		t.Fatalf("countActive(backup) = %d, want 0", got)
	}
	// Combined query used by WaveComplete.
	if got := r.countActive(ActionBackup, ActionCopy); got != 2 {
		t.Fatalf("countActive(backup,copy) = %d, want 2", got)
	}

	// Drain everything.
	for _, id := range []string{c1.ID, c2.ID, p1.ID, p2.ID} {
		waitForState(t, r, id, 3*time.Second, JobCompleted, JobFailed)
	}
	if got := r.countActive(ActionCopy, ActionPrune, ActionCheck); got != 0 {
		t.Fatalf("countActive after drain = %d, want 0", got)
	}
}
