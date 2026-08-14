package main

import (
	"os"
	"strings"
	"testing"
)

// These tests run the guard against the REAL /proc/self/mountinfo and the REAL sysfs
// of whatever machine they execute on, rather than against captured fixtures.
//
// Why this file exists separately: every other test injects a deviceKindFunc, so the
// production path — newMountGuard() parsing a live mount table and sysfsDeviceKind
// walking a live holder graph — was never executed by the suite. A guard that passes
// only against strings it was handed is a harness comparing itself.
//
// The assertions are invariants that must hold on ANY Linux host (developer laptop,
// CI runner, NUC, Pi), so this does not become a machine-specific test.

func liveGuard(t *testing.T) *mountGuard {
	t.Helper()
	if _, err := os.Stat("/proc/self/mountinfo"); err != nil {
		t.Skip("no /proc/self/mountinfo (not Linux)")
	}
	g, err := newMountGuard()
	if err != nil {
		t.Fatalf("newMountGuard against live /proc: %v", err)
	}
	if g.total < 5 {
		t.Fatalf("parsed only %d mounts from a live mountinfo — the parser is not "+
			"reading the real format", g.total)
	}
	return g
}

// The root filesystem must always be walkable. If this ever fails, the guard has
// blinded the gatherer completely and every reported size becomes zero — the failure
// mode that looks exactly like a correct answer.
//
// This is also the real-world guard against rule 3 over-firing. On an LVM host, / is
// itself a device-mapper target (/dev/mapper/<vg>-<lv>) — measured on kd-nuc01, where
// / is /dev/mapper/ubuntu--vg-ubuntu--lv. A naive "dm target → skip" would drop the
// entire root filesystem here. The rule requires an EMPTY slaves/, which a live LV
// never has, so a healthy LVM root is walked and only an orphaned map is skipped.
func TestLiveRootFilesystemIsNeverSkipped(t *testing.T) {
	g := liveGuard(t)
	if reason, skipped := g.SkipReason("/"); skipped {
		t.Fatalf("/ was skipped on a live host (%s) — this would zero every size", reason)
	}
}

// A device-mapper backed root (LVM) must be reported as neither loop-backed nor
// orphaned by the real sysfs walker. Codifies the kd-nuc01 finding above so a future
// widening of rule 3 fails here rather than in production.
func TestLiveDeviceMapperRootIsNotOrphaned(t *testing.T) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Skip("no mountinfo")
	}
	for _, line := range strings.Split(string(data), "\n") {
		e, ok := parseMountinfoLine(line)
		if !ok || e.MountPoint != "/" {
			continue
		}
		if !strings.HasPrefix(e.Source, "/dev/mapper/") {
			t.Skipf("/ is %s, not device-mapper backed on this host", e.Source)
		}
		maj, min, ok := parseMajorMinorOfMount(line)
		if !ok {
			t.Fatal("could not read major:minor for /")
		}
		loopBacked, orphaned := sysfsDeviceKind(maj, min)
		if orphaned {
			t.Fatalf("live LVM root %s reported as an orphaned dm target", e.Source)
		}
		if loopBacked {
			t.Fatalf("live LVM root %s reported as loop-backed", e.Source)
		}
		t.Logf("live dm-backed root %s correctly classified as walkable", e.Source)
		return
	}
	t.Skip("no / entry found")
}

// The kernel pseudo filesystems every Linux host has must be skipped. This is rule 1
// executing against real mountinfo rather than a fixture string.
func TestLivePseudoFilesystemsAreSkipped(t *testing.T) {
	g := liveGuard(t)
	for _, p := range []string{"/proc", "/sys", "/dev"} {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		reason, skipped := g.SkipReason(p)
		if !skipped {
			t.Errorf("%s is a live pseudo filesystem and must be skipped", p)
			continue
		}
		if !strings.Contains(reason, "pseudo filesystem") {
			t.Errorf("%s: expected the pseudo-filesystem reason, got %q", p, reason)
		}
	}
}

// Snap mounts are squashfs served from a real /dev/loopN, so on any host with snaps
// this executes sysfsDeviceKind against a real holder graph and proves rule 2 works
// on live kernel state — not just against the fake sysfs tree in the unit test.
func TestLiveLoopBackedMountsAreDetected(t *testing.T) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Skip("no mountinfo")
	}
	g := liveGuard(t)

	var checked int
	for _, line := range strings.Split(string(data), "\n") {
		e, ok := parseMountinfoLine(line)
		if !ok || !strings.HasPrefix(e.Source, "/dev/loop") {
			continue
		}
		checked++
		if _, skipped := g.SkipReason(e.MountPoint); !skipped {
			t.Errorf("%s is served from %s (a loop device) and must be skipped",
				e.MountPoint, e.Source)
		}
		// Independently confirm the sysfs walker agrees, so a pass here cannot be
		// coming from the fstype rule alone.
		maj, min, ok := parseMajorMinorOfMount(line)
		if !ok {
			continue
		}
		if loopBacked, _ := sysfsDeviceKind(maj, min); !loopBacked {
			t.Errorf("sysfsDeviceKind(%d,%d) did not report %s as loop-backed",
				maj, min, e.Source)
		}
	}
	if checked == 0 {
		t.Skip("no loop-backed mounts on this host")
	}
	t.Logf("verified %d live loop-backed mounts against real sysfs", checked)
}

// A live host must not have every mount skipped: real data filesystems have to
// survive classification, or the NAS multi-pool case is broken in production.
func TestLiveGuardDoesNotSkipEverything(t *testing.T) {
	g := liveGuard(t)
	if len(g.skip) >= g.total {
		t.Fatalf("guard skipped %d of %d live mounts — nothing would be walked",
			len(g.skip), g.total)
	}
	t.Logf("live host: %d mounts seen, %d skipped", g.total, len(g.skip))
}

// parseMajorMinorOfMount re-extracts major:minor from a raw line for the cross-check
// above, without widening mountEntry's API.
func parseMajorMinorOfMount(line string) (uint32, uint32, bool) {
	f := strings.Fields(line)
	if len(f) < 3 {
		return 0, 0, false
	}
	return parseMajorMinor(f[2])
}
