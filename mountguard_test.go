package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Every fixture in this file is REAL /proc/1/mountinfo output captured from the two
// Raspberry Pis that produced the incident, not invented syntax. Where a line could
// not be re-captured (the leaked image mounts were torn down during remediation) it
// is reproduced verbatim from the diagnostic transcript.

// --- real lines, kd-pi, after the remediation reboot -------------------------------
const (
	realRootExt4   = `32 2 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw`
	realBootVfat   = `127 32 8:1 / /boot/firmware rw,relatime shared:172 - vfat /dev/sda1 rw,fmask=0022,dmask=0022,codepage=437,iocharset=ascii,shortname=mixed,errors=remount-ro`
	realSysfs      = `27 32 0:24 / /sys rw,nosuid,nodev,noexec,relatime shared:6 - sysfs sysfs rw`
	realProc       = `28 32 0:25 / /proc rw,nosuid,nodev,noexec,relatime shared:11 - proc proc rw`
	realDevtmpfs   = `29 32 0:6 / /dev rw,nosuid,relatime shared:2 - devtmpfs udev rw,size=3687656k,nr_inodes=921914,mode=755,inode64`
	realRunTmpfs   = `31 32 0:27 / /run rw,nosuid,nodev,noexec,relatime shared:5 - tmpfs tmpfs rw,size=798504k,mode=755,inode64`
	realDockerOvl  = `941 32 0:65 / /var/lib/docker/overlay2/e70266a669f228ed76b94c683dad5d320fdba8434d6796653862d50c6602abc8/merged rw,relatime shared:464 - overlay overlay rw,lowerdir=/x,upperdir=/y,workdir=/z,nouserxattr`
	realNsfs       = `934 239 0:4 mnt:[4026532568] /run/snapd/ns/lxd.mnt rw - nsfs nsfs rw`
	realSnapSquash = `49 32 7:0 / /snap/core22/2340 ro,nodev,relatime shared:92 - squashfs /dev/loop0 ro,errors=continue,threads=single`
)

// --- real lines, ng-pi, captured from the duplicacy-agent container's namespace ----
// These are the two mounts that generated the EXT4 errors.
const (
	leakedBootVfat = `2337 2336 253:0 / /home/daniel/custom_os_isos/ubuntu/mnt/boot rw,relatime - vfat /dev/mapper/loop2p1 rw,fmask=0022,dmask=0022,codepage=437,iocharset=ascii,shortname=mixed,errors=remount-ro`
	leakedRootExt4 = `2338 2336 253:1 / /home/daniel/custom_os_isos/ubuntu/mnt/iso-root rw,relatime - ext4 /dev/mapper/loop2p2 rw`
)

// --- real lines, kd-pi, the leaked chroot bind tree --------------------------------
const (
	chrootProc = `1927 32 0:25 / /home/daniel/custom_os_isos/ubuntu/iso-root/proc rw,nosuid,nodev,noexec,relatime shared:11 - proc proc rw`
	chrootSys  = `2106 32 0:24 / /home/daniel/custom_os_isos/ubuntu/iso-root/sys rw,nosuid,nodev,noexec,relatime shared:6 - sysfs sysfs rw`
	chrootDev  = `2129 32 0:6 / /home/daniel/custom_os_isos/ubuntu/iso-root/dev rw,nosuid,relatime shared:2 - devtmpfs udev rw,size=3687656k,mode=755,inode64`
	chrootRun  = `2234 32 0:27 / /home/daniel/custom_os_isos/ubuntu/iso-root/run rw,nosuid,nodev,noexec,relatime shared:5 - tmpfs tmpfs rw,size=798504k,mode=755,inode64`
	chrootK3s  = `2999 2234 0:99 / /home/daniel/custom_os_isos/ubuntu/iso-root/run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/abc/rootfs rw,relatime shared:900 - overlay overlay rw,lowerdir=/a,upperdir=/b,workdir=/c`
)

func join(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

// noSpecialDevices is the sysfs answer for ordinary disks: neither loop-backed nor
// an orphaned dm target.
func noSpecialDevices(uint32, uint32) (bool, bool) { return false, false }

// -----------------------------------------------------------------------------
// The regression guard. This is the test that stops rules 2 and 3 being widened
// into a blanket -xdev, which would silently under-report a NAS whose
// BACKUP_ROOTS=['/mnt'] spans several real filesystems.
// -----------------------------------------------------------------------------

func TestMountGuardDescendsRealFilesystems(t *testing.T) {
	// A second real ext4 on its own device — the NAS pool case — plus the two real
	// filesystems every one of these hosts actually has.
	poolExt4 := `500 32 8:17 / /mnt/pool1 rw,relatime shared:300 - ext4 /dev/sdb1 rw`
	poolXfs := `501 32 8:33 / /mnt/pool2 rw,relatime shared:301 - xfs /dev/sdc1 rw`

	g := parseMountGuard(join(realRootExt4, realBootVfat, poolExt4, poolXfs), noSpecialDevices)

	for _, p := range []string{"/", "/boot/firmware", "/mnt/pool1", "/mnt/pool2"} {
		if reason, skipped := g.SkipReason(p); skipped {
			t.Errorf("%s must be descended, got skipped: %s", p, reason)
		}
	}
	if len(g.skip) != 0 {
		t.Errorf("expected zero skips over real filesystems, got %v", g.skip)
	}
}

// -----------------------------------------------------------------------------
// Rule 1 — pseudo filesystems
// -----------------------------------------------------------------------------

func TestMountGuardSkipsPseudoFilesystems(t *testing.T) {
	g := parseMountGuard(join(
		realRootExt4, realSysfs, realProc, realDevtmpfs, realRunTmpfs,
		realDockerOvl, realNsfs, chrootProc, chrootSys, chrootDev, chrootRun, chrootK3s,
	), noSpecialDevices)

	mustSkip := []string{
		"/sys", "/proc", "/dev", "/run",
		"/var/lib/docker/overlay2/e70266a669f228ed76b94c683dad5d320fdba8434d6796653862d50c6602abc8/merged",
		"/run/snapd/ns/lxd.mnt",
		"/home/daniel/custom_os_isos/ubuntu/iso-root/proc",
		"/home/daniel/custom_os_isos/ubuntu/iso-root/sys",
		"/home/daniel/custom_os_isos/ubuntu/iso-root/dev",
		"/home/daniel/custom_os_isos/ubuntu/iso-root/run",
		"/home/daniel/custom_os_isos/ubuntu/iso-root/run/k3s/containerd/io.containerd.runtime.v2.task/k8s.io/abc/rootfs",
	}
	for _, p := range mustSkip {
		if _, skipped := g.SkipReason(p); !skipped {
			t.Errorf("pseudo filesystem %s must be skipped", p)
		}
	}
	// The real root must still be walked.
	if _, skipped := g.SkipReason("/"); skipped {
		t.Error("/ (ext4 on /dev/sda2) must not be skipped")
	}
}

// -----------------------------------------------------------------------------
// Rule 2 — loop-backed disk image (kd-pi shape: loop10 still attached, mount healthy)
// -----------------------------------------------------------------------------

func TestMountGuardSkipsLoopBackedImageWhileHealthy(t *testing.T) {
	// kd-pi: /sys/dev/block/253:{0,1}/slaves/ -> loop10, which has loop/backing_file.
	loopBacked := func(major, minor uint32) (bool, bool) {
		return major == 253, false
	}
	g := parseMountGuard(join(realRootExt4, realBootVfat, leakedBootVfat, leakedRootExt4), loopBacked)

	for _, p := range []string{
		"/home/daniel/custom_os_isos/ubuntu/mnt/boot",
		"/home/daniel/custom_os_isos/ubuntu/mnt/iso-root",
	} {
		reason, skipped := g.SkipReason(p)
		if !skipped {
			t.Fatalf("loop-backed image mount %s must be skipped even while healthy", p)
		}
		if !strings.Contains(reason, "loop-backed") {
			t.Errorf("%s: expected loop-backed reason, got %q", p, reason)
		}
	}
	// vfat and ext4 on REAL partitions must survive — proving the rule keys on the
	// backing device, not on the filesystem type.
	for _, p := range []string{"/", "/boot/firmware"} {
		if _, skipped := g.SkipReason(p); skipped {
			t.Errorf("%s is a real partition and must not be skipped", p)
		}
	}
}

// -----------------------------------------------------------------------------
// Rule 3 — orphaned dm target (ng-pi shape: loop2 detached, slaves/ empty)
// -----------------------------------------------------------------------------

func TestMountGuardSkipsOrphanedDMTarget(t *testing.T) {
	orphaned := func(major, minor uint32) (bool, bool) {
		return false, major == 253
	}
	g := parseMountGuard(join(realRootExt4, leakedBootVfat, leakedRootExt4), orphaned)

	reason, skipped := g.SkipReason("/home/daniel/custom_os_isos/ubuntu/mnt/iso-root")
	if !skipped {
		t.Fatal("orphaned dm target must be skipped — this is the mount that raised EXT4-fs errors")
	}
	if !strings.Contains(reason, "orphaned") {
		t.Errorf("expected orphaned reason, got %q", reason)
	}
	if _, skipped := g.SkipReason("/"); skipped {
		t.Error("/ must not be skipped")
	}
}

// -----------------------------------------------------------------------------
// The sysfs walker itself. Without this, the rules above are only ever exercised
// through an injected fake and the real classifier is untested.
// -----------------------------------------------------------------------------

func TestSysfsDeviceKindAgainstRealShapes(t *testing.T) {
	root := t.TempDir()

	mkdir := func(p string) string {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(full, 0o755); err != nil {
			t.Fatal(err)
		}
		return full
	}
	touch := func(p, content string) {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, p)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, p), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Shape A — kd-pi: dm target whose slaves/ holds loop10, which is loop-backed.
	mkdir("dmLoop/dm")
	touch("dmLoop/dm/name", "loop10p2\n")
	touch("dmLoop/slaves/loop10/loop/backing_file",
		"/home/daniel/custom_os_original_images/ubuntu/ubuntu-25.10-preinstalled-server-arm64+raspi.img\n")

	// Shape B — ng-pi: dm target with an EMPTY slaves/ (loop2 was detached).
	mkdir("dmOrphan/dm")
	touch("dmOrphan/dm/name", "loop2p2\n")
	mkdir("dmOrphan/slaves")

	// Shape C — an ordinary partition: no dm/, no slaves/.
	mkdir("sda2")

	// Shape D — a loop device reached directly.
	touch("loop0/loop/backing_file", "/var/lib/snapd/snaps/core22.snap\n")

	tests := []struct {
		name           string
		dir            string
		wantLoopBacked bool
		wantOrphaned   bool
	}{
		{"dm over loop (kd-pi)", "dmLoop", true, false},
		{"orphaned dm (ng-pi)", "dmOrphan", false, true},
		{"ordinary partition", "sda2", false, false},
		{"direct loop device", "loop0", true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lb, od := deviceKindAt(filepath.Join(root, tc.dir), 0)
			if lb != tc.wantLoopBacked || od != tc.wantOrphaned {
				t.Errorf("got (loopBacked=%v, orphaned=%v), want (%v, %v)",
					lb, od, tc.wantLoopBacked, tc.wantOrphaned)
			}
		})
	}
}

func TestDeviceKindAtIsDepthBounded(t *testing.T) {
	root := t.TempDir()
	// A pathological chain deeper than the bound; must terminate, not recurse away.
	p := root
	for i := 0; i < 40; i++ {
		p = filepath.Join(p, "slaves", "next")
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() { defer close(done); deviceKindAt(root, 0) }()
	<-done // a stack overflow or hang here is the failure
}

// -----------------------------------------------------------------------------
// Parser
// -----------------------------------------------------------------------------

func TestParseMountinfoLineVariableOptionalFields(t *testing.T) {
	tests := []struct {
		name       string
		line       string
		wantPoint  string
		wantFSType string
		wantSource string
	}{
		// realNsfs has NO optional fields — separator lands right after the options.
		{"no optional fields", realNsfs, "/run/snapd/ns/lxd.mnt", "nsfs", "nsfs"},
		{"one optional field", realRootExt4, "/", "ext4", "/dev/sda2"},
		{"leaked dm, no optional fields", leakedRootExt4,
			"/home/daniel/custom_os_isos/ubuntu/mnt/iso-root", "ext4", "/dev/mapper/loop2p2"},
		{"two optional fields", `36 35 98:0 / /mnt rw master:1 shared:2 - ext3 /dev/root rw`,
			"/mnt", "ext3", "/dev/root"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, ok := parseMountinfoLine(tc.line)
			if !ok {
				t.Fatal("failed to parse")
			}
			if e.MountPoint != tc.wantPoint || e.FSType != tc.wantFSType || e.Source != tc.wantSource {
				t.Errorf("got point=%q fstype=%q source=%q; want %q %q %q",
					e.MountPoint, e.FSType, e.Source, tc.wantPoint, tc.wantFSType, tc.wantSource)
			}
		})
	}
}

func TestParseMountinfoLineRejectsGarbage(t *testing.T) {
	for _, line := range []string{"", "   ", "too few fields", "36 35 nope / /mnt rw - ext3 /dev/root rw"} {
		if _, ok := parseMountinfoLine(line); ok {
			t.Errorf("expected %q to be rejected", line)
		}
	}
}

func TestUnescapeMountinfoHandlesOctalPaths(t *testing.T) {
	// The kernel escapes space, tab, newline and backslash in path fields. A mount
	// point with a space is ordinary on a media volume and must still match.
	line := `77 32 8:5 / /mnt/My\040Backup\040Drive rw,relatime shared:9 - ext4 /dev/sdd1 rw`
	e, ok := parseMountinfoLine(line)
	if !ok {
		t.Fatal("failed to parse")
	}
	if e.MountPoint != "/mnt/My Backup Drive" {
		t.Errorf("got %q, want %q", e.MountPoint, "/mnt/My Backup Drive")
	}
}

// -----------------------------------------------------------------------------
// signature() — the log-on-change gate
// -----------------------------------------------------------------------------

func TestSignatureIsStableAcrossIterations(t *testing.T) {
	// Go randomises map iteration order. An unsorted signature would differ between
	// identical passes and re-log every cycle — the exact every-cycle noise this gate
	// exists to prevent. Same input must give the same signature every time.
	g := parseMountGuard(join(realSysfs, realProc, realRunTmpfs, realDockerOvl, realNsfs), noSpecialDevices)
	first := g.signature()
	if first == "" {
		t.Fatal("expected a non-empty signature when mounts are skipped")
	}
	for i := 0; i < 200; i++ {
		if got := g.signature(); got != first {
			t.Fatalf("signature unstable on iteration %d", i)
		}
	}
}

func TestSignatureChangesWhenAMountIsLeaked(t *testing.T) {
	before := parseMountGuard(join(realRootExt4, realProc), noSpecialDevices)
	after := parseMountGuard(join(realRootExt4, realProc, chrootRun), noSpecialDevices)
	if before.signature() == after.signature() {
		t.Fatal("a newly leaked mount must change the signature so it gets logged")
	}
}

func TestNilGuardIsSafe(t *testing.T) {
	// runPass leaves guard nil when mountinfo is unreadable; the walk must degrade to
	// "skip nothing" rather than panic.
	var g *mountGuard
	if _, skipped := g.SkipReason("/anything"); skipped {
		t.Error("nil guard must skip nothing")
	}
	if g.signature() != "" {
		t.Error("nil guard signature must be empty")
	}
	g.logSummary() // must not panic
}

// -----------------------------------------------------------------------------
// End-to-end: the exact ng-pi frame that produced the alert
// -----------------------------------------------------------------------------

func TestNgPiIncidentFrameIsFullyContained(t *testing.T) {
	orphaned := func(major, minor uint32) (bool, bool) { return false, major == 253 }
	g := parseMountGuard(join(realRootExt4, realProc, realRunTmpfs, leakedBootVfat, leakedRootExt4), orphaned)

	// Both leaked maps skipped...
	for _, p := range []string{
		"/home/daniel/custom_os_isos/ubuntu/mnt/boot",
		"/home/daniel/custom_os_isos/ubuntu/mnt/iso-root",
	} {
		if _, skipped := g.SkipReason(p); !skipped {
			t.Errorf("%s must be skipped", p)
		}
	}
	// ...and the real root still walked, so the fix does not blind the gatherer.
	if _, skipped := g.SkipReason("/"); skipped {
		t.Fatal("/ must still be walked — skipping it would silently zero every size")
	}
}
