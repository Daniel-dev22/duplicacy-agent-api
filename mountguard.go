package main

// Mount classification for the directory-size walker.
//
// WHY THIS EXISTS
//
// The gatherer's roots are host territory (/home/daniel, /docker_container_volumes,
// /var/lib/rancher/k3s). Anything may appear underneath them, and on both Pis an
// OS-image build did: it loop-mounted a Raspberry Pi image at
// custom_os_isos/ubuntu/mnt/{boot,iso-root} via kpartx, and bind-mounted /proc,
// /sys, /dev, /dev/pts and /run under iso-root/ for its chroot. The build's cleanup
// then detached the loop device WITHOUT removing the kpartx maps (they were pinned
// by a container's private mount namespace), leaving two device-mapper targets whose
// backing device no longer exists. Every read returns EIO.
//
// The walker descended into them nightly and the kernel logged:
//
//	EXT4-fs error (device dm-1): ext4_get_inode_loc:5004: inode #2: block 486:
//	  comm duplicacy-agent: unable to read itable block
//	Aborting journal on device dm-1-8.
//
// Measured blast radius before this guard: 142,066 of 143,203 cached directory keys
// on ng-pi (99.2%) and 176,998 of 179,918 on kd-pi (98.4%) were phantom entries under
// that image tree — 5.25 million files "counted" that do not exist. dir_sizes.json
// reached 28.5 MB / 35 MB and was re-uploaded to the backup every night.
//
// WHY NOT A PATH LIST
//
// A hardcoded exclude only covers the leak we already found. These three signals
// classify the mount at runtime, so the NEXT leaked image build is skipped on sight.
// All three were verified against the live hosts; see the per-rule comments.
//
// WHY NOT A BLANKET -xdev
//
// "device differs → skip" is WRONG here. docker-compose.yml.j2 records that on the
// NAS, BACKUP_ROOTS=['/mnt'] — if those pools are separate filesystems, an xdev guard
// silently under-reports every one of them and the failure looks like a correct
// answer. So we classify by fstype / backing / liveness, and a real data filesystem
// on its own device is still descended.

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// pseudoFSTypes are kernel-synthetic or overlay filesystems that hold no backup
// payload. Sizing them is meaningless and descending them is actively harmful: on
// kd-pi the leaked chroot's iso-root/run was a peer of the host's live /run, so the
// walk reached 16 k3s container `overlay` rootfs, 9 `nsfs` netns and 8 `shm` mounts —
// every running container's filesystem, counted as if it were the user's data.
var pseudoFSTypes = map[string]struct{}{
	"autofs": {}, "binfmt_misc": {}, "bpf": {}, "cgroup": {}, "cgroup2": {},
	"configfs": {}, "debugfs": {}, "devpts": {}, "devtmpfs": {}, "efivarfs": {},
	"fusectl": {}, "hugetlbfs": {}, "mqueue": {}, "nsfs": {}, "overlay": {},
	"proc": {}, "pstore": {}, "ramfs": {}, "rpc_pipefs": {}, "securityfs": {},
	"selinuxfs": {}, "squashfs": {}, "sysfs": {}, "tmpfs": {}, "tracefs": {},
}

// mountEntry is one line of /proc/self/mountinfo, reduced to what the guard needs.
type mountEntry struct {
	Dev        uint64 // unix.Mkdev(major, minor) — comparable to unix.Stat_t.Dev
	MountPoint string
	FSType     string
	Source     string
}

// mountGuard answers one question: "may the walker descend into this directory?"
//
// It is built ONCE per gatherer pass (one file read plus a little sysfs), not per
// directory, and consulted only when the walker is about to descend into a path that
// is itself a mount point.
type mountGuard struct {
	// skip maps an absolute mount point to the reason it must not be descended.
	// Keyed by path rather than by device on purpose: a bind mount of a directory
	// on the SAME filesystem shares its st_dev, so a device-keyed map would miss it.
	// A path lookup also costs less than the openat+getdents the walker performs for
	// every directory anyway, so there is nothing to gain from a device pre-filter.
	skip map[string]string
	// total is the number of mounts parsed, for the summary log.
	total int
}

// newMountGuard reads /proc/self/mountinfo and classifies every mount.
//
// Reading /proc/SELF/mountinfo is deliberate: inside a container this is the
// container's own namespace, which is exactly where the stale mounts live. On ng-pi
// the host had already unmounted its copies and detached the loop device, yet the
// agent's namespace still held both — reading the host's table would have shown
// nothing to skip while the walker walked straight into the dead filesystem.
func newMountGuard() (*mountGuard, error) {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return nil, fmt.Errorf("read mountinfo: %w", err)
	}
	return parseMountGuard(string(data), sysfsDeviceKind), nil
}

// deviceKindFunc classifies a block device by major:minor. Injected so tests can
// exercise the rules against captured real-world sysfs shapes without root or mounts.
type deviceKindFunc func(major, minor uint32) (loopBacked, orphanedDM bool)

// parseMountGuard is the testable core: pure parsing plus classification.
func parseMountGuard(mountinfo string, kind deviceKindFunc) *mountGuard {
	g := &mountGuard{skip: map[string]string{}}
	for _, line := range strings.Split(mountinfo, "\n") {
		e, ok := parseMountinfoLine(line)
		if !ok {
			continue
		}
		g.total++
		if reason, skip := classifyMount(e, kind); skip {
			g.skip[e.MountPoint] = reason
		}
	}
	return g
}

// classifyMount applies the three rules. Order matters only for the reason string.
func classifyMount(e mountEntry, kind deviceKindFunc) (string, bool) {
	// Rule 1 — kernel-synthetic filesystem. On kd-pi this alone removed 41 of the
	// 43 mounts under the leaked chroot.
	if _, ok := pseudoFSTypes[e.FSType]; ok {
		return "pseudo filesystem (" + e.FSType + ")", true
	}

	major, minor := unix.Major(e.Dev), unix.Minor(e.Dev)
	loopBacked, orphanedDM := kind(major, minor)

	// Rule 2 — a mounted disk image. The two image partitions are vfat and ext4, so
	// rule 1 passes them; this is what catches them, and it catches them while the
	// mount is still HEALTHY. Verified on kd-pi:
	//   /sys/dev/block/253:0/slaves/        -> loop10
	//   /sys/block/loop10/loop/backing_file -> …/custom_os_original_images/…img
	if loopBacked {
		return "loop-backed disk image (" + e.Source + ")", true
	}

	// Rule 3 — an orphaned device-mapper target: the map outlived its backing device,
	// so every I/O returns EIO. Verified on ng-pi, where both
	// /sys/dev/block/253:{0,1}/slaves/ were empty because loop2 had been detached.
	if orphanedDM {
		return "orphaned device-mapper target, no backing device (" + e.Source + ")", true
	}

	return "", false
}

// SkipReason reports why the walker must not descend into path, if it must not.
// A path that is not a mount point is never skipped here — ordinary directories are
// the filter set's business, not the mount guard's.
func (g *mountGuard) SkipReason(path string) (string, bool) {
	if g == nil {
		return "", false
	}
	reason, ok := g.skip[path]
	return reason, ok
}

// parseMountinfoLine parses one /proc/*/mountinfo record.
//
//	36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue
//	                 ^mountpoint                 ^sep ^fstype ^source
//
// Optional fields (master:, shared:, propagate_from:) are variable in number and end
// at the "-" separator, so the tail must be located rather than indexed.
func parseMountinfoLine(line string) (mountEntry, bool) {
	f := strings.Fields(line)
	if len(f) < 10 {
		return mountEntry{}, false
	}
	sep := -1
	for i := 6; i < len(f); i++ {
		if f[i] == "-" {
			sep = i
			break
		}
	}
	if sep < 0 || sep+2 >= len(f) {
		return mountEntry{}, false
	}
	major, minor, ok := parseMajorMinor(f[2])
	if !ok {
		return mountEntry{}, false
	}
	return mountEntry{
		Dev:        unix.Mkdev(major, minor),
		MountPoint: unescapeMountinfo(f[4]),
		FSType:     f[sep+1],
		Source:     unescapeMountinfo(f[sep+2]),
	}, true
}

func parseMajorMinor(s string) (uint32, uint32, bool) {
	maj, min, found := strings.Cut(s, ":")
	if !found {
		return 0, 0, false
	}
	a, err1 := strconv.ParseUint(maj, 10, 32)
	b, err2 := strconv.ParseUint(min, 10, 32)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return uint32(a), uint32(b), true
}

// unescapeMountinfo decodes the octal escapes the kernel applies to space, tab,
// newline and backslash in mountinfo path fields.
func unescapeMountinfo(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+3 < len(s) {
			if v, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(v))
				i += 4
				continue
			}
		}
		b.WriteByte(s[i])
		i++
	}
	return b.String()
}

// sysfsDeviceKind walks the sysfs holder graph for a block device.
//
// A plain partition (/sys/dev/block/8:2) has no "slaves" directory and is neither
// loop-backed nor orphaned — it is descended normally, which is what keeps a real
// data filesystem on its own device working.
func sysfsDeviceKind(major, minor uint32) (loopBacked, orphanedDM bool) {
	return deviceKindAt(fmt.Sprintf("/sys/dev/block/%d:%d", major, minor), 0)
}

// deviceKindAt is the recursive half. Depth-bounded: the holder graph is a DAG in
// practice, but a bounded walk is cheaper than cycle tracking and 8 levels is far
// beyond any real dm stack.
func deviceKindAt(base string, depth int) (loopBacked, orphanedDM bool) {
	if depth > 8 {
		return false, false
	}
	// A loop device carries loop/backing_file naming the image it serves.
	if _, err := os.Stat(filepath.Join(base, "loop", "backing_file")); err == nil {
		return true, false
	}
	_, dmErr := os.Stat(filepath.Join(base, "dm", "name"))
	isDM := dmErr == nil

	slaves, err := os.ReadDir(filepath.Join(base, "slaves"))
	if err != nil {
		// No holder graph: an ordinary disk or partition.
		return false, false
	}
	// A device-mapper target with no slaves has lost its backing device. This is
	// exactly the ng-pi state: the kpartx maps survived `losetup -d` because a
	// container namespace pinned them, and every read against them returns EIO.
	if isDM && len(slaves) == 0 {
		return false, true
	}
	for _, s := range slaves {
		lb, od := deviceKindAt(filepath.Join(base, "slaves", s.Name()), depth+1)
		if lb {
			return true, false
		}
		if od {
			orphanedDM = true
		}
	}
	return false, orphanedDM
}

// signature is a stable digest of the skip set, so the caller can log on CHANGE
// rather than every pass. Sorted because Go map iteration order is randomised and
// an unsorted join would report a change on every pass — which is precisely the
// every-cycle logging this is meant to avoid.
func (g *mountGuard) signature() string {
	if g == nil || len(g.skip) == 0 {
		return ""
	}
	parts := make([]string, 0, len(g.skip))
	for p, reason := range g.skip {
		parts = append(parts, p+"\x00"+reason)
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x01")
}

// logSummary reports what the guard will skip, so a newly-leaked mount is visible in
// the agent log the night it appears rather than only as a kernel EXT4 error.
func (g *mountGuard) logSummary() {
	if g == nil || len(g.skip) == 0 {
		return
	}
	paths := make([]string, 0, len(g.skip))
	for p, reason := range g.skip {
		paths = append(paths, p+" ("+reason+")")
	}
	sort.Strings(paths)
	slog.Info("dir size gatherer: mount guard active",
		"mounts_seen", g.total, "mounts_skipped", len(g.skip), "skipped", paths)
}
