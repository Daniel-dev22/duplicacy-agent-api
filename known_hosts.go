package main

// Agent-managed SSH known_hosts for SFTP storages.
//
// duplicacy's SFTP backend (and Go's crypto/ssh more generally) refuse to
// dial an SSH host whose key isn't pre-trusted. Inside the agent container
// /root/.ssh/known_hosts doesn't exist on a fresh boot, so the first
// `duplicacy list/backup/restore` against an sftp:// storage fails the SSH
// handshake before ever reaching the storage path.
//
// We handle this entirely agent-side:
//
//   ensureKnownHostsSetup() — called once at startup. Idempotent: creates
//     /root/.ssh, touches <CONFIG_DIR>/known_hosts if missing, and symlinks
//     /root/.ssh/known_hosts to it so duplicacy finds the file where its
//     SSH library expects.
//
//   ensureHostKey(host) — called before any agent-initiated SSH handshake
//     (i.e. ensureSFTPStorageDir's pre-flight). Idempotent: dials the host,
//     captures the offered host key, and appends a known_hosts line if no
//     entry for this (host, key-type) is already present. State persists
//     across container restarts via the existing state bind mount; in
//     steady state subsequent calls are a no-op file-stat + grep.
//
// We deliberately do NOT trust on first use blindly — we still verify the
// key against the cached entry on every subsequent dial. The "trust" is
// only widened on the very first contact per (host, key-type).

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// sftpHostFromURL extracts host[:port] from an sftp:// URL. Returns ("", false)
// for non-SFTP URLs or unparseable inputs. Defaults port to 22 if missing so
// the value is directly usable with net.Dial / known_hosts lookup.
func sftpHostFromURL(storageURL string) (string, bool) {
	if !strings.HasPrefix(storageURL, "sftp://") {
		return "", false
	}
	u, err := url.Parse(storageURL)
	if err != nil || u.Host == "" {
		return "", false
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":22"
	}
	return host, true
}

// known_hosts file path inside the persistent state mount. /root/.ssh/known_hosts
// is symlinked here at startup so duplicacy's invocations transparently use it.
const (
	knownHostsContainerSymlink = "/root/.ssh/known_hosts"
)

// knownHostsAppendMu serializes appends so concurrent ensureSFTPStorageDir
// calls don't interleave-write to the file.
var knownHostsAppendMu sync.Mutex

// knownHostsPath returns the persistent location for the file under the
// configured state dir.
func knownHostsPath(cfg Config) string {
	return filepath.Join(cfg.ConfigDir, "known_hosts")
}

// ensureKnownHostsSetup ensures /root/.ssh exists and symlinks
// /root/.ssh/known_hosts → <CONFIG_DIR>/known_hosts. Safe to call on every
// startup — does nothing on repeated calls. Returns an error only if a
// filesystem op fails in a way that would block SSH from working at all.
func ensureKnownHostsSetup(cfg Config) error {
	if err := os.MkdirAll(filepath.Dir(knownHostsContainerSymlink), 0700); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(knownHostsContainerSymlink), err)
	}
	target := knownHostsPath(cfg)
	// Touch the target file if missing so the symlink resolves to a valid
	// path on the very first SSH handshake (Go's crypto/ssh tolerates an
	// empty file but not a dangling symlink).
	if _, err := os.Stat(target); errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("create %s: %w", target, err)
		}
		_ = f.Close()
	} else if err != nil {
		return fmt.Errorf("stat %s: %w", target, err)
	}

	// Replace any existing /root/.ssh/known_hosts (regular file or stale
	// symlink) with a fresh symlink to the state-dir file. We use a tmp
	// + rename so the symlink swap is atomic; this lets the symlink target
	// move (e.g. operator changes CONFIG_DIR) without leaving a window
	// where the file is missing.
	existing, err := os.Lstat(knownHostsContainerSymlink)
	if err == nil {
		// If it's already a symlink pointing where we want, no-op.
		if existing.Mode()&os.ModeSymlink != 0 {
			if cur, err := os.Readlink(knownHostsContainerSymlink); err == nil && cur == target {
				return nil
			}
		}
		// Anything else (regular file from a previous bind-mount, dangling
		// symlink, etc.) — replace it.
		if err := os.Remove(knownHostsContainerSymlink); err != nil {
			return fmt.Errorf("remove old %s: %w", knownHostsContainerSymlink, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("lstat %s: %w", knownHostsContainerSymlink, err)
	}
	if err := os.Symlink(target, knownHostsContainerSymlink); err != nil {
		return fmt.Errorf("symlink %s -> %s: %w", knownHostsContainerSymlink, target, err)
	}
	return nil
}

// ensureHostKey makes sure the persistent known_hosts file has an entry for
// `host` (a hostport like "nas.example.com:22"). If the host is already
// recorded, returns immediately. Otherwise opens a one-shot TCP connection,
// performs the SSH handshake (capturing the offered host key in the
// HostKeyCallback), and appends a properly-formatted known_hosts line.
//
// The captured key is the server's actual host key — we use it as
// authoritative on first contact. Every subsequent dial verifies against
// this stored entry via Go's standard knownhosts.New() callback.
func ensureHostKey(cfg Config, host string) error {
	if host == "" {
		return errors.New("host is required")
	}
	if !strings.Contains(host, ":") {
		host = host + ":22"
	}

	khFile := knownHostsPath(cfg)

	// Fast path: ssh/knownhosts can parse the file. If it has any entry
	// matching the (host, *) tuple we're done — even if the algorithm
	// differs, duplicacy will pick one that matches.
	if hostInKnownHosts(khFile, host) {
		return nil
	}

	// Slow path: dial + capture key. HostKeyCallback receives every key the
	// server offers; we record the first one and proceed (duplicacy will
	// pick its own algorithm from the cached entry on subsequent dials).
	var captured ssh.PublicKey
	cfgSSH := &ssh.ClientConfig{
		// User-less, auth-less — we only want the handshake to learn the
		// host key. The server may close after auth fails; we don't care
		// because we already got the key in HostKeyCallback.
		User: "duplicacy-agent-keyscan",
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			captured = key
			return nil
		},
		Timeout: 10 * time.Second,
	}
	conn, err := net.DialTimeout("tcp", host, cfgSSH.Timeout)
	if err != nil {
		return fmt.Errorf("dial %s for keyscan: %w", host, err)
	}
	defer conn.Close()
	c, chans, reqs, err := ssh.NewClientConn(conn, host, cfgSSH)
	if err == nil {
		// Don't actually use the client; just close it.
		ssh.NewClient(c, chans, reqs).Close()
	}
	if captured == nil {
		return fmt.Errorf("keyscan %s: server did not offer a host key", host)
	}

	line := knownhosts.Line([]string{host}, captured)

	knownHostsAppendMu.Lock()
	defer knownHostsAppendMu.Unlock()
	// Re-check under lock to handle the race where two goroutines target
	// the same host simultaneously.
	if hostInKnownHosts(khFile, host) {
		return nil
	}
	f, err := os.OpenFile(khFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("open %s for append: %w", khFile, err)
	}
	defer f.Close()
	if _, err := f.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("append to %s: %w", khFile, err)
	}
	return nil
}

// hostInKnownHosts returns true if the file already has any entry for the
// given hostport. Uses crypto/ssh/knownhosts so it understands hashed
// entries and port stripping.
func hostInKnownHosts(file, host string) bool {
	cb, err := knownhosts.New(file)
	if err != nil {
		// Empty/missing file is fine — no entries means "not present".
		return false
	}
	// knownhosts.New returns a HostKeyCallback that returns nil on a match.
	// To check membership without a real handshake, query against a
	// throwaway "test" public key — any KeyError where Want is non-empty
	// means "we have entries for this host". The Want slice is exactly the
	// stored entries for the given host.
	dummyKey, _, _, _, err := ssh.ParseAuthorizedKey([]byte(
		"ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDtest test@dummy",
	))
	if err != nil {
		return false
	}
	addr, err := net.ResolveTCPAddr("tcp", host)
	if err != nil {
		// knownhosts API tolerates an unresolved address — we still pass
		// the original string.
		addr = &net.TCPAddr{}
	}
	err = cb(host, addr, dummyKey)
	if err == nil {
		// Match against the dummy key — extraordinarily unlikely; treat as present.
		return true
	}
	var ke *knownhosts.KeyError
	if errors.As(err, &ke) {
		return len(ke.Want) > 0
	}
	return false
}
