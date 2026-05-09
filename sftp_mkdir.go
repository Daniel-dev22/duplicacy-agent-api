package main

// Pre-init helper: ensure the SFTP storage directory exists on the remote host
// before invoking `duplicacy init`. duplicacy stats the storage path during
// init and aborts with "Can't access the storage path …: file does not exist"
// if the directory is missing — preventing a fresh repo from ever starting on
// a host that hasn't been hand-prepared.
//
// We open a single SSH session reusing the same key/passphrase the credential
// would have given to duplicacy, run `mkdir -p PATH`, and close. Idempotent:
// `mkdir -p` on an existing dir is a no-op.
//
// Only sftp:// URLs are handled — other backends (b2, s3, gcs, azure) create
// their own buckets/containers as needed.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

// ensureSFTPStorageDir parses storageURL, dials the SSH host with keyFile (and
// optional passphrase), and runs `mkdir -p` on the path component.
//
// Non-sftp URLs are a no-op success. Missing key file returns an error so the
// caller can decide whether to abort init or proceed (we abort — without the
// dir duplicacy will fail anyway).
func ensureSFTPStorageDir(ctx context.Context, storageURL, keyFile, passphrase string) error {
	if !strings.HasPrefix(storageURL, "sftp://") {
		return nil
	}
	u, err := url.Parse(storageURL)
	if err != nil {
		return fmt.Errorf("parse sftp url: %w", err)
	}
	if u.User == nil || u.User.Username() == "" {
		return errors.New("sftp url missing username")
	}
	if u.Host == "" {
		return errors.New("sftp url missing host")
	}
	host := u.Host
	if !strings.Contains(host, ":") {
		host += ":22"
	}
	// url.Parse for sftp://host//abs/path leaves u.Path = "//abs/path".
	// Collapse to a single leading slash for the remote `mkdir` argument —
	// the double-slash is only a URL convention, the actual path on the
	// SFTP server is the absolute filesystem path.
	remotePath := strings.TrimLeft(u.Path, "/")
	if remotePath == "" {
		return errors.New("sftp url missing path")
	}
	remotePath = "/" + remotePath

	if keyFile == "" {
		return errors.New("ssh key file path is empty")
	}
	keyBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return fmt.Errorf("read ssh key: %w", err)
	}
	var signer ssh.Signer
	if passphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(passphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(keyBytes)
	}
	if err != nil {
		return fmt.Errorf("parse ssh key: %w", err)
	}

	cfg := &ssh.ClientConfig{
		User: u.User.Username(),
		Auth: []ssh.AuthMethod{ssh.PublicKeys(signer)},
		// We don't pin host keys here — the credential's existing init/backup
		// path doesn't either (duplicacy itself would also accept-on-first-use).
		// The local network and traffic is already inside the home network's
		// trust boundary.
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         15 * time.Second,
	}

	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	d := net.Dialer{Timeout: cfg.Timeout}
	conn, err := d.DialContext(dialCtx, "tcp", host)
	if err != nil {
		return fmt.Errorf("ssh dial %s: %w", host, err)
	}
	c, chans, reqs, err := ssh.NewClientConn(conn, host, cfg)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("ssh handshake: %w", err)
	}
	client := ssh.NewClient(c, chans, reqs)
	defer client.Close()

	// Defence-in-depth: reject anything outside our allowlist before
	// templating the path into a shell command. The URL came from our own
	// credential template, but the check is cheap.
	if !validRemotePath(remotePath) {
		return fmt.Errorf("remote path %q contains disallowed characters", remotePath)
	}

	// Phase 1 — probe with the credential. `test -d PATH` returns 0 if the
	// directory exists and the user can read it. Any non-zero exit means
	// either the path is missing or the SSH user lacks permission. Doing
	// this first gives us a clear "the cred works, we just need to create"
	// signal vs. surfacing an mkdir-failed error that confuses missing-path
	// with no-permission.
	probeSess, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("ssh session (probe): %w", err)
	}
	probeErr := probeSess.Run(fmt.Sprintf("test -d %q", remotePath))
	probeSess.Close()
	if probeErr == nil {
		log.Info().Str("host", host).Str("path", remotePath).Msg("sftp storage dir already exists; skipping mkdir")
		return nil
	}

	// Phase 2 — try to create. If this fails the credential is bad or the
	// parent path is unwritable; bubble both signals up.
	mkSess, err := client.NewSession()
	if err != nil {
		return fmt.Errorf("ssh session (mkdir): %w", err)
	}
	defer mkSess.Close()
	out, err := mkSess.CombinedOutput(fmt.Sprintf("mkdir -p %q", remotePath))
	if err != nil {
		return fmt.Errorf("remote mkdir -p %s failed: %w (output: %s)", remotePath, err, strings.TrimSpace(string(out)))
	}
	log.Info().Str("host", host).Str("path", remotePath).Msg("created sftp storage dir")
	return nil
}

// validRemotePath restricts to characters safe in a "%q" Go-quoted shell
// argument (no $, backtick, newline, etc.). Allows the typical paths we use
// (/mnt/array/...).
func validRemotePath(p string) bool {
	if p == "" {
		return false
	}
	for _, r := range p {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '/', r == '-', r == '_', r == '.':
			continue
		default:
			return false
		}
	}
	return true
}
