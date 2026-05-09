package main

// Pre-init helper: ensure the SFTP storage directory exists on the remote host
// before invoking `duplicacy init`. duplicacy stats the storage path during
// init and aborts with "Can't access the storage path …: file does not exist"
// if the directory is missing — preventing a fresh repo from ever starting on
// a host that hasn't been hand-prepared.
//
// We talk SFTP (not SSH-exec) because the typical `duplicacy@host` user is
// shell-less (e.g. /sbin/nologin) for security: SSH command execution is
// rejected with "This account is currently not available." even though the
// SFTP subsystem is enabled. duplicacy itself uses the SFTP protocol, so
// reusing it here matches the credential's actual capability.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

// ensureSFTPStorageDir parses storageURL, opens an SFTP session over the
// supplied SSH key (and optional passphrase), and recursively creates the
// path component if missing. Idempotent.
//
// Non-sftp URLs are a no-op success. Other backends (b2, s3, gcs, azure)
// create their own buckets/containers as needed.
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
	// Collapse to a single leading slash for the remote path arg — the
	// double slash is only a URL convention; the actual filesystem path
	// is the absolute /-rooted form.
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
		User:            u.User.Username(),
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // same posture as duplicacy itself for sftp
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
	sshClient := ssh.NewClient(c, chans, reqs)
	defer sshClient.Close()

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return fmt.Errorf("open sftp subsystem: %w", err)
	}
	defer sftpClient.Close()

	// Phase 1 — probe. Stat tells us "exists and reachable" vs "missing"
	// vs "permission denied" with clearer error semantics than blindly
	// calling MkdirAll.
	if info, err := sftpClient.Stat(remotePath); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("remote path %s exists but is not a directory", remotePath)
		}
		log.Info().Str("host", host).Str("path", remotePath).Msg("sftp storage dir already exists; skipping mkdir")
		return nil
	} else if !os.IsNotExist(err) {
		// Some non-"missing" error — auth/permission/network. Surface it
		// clearly so the operator doesn't chase a phantom mkdir failure.
		return fmt.Errorf("stat %s via sftp: %w", remotePath, err)
	}

	// Phase 2 — create recursively.
	if err := sftpClient.MkdirAll(remotePath); err != nil {
		return fmt.Errorf("sftp MkdirAll %s: %w", remotePath, err)
	}
	log.Info().Str("host", host).Str("path", remotePath).Msg("created sftp storage dir")
	return nil
}
