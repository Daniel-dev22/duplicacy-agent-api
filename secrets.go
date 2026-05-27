package main

// Per-run credential injection for duplicacy invocations.
//
// Three responsibilities:
//
//  1. SecretsBundle — what the controller hands us when we vend secrets.
//
//  2. buildEnv(storageType, storageAlias, isPrimary, bundle) — produces:
//       - the DUPLICACY_*_* env strings for that storage (primary always uses
//         the bare DUPLICACY_ prefix; non-primary non-"default" aliases get
//         DUPLICACY_<ALIAS>_)
//       - any /dev/shm tmpfiles created (PEM keys for SFTP, JSON keys for GCS)
//     Caller is responsible for unlinking the tmpfiles after duplicacy exits.
//
//  3. secretCache — 60s TTL cache keyed by credential_id, with manual
//     invalidation hook (controller calls /internal/credentials/:id/invalidate).
//     Cache entries also auto-evict when a fresh vend response shows a newer
//     UpdatedAt than what we cached.

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// convertRSAPrivKeyToPKCS1 normalises an RSA private key PEM to PKCS#1
// ("BEGIN RSA PRIVATE KEY"), which is the only format duplicacy 3.2.5
// supports. Unencrypted PKCS#8 keys ("BEGIN PRIVATE KEY") are converted
// in-memory; PKCS#1 input is returned as-is. Encrypted PKCS#8
// ("BEGIN ENCRYPTED PRIVATE KEY") is left alone — duplicacy's
// -key-passphrase handles those itself.
//
// Witness: 2026-05-27 first-night copy attempts failed with "Unsupported
// private key type PRIVATE KEY" because Bitwarden/operator-stored keys are
// PKCS#8 by default and we never normalised before materialising.
func convertRSAPrivKeyToPKCS1(input []byte) []byte {
	block, _ := pem.Decode(input)
	if block == nil || block.Type != "PRIVATE KEY" {
		return input
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return input
	}
	rsaKey, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return input
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})
}

// SecretsBundle mirrors the JSON returned by controller's
// GET /api/duplicacy/credentials/:id/secrets-for-node/:node endpoint.
type SecretsBundle struct {
	StorageURL         string            `json:"storage_url"`
	StorageType        string            `json:"storage_type"`
	EncryptionPassword string            `json:"encryption_password"`
	Backend            map[string]string `json:"backend,omitempty"`
	UpdatedAt          time.Time         `json:"updated_at"`
}

// envSpec is one env var to set on the duplicacy command.
type envSpec struct {
	Name  string
	Value string
}

// buildEnvResult collects the env additions and any tmpfiles to clean up.
type buildEnvResult struct {
	Env      []string // formatted as "NAME=VALUE", ready to append to cmd.Env
	Tmpfiles []string // absolute paths to unlink after duplicacy exits
	// RSA asymmetric encryption paths, populated when the credential bundle
	// carries rsa_public_key / rsa_private_key. Always /dev/shm tmpfiles —
	// already included in Tmpfiles for cleanup. Empty string when not present.
	RSAPubPath  string // pass via -key on duplicacy init / duplicacy add
	RSAPrivPath string // pass via -key on duplicacy restore
}

// buildEnv produces env vars for one storage. storageAlias is the duplicacy
// storage name ("default" for the primary, custom strings for secondaries).
//
// isPrimary forces the bare DUPLICACY_ prefix regardless of storageAlias.
// Rationale: `duplicacy init` always creates the primary preference with
// name="default" (cmd flag -storage-name unused; see duplicacy_main.go:263-267),
// and duplicacy's env-var lookup uses preference.Name (utils.go:165-186) — so
// for the primary we must emit unprefixed DUPLICACY_* even if the controller
// gave us a non-"default" alias. Without this, a primary registered with
// alias "kd-nas" would have its DUPLICACY_KD-NAS_SSH_KEY_FILE set but
// duplicacy would look up DUPLICACY_SSH_KEY_FILE — finding nothing, falling
// to interactive prompt, failing init with "No private key file is provided".
//
// Mapping rules (primary OR alias=="default" uses bare prefix; other aliased
// storages prepend DUPLICACY_<ALIAS_UPPER>_ where the var name lives):
//
//   - encryption_password         → DUPLICACY_PASSWORD              (or DUPLICACY_<ALIAS>_PASSWORD)
//   - storage_type b2:
//     backend.b2_id             → DUPLICACY_B2_ID                 (or DUPLICACY_<ALIAS>_B2_ID)
//     backend.b2_key            → DUPLICACY_B2_KEY                (or DUPLICACY_<ALIAS>_B2_KEY)
//   - storage_type s3:
//     backend.s3_id             → DUPLICACY_S3_ID
//     backend.s3_secret         → DUPLICACY_S3_SECRET
//     backend.s3_token          → DUPLICACY_S3_TOKEN              (optional)
//   - storage_type sftp:
//     if backend.ssh_key set    → write to /dev/shm tmp + DUPLICACY_SSH_KEY_FILE
//     backend.ssh_key_passphrase→ DUPLICACY_SSH_KEY_PASSPHRASE
//     backend.ssh_password      → DUPLICACY_SSH_PASSWORD
//   - storage_type gcs:
//     backend.gcs_service_account_json → write to /dev/shm tmp + DUPLICACY_GCS_TOKEN_FILE
//   - storage_type azure:
//     backend.azure_account     → DUPLICACY_AZURE_ACCOUNT
//     backend.azure_key         → DUPLICACY_AZURE_KEY
//   - storage_type local:         no backend env — only the encryption password.
//
// RSA asymmetric encryption (orthogonal to storage_type — any backend can opt
// in by setting these on the credential):
//   - backend.rsa_public_key   → write to /dev/shm tmp + RSAPubPath  (pass via -key on init/add)
//   - backend.rsa_private_key  → write to /dev/shm tmp + RSAPrivPath (pass via -key on restore)
//   - backend.rsa_passphrase   → DUPLICACY_RSA_PASSPHRASE            (unprefixed — duplicacy global)
//
// Any unrecognised backend keys cause buildEnv to return an error rather than
// silently dropping them — surfaces typos and schema drift early.
func buildEnv(storageType, storageAlias string, isPrimary bool, b SecretsBundle) (buildEnvResult, error) {
	if b.EncryptionPassword == "" {
		return buildEnvResult{}, errors.New("encryption_password missing in secrets bundle")
	}

	prefix := "DUPLICACY_"
	if !isPrimary && storageAlias != "" && !strings.EqualFold(storageAlias, "default") {
		prefix = "DUPLICACY_" + strings.ToUpper(storageAlias) + "_"
	}

	res := buildEnvResult{}
	add := func(name, value string) {
		res.Env = append(res.Env, prefix+name+"="+value)
	}
	add("PASSWORD", b.EncryptionPassword)

	known := map[string]bool{}
	mark := func(k string) { known[k] = true }

	switch storageType {
	case "b2":
		if v, ok := b.Backend["b2_id"]; ok {
			add("B2_ID", v)
			mark("b2_id")
		} else {
			return res, errors.New("b2 storage missing backend.b2_id")
		}
		if v, ok := b.Backend["b2_key"]; ok {
			add("B2_KEY", v)
			mark("b2_key")
		} else {
			return res, errors.New("b2 storage missing backend.b2_key")
		}

	case "s3":
		if v, ok := b.Backend["s3_id"]; ok {
			add("S3_ID", v)
			mark("s3_id")
		} else {
			return res, errors.New("s3 storage missing backend.s3_id")
		}
		if v, ok := b.Backend["s3_secret"]; ok {
			add("S3_SECRET", v)
			mark("s3_secret")
		} else {
			return res, errors.New("s3 storage missing backend.s3_secret")
		}
		if v, ok := b.Backend["s3_token"]; ok && v != "" {
			add("S3_TOKEN", v)
			mark("s3_token")
		}

	case "sftp":
		hasKey, _ := b.Backend["ssh_key"]
		hasPwd, _ := b.Backend["ssh_password"]
		if hasKey == "" && hasPwd == "" {
			return res, errors.New("sftp storage requires backend.ssh_key or backend.ssh_password")
		}
		if hasKey != "" {
			tmp, err := writeShmTmp("duplicacy-sshkey-", []byte(hasKey))
			if err != nil {
				return res, fmt.Errorf("materialize ssh_key: %w", err)
			}
			add("SSH_KEY_FILE", tmp)
			res.Tmpfiles = append(res.Tmpfiles, tmp)
			mark("ssh_key")
			if pp, ok := b.Backend["ssh_key_passphrase"]; ok && pp != "" {
				add("SSH_KEY_PASSPHRASE", pp)
				mark("ssh_key_passphrase")
			}
		}
		if hasPwd != "" {
			add("SSH_PASSWORD", hasPwd)
			mark("ssh_password")
		}

	case "gcs":
		v, ok := b.Backend["gcs_service_account_json"]
		if !ok || v == "" {
			return res, errors.New("gcs storage missing backend.gcs_service_account_json")
		}
		tmp, err := writeShmTmp("duplicacy-gcskey-", []byte(v))
		if err != nil {
			return res, fmt.Errorf("materialize gcs_service_account_json: %w", err)
		}
		add("GCS_TOKEN_FILE", tmp)
		res.Tmpfiles = append(res.Tmpfiles, tmp)
		mark("gcs_service_account_json")

	case "azure":
		if v, ok := b.Backend["azure_account"]; ok && v != "" {
			add("AZURE_ACCOUNT", v)
			mark("azure_account")
		}
		if v, ok := b.Backend["azure_key"]; ok && v != "" {
			add("AZURE_KEY", v)
			mark("azure_key")
		} else {
			return res, errors.New("azure storage missing backend.azure_key")
		}

	case "local":
		// no backend secrets — encryption password handled above

	default:
		return res, fmt.Errorf("unknown storage_type %q", storageType)
	}

	// RSA asymmetric encryption (-e -key) is orthogonal to storage_type — any
	// backend may carry an RSA keypair. Materialize each PEM to /dev/shm and
	// return the paths so init/add/restore can pass `-key <path>`. The
	// passphrase, when present, sets DUPLICACY_RSA_PASSPHRASE (unprefixed —
	// duplicacy reads only the global form for restore key decryption).
	if v, ok := b.Backend["rsa_public_key"]; ok && v != "" {
		tmp, err := writeShmTmp("duplicacy-rsa-pub-", []byte(v))
		if err != nil {
			cleanupTmpfiles(res.Tmpfiles)
			return buildEnvResult{}, fmt.Errorf("materialize rsa_public_key: %w", err)
		}
		res.RSAPubPath = tmp
		res.Tmpfiles = append(res.Tmpfiles, tmp)
		mark("rsa_public_key")
	}
	if v, ok := b.Backend["rsa_private_key"]; ok && v != "" {
		tmp, err := writeShmTmp("duplicacy-rsa-priv-", convertRSAPrivKeyToPKCS1([]byte(v)))
		if err != nil {
			cleanupTmpfiles(res.Tmpfiles)
			return buildEnvResult{}, fmt.Errorf("materialize rsa_private_key: %w", err)
		}
		res.RSAPrivPath = tmp
		res.Tmpfiles = append(res.Tmpfiles, tmp)
		mark("rsa_private_key")
	}
	if v, ok := b.Backend["rsa_passphrase"]; ok && v != "" {
		res.Env = append(res.Env, "DUPLICACY_RSA_PASSPHRASE="+v)
		mark("rsa_passphrase")
	}

	for k := range b.Backend {
		if !known[k] {
			cleanupTmpfiles(res.Tmpfiles)
			return buildEnvResult{}, fmt.Errorf(
				"unrecognised backend key %q for storage_type %s", k, storageType)
		}
	}

	return res, nil
}

// writeShmTmp creates a /dev/shm tmpfile (mode 0600) with the given content
// and returns its absolute path. /dev/shm is tmpfs (RAM-backed) on Linux, so
// the bytes never touch disk. Caller MUST unlink afterwards.
func writeShmTmp(prefix string, data []byte) (string, error) {
	const dir = "/dev/shm"
	f, err := os.CreateTemp(dir, prefix+"*")
	if err != nil {
		return "", err
	}
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// cleanupTmpfiles unlinks each path; logs a warning on individual failures
// but never returns one (cleanup is best-effort).
func cleanupTmpfiles(paths []string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// -----------------------------------------------------------------------------
// secretCache — short-lived in-memory cache keyed by credential_id
// -----------------------------------------------------------------------------

const secretCacheTTL = 60 * time.Second

type cachedSecret struct {
	bundle   SecretsBundle
	cachedAt time.Time
}

type secretCache struct {
	mu      sync.RWMutex
	entries map[string]cachedSecret // key = credential_id
}

func newSecretCache() *secretCache {
	return &secretCache{entries: map[string]cachedSecret{}}
}

// get returns the cached bundle if it is still fresh. Otherwise (false, _) and
// the caller must re-vend.
func (c *secretCache) get(credentialID string) (SecretsBundle, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[credentialID]
	if !ok {
		return SecretsBundle{}, false
	}
	if time.Since(e.cachedAt) > secretCacheTTL {
		return SecretsBundle{}, false
	}
	return e.bundle, true
}

// put stores a freshly-vended bundle. If a cache entry already exists with a
// newer UpdatedAt than the incoming bundle, the existing entry is preserved
// (defensive — should not normally happen because the controller is the
// single source of truth).
func (c *secretCache) put(credentialID string, b SecretsBundle) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[credentialID]; ok {
		if !b.UpdatedAt.IsZero() && existing.bundle.UpdatedAt.After(b.UpdatedAt) {
			return
		}
	}
	c.entries[credentialID] = cachedSecret{bundle: b, cachedAt: time.Now()}
}

// invalidate drops the entry for credentialID. Called by the
// /internal/credentials/:id/invalidate handler.
func (c *secretCache) invalidate(credentialID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, credentialID)
}

// invalidateAll drops every entry. Useful on agent SIGHUP / config reload.
func (c *secretCache) invalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = map[string]cachedSecret{}
}
