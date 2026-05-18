package main

// Bootstrap endpoint: POST /repos/init.
//
// Called by controller after a duplicacy_repos row has been inserted with
// init_status='pending'. Body carries the list of storages and their plaintext
// secrets (the only time plaintext secrets cross to the agent during bootstrap;
// subsequent backup/restore/check/prune pulls from the controller).
//
// Steps per the design doc:
//   1. Validate repo_path is inside an allowed BACKUP_ROOTS mount
//   2. mkdir -p the repo root if missing (mode 0700)
//   3. Run `duplicacy init -encrypt -no-save-password <id> <url>` for primary
//      with env vars built from the primary storage's secrets
//   4. For each secondary, `duplicacy add -encrypt -no-save-password <alias> <id> <url>`
//   5. Scrub .duplicacy/preferences (drop encrypted_password + keys, set
//      no_save_password=true)
//   6. Persist a non-secret repos.json mapping
//   7. Unlink all /dev/shm tmpfiles created in steps 3+4
//   8. Return success/failure + last lines of stderr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// initStorageReq is one storage entry in the init body.
type initStorageReq struct {
	StorageAlias   string        `json:"storage_alias"`
	CredentialID   string        `json:"credential_id"`
	IsPrimary      bool          `json:"is_primary"`
	StorageType    string        `json:"storage_type"`
	StorageURL     string        `json:"storage_url"`
	ServerOverride string        `json:"server_override,omitempty"`
	Secrets        SecretsBundle `json:"secrets"`
}

// initRepoReq is the full init body.
type initRepoReq struct {
	RepoPath string           `json:"repo_path"`
	RepoID   string           `json:"repo_id"`
	RepoUUID string           `json:"repo_uuid"` // duplicacy_repos.id (UUID) — needed for reconcile orphan-detection
	Storages []initStorageReq `json:"storages"`
}

func (a *app) handleInitRepoNew(c *gin.Context) {
	var req initRepoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := a.validateInitReq(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Resolve any {server}/{server_type}/{site}/{home}/{remote_home}/{repo_id}
	// placeholders in each storage_url against this agent's identity + the
	// request's repo_id. Per-storage server_override (if set) overrides the
	// {server} placeholder so multi-storage repos that target different server
	// subdirs can share one credential template. The expanded URL is what
	// gets baked into .duplicacy/preferences by `duplicacy init`, so subsequent
	// backup/restore/check/prune ops never need to re-resolve. Unknown
	// placeholders fail loud here, before any side effects on the storage backend.
	tctx := a.cfg.baseTplCtx()
	tctx.RepoID = req.RepoID
	for i := range req.Storages {
		expanded, err := expandStorageURL(req.Storages[i].StorageURL, tctx, req.Storages[i].ServerOverride)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("storages[%d].storage_url: %v", i, err)})
			return
		}
		req.Storages[i].StorageURL = expanded
	}

	// Ensure repo dir exists.
	if err := ensureDir(req.RepoPath); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "ensure repo dir: " + err.Error()})
		return
	}

	// Locate primary storage (caller-supplied is_primary flag is the source
	// of truth; validate exactly one).
	var primary *initStorageReq
	for i := range req.Storages {
		if req.Storages[i].IsPrimary {
			primary = &req.Storages[i]
			break
		}
	}
	if primary == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no primary storage in request"})
		return
	}

	// Build env + tmpfiles for ALL storages up front. If any buildEnv fails,
	// abort before invoking duplicacy.
	staged := make([]stagedStorage, 0, len(req.Storages))
	rollback := func() {
		for _, s := range staged {
			cleanupTmpfiles(s.tmpfiles)
		}
	}
	for _, s := range req.Storages {
		built, err := buildEnv(s.StorageType, s.StorageAlias, s.IsPrimary, s.Secrets)
		if err != nil {
			rollback()
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("buildEnv %s: %v", s.StorageAlias, err)})
			return
		}
		staged = append(staged, stagedStorage{
			req:        s,
			env:        built.Env,
			tmpfiles:   built.Tmpfiles,
			rsaPubPath: built.RSAPubPath,
		})
	}
	defer rollback() // cleanup unconditionally — duplicacy has already read the files by the time it returns

	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Minute)
	defer cancel()

	// Pre-create the storage directory for any sftp:// backends. duplicacy
	// stats the storage path during init and fails if missing — for a fresh
	// node nobody has hand-prepared the dir yet, so do it ourselves over the
	// same SSH credential. Idempotent (mkdir -p). Non-sftp storages no-op.
	for _, s := range staged {
		if !strings.HasPrefix(s.req.StorageURL, "sftp://") {
			continue
		}
		keyFile, passphrase := sshAuthFromEnv(s.env, s.req.StorageAlias, s.req.IsPrimary)
		if keyFile == "" {
			// No key in env means buildEnv already errored above; the only
			// way we get here without one is sftp-with-password, which
			// duplicacy supports but our pre-mkdir path doesn't yet. Let
			// duplicacy itself handle it — if the dir's missing, init
			// returns the existing clear error.
			continue
		}
		if err := ensureSFTPStorageDir(ctx, s.req.StorageURL, keyFile, passphrase); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("ensure sftp storage dir for %s: %v", s.req.StorageAlias, err),
			})
			return
		}
	}

	// Run init for the primary.
	primaryStaged := stagedFor(staged, primary.StorageAlias)
	initInv := invocationForInit(req.RepoPath, req.RepoID, primary.StorageURL, true /*encrypted*/, primaryStaged.rsaPubPath)
	initInv.EnvAdds = append(initInv.EnvAdds, primaryStaged.env...)
	if out, err := runSync(ctx, a.cfg.DuplicacyBinary, initInv, 4*time.Minute); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":  "duplicacy init failed: " + err.Error(),
			"output": tailString(string(out), 4096),
		})
		return
	}

	// Run add for each secondary, in caller-supplied order.
	for _, s := range staged {
		if s.req.IsPrimary {
			continue
		}
		addInv := invocationForAdd(req.RepoPath, s.req.StorageAlias, req.RepoID, s.req.StorageURL, true, s.rsaPubPath)
		// duplicacy add reads env vars for BOTH the primary (so it can re-init
		// the metadata layer) and the new alias. Pass primary + this storage.
		addInv.EnvAdds = append(addInv.EnvAdds, primaryStaged.env...)
		addInv.EnvAdds = append(addInv.EnvAdds, s.env...)
		if out, err := runSync(ctx, a.cfg.DuplicacyBinary, addInv, 4*time.Minute); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   fmt.Sprintf("duplicacy add %s failed: %v", s.req.StorageAlias, err),
				"output":  tailString(string(out), 4096),
				"primary": "init succeeded; secondary failed",
			})
			return
		}
	}

	// Scrub the preferences file.
	if err := scrubPreferences(req.RepoPath); err != nil {
		slog.Warn("scrub preferences after init failed (continuing)", "error", err, "repo", req.RepoPath)
	}

	// Persist mapping.
	mapping := RepoMapping{
		RepoPath:     req.RepoPath,
		RepoID:       req.RepoID,
		UUID:         req.RepoUUID,
		Storages:     toMappingStorages(req.Storages),
		RegisteredAt: time.Now().UTC(),
	}
	if err := a.mapping.upsert(mapping); err != nil {
		slog.Error("persist repo mapping failed", "error", err, "repo", req.RepoPath)
	}

	// Refresh the in-memory repo index so /repos returns the new repo.
	// ScanForce bypasses the TTL cache — the just-created repo must be visible
	// on the very next /repos poll.
	if err := a.repos.ScanForce(); err != nil {
		slog.Warn("post-init repo scan failed", "error", err)
	}
	// Push the new state to any connected fleet WS subscribers.
	a.fleet.Trigger()

	c.JSON(http.StatusOK, gin.H{
		"status":    "initialized",
		"repo_path": req.RepoPath,
		"repo_id":   req.RepoID,
	})
}

func (a *app) validateInitReq(req *initRepoReq) error {
	if !strings.HasPrefix(req.RepoPath, "/") {
		return errors.New("repo_path must be an absolute path")
	}
	clean := filepath.Clean(req.RepoPath)
	if clean != req.RepoPath {
		return fmt.Errorf("repo_path is not canonical (got %q, want %q)", req.RepoPath, clean)
	}
	// Mounts are mirrored (host path == container path) — the caller's path
	// is already directly usable. We just require it to fall under one of the
	// agent's BACKUP_ROOTS so a malformed request can't init outside the
	// bind-mounted area.
	if !pathInsideAny(clean, a.cfg.BackupRoots) {
		return fmt.Errorf("repo_path %s is not inside any BACKUP_ROOTS (%v)",
			clean, a.cfg.BackupRoots)
	}
	if strings.TrimSpace(req.RepoID) == "" {
		return errors.New("repo_id is required")
	}
	if len(req.Storages) == 0 {
		return errors.New("at least one storage is required")
	}
	primaries := 0
	seen := map[string]bool{}
	for i, s := range req.Storages {
		if s.CredentialID == "" {
			return fmt.Errorf("storages[%d].credential_id is required", i)
		}
		if strings.TrimSpace(s.StorageAlias) == "" {
			return fmt.Errorf("storages[%d].storage_alias is required", i)
		}
		if seen[s.StorageAlias] {
			return fmt.Errorf("duplicate storage_alias %q", s.StorageAlias)
		}
		seen[s.StorageAlias] = true
		if !isAgentValidStorageType(s.StorageType) {
			return fmt.Errorf("storages[%d].storage_type %q invalid", i, s.StorageType)
		}
		if s.StorageURL == "" {
			return fmt.Errorf("storages[%d].storage_url is required", i)
		}
		if s.Secrets.EncryptionPassword == "" {
			return fmt.Errorf("storages[%d].secrets.encryption_password is required", i)
		}
		if s.IsPrimary {
			primaries++
		}
	}
	if primaries != 1 {
		return fmt.Errorf("exactly one storage must be primary (got %d)", primaries)
	}
	return nil
}

// pathInsideAny returns true if path is inside any of roots (or equal to a
// root). Both path and roots must be canonical.
func pathInsideAny(path string, roots []string) bool {
	for _, r := range roots {
		rc := filepath.Clean(r)
		if path == rc {
			return true
		}
		if strings.HasPrefix(path, rc+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// translateHostPath is intentionally removed. Mounts are mirrored — host path
// equals container path — so no translation layer exists. Use pathInsideAny
// to validate that a caller-supplied path is under the agent's BACKUP_ROOTS.

func isAgentValidStorageType(s string) bool {
	switch s {
	case "b2", "s3", "sftp", "gcs", "azure", "local":
		return true
	}
	return false
}

type stagedStorage struct {
	req        initStorageReq
	env        []string
	tmpfiles   []string
	rsaPubPath string // /dev/shm path of rsa_public_key PEM, when this storage uses RSA encryption
}

func stagedFor(staged []stagedStorage, alias string) stagedStorage {
	for _, s := range staged {
		if s.req.StorageAlias == alias {
			return s
		}
	}
	return stagedStorage{}
}

// sshAuthFromEnv extracts the SSH key file path and passphrase the secrets
// pipeline materialised for this storage. Mirrors buildEnv's prefix logic:
// primary storages (and any storage with alias "default" or empty) use bare
// DUPLICACY_SSH_KEY_FILE; other aliased storages use DUPLICACY_<ALIAS>_SSH_KEY_FILE.
// Returns empty strings if the storage is password-auth or has no key configured.
func sshAuthFromEnv(env []string, alias string, isPrimary bool) (keyFile, passphrase string) {
	prefix := "DUPLICACY_"
	if !isPrimary && alias != "" && !strings.EqualFold(alias, "default") {
		prefix = "DUPLICACY_" + strings.ToUpper(alias) + "_"
	}
	for _, kv := range env {
		eq := strings.IndexByte(kv, '=')
		if eq < 0 {
			continue
		}
		k, v := kv[:eq], kv[eq+1:]
		switch k {
		case prefix + "SSH_KEY_FILE":
			keyFile = v
		case prefix + "SSH_KEY_PASSPHRASE":
			passphrase = v
		}
	}
	return
}

func toMappingStorages(in []initStorageReq) []RepoStorageMapping {
	out := make([]RepoStorageMapping, 0, len(in))
	for _, s := range in {
		out = append(out, RepoStorageMapping{
			StorageAlias: s.StorageAlias,
			CredentialID: s.CredentialID,
			StorageType:  s.StorageType,
			IsPrimary:    s.IsPrimary,
		})
	}
	return out
}

func tailString(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// -----------------------------------------------------------------------------
// Bind endpoint: POST /repos/bind
//
// Same payload as /repos/init but skips the actual `duplicacy init`/`add`
// calls — purely refreshes the agent-local controller mapping (repos.json)
// for a repo that is already initialized on disk. Use when the agent's
// repos.json has been wiped (volume reset, post-migration cleanup) but the
// repo's .duplicacy/preferences is intact. Idempotent.
//
// Distinct from /repos/init because init's first step is `duplicacy init`
// which refuses with "The repository … has already been initialized" — so
// /repos/init cannot be a re-bind safely.
// -----------------------------------------------------------------------------

func (a *app) handleBindRepo(c *gin.Context) {
	var req initRepoReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := a.validateInitReq(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Expand storage URL placeholders against this agent's identity. Mirrors
	// /repos/init so the persisted mapping reflects what duplicacy actually
	// reads from preferences.
	tctx := a.cfg.baseTplCtx()
	tctx.RepoID = req.RepoID
	for i := range req.Storages {
		expanded, err := expandStorageURL(req.Storages[i].StorageURL, tctx, req.Storages[i].ServerOverride)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("storages[%d].storage_url: %v", i, err)})
			return
		}
		req.Storages[i].StorageURL = expanded
	}

	// Refuse if .duplicacy/preferences is missing — bind is only valid for
	// already-initialized repos. For fresh init, callers should use /repos/init.
	prefsPath := filepath.Join(req.RepoPath, ".duplicacy", "preferences")
	if _, err := os.Stat(prefsPath); err != nil {
		c.JSON(http.StatusConflict, gin.H{
			"error": fmt.Sprintf("repo at %s is not initialized (%s missing); use /repos/init instead", req.RepoPath, prefsPath),
		})
		return
	}

	// Persist mapping. Same shape as /repos/init.
	mapping := RepoMapping{
		RepoPath:     req.RepoPath,
		RepoID:       req.RepoID,
		UUID:         req.RepoUUID,
		Storages:     toMappingStorages(req.Storages),
		RegisteredAt: time.Now().UTC(),
	}
	if err := a.mapping.upsert(mapping); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "persist repo mapping: " + err.Error()})
		return
	}

	// Drop any cached credentials so the very next vend re-fetches with the
	// fresh mapping. Cheap insurance against stale-credential bugs where the
	// mapping changes but the cache still has the old binding.
	for _, s := range req.Storages {
		a.secrets.invalidate(s.CredentialID)
	}

	// Force a repo rescan so /repos picks up the now-bound state, and notify
	// any WS subscribers.
	if err := a.repos.ScanForce(); err != nil {
		slog.Warn("post-bind repo scan failed", "error", err)
	}
	a.fleet.Trigger()

	c.JSON(http.StatusOK, gin.H{
		"status":    "bound",
		"repo_path": req.RepoPath,
		"repo_id":   req.RepoID,
	})
}

// -----------------------------------------------------------------------------
// Cache invalidation endpoint
// -----------------------------------------------------------------------------

func (a *app) handleInvalidateCredential(c *gin.Context) {
	credentialID := c.Param("id")
	if credentialID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "credential id required"})
		return
	}
	a.secrets.invalidate(credentialID)
	c.Status(http.StatusNoContent)
}
