package main

// Bootstrap endpoint: POST /repos/init.
//
// Called by the controller after a repo row has been inserted with
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
	"bytes"
	"context"
	"encoding/json"
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
		// Authoritative region guard: the URL is now fully expanded, so an empty
		// S3 region with a custom endpoint here would make duplicacy fail with a
		// cryptic "MissingRegion". Fail loud before any side effects instead.
		if s3URLMissingRegion(req.Storages[i].StorageType, expanded) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("storages[%d].storage_url: %s", i, errMissingS3Region)})
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

	// Run init for the primary. duplicacy init refuses ("has already been
	// initialized") when .duplicacy/preferences already exists — which happens
	// on a re-register, or when the self-init sweep races the first init. Rather
	// than fail, adopt the existing repo IF its on-disk preferences match the
	// requested storages; if a different repo lives here (e.g. a Duplicacy Web
	// repo) we refuse rather than hijack it.
	primaryStaged := stagedFor(staged, primary.StorageAlias)
	initInv := invocationForInit(req.RepoPath, req.RepoID, primary.StorageURL, true /*encrypted*/, primaryStaged.rsaPubPath, cloudOptimizedChunks)
	initInv.EnvAdds = append(initInv.EnvAdds, primaryStaged.env...)
	adopt := false
	if out, err := runSync(ctx, a.cfg.DuplicacyBinary, initInv, 4*time.Minute); err != nil {
		if !isAlreadyInitialized(out) {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":  "duplicacy init failed: " + err.Error(),
				"output": tailString(string(out), 4096),
			})
			return
		}
		adopt = true
	}

	// When adopting, load existing preferences to verify the primary matches and
	// to skip secondaries that are already present.
	var existing map[string]string // storage_alias -> storage URL
	if adopt {
		var perr error
		existing, perr = readPreferenceURLs(req.RepoPath)
		if perr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "read existing preferences: " + perr.Error()})
			return
		}
		if got := existing[primary.StorageAlias]; got != primary.StorageURL {
			c.JSON(http.StatusConflict, gin.H{
				"error": fmt.Sprintf("repo at %s is already initialized with a different primary storage (have %q, want %q) — refusing to adopt", req.RepoPath, got, primary.StorageURL),
			})
			return
		}
	}

	// Run add for each secondary, in caller-supplied order. When adopting, skip
	// any storage already present with a matching URL; conflict on a mismatch.
	for _, s := range staged {
		if s.req.IsPrimary {
			continue
		}
		if adopt {
			if got, ok := existing[s.req.StorageAlias]; ok {
				if got != s.req.StorageURL {
					c.JSON(http.StatusConflict, gin.H{
						"error": fmt.Sprintf("storage %q already initialized with a different URL (have %q, want %q)", s.req.StorageAlias, got, s.req.StorageURL),
					})
					return
				}
				continue // already configured with the same URL — idempotent skip
			}
		}
		// Make secondaries copy-compatible with the primary so `duplicacy copy`
		// can ship chunks without re-chunking. `-bit-identical` additionally lets
		// copy skip re-encryption, but only when the secondary uses the same
		// encryption key as the primary — gate on encryption_password match.
		bitIdentical := s.req.Secrets.EncryptionPassword != "" &&
			s.req.Secrets.EncryptionPassword == primary.Secrets.EncryptionPassword
		addInv := invocationForAdd(req.RepoPath, s.req.StorageAlias, req.RepoID, s.req.StorageURL, true, s.rsaPubPath, primary.StorageAlias, bitIdentical)
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

	// Refresh the in-memory repo index in the BACKGROUND, then notify fleet WS
	// subscribers. A full ScanForce walks all of BACKUP_ROOTS, which on a large
	// host (e.g. a NAS with BACKUP_ROOTS=/mnt) can take minutes — running it
	// inline made init time out and falsely report 'failed' even though
	// preferences+mapping were already persisted. Detach it from the request.
	a.rescanAsync()

	status := "initialized"
	if adopt {
		status = "adopted"
	}
	c.JSON(http.StatusOK, gin.H{
		"status":    status,
		"repo_path": req.RepoPath,
		"repo_id":   req.RepoID,
	})
}

// rescanAsync refreshes the repo index off the request path and notifies fleet
// WS subscribers. Mutating handlers (init/bind/delete) use this so a slow
// full-filesystem scan can't make the operation time out after its real work
// (preferences + mapping) is already durably persisted.
func (a *app) rescanAsync() {
	go func() {
		if err := a.repos.ScanForce(); err != nil {
			slog.Warn("background repo rescan failed", "error", err)
		}
		a.fleet.Trigger()
	}()
}

// isAlreadyInitialized reports whether `duplicacy init` failed only because the
// directory already has a .duplicacy/preferences file.
func isAlreadyInitialized(out []byte) bool {
	return bytes.Contains(out, []byte("has already been initialized"))
}

// readPreferenceURLs parses <repo>/.duplicacy/preferences into a
// storage_alias -> storage URL map, used to safely adopt an already-initialized
// repo (verify it matches the requested storages before binding to it).
func readPreferenceURLs(repoPath string) (map[string]string, error) {
	data, err := os.ReadFile(filepath.Join(repoPath, ".duplicacy", "preferences"))
	if err != nil {
		return nil, err
	}
	var raws []rawPreference
	if err := json.Unmarshal(data, &raws); err != nil {
		return nil, fmt.Errorf("parse preferences: %w", err)
	}
	m := make(map[string]string, len(raws))
	for _, p := range raws {
		m[p.Name] = p.StorageURL
	}
	return m, nil
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
		// Authoritative region guard: the URL is now fully expanded, so an empty
		// S3 region with a custom endpoint here would make duplicacy fail with a
		// cryptic "MissingRegion". Fail loud before any side effects instead.
		if s3URLMissingRegion(req.Storages[i].StorageType, expanded) {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("storages[%d].storage_url: %s", i, errMissingS3Region)})
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

	// Safety: only bind if the on-disk preferences match the requested storages.
	// Binding maps the central UUID to whatever repo is on disk; if a DIFFERENT
	// repo lives here (different storage URL for an alias), mapping it would make
	// the controller vend credentials for storages that aren't actually here.
	existing, perr := readPreferenceURLs(req.RepoPath)
	if perr != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read existing preferences: " + perr.Error()})
		return
	}
	for _, s := range req.Storages {
		got, ok := existing[s.StorageAlias]
		if !ok {
			c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("storage %q is not in the on-disk preferences at %s — refusing to bind", s.StorageAlias, req.RepoPath)})
			return
		}
		if got != s.StorageURL {
			c.JSON(http.StatusConflict, gin.H{"error": fmt.Sprintf("storage %q on disk has a different URL (have %q, want %q) — refusing to bind", s.StorageAlias, got, s.StorageURL)})
			return
		}
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

	// Refresh the repo index in the background (a full scan can take minutes on
	// large hosts; don't block the bind response on it) and notify WS subscribers.
	a.rescanAsync()

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
