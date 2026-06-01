package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// cliInvocation describes one CLI run we want to spawn.
// Constructed by handlers, fed to the jobs subsystem (task #3) which
// actually starts the process and tails output.
type cliInvocation struct {
	RepoRoot string
	Args     []string // duplicacy args (e.g. ["backup", "-stats", "-storage", "default"])
	EnvAdds  []string // additional env vars (e.g. DUPLICACY_S3_SECRET=...)
}

// command builds an *exec.Cmd ready to run. Caller is responsible for stdout/stderr piping
// and Start/Wait — we don't run it here so the jobs registry can wire up streaming first.
//
// Stdin is set to a newline buffer so duplicacy's interactive prompts —
// specifically the "Enter the passphrase for <key>" the RSA priv-key
// loader emits BEFORE checking whether the key is encrypted — read an
// empty passphrase instead of hitting EOF. Witnessed 2026-05-27: every
// copy with -key against an unencrypted PKCS#1 priv key failed with
// "Failed to read the password: EOF" because exec.Cmd's default Stdin
// is nil (immediate EOF), even though the key carried no passphrase.
// The newline is harmless when no prompt fires; required when one does.
func (i cliInvocation) command(ctx context.Context, binary string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, binary, i.Args...)
	cmd.Dir = i.RepoRoot
	if len(i.EnvAdds) > 0 {
		cmd.Env = append(os.Environ(), i.EnvAdds...)
	}
	cmd.Stdin = strings.NewReader("\n")
	return cmd
}

// runSync runs a CLI invocation to completion and returns the combined stdout+stderr.
// Used for short queries like `list -id <snapshotid>`. Long-running commands go through
// the jobs subsystem instead.
func runSync(ctx context.Context, binary string, inv cliInvocation, timeout time.Duration) ([]byte, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cmd := inv.command(ctx, binary)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("duplicacy %s: %w (output: %s)", strings.Join(inv.Args, " "), err, truncate(string(out), 400))
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// streamLines reads from r line-by-line, sending each line to lines until r is exhausted
// or ctx is cancelled. Closes lines on exit.
func streamLines(ctx context.Context, r io.Reader, lines chan<- string) {
	defer close(lines)
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1024*1024), 8*1024*1024) // tolerate long backup-progress lines
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		case lines <- scanner.Text():
		}
	}
}

// --- output parsing helpers ---

// snapshotLine matches the per-revision rows from `duplicacy list`.
//
// duplicacy 3.2.5 emits two formats interchangeably within a single list
// output:
//
//	Snapshot home revision 1 created at 2026-04-30 02:00:11 -hash
//	Snapshot home revision 2 created at 2026-05-01 02:00
//
// (Same storage, same repo — the trailing ` -hash` is present on older
// revisions and missing on newer ones; seconds are sometimes truncated.)
// The earlier regex required exactly three whitespace-separated tokens
// after `created at`, so any revision without the `-hash` suffix was
// silently dropped from /repos/:id/snapshots — operators saw only the
// first revision in the UI even when the storage had many more.
//
// Capture only the date + time (always present) and tolerate optional
// trailing fragments. parseTime below handles both timestamp variants.
var snapshotLineRe = regexp.MustCompile(
	`^Snapshot\s+(\S+)\s+revision\s+(\d+)\s+created at\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)`)

type Snapshot struct {
	SnapshotID string    `json:"snapshot_id"`
	Revision   int       `json:"revision"`
	CreatedAt  time.Time `json:"created_at"`
	Raw        string    `json:"raw"`
}

func parseListOutput(out string) []Snapshot {
	// Initialize as empty slice (not nil) so JSON marshals as `[]` rather than
	// `null` for a fresh repo with zero snapshots — the frontend's snapshots
	// state is typed Snapshot[] and `null` makes `snapshots.length` crash the
	// repo detail page on the very first click after init.
	snaps := make([]Snapshot, 0)
	for _, line := range strings.Split(out, "\n") {
		m := snapshotLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		rev, _ := strconv.Atoi(m[2])
		// duplicacy emits either "YYYY-MM-DD HH:MM" or "YYYY-MM-DD HH:MM:SS"
		// depending on the revision; try the second-precision form first and
		// fall back to minute precision. Zero-value time is acceptable —
		// frontend renders "Invalid Date" rather than rejecting the row.
		t, err := time.Parse("2006-01-02 15:04:05", m[3])
		if err != nil {
			t, _ = time.Parse("2006-01-02 15:04", m[3])
		}
		snaps = append(snaps, Snapshot{
			SnapshotID: m[1],
			Revision:   rev,
			CreatedAt:  t,
			Raw:        strings.TrimSpace(line),
		})
	}
	return snaps
}

// --- HTTP handlers (override placeholders) ---

// GET /repos/:id/snapshots — runs `duplicacy list` and parses revisions.
//
// Vends per-storage credentials before invoking duplicacy: the SFTP/S3/B2/etc
// secrets live in the controller (Bitwarden-vended) and never on agent disk,
// so without prepareEnvForRepo duplicacy fails to authenticate against the
// storage backend ("No private key file is provided" for SFTP, etc.) and the
// UI sees zero snapshots even when the storage is full of them.
func (a *app) handleListSnapshots(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	storage := c.Query("storage")

	env, rsaPriv, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	defer cleanup()

	// For an RSA-encrypted storage `duplicacy list` needs the private key to
	// read the snapshot file index; pass it the same way handleRestore /
	// handleSnapshotFiles do. -key must precede the other flags.
	keyAlias := storage
	if keyAlias == "" {
		keyAlias = "default"
	}
	args := []string{"list"}
	if kp := rsaPriv[keyAlias]; kp != "" {
		args = append(args, "-key", kp)
	}
	// ?id scopes the revision list to one snapshot id — required when listing a
	// relay/hub repo whose storage pools many source repos (restore-via-relay).
	if id := c.Query("id"); id != "" {
		args = append(args, "-id", id)
	}
	if storage != "" {
		args = append(args, "-storage", storage)
	}
	out, err := runSync(c.Request.Context(), a.cfg.DuplicacyBinary, cliInvocation{
		RepoRoot: repo.Path,
		Args:     args,
		EnvAdds:  env,
	}, 60*time.Second)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error":  err.Error(),
			"output": string(out),
		})
		return
	}
	resp := gin.H{
		"repo":      repo.ID,
		"snapshots": parseListOutput(string(out)),
	}
	// ?debug=1 returns the raw duplicacy stdout/stderr alongside the parsed
	// snapshots so operators can diagnose mismatches between what's on the
	// storage and what duplicacy reports (e.g. parsing regex misses, version-
	// specific output format changes). Off by default — adds a couple KiB
	// per response. The full body is bounded by duplicacy's own output cap.
	if c.Query("debug") == "1" {
		resp["raw"] = string(out)
	}
	c.JSON(http.StatusOK, resp)
}

// GET /repos/:id/snapshots/:rev/files — `duplicacy list -files -r <rev>`
//
// Same credential-vending requirement as handleListSnapshots — file listing
// reads chunk metadata from the storage backend.
func (a *app) handleSnapshotFiles(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	rev, err := strconv.Atoi(c.Param("rev"))
	if err != nil || rev <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "revision must be a positive integer"})
		return
	}
	storage := c.Query("storage")

	env, rsaPriv, cleanup, err := a.prepareEnvForRepo(c.Request.Context(), repo)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "vend secrets: " + err.Error()})
		return
	}
	defer cleanup()

	// `list -files` reads chunk/file metadata INSIDE a revision, which for an
	// RSA-encrypted storage requires the private key to decrypt. Backup/copy
	// only write (public key), so they never needed -key. Pass the per-storage
	// private key the same way handleRestore does. -key must come before the
	// other flags per duplicacy's arg parser.
	keyAlias := storage
	if keyAlias == "" {
		keyAlias = "default"
	}
	args := []string{"list", "-files", "-r", strconv.Itoa(rev)}
	if kp := rsaPriv[keyAlias]; kp != "" {
		args = append(args, "-key", kp)
	}
	// ?id scopes the listing to one snapshot id. Required when listing against a
	// relay/hub repo whose storage pools many source repos' snapshots (e.g.
	// restoring a node's offsite copy via the NAS relay): revision numbers are
	// per-snapshot-id, so without -id duplicacy can't resolve -r <rev>.
	if id := c.Query("id"); id != "" {
		args = append(args, "-id", id)
	}
	if storage != "" {
		args = append(args, "-storage", storage)
	}
	out, err := runSync(c.Request.Context(), a.cfg.DuplicacyBinary, cliInvocation{
		RepoRoot: repo.Path,
		Args:     args,
		EnvAdds:  env,
	}, 5*time.Minute)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error(), "output": string(out)})
		return
	}
	c.Data(http.StatusOK, "text/plain; charset=utf-8", out)
}

// --- helpers used by the jobs subsystem ---

// invocationForBackup builds the args for `duplicacy backup`. When threads
// <= 0, falls back to the agent-computed auto value (max(1, NumCPU/2)) and
// emits an explicit -threads N so the resolved value is logged and the
// duplicacy CLI default (1) doesn't kick in.
func invocationForBackup(repo *Repo, storageName, tag string, threads int) cliInvocation {
	args := []string{"backup", "-stats"}
	if storageName != "" {
		args = append(args, "-storage", storageName)
	}
	if tag != "" {
		args = append(args, "-t", tag)
	}
	if threads <= 0 {
		threads = autoThreads()
	}
	args = append(args, "-threads", strconv.Itoa(threads))
	return cliInvocation{RepoRoot: repo.Path, Args: args}
}

// invocationForRestore builds args for `duplicacy restore`.
// rsaPrivKeyPath is the /dev/shm path to the rsa_private_key PEM when the
// targeted storage is RSA-encrypted; empty string otherwise.
// targetRoot, when non-empty, overrides the repo's root via duplicacy's
// global -repository flag — the agent uses this to land restores in a
// scratch dir by default, keeping operators from accidentally overwriting
// prod files when they just wanted to inspect a revision.
func invocationForRestore(repo *Repo, storageName string, revision int, paths []string, overwrite bool, rsaPrivKeyPath, targetRoot string) cliInvocation {
	args := []string{"restore", "-r", strconv.Itoa(revision)}
	if storageName != "" {
		args = append(args, "-storage", storageName)
	}
	if overwrite {
		args = append(args, "-overwrite")
	}
	if rsaPrivKeyPath != "" {
		args = append(args, "-key", rsaPrivKeyPath)
	}
	args = append(args, paths...)
	root := repo.Path
	if targetRoot != "" {
		root = targetRoot
	}
	return cliInvocation{RepoRoot: root, Args: args}
}

// invocationForCheck builds args for `duplicacy check`.
// snapshotID, when non-empty, scopes the check to a single snapshot id via -id;
// the hub-and-spoke relay repo uses this so a single check schedule row targets
// one source repo's snapshots inside the shared chunk pool.
//
// -tabular is always appended: it emits the per-revision dedup table
// (total / unique / new bytes) the storage dashboard needs, and the
// check_tabular parser only triggers on the table form so manual runs
// stay consistent with scheduled runs.
func invocationForCheck(repo *Repo, storageName string, revisions string, all bool, snapshotID string) cliInvocation {
	args := []string{"check"}
	if storageName != "" {
		args = append(args, "-storage", storageName)
	}
	if snapshotID != "" {
		args = append(args, "-id", snapshotID)
	}
	if revisions != "" {
		args = append(args, "-r", revisions)
	}
	if all {
		args = append(args, "-all")
	}
	args = append(args, "-tabular")
	return cliInvocation{RepoRoot: repo.Path, Args: args}
}

// invocationForPrune builds args for `duplicacy prune`.
// snapshotID, when non-empty, scopes the prune to a single snapshot id via -id;
// used by the hub relay repo to apply per-source-repo retention.
func invocationForPrune(repo *Repo, storageName string, keepRules []string, exclusive, exhaustive bool, snapshotID string) cliInvocation {
	args := []string{"prune"}
	if storageName != "" {
		args = append(args, "-storage", storageName)
	}
	if snapshotID != "" {
		args = append(args, "-id", snapshotID)
	}
	for _, k := range keepRules {
		args = append(args, "-keep", k)
	}
	if exclusive {
		args = append(args, "-exclusive")
	}
	if exhaustive {
		args = append(args, "-exhaustive")
	}
	return cliInvocation{RepoRoot: repo.Path, Args: args}
}

// invocationForCopy builds args for `duplicacy copy -from <a> -to <b>`. Used by
// the NAS relay repo to ship chunks from the shared NAS chunk pool (alias
// "default") to a secondary (remote-NAS, B2, Storj, …). snapshotID scopes the
// copy to one source repo's snapshots via -id; required for per-policy
// granularity in the hub-and-spoke topology.
//
// sourceRSAPrivKeyPath, when non-empty, is the /dev/shm path to the SOURCE
// storage's RSA private key. `duplicacy copy` needs it to read the source's
// RSA-encrypted snapshot index even when the destination shares the same
// chunk encryption (bit-identical applies to chunks, not the index). Witness:
// 2026-05-27 overnight — destinations were rebuilt RSA-compatible but copies
// still failed with "An RSA private key is required to decrypt the chunk"
// because this -key flag wasn't being passed.
//
// sourceRSAPassphrase is the matching passphrase if the priv key is
// encrypted; empty otherwise.
func invocationForCopy(repo *Repo, fromStorage, toStorage string, threads int, snapshotID, sourceRSAPrivKeyPath, sourceRSAPassphrase string) cliInvocation {
	args := []string{"copy"}
	if fromStorage != "" {
		args = append(args, "-from", fromStorage)
	}
	if toStorage != "" {
		args = append(args, "-to", toStorage)
	}
	if snapshotID != "" {
		args = append(args, "-id", snapshotID)
	}
	if threads <= 0 {
		threads = autoThreads()
	}
	args = append(args, "-threads", strconv.Itoa(threads))
	if sourceRSAPrivKeyPath != "" {
		args = append(args, "-key", sourceRSAPrivKeyPath)
		if sourceRSAPassphrase != "" {
			args = append(args, "-key-passphrase", sourceRSAPassphrase)
		}
	}
	return cliInvocation{RepoRoot: repo.Path, Args: args}
}

// chunkSizing carries the duplicacy chunk-size flags (-c / -min / -max) applied
// at chunk-pool creation. Once a pool exists these settings are immutable —
// every -copy-compatible secondary inherits them — so the value we pass on
// the first init of `default` IS the chunk size used by every storage backend
// the relay later adds, including cloud (Storj, B2, …).
//
// Sizes are duplicacy-CLI strings ("16M", "4M", …); empty fields omit the
// flag (letting duplicacy pick its 4MB default).
type chunkSizing struct {
	Avg string // -c
	Min string // -min-chunk-size
	Max string // -max-chunk-size
}

// cloudOptimizedChunks is the canonical chunk size for new pools. 16MB average
// is ~4× larger than duplicacy's default (4MB), giving 4× fewer Storj segments
// (Storj charges per-segment) while preserving reasonable small-file dedup —
// the duplicacy forum's 32MB recommendation is more aggressive but harms dedup
// on frequently-edited small files. Min/Max bracket the average at 4MB..64MB.
var cloudOptimizedChunks = chunkSizing{Avg: "16M", Min: "4M", Max: "64M"}

// invocationForInit builds args for `duplicacy init <snapshot-id> <storage-url>`.
// rsaPubKeyPath is the /dev/shm path to the rsa_public_key PEM when this
// storage uses RSA asymmetric encryption; empty string otherwise.
//
// chunks controls duplicacy's chunk parameters (-c / -min / -max). For new pool
// creations callers pass cloudOptimizedChunks; the zero value lets duplicacy
// pick its 4MB default (used by pools created before this change).
func invocationForInit(repoRoot, snapshotID, storageURL string, encrypted bool, rsaPubKeyPath string, chunks chunkSizing) cliInvocation {
	// Note: duplicacy 3.2.5 does not have a -no-save-password flag (neither
	// global nor on init). The "do not save the password" intent is
	// realised by the post-init scrub step that rewrites
	// `.duplicacy/preferences` with `no_save_password: true`. Adding the
	// flag here makes urfave/cli reject the call with "Incorrect Usage".
	args := []string{"init"}
	if encrypted {
		args = append(args, "-encrypt")
	}
	if rsaPubKeyPath != "" {
		args = append(args, "-key", rsaPubKeyPath)
	}
	if chunks.Avg != "" {
		args = append(args, "-c", chunks.Avg)
	}
	if chunks.Min != "" {
		args = append(args, "-min", chunks.Min)
	}
	if chunks.Max != "" {
		args = append(args, "-max", chunks.Max)
	}
	args = append(args, snapshotID, storageURL)
	return cliInvocation{RepoRoot: repoRoot, Args: args}
}

// invocationForAdd builds args for `duplicacy add <storage-name> <snapshot-id> <storage-url>`.
// Used to register secondary storages on a repo that already has a primary.
// Same -no-save-password rationale as invocationForInit.
// rsaPubKeyPath is the /dev/shm path to the rsa_public_key PEM when this
// secondary storage uses RSA asymmetric encryption; empty string otherwise.
// copyFrom, when non-empty, emits `-copy <copyFrom>` so the new storage is
// copy-compatible with that existing alias (shared chunk-splitting config) —
// required for `duplicacy copy` to ship chunks without re-chunking. bitIdentical
// additionally emits `-bit-identical` so chunks are byte-identical (no
// re-encrypt during copy); requires the new storage to share the source's
// encryption key. Callers should only set bitIdentical when the secondary uses
// the same encryption_password as the primary.
func invocationForAdd(repoRoot, storageName, snapshotID, storageURL string, encrypted bool, rsaPubKeyPath, copyFrom string, bitIdentical bool) cliInvocation {
	// duplicacy 3.2.5 has no -no-save-password flag — see invocationForInit.
	args := []string{"add"}
	if encrypted {
		args = append(args, "-encrypt")
	}
	if rsaPubKeyPath != "" {
		args = append(args, "-key", rsaPubKeyPath)
	}
	if copyFrom != "" {
		args = append(args, "-copy", copyFrom)
		if bitIdentical {
			args = append(args, "-bit-identical")
		}
	}
	args = append(args, storageName, snapshotID, storageURL)
	return cliInvocation{RepoRoot: repoRoot, Args: args}
}

// ensureDir creates a directory if missing, propagating mode 0700.
// Used by handleInitRepo when initializing a new repo root.
func ensureDir(p string) error {
	return os.MkdirAll(filepath.Clean(p), 0700)
}

// scrubPreferences strips credential material from .duplicacy/preferences in
// the given repo root. duplicacy occasionally persists secrets despite
// -no-save-password (different versions, edge cases) — this is defense in
// depth so the on-disk preferences file never carries cred material.
//
// Operations per storage entry:
//   - encrypted_password   → ""
//   - keys[*]              → cleared (whole map replaced with empty)
//   - no_save_password     → true
//
// The 'name', 'id', 'storage', and 'encrypted' fields are preserved (they're
// the duplicacy-side routing info; non-secret).
func scrubPreferences(repoRoot string) error {
	prefsPath := filepath.Join(repoRoot, ".duplicacy", "preferences")
	data, err := os.ReadFile(prefsPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", prefsPath, err)
	}
	var raws []rawPreference
	if err := json.Unmarshal(data, &raws); err != nil {
		return fmt.Errorf("parse %s: %w", prefsPath, err)
	}
	for i := range raws {
		raws[i].Keys = map[string]string{}
		raws[i].DoNotSavePassword = true
		// rawPreference doesn't have encrypted_password — duplicacy emits it
		// as part of Keys when present, so clearing Keys covers it.
	}
	out, err := json.MarshalIndent(raws, "", "    ")
	if err != nil {
		return fmt.Errorf("marshal scrubbed prefs: %w", err)
	}
	tmp := prefsPath + ".tmp"
	if err := os.WriteFile(tmp, out, 0600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, prefsPath); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", prefsPath, err)
	}
	return nil
}
