package main

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTestRepo creates <root>/.duplicacy/preferences with one storage so
// loadRepo succeeds, and returns root.
func writeTestRepo(t *testing.T, root, snapshotID string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, ".duplicacy"), 0o700); err != nil {
		t.Fatal(err)
	}
	prefs := `[{"name":"default","id":"` + snapshotID + `","storage":"sftp://duplicacy@host:22//backup","encrypted":true}]`
	if err := os.WriteFile(filepath.Join(root, ".duplicacy", "preferences"), []byte(prefs), 0o600); err != nil {
		t.Fatal(err)
	}
	return root
}

// TestScanListsFromRegistryNotFilesystem proves the repo index is built from the
// durable mapping (repos.json), not a filesystem crawl: only registered,
// loadable, non-web-cache paths appear — and an on-disk .duplicacy repo that is
// NOT registered is never discovered. This is the guarantee that makes listing
// immune to BACKUP_ROOTS size (the cold-cache /mnt walk that hung after a NAS
// reboot).
func TestScanListsFromRegistryNotFilesystem(t *testing.T) {
	base := t.TempDir()

	valid1 := writeTestRepo(t, filepath.Join(base, "repoA"), "snap-a")
	valid2 := writeTestRepo(t, filepath.Join(base, "repoB"), "snap-b")

	// Web-cache repo: has a valid preferences file but must be excluded.
	webCache := writeTestRepo(t, filepath.Join(base, "duplicacy", "cache", "localhost", "0"), "snap-web")

	// Registered path whose .duplicacy/preferences does not exist (deleted
	// out-of-band) — must be skipped, not error the whole rebuild.
	missing := filepath.Join(base, "gone")

	// On-disk repo that is NOT in the registry — must NOT be discovered (proves
	// there is no filesystem crawl).
	unregistered := writeTestRepo(t, filepath.Join(base, "unregistered"), "snap-unreg")

	mapping := newRepoMappingStore(t.TempDir())
	for _, p := range []string{valid1, valid2, webCache, missing} {
		if err := mapping.upsert(RepoMapping{RepoPath: p}); err != nil {
			t.Fatalf("upsert %s: %v", p, err)
		}
	}

	idx := newRepoIndex("duplicacy", nil, mapping)
	if err := idx.ScanForce(); err != nil {
		t.Fatalf("ScanForce: %v", err)
	}

	got := map[string]bool{}
	for _, r := range idx.list() {
		got[r.Path] = true
	}

	if !got[valid1] || !got[valid2] {
		t.Errorf("expected both registered repos in %v", got)
	}
	if got[webCache] {
		t.Errorf("web-cache repo should be excluded: %v", got)
	}
	if got[missing] {
		t.Errorf("missing repo should be skipped: %v", got)
	}
	if got[unregistered] {
		t.Errorf("unregistered on-disk repo must not be discovered (no crawl): %v", got)
	}
	if len(got) != 2 {
		t.Errorf("expected exactly 2 repos, got %d: %v", len(got), got)
	}
}

// TestScanSnapshotIDLookupAfterRegistryRebuild confirms getBySnapshotID works on
// the registry-built index (the scheduler relies on it).
func TestScanSnapshotIDLookupAfterRegistryRebuild(t *testing.T) {
	base := t.TempDir()
	repo := writeTestRepo(t, filepath.Join(base, "repoA"), "snap-a")

	mapping := newRepoMappingStore(t.TempDir())
	if err := mapping.upsert(RepoMapping{RepoPath: repo}); err != nil {
		t.Fatal(err)
	}
	idx := newRepoIndex("duplicacy", nil, mapping)
	if err := idx.ScanForce(); err != nil {
		t.Fatalf("ScanForce: %v", err)
	}
	r, ok := idx.getBySnapshotID("snap-a")
	if !ok || r.Path != repo {
		t.Fatalf("getBySnapshotID = %+v, ok=%v", r, ok)
	}
}
