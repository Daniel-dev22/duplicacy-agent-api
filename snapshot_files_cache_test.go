package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestFilesCache(t *testing.T, maxBytes int64, warmKeepN int) *snapshotFilesCacheStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	// Pin to one connection so the in-memory DB is shared across our sequential
	// statements (each new conn would otherwise get a fresh empty DB).
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`
		CREATE TABLE snapshot_files_cache (
			snapshot_id   TEXT    NOT NULL,
			revision      INTEGER NOT NULL,
			storage_name  TEXT    NOT NULL,
			repo_id       TEXT    NOT NULL,
			gz_output     BLOB    NOT NULL,
			raw_bytes     INTEGER NOT NULL,
			gz_bytes      INTEGER NOT NULL,
			cached_at     TIMESTAMP NOT NULL,
			last_access   TIMESTAMP NOT NULL,
			PRIMARY KEY (snapshot_id, revision, storage_name)
		);
		CREATE INDEX idx_sfc_repo ON snapshot_files_cache (repo_id);
		CREATE INDEX idx_sfc_access ON snapshot_files_cache (last_access);
		CREATE INDEX idx_sfc_storage ON snapshot_files_cache (storage_name);
	`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return newSnapshotFilesCacheStore(db, maxBytes, warmKeepN)
}

func TestFilesCacheRoundTrip(t *testing.T) {
	c := newTestFilesCache(t, 0, 5)
	ctx := context.Background()
	raw := []byte("1234 2026-06-02 12:00:00 abc /etc/passwd\n5678 2026-06-02 12:00:00 def /etc/hosts\n")

	if _, hit, err := c.get(ctx, "snapA", 3, "default"); err != nil || hit {
		t.Fatalf("expected clean miss, got hit=%v err=%v", hit, err)
	}
	if err := c.put(ctx, "snapA", 3, "default", "repo1", raw); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, hit, err := c.get(ctx, "snapA", 3, "default")
	if err != nil || !hit {
		t.Fatalf("expected hit, got hit=%v err=%v", hit, err)
	}
	if !bytes.Equal(got, raw) {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", got, raw)
	}

	// has reflects presence; wrong key still misses.
	if ok, _ := c.has(ctx, "snapA", 3, "default"); !ok {
		t.Fatalf("has should be true for stored key")
	}
	if ok, _ := c.has(ctx, "snapA", 3, "storj"); ok {
		t.Fatalf("has should be false for different storage")
	}
	if _, hit, _ := c.get(ctx, "snapA", 4, "default"); hit {
		t.Fatalf("different revision should miss")
	}
}

// setLastAccess forces a row's last_access so the LRU order is deterministic
// (put() always stamps now()).
func setLastAccess(t *testing.T, c *snapshotFilesCacheStore, snap string, rev int, storage string, at time.Time) {
	t.Helper()
	if _, err := c.db.Exec(`UPDATE snapshot_files_cache SET last_access=? WHERE snapshot_id=? AND revision=? AND storage_name=?`, at, snap, rev, storage); err != nil {
		t.Fatalf("set last_access: %v", err)
	}
}

func TestFilesCacheEvictPinsNewestN(t *testing.T) {
	// warmKeepN=1 → the newest revision per (snapshot,storage) is pinned and
	// must survive eviction even under an aggressively small cap.
	c := newTestFilesCache(t, 1 /*maxBytes*/, 1 /*warmKeepN*/)
	ctx := context.Background()
	big := bytes.Repeat([]byte("x /some/long/path/that/does/not/compress/away/easily\n"), 200)

	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for rev := 1; rev <= 3; rev++ {
		if err := c.put(ctx, "snapA", rev, "default", "repo1", append([]byte(fmt.Sprintf("rev%d\n", rev)), big...)); err != nil {
			t.Fatalf("put rev%d: %v", rev, err)
		}
		// Older revision = older access; rev3 (newest) accessed most recently.
		setLastAccess(t, c, "snapA", rev, "default", base.Add(time.Duration(rev)*time.Hour))
	}
	if err := c.evictBySize(ctx); err != nil {
		t.Fatalf("evict: %v", err)
	}

	// rev3 is the newest revision → pinned → always present.
	if ok, _ := c.has(ctx, "snapA", 3, "default"); !ok {
		t.Fatalf("newest revision (pinned) must survive eviction")
	}
	// rev1/rev2 are outside the warm window and over the cap → evicted.
	for _, rev := range []int{1, 2} {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); ok {
			t.Fatalf("rev%d should have been evicted under the size cap", rev)
		}
	}
}

func TestFilesCacheReconcilePrunesMissingRevisions(t *testing.T) {
	c := newTestFilesCache(t, 0, 5)
	ctx := context.Background()
	for rev := 1; rev <= 3; rev++ {
		if err := c.put(ctx, "snapA", rev, "storj", "repo1", []byte(fmt.Sprintf("rev%d\n", rev))); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	// A different storage's row must be untouched by a storj reconcile.
	if err := c.put(ctx, "snapA", 1, "default", "repo1", []byte("d\n")); err != nil {
		t.Fatalf("put default: %v", err)
	}

	// Live set after a prune: only rev2 + rev3 still exist on storj.
	live := []Snapshot{{SnapshotID: "snapA", Revision: 2}, {SnapshotID: "snapA", Revision: 3}}
	n, err := c.reconcile(ctx, "storj", live)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 eviction (rev1), got %d", n)
	}
	if ok, _ := c.has(ctx, "snapA", 1, "storj"); ok {
		t.Fatalf("pruned rev1 should be evicted")
	}
	for _, rev := range []int{2, 3} {
		if ok, _ := c.has(ctx, "snapA", rev, "storj"); !ok {
			t.Fatalf("live rev%d should remain", rev)
		}
	}
	if ok, _ := c.has(ctx, "snapA", 1, "default"); !ok {
		t.Fatalf("reconcile on storj must not touch the default storage row")
	}
}

func TestNewestRevisionsPerSnapshot(t *testing.T) {
	snaps := []Snapshot{
		{SnapshotID: "a", Revision: 1}, {SnapshotID: "a", Revision: 5}, {SnapshotID: "a", Revision: 3},
		{SnapshotID: "b", Revision: 9}, {SnapshotID: "b", Revision: 2},
	}
	got := newestRevisionsPerSnapshot(snaps, 2)
	// Expect a:{5,3}, b:{9,2} — 4 entries, newest-2 per id.
	want := map[string]map[int]bool{
		"a": {5: true, 3: true},
		"b": {9: true, 2: true},
	}
	if len(got) != 4 {
		t.Fatalf("expected 4 entries, got %d: %+v", len(got), got)
	}
	for _, s := range got {
		if !want[s.SnapshotID][s.Revision] {
			t.Fatalf("unexpected revision in result: %s r%d", s.SnapshotID, s.Revision)
		}
	}
}
