package main

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func newTestStatsStore(t *testing.T) *snapshotStatsStore {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`
		CREATE TABLE snapshot_stats (
			snapshot_id        TEXT NOT NULL,
			revision           INTEGER NOT NULL,
			repo_id            TEXT NOT NULL,
			storage_name       TEXT NOT NULL,
			destination_key    TEXT NOT NULL,
			destination_label  TEXT NOT NULL,
			files              INTEGER,
			bytes              INTEGER,
			bytes_pretty       TEXT,
			total_chunks       INTEGER,
			total_bytes        INTEGER,
			total_bytes_pretty TEXT,
			uniq_chunks        INTEGER,
			uniq_bytes         INTEGER,
			uniq_bytes_pretty  TEXT,
			new_chunks         INTEGER,
			new_bytes          INTEGER,
			new_bytes_pretty   TEXT,
			pool_bytes         INTEGER,
			pool_chunks        INTEGER,
			captured_at        TIMESTAMP NOT NULL,
			PRIMARY KEY (snapshot_id, revision, storage_name)
		);
		CREATE INDEX idx_snapshot_stats_dest_time ON snapshot_stats (destination_key, captured_at);
		CREATE INDEX idx_snapshot_stats_repo ON snapshot_stats (repo_id);
	`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return newSnapshotStatsStore(db)
}

func mkRow(snap string, rev int, total, uniq, newB int64) *snapshotStatRow {
	return &snapshotStatRow{
		SnapshotID:       snap,
		Revision:         rev,
		Files:            100,
		Bytes:            total,
		BytesPretty:      "x",
		TotalChunks:      10,
		TotalBytes:       total,
		TotalBytesPretty: "x",
		UniqChunks:       int(uniq / (1 << 20)),
		UniqBytes:        uniq,
		UniqBytesPretty:  "x",
		NewChunks:        int(newB / (1 << 20)),
		NewBytes:         newB,
		NewBytesPretty:   "x",
	}
}

func TestSnapshotStatsUpsertAndRollup(t *testing.T) {
	store := newTestStatsStore(t)
	ctx := context.Background()
	t0 := time.Date(2026, 5, 20, 3, 0, 0, 0, time.UTC)

	// Run 1: repo A → Storj, two revisions.
	err := store.upsertCheckRun(ctx, "repoA", "storj", "s3://us1.storj.io/bucket-a", "Storj (bucket-a)", t0, 400<<20, 100, []*snapshotStatRow{
		mkRow("host01", 1, 400<<20, 400<<20, 400<<20),
		mkRow("host01", 2, 408<<20, 8<<20, 8<<20),
	})
	if err != nil {
		t.Fatalf("upsert run 1: %v", err)
	}

	// Run 2 — same repo, same revisions but new captured_at; verifies UPSERT
	// overwrites, doesn't duplicate.
	t1 := t0.Add(24 * time.Hour)
	err = store.upsertCheckRun(ctx, "repoA", "storj", "s3://us1.storj.io/bucket-a", "Storj (bucket-a)", t1, 412<<20, 103, []*snapshotStatRow{
		mkRow("host01", 1, 400<<20, 400<<20, 400<<20),
		mkRow("host01", 2, 408<<20, 8<<20, 8<<20),
		mkRow("host01", 3, 412<<20, 4<<20, 4<<20),
	})
	if err != nil {
		t.Fatalf("upsert run 2: %v", err)
	}

	// Run 3 — second repo on same destination so we can prove the rollup sums
	// across repos pointing at the same physical place.
	err = store.upsertCheckRun(ctx, "repoB", "storj", "s3://us1.storj.io/bucket-a", "Storj (bucket-a)", t1, 412<<20, 103, []*snapshotStatRow{
		mkRow("host02", 1, 200<<20, 200<<20, 200<<20),
	})
	if err != nil {
		t.Fatalf("upsert run 3: %v", err)
	}

	t.Run("listByRepo returns rev-desc rows", func(t *testing.T) {
		rows, err := store.listByRepo(ctx, "repoA", "")
		if err != nil {
			t.Fatalf("listByRepo: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("got %d rows, want 3 (rev 3, 2, 1)", len(rows))
		}
		if rows[0].Revision != 3 || rows[1].Revision != 2 || rows[2].Revision != 1 {
			t.Errorf("ordering wrong: %d, %d, %d", rows[0].Revision, rows[1].Revision, rows[2].Revision)
		}
	})

	t.Run("rollup destination sums across repos", func(t *testing.T) {
		rollup, err := store.rollup(ctx, nil, "", time.Time{})
		if err != nil {
			t.Fatalf("rollup: %v", err)
		}
		if len(rollup.Destinations) != 1 {
			t.Fatalf("got %d destinations, want 1 (same bucket)", len(rollup.Destinations))
		}
		dest := rollup.Destinations[0]
		// Latest rev per snapshot+storage: host01 rev3=412M, host02 rev1=200M.
		want := int64((412 + 200) << 20)
		if dest.CurrentBytes != want {
			t.Errorf("CurrentBytes = %d (=%s), want %d (=%s)", dest.CurrentBytes, formatPrettyBytes(dest.CurrentBytes), want, formatPrettyBytes(want))
		}
		if dest.SnapshotCount != 2 {
			t.Errorf("SnapshotCount = %d want 2 (host01 + host02)", dest.SnapshotCount)
		}
	})

	t.Run("rollup repo-scoped excludes other repo", func(t *testing.T) {
		rollup, err := store.rollup(ctx, []string{"repoA"}, "", time.Time{})
		if err != nil {
			t.Fatalf("rollup: %v", err)
		}
		if len(rollup.Destinations) != 1 {
			t.Fatalf("got %d destinations, want 1", len(rollup.Destinations))
		}
		// Only repoA — should NOT include host02's 200M.
		want := int64(412 << 20)
		if rollup.Destinations[0].CurrentBytes != want {
			t.Errorf("CurrentBytes = %d want %d (repoA only)", rollup.Destinations[0].CurrentBytes, want)
		}
	})

	t.Run("series has one point per (destination, captured_at)", func(t *testing.T) {
		// Add a t0 row on a DIFFERENT snapshot so it doesn't get UPSERTed
		// over by the t1 runs above. Without this, the test would show only
		// t1 because every run-1 row was overwritten by run 2 (same
		// snapshot/revision/storage PK → UPDATE captured_at).
		t0only := t0.Add(-1 * time.Hour)
		err := store.upsertCheckRun(ctx, "repoA", "storj", "s3://us1.storj.io/bucket-a", "Storj (bucket-a)", t0only, 50<<20, 12, []*snapshotStatRow{
			mkRow("retired-host", 99, 50<<20, 50<<20, 50<<20),
		})
		if err != nil {
			t.Fatalf("seed t0 row: %v", err)
		}

		rollup, err := store.rollup(ctx, nil, "", time.Time{})
		if err != nil {
			t.Fatalf("rollup: %v", err)
		}
		if len(rollup.Series) != 1 {
			t.Fatalf("got %d series, want 1", len(rollup.Series))
		}
		pts := rollup.Series[0].Points
		if len(pts) != 2 {
			t.Fatalf("got %d points, want 2 (t0only, t1)", len(pts))
		}
		// Ordering is ASC by captured_at.
		if !pts[0].TS.Equal(t0only) || !pts[1].TS.Equal(t1) {
			t.Errorf("timestamps unexpected: %v, %v", pts[0].TS, pts[1].TS)
		}
		// Series semantics: pool_bytes at this captured_at (NOT a sum across
		// snapshots — those are denormalised duplicates). At t1, both repoA
		// and repoB runs upserted pool_bytes=412M, so MAX = 412M. That's the
		// actual disk usage on the destination at that moment.
		wantT1 := int64(412 << 20)
		if pts[1].Bytes != wantT1 {
			t.Errorf("t1 point bytes = %d (=%s) want %d (=%s)", pts[1].Bytes, formatPrettyBytes(pts[1].Bytes), wantT1, formatPrettyBytes(wantT1))
		}
	})
}

// TestFormatPrettyBytes pins the inverse of parsePrettyBytes — the JSON
// "1.2G" string the UI consumes for summary-card display.
func TestFormatPrettyBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{512, "512"},
		{1024, "1.0K"},
		{1500, "1.5K"},
		{1 << 20, "1.0M"},
		{1 << 30, "1.0G"},
		{1 << 40, "1.0T"},
	}
	for _, c := range cases {
		if got := formatPrettyBytes(c.in); got != c.want {
			t.Errorf("formatPrettyBytes(%d) = %q want %q", c.in, got, c.want)
		}
	}
}
