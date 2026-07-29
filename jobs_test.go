package main

import (
	"fmt"
	"math"
	"slices"
	"testing"
)

// Verifies parseProgressLine against verbatim duplicacy 3.2.5 output. The
// regex is the load-bearing piece of the live-progress feature — if its
// capture groups drift, the fleet WS will silently fail to update bars.
func TestParseProgressLine(t *testing.T) {
	cases := []struct {
		line      string
		wantMatch bool
		wantChunk int
		wantSpeed string
		wantETA   string
		wantPct   float64
	}{
		// Restore variant from real log: backuproot/duplicacy/logs/restore-…
		{
			line:      "Downloaded chunk 1 size 10294316, 7.69MB/s n/a 453.7%",
			wantMatch: true,
			wantChunk: 1,
			wantSpeed: "7.69MB/s",
			wantETA:   "n/a",
			wantPct:   100, // capped from 453.7
		},
		// Backup variant with multi-token ETA from PrettyTime
		{
			line:      "Uploaded chunk 17 size 4194304, 1.20MB/s 0h 2m 30s 12.4%",
			wantMatch: true,
			wantChunk: 17,
			wantSpeed: "1.20MB/s",
			wantETA:   "0h 2m 30s",
			wantPct:   12.4,
		},
		// HH:MM:SS-style ETA
		{
			line:      "Uploaded chunk 999 size 5478818, 7.90MB/s 03:12:54 0.7%",
			wantMatch: true,
			wantChunk: 999,
			wantSpeed: "7.90MB/s",
			wantETA:   "03:12:54",
			wantPct:   0.7,
		},
		// Copy variant — different shape than backup/restore. Hash instead
		// of bare idx, parenthesized (idx/total), no "size N," segment.
		// Witness 2026-05-27: every cross-site copy ran without progress
		// because the original regex didn't match these lines.
		{
			line:      "Copied chunk 4ac96c2e6b3f06f9dbb860a8bee48947e3a1ba18fadf78b0029268e791f719d3 (2/1274) 4.15MB/s 00:19:32 0.2%",
			wantMatch: true,
			wantChunk: 2,
			wantSpeed: "4.15MB/s",
			wantETA:   "00:19:32",
			wantPct:   0.2,
		},
		{
			line:      "Copied chunk abc (1274/1274) 10.5MB/s 0h 0m 0s 100.0%",
			wantMatch: true,
			wantChunk: 1274,
			wantSpeed: "10.5MB/s",
			wantETA:   "0h 0m 0s",
			wantPct:   100.0,
		},
		// Non-progress lines should be ignored
		{line: "Listing all chunks", wantMatch: false},
		{line: "INFO BACKUP_START Last backup at revision 28956 found", wantMatch: false},
		{line: "ERROR STORAGE_CREATE Failed to load the SFTP storage at sftp://…", wantMatch: false},
		{line: "", wantMatch: false},
	}

	for _, c := range cases {
		t.Run(c.line, func(t *testing.T) {
			j := &Job{}
			got := j.parseProgressLine(c.line)
			if got != c.wantMatch {
				t.Fatalf("match=%v, want %v", got, c.wantMatch)
			}
			if !c.wantMatch {
				if j.Progress != nil {
					t.Fatalf("Progress should be nil on miss, got %+v", j.Progress)
				}
				return
			}
			if j.Progress == nil {
				t.Fatalf("Progress is nil after positive match")
			}
			if j.Progress.LastChunk != c.wantChunk {
				t.Errorf("LastChunk=%d want %d", j.Progress.LastChunk, c.wantChunk)
			}
			if j.Progress.Speed != c.wantSpeed {
				t.Errorf("Speed=%q want %q", j.Progress.Speed, c.wantSpeed)
			}
			if j.Progress.ETA != c.wantETA {
				t.Errorf("ETA=%q want %q", j.Progress.ETA, c.wantETA)
			}
			if j.Progress.Percent != c.wantPct {
				t.Errorf("Percent=%v want %v", j.Progress.Percent, c.wantPct)
			}
		})
	}
}

// TestParseStatsLine covers BACKUP_STATS All chunks summary and the
// trailing Total running time line. duplicacy emits these once per backup
// completion; together they populate Job.Progress with the operator-
// facing totals (chunks, bytes uploaded, wall-clock duration).
func TestParseStatsLine(t *testing.T) {
	t.Run("all_chunks_with_log_prefix", func(t *testing.T) {
		// Line as it would appear if duplicacy was run with -log (we don't
		// pass it but the regex shouldn't care).
		j := &Job{}
		ok := j.parseStatsLine("INFO BACKUP_STATS All chunks: 1472 total, 8,353M bytes; 5 new, 7,984K bytes, 669K bytes uploaded")
		if !ok {
			t.Fatalf("expected match")
		}
		if j.Progress.TotalChunks != 1472 {
			t.Errorf("TotalChunks=%d want 1472", j.Progress.TotalChunks)
		}
		if j.Progress.NewChunks != 5 {
			t.Errorf("NewChunks=%d want 5", j.Progress.NewChunks)
		}
		if j.Progress.BytesUploaded != "669K" {
			t.Errorf("BytesUploaded=%q want %q", j.Progress.BytesUploaded, "669K")
		}
	})
	t.Run("all_chunks_bare_line", func(t *testing.T) {
		j := &Job{}
		ok := j.parseStatsLine("All chunks: 100 total, 1G bytes; 0 new, 0 bytes, 0 bytes uploaded")
		if !ok {
			t.Fatalf("expected match")
		}
		if j.Progress.TotalChunks != 100 || j.Progress.NewChunks != 0 || j.Progress.BytesUploaded != "0" {
			t.Errorf("got %+v", j.Progress)
		}
	})
	t.Run("running_time", func(t *testing.T) {
		j := &Job{}
		ok := j.parseStatsLine("INFO BACKUP_STATS Total running time: 00:00:02")
		if !ok {
			t.Fatalf("expected match")
		}
		if j.Progress.Duration != "00:00:02" {
			t.Errorf("Duration=%q want %q", j.Progress.Duration, "00:00:02")
		}
	})
	t.Run("misses_unrelated_lines", func(t *testing.T) {
		j := &Job{}
		if j.parseStatsLine("Listing all chunks") {
			t.Errorf("should not match listing line")
		}
		if j.parseStatsLine("Files: 4440 total, 361,404K bytes; 3 new, 10,187K bytes") {
			t.Errorf("should not match Files: line — only All chunks:")
		}
	})
}

// TestParseCheckLine covers the SNAPSHOT_CHECK total + per-revision lines.
// Percent is derived as verified/total*100; the bar should reach exactly
// 100.0 after the last revision regardless of float rounding.
func TestParseCheckLine(t *testing.T) {
	t.Run("total_then_three_revisions", func(t *testing.T) {
		j := &Job{}
		if !j.parseCheckLine("INFO SNAPSHOT_CHECK 3 snapshots and 3 revisions") {
			t.Fatalf("expected total line to match")
		}
		if j.Progress.CheckRevisionsTotal != 3 {
			t.Fatalf("CheckRevisionsTotal=%d want 3", j.Progress.CheckRevisionsTotal)
		}
		if j.Progress.Percent != 0 {
			t.Errorf("Percent should still be 0 (no revisions verified yet), got %v", j.Progress.Percent)
		}

		want := []float64{100.0 / 3, 200.0 / 3, 100.0}
		lines := []string{
			"INFO SNAPSHOT_CHECK All chunks referenced by snapshot host01 at revision 1 exist",
			"INFO SNAPSHOT_CHECK All chunks referenced by snapshot host01 at revision 2 exist",
			"INFO SNAPSHOT_CHECK All chunks referenced by snapshot host02 at revision 5 exist",
		}
		for i, line := range lines {
			if !j.parseCheckLine(line) {
				t.Fatalf("expected revision line %d to match", i+1)
			}
			if j.Progress.CheckRevisionsVerified != i+1 {
				t.Errorf("after line %d, verified=%d want %d", i+1, j.Progress.CheckRevisionsVerified, i+1)
			}
			if math.Abs(j.Progress.Percent-want[i]) > 1e-9 {
				t.Errorf("after line %d, Percent=%v want %v", i+1, j.Progress.Percent, want[i])
			}
		}
		// Final assertion: bar reaches exactly 100.0 at the end (the integer
		// case avoids float rounding, so an exact compare is fine here).
		if j.Progress.Percent != 100.0 {
			t.Errorf("final Percent=%v want 100", j.Progress.Percent)
		}
	})
	t.Run("misses_unrelated_lines", func(t *testing.T) {
		j := &Job{}
		if j.parseCheckLine("INFO SNAPSHOT_CHECK Listing all chunks") {
			t.Errorf("listing line should not match")
		}
		if j.parseCheckLine("Uploaded chunk 1 size 4194304, 7.50MB/s 03:00:00 1.4%") {
			t.Errorf("backup line should not match check parser")
		}
		if j.Progress != nil {
			t.Errorf("Progress should remain nil on miss-only input, got %+v", j.Progress)
		}
	})
	t.Run("bare_lines_without_log_prefix", func(t *testing.T) {
		// duplicacy WITHOUT `-log` (our default invocation) emits the bare
		// text — no "INFO SNAPSHOT_CHECK " prefix. Verified against actual
		// job log on kd-nuc01 2026-05-28. Regex must accept both forms or
		// the entire check counter / pool size path silently no-ops.
		j := &Job{}
		if !j.parseCheckLine("11 snapshots and 9 revisions") {
			t.Fatalf("bare 'snapshots and revisions' line should match")
		}
		if j.Progress.CheckRevisionsTotal != 9 {
			t.Errorf("CheckRevisionsTotal=%d want 9", j.Progress.CheckRevisionsTotal)
		}
		if !j.parseCheckLine("All chunks referenced by snapshot nuc-home-user at revision 1 exist") {
			t.Fatalf("bare 'chunks referenced' line should match")
		}
		if j.Progress.CheckRevisionsVerified != 1 {
			t.Errorf("CheckRevisionsVerified=%d want 1", j.Progress.CheckRevisionsVerified)
		}
		if !j.parseCheckLine("Total chunk size is 147,748M in 10573 chunks") {
			t.Fatalf("bare 'Total chunk size' line should match")
		}
		if j.Progress.CheckPoolBytesPretty != "147,748M" {
			t.Errorf("CheckPoolBytesPretty=%q want 147,748M", j.Progress.CheckPoolBytesPretty)
		}
		if j.Progress.CheckPoolChunks != 10573 {
			t.Errorf("CheckPoolChunks=%d want 10573", j.Progress.CheckPoolChunks)
		}
	})
	t.Run("pool_size_line", func(t *testing.T) {
		j := &Job{}
		// "Total chunk size is X in N chunks" — the destination's actual
		// deduplicated disk usage. Drives the storage dashboard's headline.
		if !j.parseCheckLine("INFO SNAPSHOT_CHECK Total chunk size is 350.4M in 8234 chunks") {
			t.Fatalf("pool size line should match")
		}
		if j.Progress.CheckPoolBytesPretty != "350.4M" {
			t.Errorf("CheckPoolBytesPretty=%q want 350.4M", j.Progress.CheckPoolBytesPretty)
		}
		// Compute via a function call so Go doesn't fold the float literal at
		// compile-time (constant float→int64 conversion is rejected by the spec).
		mul := func(f float64, sh uint) int64 { return int64(f * float64(int64(1)<<sh)) }
		wantBytes := mul(350.4, 20)
		if j.Progress.CheckPoolBytes != wantBytes {
			t.Errorf("CheckPoolBytes=%d want %d", j.Progress.CheckPoolBytes, wantBytes)
		}
		if j.Progress.CheckPoolChunks != 8234 {
			t.Errorf("CheckPoolChunks=%d want 8234", j.Progress.CheckPoolChunks)
		}
	})
}

// TestParsePruneLine covers the three counter lines and ensures unrelated
// SNAPSHOT_DELETE / FOSSIL_* variants (e.g. "would be removed" dry-run
// lines) are NOT counted.
func TestParsePruneLine(t *testing.T) {
	j := &Job{}

	if !j.parsePruneLine("INFO SNAPSHOT_DELETE The snapshot host01 at revision 7 has been removed") {
		t.Fatalf("snapshot-removed line should match")
	}
	if !j.parsePruneLine("INFO CHUNK_DELETE The chunk abc123 has been permanently removed") {
		t.Fatalf("chunk-deleted line should match")
	}
	if !j.parsePruneLine("INFO CHUNK_DELETE The chunk def456 has been permanently removed") {
		t.Fatalf("second chunk-deleted line should match")
	}
	if !j.parsePruneLine("INFO FOSSIL_COLLECT Fossil collection 4 saved") {
		t.Fatalf("fossil-collect line should match")
	}

	if j.Progress.PruneSnapshotsRemoved != 1 {
		t.Errorf("PruneSnapshotsRemoved=%d want 1", j.Progress.PruneSnapshotsRemoved)
	}
	if j.Progress.PruneChunksDeleted != 2 {
		t.Errorf("PruneChunksDeleted=%d want 2", j.Progress.PruneChunksDeleted)
	}
	if j.Progress.PruneFossilsProcessed != 1 {
		t.Errorf("PruneFossilsProcessed=%d want 1", j.Progress.PruneFossilsProcessed)
	}
	if j.Progress.Percent != 0 {
		t.Errorf("Prune Percent should stay 0, got %v", j.Progress.Percent)
	}

	// Dry-run "would be" lines must not increment counters; they're future-
	// tense and don't represent completed work.
	startSnaps := j.Progress.PruneSnapshotsRemoved
	startChunks := j.Progress.PruneChunksDeleted
	if j.parsePruneLine("INFO FOSSIL_DELETE The chunk xyz would be permanently removed") {
		t.Errorf("would-be-removed (dry run) line should NOT match")
	}
	if j.parsePruneLine("INFO FOSSIL_RESURRECT Fossil xyz would be resurrected") {
		t.Errorf("would-resurrect line should NOT match")
	}
	if j.parsePruneLine("INFO FOSSIL_NONE No fossil collection found") {
		t.Errorf("no-fossil line should NOT match")
	}
	if j.Progress.PruneSnapshotsRemoved != startSnaps || j.Progress.PruneChunksDeleted != startChunks {
		t.Errorf("counters changed on dry-run lines")
	}
}

// TestParsePrunePreviewLine covers the -dry-run counters. duplicacy emits both
// lines at INFO from pruneSnapshotsNonExhaustive, so the preview works without
// the global -debug flag.
func TestParsePrunePreviewLine(t *testing.T) {
	t.Run("collects revisions and unreferenced chunks", func(t *testing.T) {
		j := &Job{}
		for _, line := range []string{
			"INFO SNAPSHOT_DELETE Deleting snapshot nuc-home-user at revision 21",
			"INFO SNAPSHOT_DELETE Deleting snapshot nuc-home-user at revision 23",
			"Deleting snapshot pi-home-user at revision 4", // no INFO prefix — agent runs without -log
			"INFO CHUNK_UNREFERENCED Found unreferenced chunk a1b2c3",
			"Found unreferenced chunk d4e5f6",
		} {
			if !j.parsePruneLine(line) {
				t.Errorf("line should match: %q", line)
			}
		}

		want := []string{"nuc-home-user@21", "nuc-home-user@23", "pi-home-user@4"}
		if !slices.Equal(j.Progress.PrunePreviewRevisions, want) {
			t.Errorf("PrunePreviewRevisions = %v want %v", j.Progress.PrunePreviewRevisions, want)
		}
		if j.Progress.PrunePreviewChunks != 2 {
			t.Errorf("PrunePreviewChunks = %d want 2", j.Progress.PrunePreviewChunks)
		}
		// A preview must never look like it removed anything.
		if j.Progress.PruneSnapshotsRemoved != 0 || j.Progress.PruneChunksDeleted != 0 {
			t.Errorf("preview lines bumped the real counters: removed=%d deleted=%d",
				j.Progress.PruneSnapshotsRemoved, j.Progress.PruneChunksDeleted)
		}
	})

	t.Run("past-tense removal is not counted as a preview", func(t *testing.T) {
		j := &Job{}
		if !j.parsePruneLine("INFO SNAPSHOT_DELETE The snapshot host01 at revision 7 has been removed") {
			t.Fatal("removal line should match")
		}
		if len(j.Progress.PrunePreviewRevisions) != 0 {
			t.Errorf("removal line leaked into the preview list: %v", j.Progress.PrunePreviewRevisions)
		}
		if j.Progress.PruneSnapshotsRemoved != 1 {
			t.Errorf("PruneSnapshotsRemoved = %d want 1", j.Progress.PruneSnapshotsRemoved)
		}
	})

	t.Run("revision list is capped but the chunk count is not", func(t *testing.T) {
		j := &Job{}
		for i := 0; i < prunePreviewRevisionCap+50; i++ {
			j.parsePruneLine(fmt.Sprintf("Deleting snapshot snap at revision %d", i))
			j.parsePruneLine(fmt.Sprintf("Found unreferenced chunk %d", i))
		}
		if len(j.Progress.PrunePreviewRevisions) != prunePreviewRevisionCap {
			t.Errorf("revisions = %d want cap %d", len(j.Progress.PrunePreviewRevisions), prunePreviewRevisionCap)
		}
		// The count drives the reclaim estimate, so it must keep going past
		// the cap on the list.
		if j.Progress.PrunePreviewChunks != prunePreviewRevisionCap+50 {
			t.Errorf("chunks = %d want %d", j.Progress.PrunePreviewChunks, prunePreviewRevisionCap+50)
		}
	})

	t.Run("near-miss lines do not match", func(t *testing.T) {
		j := &Job{}
		for _, line := range []string{
			"Deleting snapshot snap at revision x",   // non-numeric revision
			"Deleting snapshot at revision 4",        // missing id
			"Found unreferenced chunks a1b2",         // plural
			"Deleting snapshot snap at revision 4 !", // trailing junk
		} {
			if j.parsePruneLine(line) {
				t.Errorf("line should NOT match: %q", line)
			}
		}
		// Progress is allocated lazily on first match, so a clean miss must
		// leave it nil entirely — not merely zeroed.
		if j.Progress != nil {
			t.Errorf("near-miss lines allocated Progress: %+v", j.Progress)
		}
	})
}

// parseErrorLine should overwrite ErrorMsg only when the line has the
// ERROR-tag prefix; arbitrary lines must leave it alone. The "TAG: msg"
// formatting is what surfaces in the UI's red error banner.
func TestParseErrorLine(t *testing.T) {
	cases := []struct {
		line string
		want string // expected ErrorMsg after this single line; "" means unchanged
	}{
		{
			line: "ERROR STORAGE_CREATE Failed to load the SFTP storage at sftp://…: Can't access the storage path /mnt/…",
			want: "STORAGE_CREATE: Failed to load the SFTP storage at sftp://…: Can't access the storage path /mnt/…",
		},
		{
			line: "ERROR UPLOAD_CHUNK Failed to upload the chunk abc123: RequestError: send request failed",
			want: "UPLOAD_CHUNK: Failed to upload the chunk abc123: RequestError: send request failed",
		},
		{line: "INFO BACKUP_END Backup for /home/user at revision 42 completed", want: ""},
		{line: "Uploaded chunk 1 size 4194304, 7.50MB/s 03:00:00 1.4%", want: ""},
		{line: "", want: ""},
	}
	for _, c := range cases {
		t.Run(c.line, func(t *testing.T) {
			j := &Job{}
			j.parseErrorLine(c.line)
			if j.ErrorMsg != c.want {
				t.Errorf("ErrorMsg=%q want %q", j.ErrorMsg, c.want)
			}
		})
	}

	// Last-error-wins: multiple ERROR lines should leave ErrorMsg as the LAST
	// captured one (most recent failure cause is most actionable).
	t.Run("last_error_wins", func(t *testing.T) {
		j := &Job{}
		j.parseErrorLine("ERROR FIRST_TAG First error")
		j.parseErrorLine("ERROR SECOND_TAG Second error")
		if j.ErrorMsg != "SECOND_TAG: Second error" {
			t.Errorf("ErrorMsg=%q want %q", j.ErrorMsg, "SECOND_TAG: Second error")
		}
	})
}
