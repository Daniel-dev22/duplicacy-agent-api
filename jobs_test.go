package main

import "testing"

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
