package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPruneJobLogs(t *testing.T) {
	dir := t.TempDir()
	// Create 5 files with stepped mtimes so we know which are "newest".
	for i := 0; i < 5; i++ {
		p := filepath.Join(dir, "job"+string(rune('a'+i))+".log")
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		mtime := time.Now().Add(time.Duration(-i) * time.Hour)
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}
	// Non-log file should be ignored
	if err := os.WriteFile(filepath.Join(dir, "ignored.txt"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}

	pruneJobLogs(dir, 2) // keep 2 newest

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	logCount := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".log" {
			logCount++
		}
	}
	if logCount != 2 {
		t.Fatalf("expected 2 logs kept, got %d", logCount)
	}
	// Newest (joba.log = now, jobb.log = -1h) must survive.
	if _, err := os.Stat(filepath.Join(dir, "joba.log")); err != nil {
		t.Fatalf("newest survived? got err: %v", err)
	}
	// Oldest (jobe.log = -4h) must be pruned.
	if _, err := os.Stat(filepath.Join(dir, "jobe.log")); !os.IsNotExist(err) {
		t.Fatalf("expected jobe.log pruned, stat err: %v", err)
	}
}

func TestPruneJobLogsMissingDir(t *testing.T) {
	// Should not panic / err for a non-existent dir.
	pruneJobLogs(filepath.Join(t.TempDir(), "does-not-exist"), 10)
}

func TestPruneJobLogsBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		p := filepath.Join(dir, "k"+string(rune('1'+i))+".log")
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	pruneJobLogs(dir, 10) // threshold above count → no-op
	entries, _ := os.ReadDir(dir)
	if len(entries) != 3 {
		t.Fatalf("expected 3 files kept, got %d", len(entries))
	}
}
