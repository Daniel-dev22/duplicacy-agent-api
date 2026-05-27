package main

import (
	"slices"
	"strings"
	"testing"
)

func TestInvocationForCopy(t *testing.T) {
	repo := &Repo{Path: "/relay"}
	cases := []struct {
		name        string
		from, to    string
		threads     int
		snapshotID  string
		wantContain []string
		wantNotHave []string
	}{
		{
			name:        "basic from+to",
			from:        "default",
			to:          "b2",
			wantContain: []string{"copy", "-from", "default", "-to", "b2", "-threads"},
		},
		{
			name:        "with snapshot id scoping",
			from:        "default",
			to:          "b2",
			snapshotID:  "pi-home",
			wantContain: []string{"copy", "-from", "default", "-to", "b2", "-id", "pi-home"},
		},
		{
			name:        "explicit threads",
			from:        "default",
			to:          "storj",
			threads:     8,
			wantContain: []string{"-threads", "8"},
		},
		{
			name:        "auto threads when zero",
			from:        "default",
			to:          "storj",
			threads:     0,
			wantContain: []string{"-threads"}, // value is autoThreads(), not asserted
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := invocationForCopy(repo, tc.from, tc.to, tc.threads, tc.snapshotID, "", "")
			if inv.RepoRoot != "/relay" {
				t.Errorf("RepoRoot = %q, want /relay", inv.RepoRoot)
			}
			joined := strings.Join(inv.Args, " ")
			for _, want := range tc.wantContain {
				if !slices.Contains(inv.Args, want) {
					t.Errorf("missing %q in args: %s", want, joined)
				}
			}
			for _, notWant := range tc.wantNotHave {
				if slices.Contains(inv.Args, notWant) {
					t.Errorf("unexpected %q in args: %s", notWant, joined)
				}
			}
		})
	}
}

func TestInvocationForAddCopyCompat(t *testing.T) {
	cases := []struct {
		name         string
		copyFrom     string
		bitIdentical bool
		wantCopy     bool
		wantBit      bool
	}{
		{name: "no copy (legacy independent storage)", copyFrom: "", bitIdentical: false, wantCopy: false, wantBit: false},
		{name: "copy-compatible only", copyFrom: "default", bitIdentical: false, wantCopy: true, wantBit: false},
		{name: "copy + bit-identical (most efficient)", copyFrom: "default", bitIdentical: true, wantCopy: true, wantBit: true},
		// bit-identical without -copy is meaningless and the builder should ignore it.
		{name: "bit-identical without copy is ignored", copyFrom: "", bitIdentical: true, wantCopy: false, wantBit: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := invocationForAdd("/repo", "b2", "snap-x", "b2://bucket", true, "", tc.copyFrom, tc.bitIdentical)
			hasCopy := slices.Contains(inv.Args, "-copy")
			hasBit := slices.Contains(inv.Args, "-bit-identical")
			if hasCopy != tc.wantCopy {
				t.Errorf("-copy: got %v want %v (args=%v)", hasCopy, tc.wantCopy, inv.Args)
			}
			if hasBit != tc.wantBit {
				t.Errorf("-bit-identical: got %v want %v (args=%v)", hasBit, tc.wantBit, inv.Args)
			}
			if tc.wantCopy {
				// -copy must be followed by the source alias.
				for i, a := range inv.Args {
					if a == "-copy" && (i+1 >= len(inv.Args) || inv.Args[i+1] != tc.copyFrom) {
						t.Errorf("-copy value: got %q want %q", inv.Args[i+1], tc.copyFrom)
					}
				}
			}
		})
	}
}

func TestInvocationForCheckSnapshotID(t *testing.T) {
	repo := &Repo{Path: "/repo"}
	inv := invocationForCheck(repo, "b2", "1-10", true, "pi-home")
	if !slices.Contains(inv.Args, "-id") {
		t.Fatalf("missing -id: %v", inv.Args)
	}
	for i, a := range inv.Args {
		if a == "-id" && inv.Args[i+1] != "pi-home" {
			t.Errorf("-id value = %q want pi-home", inv.Args[i+1])
		}
	}
	// Bare check (no snapshot id) must omit -id.
	bare := invocationForCheck(repo, "b2", "", false, "")
	if slices.Contains(bare.Args, "-id") {
		t.Errorf("bare check should not have -id: %v", bare.Args)
	}
}

// TestInvocationForCheckAlwaysTabular pins the always-on -tabular flag. The
// storage-dashboard's per-snapshot dedup column depends on the tabular table
// being emitted by every check run (manual + scheduled). If this regresses,
// the snapshots list and the rollup chart go cold for newly-checked repos
// even though the job still "succeeds".
func TestInvocationForCheckAlwaysTabular(t *testing.T) {
	repo := &Repo{Path: "/repo"}
	cases := []cliInvocation{
		invocationForCheck(repo, "", "", false, ""),
		invocationForCheck(repo, "default", "", false, ""),
		invocationForCheck(repo, "default", "1-10", true, "pi-home"),
		invocationForCheck(repo, "b2", "", false, "host-x"),
	}
	for i, inv := range cases {
		if !slices.Contains(inv.Args, "-tabular") {
			t.Errorf("case %d: -tabular missing from %v", i, inv.Args)
		}
		// -tabular must come AFTER -storage/-id/-r/-all (duplicacy rejects
		// flags after positional args, but check has none — still keep the
		// stable trailing position to match the order tests above).
		if inv.Args[len(inv.Args)-1] != "-tabular" {
			t.Errorf("case %d: -tabular should be the last arg, got %v", i, inv.Args)
		}
	}
}

// TestInvocationForInitChunkSize pins the chunk-size flag emission. Once a
// chunk pool is initialized these flags can never change, so the behaviour
// here is load-bearing for every -copy-compatible secondary that inherits
// the pool's chunk parameters (notably Storj).
func TestInvocationForInitChunkSize(t *testing.T) {
	t.Run("cloudOptimizedChunks emits -c 16M -min 4M -max 64M before positional args", func(t *testing.T) {
		inv := invocationForInit("/repo", "snap-x", "sftp://host/path", true, "", cloudOptimizedChunks)
		joined := strings.Join(inv.Args, " ")
		for _, pair := range [][2]string{
			{"-c", "16M"},
			{"-min", "4M"},
			{"-max", "64M"},
		} {
			flag, value := pair[0], pair[1]
			idx := slices.Index(inv.Args, flag)
			if idx < 0 {
				t.Errorf("missing %s in args: %s", flag, joined)
				continue
			}
			if idx+1 >= len(inv.Args) || inv.Args[idx+1] != value {
				t.Errorf("%s = %q, want %q", flag, inv.Args[idx+1], value)
			}
			// Chunk flags MUST precede the positional snapshot-id; duplicacy
			// rejects flags after positional args.
			snapIdx := slices.Index(inv.Args, "snap-x")
			if snapIdx >= 0 && idx > snapIdx {
				t.Errorf("%s came AFTER positional snapshot-id (idx %d > %d)", flag, idx, snapIdx)
			}
		}
	})

	t.Run("zero chunkSizing omits flags (back-compat with old pools)", func(t *testing.T) {
		inv := invocationForInit("/repo", "snap-x", "sftp://host/path", false, "", chunkSizing{})
		for _, flag := range []string{"-c", "-min", "-max"} {
			if slices.Contains(inv.Args, flag) {
				t.Errorf("zero chunkSizing should omit %s, got: %v", flag, inv.Args)
			}
		}
	})
}

func TestInvocationForPruneSnapshotID(t *testing.T) {
	repo := &Repo{Path: "/repo"}
	inv := invocationForPrune(repo, "b2", []string{"1:7", "7:30"}, false, false, "pi-home")
	if !slices.Contains(inv.Args, "-id") {
		t.Fatalf("missing -id: %v", inv.Args)
	}
	// keep_rules should still emit -keep entries.
	keeps := 0
	for _, a := range inv.Args {
		if a == "-keep" {
			keeps++
		}
	}
	if keeps != 2 {
		t.Errorf("-keep count = %d want 2 (args=%v)", keeps, inv.Args)
	}
}
