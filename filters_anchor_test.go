package main

import (
	"strings"
	"testing"
)

// These tests encode duplicacy's ACTUAL matching contract, read from its source
// (github.com/gilbertchen/duplicacy) rather than from its docs:
//
//   - duplicacy_entry.go CreateEntryFromFileInfo builds each entry's path as
//     `directory + name`, starting from "" at the repository root. Every path
//     handed to the matcher is therefore RELATIVE and never has a leading "/".
//   - the same function appends "/" to directories.
//   - duplicacy_utils.go MatchPath returns on the FIRST '+' or '-' that matches.
//     First match wins; later lines are not consulted.
//   - duplicacy_entry.go's walk `continue`s past a directory that fails
//     MatchPath and never adds it to directoryList, so excluding a directory
//     prunes its whole subtree. That is where recursion comes from — not from
//     the pattern, and no wildcard is needed or wanted.
//
// Confirmed empirically against duplicacy's own matchPattern: the absolute form
// `/home/user/os-images/` returns false against `os-images/`, and so
// does `/home/user/os-images/*`. Appending a wildcard was never the fix.

func TestAnchorPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		root    string
		want    string
		wantOK  bool
	}{
		// The reported bug: this rule had never matched anything.
		{"absolute inside repo", "/home/user/os-images/", "/home/user", "os-images/", true},
		{"absolute file inside repo", "/mnt/storage/x.iso", "/mnt/storage", "x.iso", true},
		{"nested", "/srv/containers/homeassistant/home-assistant_v2.db", "/srv/containers",
			"homeassistant/home-assistant_v2.db", true},

		// A single org set renders onto repos with different roots; a rule for
		// another repo must be dropped, not emitted as dead weight.
		{"different repo", "/srv/containers/duplicacy/cache/", "/home/user", "", false},
		{"sibling prefix must not match", "/home/user2/secrets/", "/home/user", "", false},

		// Stripping these would leave "", and an empty pattern matches
		// everything — excluding the entire repository.
		{"equals root", "/home/user", "/home/user", "", false},
		{"equals root with slash", "/home/user/", "/home/user", "", false},

		{"trailing slash on root is tolerated", "/home/user/x/", "/home/user/", "x/", true},
		{"already relative passes through", "os-images/", "/home/user", "os-images/", true},
		{"relative wildcard passes through", "*.iso", "/home/user", "*.iso", true},
		{"repo rooted at /", "/etc/shadow", "/", "etc/shadow", true},
		{"empty pattern", "", "/home/user", "", false},

		// Not anchorable by prefix. Dropping loses nothing: duplicacy could not
		// have matched it in absolute form either.
		{"wildcard in leading component", "/home/*/os-images/", "/home/user", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := anchorPattern(tc.pattern, tc.root)
			if got != tc.want || ok != tc.wantOK {
				t.Fatalf("anchorPattern(%q, %q) = (%q, %v), want (%q, %v)",
					tc.pattern, tc.root, got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

// An anchored pattern has to survive into the rendered file in the form
// duplicacy will actually compare against.
func TestWriteRulesAnchorsAndAnnotates(t *testing.T) {
	var b strings.Builder
	writeRules(&b, []FilterRule{
		{Position: 0, Action: "exclude", Pattern: "/home/user/os-images/"},
		{Position: 1, Action: "exclude", Pattern: "/mnt/storage/vms/"}, // other repo
		{Position: 2, Action: "include", Pattern: "/home/user/keep.txt"},
	}, "/home/user")

	got := b.String()
	for _, want := range []string{"-os-images/\n", "+keep.txt\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered file missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "-/home/user") {
		t.Errorf("an absolute pattern survived into the file; it can never match:\n%s", got)
	}
	// Out-of-repo rules are commented, not dropped silently — an operator
	// reading the file must be able to tell "does not apply here" from "the sync
	// is broken".
	if !strings.Contains(got, "# (not in this repo) /mnt/storage/vms/") {
		t.Errorf("out-of-repo rule was not annotated:\n%s", got)
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if strings.HasPrefix(line, "-/") || strings.HasPrefix(line, "+/") {
			t.Errorf("emitted an absolute pattern: %q", line)
		}
	}
}

// Rules keep their operator-defined order within a set; only the leading path is
// rewritten.
func TestWriteRulesPreservesPositionOrder(t *testing.T) {
	var b strings.Builder
	writeRules(&b, []FilterRule{
		{Position: 2, Action: "exclude", Pattern: "/home/user/c/"},
		{Position: 0, Action: "exclude", Pattern: "/home/user/a/"},
		{Position: 1, Action: "exclude", Pattern: "/home/user/b/"},
	}, "/home/user")
	if got, want := b.String(), "-a/\n-b/\n-c/\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// matchRoot prefers the repository root from preferences, because that is what
// duplicacy resolves entry paths against.
func TestRepoMatchRoot(t *testing.T) {
	if got := (&Repo{Path: "/a", SourcePath: "/b"}).matchRoot(); got != "/b" {
		t.Errorf("SourcePath should win, got %q", got)
	}
	if got := (&Repo{Path: "/a"}).matchRoot(); got != "/a" {
		t.Errorf("should fall back to Path, got %q", got)
	}
}

// scopeOrder ranks MOST SPECIFIC FIRST. duplicacy stops at the first matching
// pattern, so a set written later in the file can never override one written
// earlier — the previous org-first ordering made per-repo and site overrides
// unreachable whenever a broader rule already matched.
func TestScopeOrderPutsSpecificFirst(t *testing.T) {
	if scopeOrder("site") >= scopeOrder("org") {
		t.Fatalf("site (%d) must be written before org (%d): first match wins",
			scopeOrder("site"), scopeOrder("org"))
	}
}
