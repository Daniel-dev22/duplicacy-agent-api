package main

import "testing"

// TestParseListOutput covers the duplicacy `list` output variants the parser
// must tolerate. The RSA-passphrase-prompt case is a regression guard: duplicacy
// emits `Enter the passphrase for <keyfile>:` with no trailing newline, so the
// first `Snapshot … revision 1 …` row is concatenated onto the prompt. An
// anchored `^Snapshot` regex dropped revision 1 entirely — hiding the oldest
// revision from the list/retention/restore-picker for every RSA-encrypted
// storage. (Surfaced by a restore integration test.)
func TestParseListOutput(t *testing.T) {
	cases := []struct {
		name     string
		out      string
		wantRevs []int
		wantID   string
	}{
		{
			name: "plain rows with and without -hash suffix",
			out: "Storage set to sftp://host//path\n" +
				"Snapshot home revision 1 created at 2026-04-30 02:00:11 -hash\n" +
				"Snapshot home revision 2 created at 2026-05-01 02:00\n",
			wantRevs: []int{1, 2},
			wantID:   "home",
		},
		{
			name: "rsa passphrase prompt prepends revision 1 (no newline)",
			out: "Storage set to sftp://backup@nas:22//mnt/array/duplicacy\n" +
				"Enter the passphrase for /dev/shm/duplicacy-rsa-priv-921221753:" +
				"Snapshot restore-test-nuc revision 1 created at 2026-06-13 21:13 -hash\n" +
				"Snapshot restore-test-nuc revision 2 created at 2026-06-13 21:13\n" +
				"Snapshot restore-test-nuc revision 3 created at 2026-06-13 21:13\n",
			wantRevs: []int{1, 2, 3},
			wantID:   "restore-test-nuc",
		},
		{
			name:     "empty output",
			out:      "Storage set to sftp://host//path\n",
			wantRevs: []int{},
			wantID:   "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snaps := parseListOutput(tc.out)
			if len(snaps) != len(tc.wantRevs) {
				t.Fatalf("got %d snapshots, want %d (%+v)", len(snaps), len(tc.wantRevs), snaps)
			}
			for i, want := range tc.wantRevs {
				if snaps[i].Revision != want {
					t.Errorf("snap[%d] revision = %d, want %d", i, snaps[i].Revision, want)
				}
				if tc.wantID != "" && snaps[i].SnapshotID != tc.wantID {
					t.Errorf("snap[%d] id = %q, want %q", i, snaps[i].SnapshotID, tc.wantID)
				}
			}
			// The prompt prefix must never leak into Raw.
			for _, s := range snaps {
				if len(s.Raw) < 8 || s.Raw[:8] != "Snapshot" {
					t.Errorf("Raw not cleaned to the snapshot row: %q", s.Raw)
				}
			}
		})
	}
}
