package main

import "testing"

// TestDestinationKey pins URL → (key, label) for every protocol we currently
// support. The chart aggregation depends on different repos pointing at the
// same physical destination producing the SAME key — a regression here means
// the same Storj bucket would show up as N separate lines on the chart.
func TestDestinationKey(t *testing.T) {
	cases := []struct {
		url       string
		wantKey   string
		wantLabel string
	}{
		// SFTP — NAS host with user + path + the "nas." prefix
		{
			"sftp://backup@nas.example.com/mnt/backups/repoA",
			"sftp://nas.example.com",
			"NAS (example.com)",
		},
		{
			"sftp://backup@nas.example.net:2222/mnt/backups/repoB",
			"sftp://nas.example.net",
			"NAS (example.net)",
		},
		// SFTP — non-NAS host falls back to SFTP label
		{
			"sftp://backup@example.com/backups",
			"sftp://example.com",
			"SFTP (example.com)",
		},

		// S3 — Storj
		{
			"s3://us1.storj.io/backup-bucket/path/to/repo",
			"s3://us1.storj.io/backup-bucket",
			"Storj (backup-bucket)",
		},
		{
			"s3://eu1.storj.io/cold-bucket",
			"s3://eu1.storj.io/cold-bucket",
			"Storj (cold-bucket)",
		},
		// S3 — AWS
		{
			"s3://s3.amazonaws.com/my-bucket/repo",
			"s3://s3.amazonaws.com/my-bucket",
			"S3 (my-bucket)",
		},

		// B2
		{"b2://my-bucket/repo", "b2://my-bucket", "B2 (my-bucket)"},
		{"b2://other-bucket", "b2://other-bucket", "B2 (other-bucket)"},

		// GCS
		{"gcs://my-gcs/repo", "gcs://my-gcs", "GCS (my-gcs)"},

		// Azure
		{
			"azure://account/container/path",
			"azure://account/container",
			"Azure (container)",
		},

		// Local (bare path)
		{
			"/mnt/storage/backups/repoA",
			"local:///mnt/storage/backups",
			"Local (/mnt/storage/backups)",
		},

		// Unknown / malformed
		{"", "unknown://", "Unknown"},
		{"garbage-no-scheme", "unknown://garbage-no-scheme", "Unknown"},
	}

	for _, c := range cases {
		t.Run(c.url, func(t *testing.T) {
			gotKey, gotLabel := DestinationKey(c.url)
			if gotKey != c.wantKey {
				t.Errorf("key = %q want %q", gotKey, c.wantKey)
			}
			if gotLabel != c.wantLabel {
				t.Errorf("label = %q want %q", gotLabel, c.wantLabel)
			}
		})
	}
}

// TestDestinationKeyStability proves the same logical destination produces
// identical keys across multiple repos pointing at it with different paths,
// different users, different ports — the chart aggregation invariant.
func TestDestinationKeyStability(t *testing.T) {
	groups := [][]string{
		{
			"sftp://backup@nas.example.com/mnt/backups/repoA",
			"sftp://backup@nas.example.com/mnt/backups/repoB",
			"sftp://otheruser@nas.example.com:2222/different/path",
		},
		{
			"s3://us1.storj.io/backup-bucket/host01",
			"s3://us1.storj.io/backup-bucket/host02",
			"s3://us1.storj.io/backup-bucket",
		},
	}
	for i, group := range groups {
		var first string
		for j, u := range group {
			k, _ := DestinationKey(u)
			if j == 0 {
				first = k
				continue
			}
			if k != first {
				t.Errorf("group %d url %q: key %q ≠ first %q (aggregation will split this destination)", i, u, k, first)
			}
		}
	}
}
