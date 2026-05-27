package main

import (
	"strings"
	"testing"
)

func TestValidateStorageURL(t *testing.T) {
	cases := []struct {
		name        string
		storageType string
		url         string
		wantErr     string // substring expected in err, "" = no error
	}{
		// SFTP
		{"sftp ok", "sftp", "sftp://backup@nas.example.com:22//mnt/array/site-a_backup/duplicacy", ""},
		{"sftp ok default port", "sftp", "sftp://backup@nas.example.com//mnt/path", ""},
		{"sftp missing scheme", "sftp", "duplicacy@host//path", "missing scheme"},
		{"sftp missing user", "sftp", "sftp://host:22//mnt/x", "missing username"},
		{"sftp missing host", "sftp", "sftp://user@//mnt/x", "missing host"},
		{"sftp single-slash path (the bug)", "sftp", "sftp://user@host:22/mnt/x", "missing double-slash"},
		// S3 — Storj
		{"storj US1 ok", "s3", "s3://US1@gateway.storjshare.io/site-a/kd-nas/duplicacy", ""},
		{"storj missing region (the bug)", "s3", "s3://@gateway.storjshare.io/site-a/kd-nas/duplicacy", "missing region"},
		{"storj typo region", "s3", "s3://us1@gateway.storjshare.io/bucket/path", "not in {US1,EU1,AP1}"},
		// S3 — custom endpoint requires region
		{"custom endpoint no region", "s3", "s3://wasabi.example.com/bucket/path", "must include region"},
		{"custom endpoint with region ok", "s3", "s3://us-east-1@s3.wasabisys.com/bucket/path", ""},
		// S3 — AWS (region optional)
		{"aws bare ok", "s3", "s3://amazon.com/bucket/path", ""},
		{"aws region ok", "s3", "s3://us-east-1@s3.amazonaws.com/bucket/path", ""},
		// S3 — missing bucket
		{"s3 missing bucket", "s3", "s3://US1@gateway.storjshare.io/", "missing bucket"},
		// B2
		{"b2 ok", "b2", "b2://my-bucket/prefix", ""},
		{"b2 missing bucket", "b2", "b2:///prefix", "missing bucket"},
		// GCS
		{"gcs ok", "gcs", "gcs://my-bucket", ""},
		{"gcs missing bucket", "gcs", "gcs://", "missing bucket"},
		// Templated URLs are EXPECTED in vended bundles — controller stores
		// them with {home}/{site}/{remote_home}/{server_type}; duplicacy
		// reads the resolved URL from .duplicacy/preferences at runtime,
		// the vended URL is never used as a literal connection target.
		// Validation skips host/path/bucket shape for templated URLs.
		{"templated sftp ok", "sftp", "sftp://backup@nas.{remote_home}apps.com:22//mnt/array/{home}_backup/duplicacy", ""},
		{"templated b2 ok", "b2", "b2://{home}-bucket/prefix", ""},
		{"templated gcs ok", "gcs", "gcs://{home}-bucket", ""},
		// Storj S3 region IS a literal (never templated) — region check
		// still applies even for otherwise-templated URLs.
		{"templated storj US1 ok", "s3", "s3://US1@gateway.storjshare.io/{home}/{site}-{server_type}/duplicacy", ""},
		{"templated storj BAD region", "s3", "s3://us1@gateway.storjshare.io/{home}/{site}-{server_type}/duplicacy", "not in {US1,EU1,AP1}"},
		// Unknown type
		{"unknown type → no-op", "weird-future-backend", "anything://goes", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStorageURL(tc.storageType, tc.url)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("expected nil error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got: %v", tc.wantErr, err)
			}
		})
	}
}
