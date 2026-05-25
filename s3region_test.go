package main

import "testing"

func TestS3URLMissingRegion(t *testing.T) {
	cases := []struct {
		name        string
		storageType string
		url         string
		want        bool
	}{
		{"storj empty region (the bug, expanded)", "s3", "s3://@gateway.storjshare.io/site-a/kd-nuc/duplicacy", true},
		{"custom endpoint no region no @", "s3", "s3://gateway.storjshare.io/bucket/path", true},
		{"storj with region", "s3", "s3://US1@gateway.storjshare.io/site-a/kd-nuc/duplicacy", false},
		{"wasabi with region", "s3", "s3://us-east-1@s3.wasabisys.com/bucket", false},
		{"aws default host no region", "s3", "s3://amazon.com/bucket/path", false},
		{"aws region@amazon", "s3", "s3://us-east-1@amazon.com/bucket", false},
		{"s3c custom endpoint empty region", "s3c", "s3c://@gw.example.com/bucket", true},
		{"s3c with region", "s3c", "s3c://eu1@gw.example.com/bucket", false},
		{"non-s3 sftp", "sftp", "sftp://user@host//mnt/x", false},
		{"non-s3 local", "local", "/mnt/storage/duplicacy", false},
		{"authority only no path", "s3", "s3://@gateway.storjshare.io", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := s3URLMissingRegion(tc.storageType, tc.url); got != tc.want {
				t.Fatalf("s3URLMissingRegion(%q, %q) = %v, want %v", tc.storageType, tc.url, got, tc.want)
			}
		})
	}
}
