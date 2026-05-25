package main

import "strings"

// errMissingS3Region is returned when an s3/s3c storage_url has no region but a
// custom endpoint — duplicacy would fail with "MissingRegion".
const errMissingS3Region = "s3 storage_url is missing a region: use s3://<region>@<endpoint>/... (e.g. US1 for Storj)"

// s3URLMissingRegion mirrors duplicacy's S3 URL parsing (duplicacy_storage.go):
// the region is the segment before '@'; the remaining authority token is the
// endpoint. duplicacy only auto-detects a region (CreateS3Storage, default
// us-east-1) when BOTH region and endpoint are empty — i.e. plain AWS. An empty
// region with a custom endpoint (e.g. gateway.storjshare.io) is passed straight
// to the AWS SDK and fails with "MissingRegion". Returns true for such URLs.
//
// The agent runs this on the fully-expanded URL, so there are no {placeholders}
// left; the placeholder guard is kept for parity with the router's copy.
func s3URLMissingRegion(storageType, rawURL string) bool {
	if storageType != "s3" && storageType != "s3c" {
		return false
	}
	const sep = "://"
	i := strings.Index(rawURL, sep)
	if i < 0 {
		return false // malformed — not this guard's job
	}
	authority := rawURL[i+len(sep):]
	if slash := strings.IndexByte(authority, '/'); slash >= 0 {
		authority = authority[:slash]
	}
	if strings.ContainsRune(authority, '{') {
		return false // templated authority (shouldn't happen post-expansion)
	}
	region := ""
	endpoint := authority
	if at := strings.IndexByte(authority, '@'); at >= 0 {
		region = authority[:at]
		endpoint = authority[at+1:]
	}
	if region != "" {
		return false
	}
	switch strings.ToLower(endpoint) {
	case "", "amazon", "amazon.com":
		return false // AWS default host — duplicacy auto-detects the region
	}
	return true
}
