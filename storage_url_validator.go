package main

// Storage URL shape validators for the agent's boot-time pre-flight check.
//
// Catches misconfigured storages before the scheduler starts firing them.
// Without these, a missing region in a Storj URL surfaces as
// "MissingRegion" deep inside duplicacy's S3 layer, with no clear pointer
// to the bad credential. A missing double-slash in an SFTP URL surfaces as
// "stat /home/user/path: no such file" which masks the real issue (the
// path was supposed to be /home/user//abs/path).
//
// ValidateStorageURL is called on the vend path (network.go), on every
// credential bundle the controller returns, before the bundle reaches any
// caller. A bad URL therefore fails fast with a clear, non-retryable error at
// vend time rather than as a cryptic duplicacy CLI failure minutes later
// inside a schedule fire.

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// ValidateStorageURL applies the per-backend shape check. Returns nil when
// the URL is acceptable.
//
// The controller stores URLs with templated placeholders (`{home}`,
// `{remote_home}`, `{site}`, `{server_type}`) that get resolved at
// init/add time and baked into `.duplicacy/preferences`. The vended URL
// itself isn't used as a literal connection target at runtime —
// duplicacy reads the already-resolved URL from preferences. So we skip
// per-backend host/path validation for templated URLs.
//
// We still validate the Storj S3 region for templated URLs because the
// region is a literal ("US1"/"EU1"/"AP1") that precedes the @ separator
// and can't carry a placeholder — that's the exact misconfig the
// validator was added to catch.
func ValidateStorageURL(storageType, rawURL string) error {
	if strings.Contains(rawURL, "{") || strings.Contains(rawURL, "}") {
		if storageType == "s3" || storageType == "s3c" {
			return validateS3URL(rawURL)
		}
		return nil
	}
	switch storageType {
	case "sftp":
		return validateSFTPURL(rawURL)
	case "s3":
		return validateS3URL(rawURL)
	case "b2":
		return validateB2URL(rawURL)
	case "gcs":
		return validateGCSURL(rawURL)
	case "azure":
		return validateAzureURL(rawURL)
	case "local":
		return validateLocalURL(rawURL)
	default:
		// Don't fail-fast on an unknown type — the bundle's storage_type
		// might be added later. Log + skip via the caller.
		return nil
	}
}

func validateSFTPURL(raw string) error {
	if !strings.HasPrefix(raw, "sftp://") {
		return fmt.Errorf("sftp URL missing scheme: %q", raw)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse sftp url: %w", err)
	}
	if u.User == nil || u.User.Username() == "" {
		return errors.New("sftp URL missing username (expected sftp://user@host:port//abs/path)")
	}
	if u.Host == "" {
		return errors.New("sftp URL missing host")
	}
	// Double-slash for absolute remote path is required by duplicacy/pkg-sftp.
	// url.Parse for "sftp://user@host//mnt/x" leaves Path="//mnt/x" — so we
	// look at the raw URL post-host for "//" before the path.
	hostEnd := strings.Index(raw[len("sftp://"):], "/")
	if hostEnd < 0 {
		return errors.New("sftp URL missing path")
	}
	pathPart := raw[len("sftp://")+hostEnd:]
	if !strings.HasPrefix(pathPart, "//") {
		return fmt.Errorf("sftp URL missing double-slash before absolute path: %q (expected sftp://user@host:port//abs/path)", raw)
	}
	return nil
}

// validS3Regions lists region identifiers we accept without warning.
// Empty region IS supported by duplicacy only when paired with default AWS
// endpoint (auto-detects us-east-1); any other endpoint without a region
// fails with "MissingRegion" deep inside the SDK. Witness: 2026-05-27 plan
// memo for storj specifically.
var storjRegions = map[string]bool{
	"US1": true, "EU1": true, "AP1": true,
}

func validateS3URL(raw string) error {
	if !strings.HasPrefix(raw, "s3://") {
		return fmt.Errorf("s3 URL missing scheme: %q", raw)
	}
	rest := raw[len("s3://"):]
	atIdx := strings.Index(rest, "@")
	if atIdx < 0 {
		// Bare s3://<endpoint>/<bucket> — only OK when endpoint is plain AWS
		// (duplicacy auto-detects us-east-1). Storj / Wasabi / other custom
		// endpoints will fail without a region, so we require @ for those.
		host := rest
		if slash := strings.Index(rest, "/"); slash >= 0 {
			host = rest[:slash]
		}
		if !isAWSHost(host) {
			return fmt.Errorf("s3 URL with custom endpoint %q must include region: s3://<region>@<endpoint>/<bucket>/<prefix>", host)
		}
		return nil
	}
	region := rest[:atIdx]
	endpoint := rest[atIdx+1:]
	if slash := strings.Index(endpoint, "/"); slash >= 0 {
		endpoint = endpoint[:slash]
	}
	if region == "" {
		return fmt.Errorf("s3 URL missing region (empty before @): %q", raw)
	}
	// Storj-specific: enforce the documented regions to catch typos like
	// "us1" / "us-east-1" against storj's gateway.
	if strings.Contains(endpoint, "storjshare.io") && !storjRegions[region] {
		return fmt.Errorf("storj s3 URL region %q not in {US1,EU1,AP1}: %q", region, raw)
	}
	// Bucket+prefix presence check — skipped for templated URLs since the
	// bucket name itself can be a placeholder like {home}.
	if !strings.Contains(raw, "{") {
		bp := rest[atIdx+1:]
		slashIdx := strings.Index(bp, "/")
		if slashIdx < 0 || slashIdx == len(bp)-1 {
			return fmt.Errorf("s3 URL missing bucket: %q", raw)
		}
	}
	return nil
}

func isAWSHost(h string) bool {
	h = strings.ToLower(h)
	return strings.HasSuffix(h, "amazonaws.com") ||
		h == "amazon.com" || h == "amazon" ||
		h == "" || h == "s3.amazonaws.com"
}

func validateB2URL(raw string) error {
	if !strings.HasPrefix(raw, "b2://") {
		return fmt.Errorf("b2 URL missing scheme: %q", raw)
	}
	body := raw[len("b2://"):]
	if body == "" || strings.HasPrefix(body, "/") {
		return fmt.Errorf("b2 URL missing bucket: %q", raw)
	}
	return nil
}

func validateGCSURL(raw string) error {
	if !strings.HasPrefix(raw, "gcs://") {
		return fmt.Errorf("gcs URL missing scheme: %q", raw)
	}
	body := raw[len("gcs://"):]
	if body == "" {
		return fmt.Errorf("gcs URL missing bucket: %q", raw)
	}
	return nil
}

func validateAzureURL(raw string) error {
	if !strings.HasPrefix(raw, "azure://") {
		return fmt.Errorf("azure URL missing scheme: %q", raw)
	}
	return nil
}

func validateLocalURL(raw string) error {
	// duplicacy local storage uses a bare absolute path (no scheme prefix).
	path := raw
	if strings.HasPrefix(raw, "local://") {
		path = raw[len("local://"):]
	}
	if path == "" {
		return errors.New("local URL missing path")
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("local URL must be an absolute path: %q", raw)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("local URL path stat failed: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("local URL path %q is not a directory", path)
	}
	// Write probe — create+delete a sentinel file to confirm RW.
	probe := path + "/.duplicacy-agent-rw-probe"
	if err := os.WriteFile(probe, []byte("ok"), 0o600); err != nil {
		return fmt.Errorf("local URL path %q not writable: %w", path, err)
	}
	_ = os.Remove(probe)
	return nil
}
