package main

import (
	"net/url"
	"path"
	"strings"
)

// DestinationKey reduces a duplicacy storage URL down to a stable identity
// for "the same physical place chunks live", then derives an operator-facing
// label. Multiple repos on multiple hosts can point at the same destination
// (e.g. every host's backup pointing at the same Storj bucket); aggregating
// chart series and dedup math hinges on producing the same key for those.
//
// Examples:
//
//	sftp://backup@nas.example.com/mnt/backups/repoA → key "sftp://nas.example.com", label "NAS (example.com)"
//	s3://us1.storj.io/backup-bucket/path                → key "s3://us1.storj.io/backup-bucket", label "Storj (backup-bucket)"
//	b2://my-bucket/repo                                 → key "b2://my-bucket", label "B2 (my-bucket)"
//	/mnt/storage/backups/repoA                     → key "local:///mnt/storage/backups", label "Local (/mnt/storage/backups)"
//	gcs://my-bucket/repo                                → key "gcs://my-bucket", label "GCS (my-bucket)"
//	azure://account/container                           → key "azure://account/container", label "Azure (container)"
//
// Unknown/unparseable URLs degrade to ("unknown://<raw>", "Unknown").
func DestinationKey(storageURL string) (key, label string) {
	storageURL = strings.TrimSpace(storageURL)
	if storageURL == "" {
		return "unknown://", "Unknown"
	}

	// Bare path → local storage.
	if strings.HasPrefix(storageURL, "/") {
		dir := path.Dir(storageURL)
		return "local://" + dir, "Local (" + dir + ")"
	}

	scheme, rest, ok := splitScheme(storageURL)
	if !ok {
		return "unknown://" + storageURL, "Unknown"
	}

	switch strings.ToLower(scheme) {
	case "sftp", "ftp":
		host := extractHost(rest)
		if host == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		k := scheme + "://" + host
		// Friendly label: strip a leading "nas." for NAS hosts so the chart
		// legend reads "NAS (example.com)" not "NAS (nas.example.com)".
		display := strings.TrimPrefix(host, "nas.")
		if strings.HasPrefix(host, "nas.") {
			return k, "NAS (" + display + ")"
		}
		return k, strings.ToUpper(scheme) + " (" + host + ")"

	case "s3":
		// `s3://<host>/<bucket>[/path]` — the host distinguishes providers
		// (us1.storj.io, s3.amazonaws.com, …) so bucket alone isn't unique.
		host, bucket := extractHostAndFirstPath(rest)
		if host == "" || bucket == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		k := scheme + "://" + host + "/" + bucket
		// Storj exposes S3 gateways under both us1.storj.io / eu1.storj.io
		// AND gateway.storjshare.io — match on "storj" substring so either
		// form is recognised. Pre-fix witness 2026-05-28: substring "storj.io"
		// missed gateway.storjshare.io and the destination rendered "S3 (site-a)".
		if strings.Contains(host, "storj") {
			return k, "Storj (" + bucket + ")"
		}
		return k, "S3 (" + bucket + ")"

	case "b2":
		// `b2://<bucket>[/path]`
		bucket := extractFirstSegment(rest)
		if bucket == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		return scheme + "://" + bucket, "B2 (" + bucket + ")"

	case "gcs":
		bucket := extractFirstSegment(rest)
		if bucket == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		return scheme + "://" + bucket, "GCS (" + bucket + ")"

	case "azure":
		// `azure://<account>/<container>[/path]`
		account, container := extractHostAndFirstPath(rest)
		if account == "" || container == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		k := scheme + "://" + account + "/" + container
		return k, "Azure (" + container + ")"

	case "wasabi":
		bucket := extractFirstSegment(rest)
		if bucket == "" {
			return "unknown://" + storageURL, "Unknown"
		}
		return scheme + "://" + bucket, "Wasabi (" + bucket + ")"

	default:
		// Unknown scheme — preserve raw key, derive a best-effort label from
		// the first path segment.
		seg := extractFirstSegment(rest)
		label := strings.ToUpper(scheme)
		if seg != "" {
			label += " (" + seg + ")"
		}
		return scheme + "://" + rest, label
	}
}

// splitScheme returns (scheme, rest, true) for "scheme://rest" or false
// otherwise.
func splitScheme(s string) (scheme, rest string, ok bool) {
	i := strings.Index(s, "://")
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+3:], true
}

// extractHost returns the host portion of "[user@]host[:port][/path]" — uses
// net/url where possible, falls back to plain string splitting (duplicacy
// URLs sometimes have characters net/url rejects).
func extractHost(rest string) string {
	if u, err := url.Parse("sftp://" + rest); err == nil && u.Host != "" {
		// Drop port for a stable key — same NAS reachable on multiple ports
		// is still the same destination.
		host := u.Host
		if i := strings.Index(host, ":"); i >= 0 {
			host = host[:i]
		}
		return host
	}
	// Manual fallback: strip user@ and any trailing /path.
	if i := strings.Index(rest, "@"); i >= 0 {
		rest = rest[i+1:]
	}
	if i := strings.IndexAny(rest, "/:"); i >= 0 {
		return rest[:i]
	}
	return rest
}

// extractHostAndFirstPath returns ("host", "first-path-segment") from
// "host[:port]/first/second/..." — used by s3, azure, etc.
func extractHostAndFirstPath(rest string) (string, string) {
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return extractHost(rest), ""
	}
	host := extractHost(rest[:slash])
	tail := strings.TrimLeft(rest[slash+1:], "/")
	return host, extractFirstSegment(tail)
}

// extractFirstSegment returns the first '/'-delimited segment of s.
func extractFirstSegment(s string) string {
	if i := strings.Index(s, "/"); i >= 0 {
		return s[:i]
	}
	return s
}
