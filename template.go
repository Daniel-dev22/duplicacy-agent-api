package main

// Storage URL templating.
//
// `storage_url` on a duplicacy credential may contain placeholders that the
// agent expands at duplicacy-init time using its own per-node identity. The
// resolved URL is what gets baked into `.duplicacy/preferences`; subsequent
// backup/check/restore/prune ops never re-resolve.
//
// Supported placeholders:
//   {server}       — cfg.NodeName                              (e.g. "nuc01")
//   {server_type}  — cfg.NodeName with trailing digits stripped (e.g. "nuc")
//   {site}         — cfg.SiteID                                 (e.g. "kd")
//   {home}         — cfg.SiteID + "home"                        (e.g. "site-a")
//   {repo_id}      — per-init repo's snapshot id                (e.g. "nuc01-data")
//
// Unknown placeholders are NOT silently passed through — expandStorageURL
// returns an error so a typo cannot ship a literal "{srvr}" directory name to
// the storage backend.

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

type tplCtx struct {
	Server     string
	ServerType string
	Site       string
	Home       string
	RepoID     string
}

func (c Config) baseTplCtx() tplCtx {
	return tplCtx{
		Server:     c.NodeName,
		ServerType: serverTypeFromNode(c.NodeName),
		Site:       c.SiteID,
		Home:       c.SiteID + "home",
	}
}

// serverTypeFromNode strips trailing digits from a NodeName so "nuc01" → "nuc",
// "pi3" → "pi". A node name that's all digits or empty returns the input as-is
// (we'd rather pass through a weird value than panic / produce empty strings).
func serverTypeFromNode(node string) string {
	stripped := strings.TrimRightFunc(node, unicode.IsDigit)
	if stripped == "" {
		return node
	}
	return stripped
}

var unknownPlaceholderRe = regexp.MustCompile(`\{[a-z_][a-z0-9_]*\}`)

// expandStorageURL replaces every supported placeholder in template with its
// value from ctx. Returns an error if any unsupported placeholder remains.
// Templates with no placeholders pass through unchanged.
func expandStorageURL(template string, ctx tplCtx) (string, error) {
	r := strings.NewReplacer(
		"{server}", ctx.Server,
		"{server_type}", ctx.ServerType,
		"{site}", ctx.Site,
		"{home}", ctx.Home,
		"{repo_id}", ctx.RepoID,
	)
	out := r.Replace(template)
	if m := unknownPlaceholderRe.FindString(out); m != "" {
		return "", fmt.Errorf("unknown placeholder %s in storage_url (supported: {server} {server_type} {site} {home} {repo_id})", m)
	}
	return normalizeSFTPAbsolutePath(out), nil
}

// normalizeSFTPAbsolutePath ensures an sftp:// URL whose path component
// looks like a host filesystem absolute path (starts with /mnt, /home, /var,
// /opt, /srv, /data, /backup) is encoded with the double-slash convention
// duplicacy/Go's url package expect for absolute SSH paths. A single slash
// after the authority is interpreted by SFTP servers as relative-to-the-
// SSH-user's-home-directory; absolute paths require `sftp://host//path`.
//
// If the URL already uses // we leave it alone. If it's not sftp://, we
// leave it alone. If the path looks user-relative (e.g. sftp://host/backups
// where ~/backups is intentional), we leave it alone — only well-known
// system roots get rewritten.
func normalizeSFTPAbsolutePath(u string) string {
	const prefix = "sftp://"
	if !strings.HasPrefix(u, prefix) {
		return u
	}
	rest := u[len(prefix):]
	// split authority from path on the first '/'.
	idx := strings.IndexByte(rest, '/')
	if idx < 0 {
		return u
	}
	authority := rest[:idx]
	path := rest[idx:] // includes leading '/'
	if strings.HasPrefix(path, "//") {
		return u // already absolute (double slash)
	}
	// only rewrite if the path's first segment is a well-known host root.
	systemRoots := []string{"/mnt/", "/home/", "/var/", "/opt/", "/srv/", "/data/", "/backup/"}
	for _, root := range systemRoots {
		if strings.HasPrefix(path, root) || path == strings.TrimSuffix(root, "/") {
			return prefix + authority + "/" + path
		}
	}
	return u
}
