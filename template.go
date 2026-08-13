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
//                    Per-storage server_override wins over this.
//   {server_type}  — cfg.NodeName with trailing digits stripped (e.g. "nuc")
//   {site}         — cfg.SiteID                                 (e.g. "site-a")
//   {home}         — cfg.SiteID + "home"                        (e.g. "site-ahome")
//   {remote_home}  — the PEER site's home, from REMOTE_SITE_ID
//                    (e.g. REMOTE_SITE_ID=site-b → "site-bhome")
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
	RemoteHome string
	RepoID     string
}

func (c Config) baseTplCtx() tplCtx {
	return tplCtx{
		Server:     c.NodeName,
		ServerType: serverTypeFromNode(c.NodeName),
		Site:       c.SiteID,
		Home:       c.SiteID + "home",
		RemoteHome: siteIDToRemoteHome(c.SiteID, c.RemoteSiteID),
	}
}

// siteIDToRemoteHome resolves the {remote_home} placeholder: the home string
// of the site this one replicates to.
//
// The peer site cannot be derived from the local site — it is genuinely
// deployment configuration — so it comes from the optional REMOTE_SITE_ID env
// var.
//
// When REMOTE_SITE_ID is unset this returns "", and expandStorageURL then
// REJECTS any template that uses {remote_home}. It deliberately does not fall
// back to the local site's home: that would resolve a cross-site path to this
// site's own, and because the expanded URL is baked into .duplicacy/preferences
// at init and never re-resolved, the repo would keep writing to the wrong
// destination forever with nothing to indicate it. Same principle as the
// unknown-placeholder check above — a misconfiguration must fail loudly at
// init rather than silently ship a plausible-looking wrong path.
func siteIDToRemoteHome(site, remoteSite string) string {
	if remoteSite != "" {
		return remoteSite + "home"
	}
	return ""
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
//
// serverOverride is a per-storage override: when non-empty it wins over
// ctx.Server for the {server} placeholder only. This lets one credential
// serve multiple storage aliases on the same node whose URLs differ only by
// server segment (e.g. a NAS-side {nuc,pi,nas}-storage trio).
func expandStorageURL(template string, ctx tplCtx, serverOverride string) (string, error) {
	server := ctx.Server
	if serverOverride != "" {
		server = serverOverride
	}
	if strings.Contains(template, "{remote_home}") && ctx.RemoteHome == "" {
		return "", fmt.Errorf("storage_url uses {remote_home} but REMOTE_SITE_ID is not set; refusing to resolve a cross-site path to this site's own home")
	}
	r := strings.NewReplacer(
		"{server}", server,
		"{server_type}", ctx.ServerType,
		"{site}", ctx.Site,
		"{home}", ctx.Home,
		"{remote_home}", ctx.RemoteHome,
		"{repo_id}", ctx.RepoID,
	)
	out := r.Replace(template)
	if m := unknownPlaceholderRe.FindString(out); m != "" {
		return "", fmt.Errorf("unknown placeholder %s in storage_url (supported: {server} {server_type} {site} {home} {remote_home} {repo_id})", m)
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
