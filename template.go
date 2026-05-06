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
	return out, nil
}
