package main

import (
	"strings"
	"testing"
)

func TestServerTypeFromNode(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"nuc01", "nuc"},
		{"nuc", "nuc"},
		{"pi3", "pi"},
		{"nas01", "nas"},
		{"vm-01", "vm-"},
		{"", ""},
		{"007", "007"},
	}
	for _, c := range cases {
		got := serverTypeFromNode(c.in)
		if got != c.want {
			t.Errorf("serverTypeFromNode(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestExpandStorageURL(t *testing.T) {
	ctx := tplCtx{
		Server:     "nuc01",
		ServerType: "nuc",
		Site:       "site-a",
		Home:       "site-ahome",
		RemoteHome: "site-bhome",
		RepoID:     "nuc01-data",
	}

	cases := []struct {
		name           string
		in             string
		serverOverride string
		want           string
		wantErr        string // substring; empty = no error
	}{
		{
			name: "no placeholders pass through",
			in:   "sftp://backup@nas.example.com:22//mnt/array/foo/duplicacy",
			want: "sftp://backup@nas.example.com:22//mnt/array/foo/duplicacy",
		},
		{
			name: "single server placeholder",
			in:   "sftp://x@h//srv/{server}/d",
			want: "sftp://x@h//srv/nuc01/d",
		},
		{
			name: "all six placeholders",
			in:   "{site}/{home}/{remote_home}/{server}/{server_type}/{repo_id}",
			want: "site-a/site-ahome/site-bhome/nuc01/nuc/nuc01-data",
		},
		{
			name: "S3 example",
			in:   "s3://US1@gateway.storjshare.io/{home}/{site}-{server_type}/duplicacy",
			want: "s3://US1@gateway.storjshare.io/site-ahome/site-a-nuc/duplicacy",
		},
		{
			name: "SFTP example with home and server",
			in:   "sftp://backup@nas.example.com:22//mnt/array/{home}_backup/servers/{server}/duplicacy",
			want: "sftp://backup@nas.example.com:22//mnt/array/site-ahome_backup/servers/nuc01/duplicacy",
		},
		{
			name: "cross-site backup uses {remote_home} in the destination path",
			in:   "sftp://backup@nas.example.net:22//mnt/array/{remote_home}/{home}_backup/servers/{server}/duplicacy",
			want: "sftp://backup@nas.example.net:22//mnt/array/site-bhome/site-ahome_backup/servers/nuc01/duplicacy",
		},
		{
			name:           "server_override wins over ctx.Server",
			in:             "sftp://x@h//srv/{server}/d",
			serverOverride: "pi",
			want:           "sftp://x@h//srv/pi/d",
		},
		{
			name:           "server_override only affects {server}, not {server_type}",
			in:             "{server}/{server_type}",
			serverOverride: "nuc",
			want:           "nuc/nuc",
		},
		{
			name:    "unknown placeholder rejected",
			in:      "sftp://x@h//{srvr}/d",
			wantErr: "unknown placeholder {srvr}",
		},
		{
			name:    "typo in known placeholder",
			in:      "sftp://x@h//{servers}/d",
			wantErr: "{servers}",
		},
		{
			name: "literal-looking braces left alone if not lowercase ident",
			in:   "sftp://x@h//{Server}/d", // capital S — not in our placeholder set; regex won't match either since regex is [a-z_]
			want: "sftp://x@h//{Server}/d",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := expandStorageURL(c.in, ctx, c.serverOverride)
			if c.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil (out=%q)", c.wantErr, got)
				}
				if !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("error %v does not contain %q", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != c.want {
				t.Fatalf("expandStorageURL(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestBaseTplCtx(t *testing.T) {
	cfg := Config{NodeName: "nuc02", SiteID: "site-b", RemoteSite: "site-a"}
	ctx := cfg.baseTplCtx()
	if ctx.Server != "nuc02" || ctx.ServerType != "nuc" || ctx.Site != "site-b" || ctx.Home != "site-bhome" {
		t.Fatalf("baseTplCtx wrong: %+v", ctx)
	}
	if ctx.RemoteHome != "site-ahome" {
		t.Fatalf("RemoteHome for RemoteSite=site-a should be site-ahome, got %q", ctx.RemoteHome)
	}
	if ctx.RepoID != "" {
		t.Fatalf("RepoID should default to empty (caller fills it in), got %q", ctx.RepoID)
	}
}

// TestBaseTplCtxNoPeer pins the no-peer case: with REMOTE_SITE unset there
// is no peer, so RemoteHome is EMPTY. It must not fall back to the local site's
// own home — see TestExpandRemoteHomeWithoutPeerErrors for why.
func TestBaseTplCtxNoPeer(t *testing.T) {
	cfg := Config{NodeName: "nuc02", SiteID: "site-b"}
	if got := cfg.baseTplCtx().RemoteHome; got != "" {
		t.Fatalf("RemoteHome with no peer configured = %q, want empty", got)
	}
}

func TestSiteIDToRemoteHome(t *testing.T) {
	cases := []struct {
		site, remoteSite, want string
	}{
		// Peer configured — the peer's id decides, not the local one.
		{"site-a", "site-b", "site-bhome"},
		{"site-b", "site-a", "site-ahome"},
		{"east", "west", "westhome"},
		// No peer configured — empty, NOT the local home.
		{"site-a", "", ""},
		{"xx", "", ""},
	}
	for _, c := range cases {
		if got := siteIDToRemoteHome(c.site, c.remoteSite); got != c.want {
			t.Errorf("siteIDToRemoteHome(%q, %q) = %q, want %q", c.site, c.remoteSite, got, c.want)
		}
	}
}

// TestExpandRemoteHomeWithoutPeerErrors is the regression guard for a silent
// wrong-destination bug. {remote_home} used to resolve to the LOCAL site's home
// when no peer was configured, which is not a degraded answer — it is a
// different site's path that looks entirely plausible. The expanded URL is
// baked into .duplicacy/preferences at init and never re-resolved, so such a
// repo would back up to the wrong destination indefinitely with nothing to
// signal it. Refusing to resolve is the only safe behaviour.
func TestExpandRemoteHomeWithoutPeerErrors(t *testing.T) {
	noPeer := Config{NodeName: "nuc01", SiteID: "site-a"}.baseTplCtx()

	tpl := "sftp://backup@nas.example.com:22//mnt/array/{remote_home}/servers/{server}/duplicacy"
	if _, err := expandStorageURL(tpl, noPeer, ""); err == nil {
		t.Fatal("expandStorageURL with {remote_home} and no REMOTE_SITE: want error, got nil")
	}

	// A template that does not reach for a peer is unaffected.
	if _, err := expandStorageURL("sftp://backup@nas.example.com:22//mnt/array/{home}/{server}", noPeer, ""); err != nil {
		t.Fatalf("template without {remote_home} must still expand: %v", err)
	}

	// With a peer configured it resolves to the PEER's home, not the local one.
	withPeer := Config{NodeName: "nuc01", SiteID: "site-a", RemoteSite: "site-b"}.baseTplCtx()
	got, err := expandStorageURL(tpl, withPeer, "")
	if err != nil {
		t.Fatalf("expandStorageURL with peer configured: %v", err)
	}
	if !strings.Contains(got, "/site-bhome/") {
		t.Fatalf("expanded %q, want it to contain /site-bhome/ (the peer, not the local site)", got)
	}
	if strings.Contains(got, "/site-ahome/") {
		t.Fatalf("expanded %q resolved {remote_home} to the LOCAL site", got)
	}
}
