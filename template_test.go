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
		Site:       "kd",
		Home:       "site-a",
		RemoteHome: "site-b",
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
			want: "kd/site-a/site-b/nuc01/nuc/nuc01-data",
		},
		{
			name: "user's S3 example",
			in:   "s3://US1@gateway.storjshare.io/{home}/{site}-{server_type}/duplicacy",
			want: "s3://US1@gateway.storjshare.io/site-a/kd-nuc/duplicacy",
		},
		{
			name: "user's SFTP example with home and server",
			in:   "sftp://backup@nas.{home}apps.com:22//mnt/array/{home}_backup/servers/{server}/duplicacy",
			want: "sftp://backup@nas.example.com:22//mnt/array/site-a_backup/servers/nuc01/duplicacy",
		},
		{
			name: "cross-site backup uses {remote_home} for destination host",
			in:   "sftp://backup@nas.{remote_home}apps.com:22//mnt/array/{home}_backup/servers/{server}/duplicacy",
			want: "sftp://backup@nas.example.net:22//mnt/array/site-a_backup/servers/nuc01/duplicacy",
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
	cfg := Config{NodeName: "nuc02", SiteID: "ng"}
	ctx := cfg.baseTplCtx()
	if ctx.Server != "nuc02" || ctx.ServerType != "nuc" || ctx.Site != "ng" || ctx.Home != "site-b" {
		t.Fatalf("baseTplCtx wrong: %+v", ctx)
	}
	if ctx.RemoteHome != "site-a" {
		t.Fatalf("RemoteHome for site=ng should be site-a, got %q", ctx.RemoteHome)
	}
	if ctx.RepoID != "" {
		t.Fatalf("RepoID should default to empty (caller fills it in), got %q", ctx.RepoID)
	}
}

func TestSiteIDToRemoteHome(t *testing.T) {
	cases := map[string]string{"kd": "site-b", "ng": "site-a", "xx": "xxhome"}
	for in, want := range cases {
		if got := siteIDToRemoteHome(in); got != want {
			t.Errorf("siteIDToRemoteHome(%q) = %q, want %q", in, got, want)
		}
	}
}
