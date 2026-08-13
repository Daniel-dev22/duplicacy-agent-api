package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIsAlreadyInitialized(t *testing.T) {
	cases := []struct {
		out  string
		want bool
	}{
		{"The repository /mnt/storage has already been initialized\n", true},
		{"Failed to download the configuration file from the storage: MissingRegion", false},
		{"", false},
		{"some other error", false},
	}
	for _, tc := range cases {
		if got := isAlreadyInitialized([]byte(tc.out)); got != tc.want {
			t.Errorf("isAlreadyInitialized(%q) = %v, want %v", tc.out, got, tc.want)
		}
	}
}

func TestReadPreferenceURLs(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".duplicacy"), 0o700); err != nil {
		t.Fatal(err)
	}
	prefs := `[
	  {"name":"default","id":"nas-mnt-storage","storage":"sftp://backup@nas.example.net:22//mnt/array/site-ahome_backup/servers/nas/duplicacy","encrypted":true},
	  {"name":"storj","id":"nas-mnt-storage","storage":"s3://US1@gateway.storjshare.io/site-ahome/site-a-nas/duplicacy","encrypted":true}
	]`
	if err := os.WriteFile(filepath.Join(dir, ".duplicacy", "preferences"), []byte(prefs), 0o600); err != nil {
		t.Fatal(err)
	}

	m, err := readPreferenceURLs(dir)
	if err != nil {
		t.Fatalf("readPreferenceURLs: %v", err)
	}
	if got := m["default"]; got != "sftp://backup@nas.example.net:22//mnt/array/site-ahome_backup/servers/nas/duplicacy" {
		t.Errorf("default URL = %q", got)
	}
	if got := m["storj"]; got != "s3://US1@gateway.storjshare.io/site-ahome/site-a-nas/duplicacy" {
		t.Errorf("storj URL = %q", got)
	}
	if len(m) != 2 {
		t.Errorf("len = %d, want 2", len(m))
	}
}

func TestReadPreferenceURLsMissing(t *testing.T) {
	if _, err := readPreferenceURLs(t.TempDir()); err == nil {
		t.Error("expected error for missing preferences, got nil")
	}
}
