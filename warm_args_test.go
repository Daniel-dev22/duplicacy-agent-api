package main

import (
	"strings"
	"testing"
)

func TestWarmListArgs(t *testing.T) {
	join := func(a []string) string { return strings.Join(a, " ") }
	cases := []struct {
		name, storage, key, ownID, want string
	}{
		// default = shared local pool → scope to the repo's own id (no -all, no -storage).
		{"default scopes to own id", "default", "", "nuc-home-user", "list -id nuc-home-user"},
		{"empty storage == default", "", "", "nuc-home-user", "list -id nuc-home-user"},
		{"default with key", "default", "/dev/shm/k", "nuc-home-user", "list -key /dev/shm/k -id nuc-home-user"},
		{"default no own id falls back to -all", "default", "", "", "list -all"},
		// secondaries (relay-only, pooled) → -all + -storage.
		{"remote-nas uses -all", "remote-nas", "", "relay-kd", "list -all -storage remote-nas"},
		{"storj uses -all with key", "storj", "/dev/shm/k", "relay-kd", "list -key /dev/shm/k -all -storage storj"},
	}
	for _, tc := range cases {
		if got := join(warmListArgs(tc.storage, tc.key, tc.ownID)); got != tc.want {
			t.Errorf("%s: warmListArgs(%q,%q,%q)=%q want %q", tc.name, tc.storage, tc.key, tc.ownID, got, tc.want)
		}
	}
}
