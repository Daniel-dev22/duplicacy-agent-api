package main

import (
	"strings"
	"testing"
)

// TestBuildEnvPrimaryAliasNormalized — duplicacy's `init` command always names
// the primary preference "default", so even if the controller passes a custom
// alias for the primary (e.g. "kd-nas"), the env vars must use the bare
// DUPLICACY_ prefix. Otherwise duplicacy looks up DUPLICACY_SSH_KEY_FILE,
// finds nothing, prompts interactively, and init fails with "No private key
// file is provided".
func TestBuildEnvPrimaryAliasNormalized(t *testing.T) {
	bundle := SecretsBundle{
		EncryptionPassword: "pw",
		Backend: map[string]string{
			"ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\nfake\n-----END OPENSSH PRIVATE KEY-----\n",
		},
	}

	cases := []struct {
		name           string
		alias          string
		isPrimary      bool
		wantPrefix     string
		wantKeyVarName string
	}{
		{"primary with default alias", "default", true, "DUPLICACY_", "DUPLICACY_SSH_KEY_FILE"},
		{"primary with custom alias", "kd-nas", true, "DUPLICACY_", "DUPLICACY_SSH_KEY_FILE"},
		{"primary with empty alias", "", true, "DUPLICACY_", "DUPLICACY_SSH_KEY_FILE"},
		{"secondary with default alias", "default", false, "DUPLICACY_", "DUPLICACY_SSH_KEY_FILE"},
		{"secondary with custom alias", "kd-nas", false, "DUPLICACY_KD-NAS_", "DUPLICACY_KD-NAS_SSH_KEY_FILE"},
		{"secondary with empty alias", "", false, "DUPLICACY_", "DUPLICACY_SSH_KEY_FILE"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := buildEnv("sftp", c.alias, c.isPrimary, bundle)
			if err != nil {
				t.Fatalf("buildEnv: %v", err)
			}
			defer cleanupTmpfiles(res.Tmpfiles)

			wantPwd := c.wantPrefix + "PASSWORD=pw"
			if !containsExact(res.Env, wantPwd) {
				t.Errorf("expected %q in env, got %v", wantPwd, res.Env)
			}
			found := false
			for _, kv := range res.Env {
				if strings.HasPrefix(kv, c.wantKeyVarName+"=") {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected env var %q to be set, got %v", c.wantKeyVarName, res.Env)
			}
		})
	}
}

func containsExact(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
