package main

// Runtime helpers that bridge the controller-side credential store to the
// duplicacy CLI:
//
//   - prepareEnvForRepo: resolve all storages of a repo via the local mapping,
//     fetch their secrets from the controller (with cache), and produce the
//     env vars + cleanup closure for one duplicacy invocation.
//
//   - resolveCredential: cache-aware fetch of a single credential bundle.
//
// Failures must be loud: if a credential cannot be vended AND is not cached,
// the duplicacy invocation MUST fail rather than fall through to on-disk
// credentials in .duplicacy/preferences.

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// prepareEnvForRepo gathers env vars + tmpfile cleanup for every storage of
// the given repo. Returns an error if no mapping exists OR any individual
// credential vend fails AND is not present in cache.
//
// rsaPriv maps storage_alias → /dev/shm path of that storage's rsa_private_key
// (when RSA-encrypted). Restore callers look up the alias they're restoring
// from and pass the path to invocationForRestore. Backup/check/prune do not
// need RSA private keys (the public key on the storage is sufficient).
//
// Usage:
//
//   env, rsaPriv, cleanup, err := a.prepareEnvForRepo(ctx, repo)
//   if err != nil { ... }
//   defer cleanup()  // safe — cleanup is no-op on err
//
// The cleanup closure unlinks all /dev/shm tmpfiles created for this run
// (env materialised PEMs + RSA pub/priv PEMs).
func (a *app) prepareEnvForRepo(ctx context.Context, repo *Repo) ([]string, map[string]string, func(), error) {
	mapping, ok := a.mapping.getByPath(repo.Path)
	if !ok {
		return nil, nil, func() {}, fmt.Errorf("no controller mapping for repo at %s — register it via the controller UI", repo.Path)
	}
	if len(mapping.Storages) == 0 {
		return nil, nil, func() {}, fmt.Errorf("repo mapping for %s has no storages", repo.Path)
	}

	// Bound the total fetch time at 30s — agents should fail fast if the
	// controller is slow rather than blocking duplicacy on a flaky network.
	fetchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var (
		env      []string
		tmpfiles []string
		errs     []error
		rsaPriv  = map[string]string{}
	)
	for _, s := range mapping.Storages {
		bundle, err := a.resolveCredential(fetchCtx, s.CredentialID)
		if err != nil {
			errs = append(errs, fmt.Errorf("storage %s: %w", s.StorageAlias, err))
			continue
		}
		if bundle.StorageType != "" && bundle.StorageType != s.StorageType {
			// Mapping disagrees with the controller's stored type — almost
			// certainly a stale repos.json. Trust the controller.
			s.StorageType = bundle.StorageType
		}
		built, err := buildEnv(s.StorageType, s.StorageAlias, bundle)
		if err != nil {
			errs = append(errs, fmt.Errorf("storage %s buildEnv: %w", s.StorageAlias, err))
			continue
		}
		env = append(env, built.Env...)
		tmpfiles = append(tmpfiles, built.Tmpfiles...)
		if built.RSAPrivPath != "" {
			rsaPriv[s.StorageAlias] = built.RSAPrivPath
		}
	}

	cleanup := func() { cleanupTmpfiles(tmpfiles) }

	if len(errs) > 0 {
		// On any error, clean up tmpfiles already created and return.
		cleanup()
		return nil, nil, func() {}, errors.Join(errs...)
	}
	return env, rsaPriv, cleanup, nil
}

// resolveCredential returns a bundle, hitting the in-memory cache first and
// the controller's vending endpoint on miss. Successful fetches are cached.
func (a *app) resolveCredential(ctx context.Context, credentialID string) (SecretsBundle, error) {
	if cached, ok := a.secrets.get(credentialID); ok {
		return cached, nil
	}
	b, err := fetchSecrets(ctx, a.controlCenterClient, a.cfg.ControlCenterURL, a.cfg.NodeName, credentialID)
	if err != nil {
		return SecretsBundle{}, err
	}
	a.secrets.put(credentialID, b)
	return b, nil
}
