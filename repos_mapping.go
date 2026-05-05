package main

// Local mapping of repo_path → controller-managed repo identity.
//
// The agent persists, in CONFIG_DIR/repos.json, the link between a duplicacy
// repo on disk and the controller-side credentials that own its storages.
// This file contains NO secret material — only ids, aliases, types, and the
// repo path itself. Secrets are pulled from the controller at run time.
//
// Recovery semantics: if repos.json is lost, the agent CANNOT decide which
// credential to fetch for a given repo, so backup/restore/check/prune for
// managed repos will fail loudly. The mapping is sync'd with the controller's
// duplicacy_repos table by repo_id+node — to rebuild after disk loss, fetch
// from the controller (handled by main.go on startup; see repoMapping.refreshFromController).

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// RepoStorageMapping describes one storage attached to one local repo.
// No secrets — only routing info needed to (a) tell the controller which
// credential to vend and (b) build the right env var prefix for duplicacy.
type RepoStorageMapping struct {
	StorageAlias string `json:"storage_alias"` // 'default' for primary, custom for secondaries
	CredentialID string `json:"credential_id"` // UUID of duplicacy_credentials row
	StorageType  string `json:"storage_type"`  // 'b2' | 's3' | 'sftp' | 'gcs' | 'azure' | 'local'
	IsPrimary    bool   `json:"is_primary"`
}

// RepoMapping is one entry per local duplicacy repo.
type RepoMapping struct {
	RepoPath     string               `json:"repo_path"`
	RepoID       string               `json:"repo_id"` // duplicacy snapshot id (== controller's duplicacy_repos.repo_id)
	UUID         string               `json:"uuid"`    // controller's duplicacy_repos.id
	Storages     []RepoStorageMapping `json:"storages"`
	RegisteredAt time.Time            `json:"registered_at"`
}

type repoMappingStore struct {
	path string

	mu    sync.RWMutex
	byID  map[string]*RepoMapping // keyed by 12-char Repo.ID hash from repos.go (matches Repo.ID)
}

// newRepoMappingStore constructs the store rooted at <CONFIG_DIR>/repos.json.
func newRepoMappingStore(configDir string) *repoMappingStore {
	return &repoMappingStore{
		path: filepath.Join(configDir, "repos.json"),
		byID: map[string]*RepoMapping{},
	}
}

// load reads the on-disk file. Missing file is OK (empty store). Malformed
// file is fatal — better to surface corruption than silently accept.
func (s *repoMappingStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read %s: %w", s.path, err)
	}
	if len(data) == 0 {
		return nil
	}
	var entries []*RepoMapping
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("parse %s: %w", s.path, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID = map[string]*RepoMapping{}
	for _, e := range entries {
		s.byID[repoIDFromPath(e.RepoPath)] = e
	}
	return nil
}

// upsert adds or replaces one mapping and atomically rewrites the file.
func (s *repoMappingStore) upsert(m RepoMapping) error {
	s.mu.Lock()
	id := repoIDFromPath(m.RepoPath)
	if m.RegisteredAt.IsZero() {
		m.RegisteredAt = time.Now().UTC()
	}
	mc := m
	s.byID[id] = &mc
	s.mu.Unlock()
	return s.persist()
}

// delete removes the mapping for repoPath if present.
func (s *repoMappingStore) delete(repoPath string) error {
	s.mu.Lock()
	delete(s.byID, repoIDFromPath(repoPath))
	s.mu.Unlock()
	return s.persist()
}

// get returns the mapping for the duplicacy Repo.ID (12-char hash), if any.
// The bool indicates presence so callers can distinguish "no mapping" from
// "mapping with zero storages".
func (s *repoMappingStore) get(repoID string) (*RepoMapping, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.byID[repoID]
	if !ok {
		return nil, false
	}
	c := *m
	return &c, true
}

// getByPath returns the mapping for an absolute repo path, if any.
func (s *repoMappingStore) getByPath(repoPath string) (*RepoMapping, bool) {
	return s.get(repoIDFromPath(repoPath))
}

// list returns all mappings in stable order (by path).
func (s *repoMappingStore) list() []RepoMapping {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]RepoMapping, 0, len(s.byID))
	for _, m := range s.byID {
		out = append(out, *m)
	}
	// Stable order — repos.go sorts by path elsewhere; mirror that.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].RepoPath > out[j].RepoPath; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// persist writes the current store to disk atomically (.tmp + rename).
// Caller has either released or never held the mutex when this is called.
func (s *repoMappingStore) persist() error {
	s.mu.RLock()
	entries := make([]*RepoMapping, 0, len(s.byID))
	for _, m := range s.byID {
		entries = append(entries, m)
	}
	s.mu.RUnlock()

	if err := os.MkdirAll(filepath.Dir(s.path), 0700); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(s.path), err)
	}
	data, err := json.MarshalIndent(entries, "", "    ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename %s: %w", s.path, err)
	}
	return nil
}
