package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"
)

// snapshotFilesCacheStore is the persistent cache of `duplicacy list -files`
// output, keyed by the IMMUTABLE (snapshot_id, revision, storage_name) tuple.
// A revision's file list never changes once the revision exists, so a cache
// hit is correct forever — there is no staleness and therefore no
// invalidation. The only eviction triggers are:
//
//	(a) space — a size cap (gzipped bytes) enforced by deleting the
//	    least-recently-accessed rows, EXCEPT the newest-N revisions per
//	    (snapshot_id, storage_name) which are pinned (the warm set); and
//	(b) prune — a revision leaving retention, reconciled against the live
//	    `duplicacy list` output.
//
// The table lives in the same events.sqlite the event-buffer owns; we share
// the *sql.DB (created in events.go) to avoid a second Open + WAL file.
type snapshotFilesCacheStore struct {
	db        *sql.DB
	maxBytes  int64 // size cap on SUM(gz_bytes); <=0 disables size eviction
	warmKeepN int   // newest-N revisions per (snapshot_id, storage) pinned from size eviction
}

func newSnapshotFilesCacheStore(db *sql.DB, maxBytes int64, warmKeepN int) *snapshotFilesCacheStore {
	if warmKeepN < 0 {
		warmKeepN = 0
	}
	return &snapshotFilesCacheStore{db: db, maxBytes: maxBytes, warmKeepN: warmKeepN}
}

// get returns the decompressed `list -files` output for the immutable key, or
// (nil, false, nil) on a miss. On a hit it best-effort touches last_access so
// the size-LRU keeps recently-used trees.
func (s *snapshotFilesCacheStore) get(ctx context.Context, snapshotID string, rev int, storage string) ([]byte, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, nil
	}
	var gz []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT gz_output FROM snapshot_files_cache
		  WHERE snapshot_id = ? AND revision = ? AND storage_name = ?`,
		snapshotID, rev, storage).Scan(&gz)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	raw, err := gunzip(gz)
	if err != nil {
		// A corrupt blob is not a hit — drop it so the caller re-lists and
		// re-caches cleanly rather than serving garbage.
		_, _ = s.db.ExecContext(ctx,
			`DELETE FROM snapshot_files_cache WHERE snapshot_id=? AND revision=? AND storage_name=?`,
			snapshotID, rev, storage)
		return nil, false, fmt.Errorf("cache gunzip: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		`UPDATE snapshot_files_cache SET last_access = ?
		  WHERE snapshot_id = ? AND revision = ? AND storage_name = ?`,
		time.Now().UTC(), snapshotID, rev, storage)
	return raw, true, nil
}

// has reports whether the key is already cached (used by the warm sweep to
// skip re-listing immutable revisions). Cheap PK existence check.
func (s *snapshotFilesCacheStore) has(ctx context.Context, snapshotID string, rev int, storage string) (bool, error) {
	if s == nil || s.db == nil {
		return false, nil
	}
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM snapshot_files_cache
		  WHERE snapshot_id = ? AND revision = ? AND storage_name = ?`,
		snapshotID, rev, storage).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cache has: %w", err)
	}
	return true, nil
}

// put gzips and UPSERTs the listing, then enforces the size cap. raw is the
// raw `duplicacy list -files` stdout exactly as served to clients.
func (s *snapshotFilesCacheStore) put(ctx context.Context, snapshotID string, rev int, storage, repoID string, raw []byte) error {
	if s == nil || s.db == nil {
		return nil
	}
	gz, err := gzipBytes(raw)
	if err != nil {
		return fmt.Errorf("cache gzip: %w", err)
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO snapshot_files_cache
			(snapshot_id, revision, storage_name, repo_id, gz_output, raw_bytes, gz_bytes, cached_at, last_access)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(snapshot_id, revision, storage_name) DO UPDATE SET
			repo_id     = excluded.repo_id,
			gz_output   = excluded.gz_output,
			raw_bytes   = excluded.raw_bytes,
			gz_bytes    = excluded.gz_bytes,
			cached_at   = excluded.cached_at,
			last_access = excluded.last_access`,
		snapshotID, rev, storage, repoID, gz, int64(len(raw)), int64(len(gz)), now, now,
	); err != nil {
		return fmt.Errorf("cache put: %w", err)
	}
	return s.evictBySize(ctx)
}

// evictBySize deletes least-recently-accessed rows until SUM(gz_bytes) is back
// under maxBytes, NEVER touching the newest-warmKeepN revisions per
// (snapshot_id, storage_name) (the pinned warm set). Returns the number of
// rows evicted. A no-op when the cap is disabled or not exceeded.
func (s *snapshotFilesCacheStore) evictBySize(ctx context.Context) (err error) {
	if s == nil || s.db == nil || s.maxBytes <= 0 {
		return nil
	}
	var total int64
	if err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(gz_bytes), 0) FROM snapshot_files_cache`).Scan(&total); err != nil {
		return fmt.Errorf("cache size sum: %w", err)
	}
	if total <= s.maxBytes {
		return nil
	}
	need := total - s.maxBytes

	// Eligible = NOT in the newest-warmKeepN per (snapshot_id, storage_name),
	// oldest last_access first. ROW_NUMBER ranks revisions newest-first so
	// rn > warmKeepN is everything outside the pinned warm window.
	rows, err := s.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT rowid AS rid, gz_bytes, last_access,
			       ROW_NUMBER() OVER (
			           PARTITION BY snapshot_id, storage_name
			           ORDER BY revision DESC
			       ) AS rn
			FROM snapshot_files_cache
		)
		SELECT rid, gz_bytes FROM ranked WHERE rn > ? ORDER BY last_access ASC`,
		s.warmKeepN)
	if err != nil {
		return fmt.Errorf("cache evict scan: %w", err)
	}
	defer rows.Close()

	var victims []int64
	var freed int64
	for rows.Next() {
		var rid, gzBytes int64
		if err := rows.Scan(&rid, &gzBytes); err != nil {
			return fmt.Errorf("cache evict row: %w", err)
		}
		victims = append(victims, rid)
		freed += gzBytes
		if freed >= need {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if len(victims) == 0 {
		return nil
	}
	return s.deleteByRowID(ctx, victims)
}

// reconcileAgainstKeep deletes cache rows that the warm sweep did NOT decide to
// keep, scoped to the storages the sweep actually listed. It runs ONCE at the
// end of a sweep against the union keep-set, rather than per-(repo,storage),
// for two reasons:
//
//   - Correctness: a per-storage reconcile keyed on one repo's scoped `list`
//     would delete other repos' entries on the same shared storage (default),
//     so on a multi-repo node each repo's reconcile would nuke the others'
//     freshly-warmed rows — the warm cache would thrash down to the
//     last-processed repo every sweep.
//   - Cleanup: a row in a swept storage that's in nobody's keep-set is either a
//     pruned revision OR a snapshot this node no longer warms (e.g. an edge
//     node that used to over-warm the whole shared pool). Both should go, so the
//     node converges to exactly what it warms today.
//
// `swept` is the set of storages successfully listed this sweep (a storage whose
// `list` errored is absent, so its rows are left untouched rather than wrongly
// purged on a transient failure). `keep` holds storageSnapRevKey(...) for every
// currently-existing (storage, snapshot_id, revision) discovered. Returns the
// number of rows evicted.
func (s *snapshotFilesCacheStore) reconcileAgainstKeep(ctx context.Context, swept, keep map[string]struct{}) (int, error) {
	if s == nil || s.db == nil || len(swept) == 0 {
		return 0, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT rowid, storage_name, snapshot_id, revision FROM snapshot_files_cache`)
	if err != nil {
		return 0, fmt.Errorf("reconcile scan: %w", err)
	}
	defer rows.Close()

	var victims []int64
	for rows.Next() {
		var rid int64
		var storage, sid string
		var rev int
		if err := rows.Scan(&rid, &storage, &sid, &rev); err != nil {
			return 0, fmt.Errorf("reconcile row: %w", err)
		}
		if _, ok := swept[storage]; !ok {
			continue // storage not listed this sweep — leave it alone
		}
		if _, ok := keep[storageSnapRevKey(storage, sid, rev)]; !ok {
			victims = append(victims, rid)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	if len(victims) == 0 {
		return 0, nil
	}
	if err := s.deleteByRowID(ctx, victims); err != nil {
		return 0, err
	}
	return len(victims), nil
}

// deleteByRowID removes rows by rowid in a single statement. rowids come from
// our own prior query, so direct interpolation is safe (they are int64) and
// avoids a 999-parameter IN() limit dance for large eviction batches.
func (s *snapshotFilesCacheStore) deleteByRowID(ctx context.Context, rowids []int64) error {
	if len(rowids) == 0 {
		return nil
	}
	var b bytes.Buffer
	b.WriteString("DELETE FROM snapshot_files_cache WHERE rowid IN (")
	for i, id := range rowids {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", id)
	}
	b.WriteByte(')')
	if _, err := s.db.ExecContext(ctx, b.String()); err != nil {
		return fmt.Errorf("cache delete: %w", err)
	}
	return nil
}

// stats reports the row count and total on-disk (gzipped) bytes — used for the
// periodic warm-sweep summary log so operators can see cache growth.
func (s *snapshotFilesCacheStore) stats(ctx context.Context) (rows int, gzBytes int64, err error) {
	if s == nil || s.db == nil {
		return 0, 0, nil
	}
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(gz_bytes), 0) FROM snapshot_files_cache`).Scan(&rows, &gzBytes)
	if err != nil {
		return 0, 0, fmt.Errorf("cache stats: %w", err)
	}
	return rows, gzBytes, nil
}

// storageSnapRevKey is the keep-set key: a listing is uniquely identified by
// its storage + snapshot id + revision (the same tuple the cache is keyed on).
func storageSnapRevKey(storage, snapshotID string, rev int) string {
	return storage + "\x00" + snapshotID + "\x00" + fmt.Sprint(rev)
}

func gzipBytes(raw []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gunzip(gz []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}
