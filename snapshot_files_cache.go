package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
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
	s := &snapshotFilesCacheStore{db: db, maxBytes: maxBytes, warmKeepN: warmKeepN}
	if err := s.ensureSchema(context.Background()); err != nil {
		slog.Error("list-files uncacheable-marker schema failed", "error", err)
	}
	return s
}

// ensureSchema creates the additive negative-marker table. A marker records that
// an immutable revision's listing can never be cached (its gz exceeds the row
// cap or the spool ceiling), so the warmer stops re-running it every sweep. It
// lives beside the cache in events.sqlite and so survives restarts.
func (s *snapshotFilesCacheStore) ensureSchema(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS snapshot_files_uncacheable (
			repo_id       TEXT    NOT NULL,
			storage_name  TEXT    NOT NULL,
			snapshot_id   TEXT    NOT NULL,
			revision      INTEGER NOT NULL,
			reason        TEXT    NOT NULL,
			gz_bytes      INTEGER NOT NULL,
			marked_at     TIMESTAMP NOT NULL,
			PRIMARY KEY (repo_id, storage_name, snapshot_id, revision)
		)`)
	return err
}

const (
	uncacheableRowCap  = "row_cap"
	uncacheableCeiling = "spool_ceiling"
)

func (s *snapshotFilesCacheStore) markUncacheable(ctx context.Context, repoID, storage, snapshotID string, rev int, reason string, gzBytes int64) error {
	if s == nil || s.db == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO snapshot_files_uncacheable
			(repo_id, storage_name, snapshot_id, revision, reason, gz_bytes, marked_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		repoID, storage, snapshotID, rev, reason, gzBytes, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("mark uncacheable: %w", err)
	}
	return nil
}

// uncacheable reports the marker reason for a revision ("" when unmarked).
func (s *snapshotFilesCacheStore) uncacheable(ctx context.Context, repoID, storage, snapshotID string, rev int) (string, error) {
	if s == nil || s.db == nil {
		return "", nil
	}
	var reason string
	err := s.db.QueryRowContext(ctx, `
		SELECT reason FROM snapshot_files_uncacheable
		 WHERE repo_id = ? AND storage_name = ? AND snapshot_id = ? AND revision = ?`,
		repoID, storage, snapshotID, rev).Scan(&reason)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("uncacheable lookup: %w", err)
	}
	return reason, nil
}

// pinPolicy is the ONE rule for how many newest revisions per (snapshot,
// storage) are warmed and protected from eviction. The warmer decides what to
// warm with it and the evictor decides what to protect with it, so the two can
// never disagree and re-list what the other just evicted. When keeping warmN
// revisions of every snapshot (projected from each snapshot's newest row) would
// exceed the cap, the policy drops to maxPinRelaxedKeep. The projection uses
// only the newest rows, so evicting the older ones cannot flip the answer.
func pinPolicy(newestBytes int64, warmN int, maxBytes int64) int {
	if maxBytes > 0 && newestBytes*int64(warmN) > maxBytes {
		return min(maxPinRelaxedKeep, warmN)
	}
	return warmN
}

// effectivePinN applies pinPolicy to the current cache contents.
func (s *snapshotFilesCacheStore) effectivePinN(ctx context.Context) int {
	if s == nil || s.db == nil || s.maxBytes <= 0 || s.warmKeepN <= maxPinRelaxedKeep {
		return s.warmN()
	}
	var newest int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(gz_bytes), 0) FROM (
			SELECT gz_bytes, ROW_NUMBER() OVER (
				PARTITION BY snapshot_id, storage_name ORDER BY revision DESC) AS rn
			FROM snapshot_files_cache) WHERE rn = 1`).Scan(&newest)
	if err != nil {
		slog.Warn("list-files pin policy query failed, using configured N", "error", err)
		return s.warmKeepN
	}
	return pinPolicy(newest, s.warmKeepN, s.maxBytes)
}

func (s *snapshotFilesCacheStore) warmN() int {
	if s == nil {
		return 0
	}
	return s.warmKeepN
}

// cachedListing is a stored row: the gzip stream plus the exact uncompressed
// length (the response's Content-Length).
type cachedListing struct {
	gz       []byte
	rawBytes int64
}

// open returns the stored row for the immutable key, or (nil, false, nil) on a
// miss. It never inflates: the caller streams gunzip(gz) to the client. On a hit
// it best-effort touches last_access so the size-LRU keeps recently-used trees.
func (s *snapshotFilesCacheStore) open(ctx context.Context, snapshotID string, rev int, storage string) (*cachedListing, bool, error) {
	if s == nil || s.db == nil {
		return nil, false, nil
	}
	var l cachedListing
	err := s.db.QueryRowContext(ctx,
		`SELECT gz_output, raw_bytes FROM snapshot_files_cache
		  WHERE snapshot_id = ? AND revision = ? AND storage_name = ?`,
		snapshotID, rev, storage).Scan(&l.gz, &l.rawBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	_, _ = s.db.ExecContext(ctx,
		`UPDATE snapshot_files_cache SET last_access = ?
		  WHERE snapshot_id = ? AND revision = ? AND storage_name = ?`,
		time.Now().UTC(), snapshotID, rev, storage)
	return &l, true, nil
}

// drop removes a row that failed to inflate so the caller re-lists and
// re-caches cleanly rather than serving garbage.
func (s *snapshotFilesCacheStore) drop(ctx context.Context, snapshotID string, rev int, storage string) {
	if s == nil || s.db == nil {
		return
	}
	_, _ = s.db.ExecContext(ctx,
		`DELETE FROM snapshot_files_cache WHERE snapshot_id=? AND revision=? AND storage_name=?`,
		snapshotID, rev, storage)
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

// putGzipped UPSERTs an already-gzipped COMPLETE listing, then enforces the
// size cap. rawBytes is the uncompressed length of the listing as served.
func (s *snapshotFilesCacheStore) putGzipped(ctx context.Context, snapshotID string, rev int, storage, repoID string, gz []byte, rawBytes int64) error {
	if s == nil || s.db == nil {
		return nil
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
		snapshotID, rev, storage, repoID, gz, rawBytes, int64(len(gz)), now, now,
	); err != nil {
		return fmt.Errorf("cache put: %w", err)
	}
	return s.evictBySize(ctx, nil)
}

// maxPinRelaxedKeep is how many newest revisions per (snapshot_id, storage)
// survive even when the pinned set alone exceeds the cap.
const maxPinRelaxedKeep = 2

// evictBySize deletes rows until SUM(gz_bytes) is back under maxBytes: the
// OLDEST revision first (deepest rank per snapshot), never the newest
// pinPolicy-many per (snapshot_id, storage_name). If those alone exceed the cap
// the cap still wins down to the newest maxPinRelaxedKeep (logged), so it is a
// real ceiling. Rows of exemptRepos (a repo whose listing failed this sweep) are
// never evicted. A no-op when the cap is disabled or not exceeded.
func (s *snapshotFilesCacheStore) evictBySize(ctx context.Context, exemptRepos map[string]struct{}) (err error) {
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
	pinN := s.effectivePinN(ctx)
	hardKeep := min(maxPinRelaxedKeep, s.warmKeepN)

	rows, err := s.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT rowid AS rid, gz_bytes, last_access, repo_id,
			       ROW_NUMBER() OVER (
			           PARTITION BY snapshot_id, storage_name
			           ORDER BY revision DESC
			       ) AS rn
			FROM snapshot_files_cache
		)
		SELECT rid, gz_bytes, repo_id, rn <= ? AS pinned FROM ranked
		 WHERE rn > ? ORDER BY pinned ASC, rn DESC, last_access ASC`,
		pinN, hardKeep)
	if err != nil {
		return fmt.Errorf("cache evict scan: %w", err)
	}
	defer rows.Close()

	var victims []int64
	var freed int64
	relaxed := 0
	for rows.Next() {
		var rid, gzBytes int64
		var repo string
		var pinned bool
		if err := rows.Scan(&rid, &gzBytes, &repo, &pinned); err != nil {
			return fmt.Errorf("cache evict row: %w", err)
		}
		if _, ok := exemptRepos[repo]; ok {
			continue
		}
		if pinned {
			relaxed++
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
	if relaxed > 0 {
		slog.Warn("list-files cache: pinned set exceeds cap, pin relaxed",
			"max_bytes", s.maxBytes, "total_bytes", total, "pinned_rows_evicted", relaxed, "kept_per_snapshot", hardKeep)
	}
	return s.deleteByRowID(ctx, victims)
}

// repoStorage identifies one (repo, storage) listing target of a sweep.
type repoStorage struct{ repoID, storage string }

// sweepState is what a warm sweep learned, input to reconcileAgainstKeep.
type sweepState struct {
	swept        map[repoStorage]struct{} // (repo, storage) pairs successfully listed
	sweptStorage map[string]struct{}      // storages some repo listed (for rows of repos no longer known)
	failed       map[string]struct{}      // repos whose vend or list failed: rows are exempt from reconcile AND size eviction
	known        map[string]struct{}      // repos present this sweep
	keep         map[string]struct{}      // storageSnapRevKey for every discovered live revision
}

func newSweepState() *sweepState {
	return &sweepState{
		swept:        map[repoStorage]struct{}{},
		sweptStorage: map[string]struct{}{},
		failed:       map[string]struct{}{},
		known:        map[string]struct{}{},
		keep:         map[string]struct{}{},
	}
}

// judges reports whether this sweep can say a repo's row on a storage is gone:
// only if that repo's listing of it succeeded (or the repo is no longer known and
// some repo listed the storage). A failed repo is never judged.
func (st *sweepState) judges(repo, storage string) bool {
	if _, bad := st.failed[repo]; bad {
		return false
	}
	if _, known := st.known[repo]; known {
		_, ok := st.swept[repoStorage{repo, storage}]
		return ok
	}
	_, ok := st.sweptStorage[storage]
	return ok
}

// reconcileMarkers drops uncacheable markers of revisions that were pruned.
func (s *snapshotFilesCacheStore) reconcileMarkers(ctx context.Context, st *sweepState) error {
	if s == nil || s.db == nil || st == nil || len(st.sweptStorage) == 0 {
		return nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT rowid, repo_id, storage_name, snapshot_id, revision FROM snapshot_files_uncacheable`)
	if err != nil {
		return fmt.Errorf("marker scan: %w", err)
	}
	defer rows.Close()
	var victims []int64
	for rows.Next() {
		var rid int64
		var repo, storage, sid string
		var rev int
		if err := rows.Scan(&rid, &repo, &storage, &sid, &rev); err != nil {
			return fmt.Errorf("marker row: %w", err)
		}
		if !st.judges(repo, storage) {
			continue
		}
		if _, ok := st.keep[storageSnapRevKey(storage, sid, rev)]; !ok {
			victims = append(victims, rid)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	if len(victims) == 0 {
		return nil
	}
	var b bytes.Buffer
	b.WriteString("DELETE FROM snapshot_files_uncacheable WHERE rowid IN (")
	for i, id := range victims {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", id)
	}
	b.WriteByte(')')
	if _, err := s.db.ExecContext(ctx, b.String()); err != nil {
		return fmt.Errorf("marker delete: %w", err)
	}
	return nil
}

// reconcileAgainstKeep deletes cache rows the warm sweep found no longer live,
// run ONCE at the end of a sweep against the union keep-set (a per-repo
// reconcile on a shared storage would delete the other repos' fresh rows).
//
// Deletion is scoped to (repo_id, storage): a row is judged only if ITS repo
// listed ITS storage successfully this sweep. Cache rows are immutable, so a
// transient failure (vend timeout, SFTP DNS) must never purge a repo's rows
// just because another repo listed the same storage — those repos are in
// st.failed and also exempt. Rows of a repo no longer known to the agent are
// judged against any successful listing of their storage. Returns rows evicted.
func (s *snapshotFilesCacheStore) reconcileAgainstKeep(ctx context.Context, st *sweepState) (int, error) {
	if err := s.reconcileMarkers(ctx, st); err != nil {
		return 0, err
	}
	if s == nil || s.db == nil || st == nil || len(st.sweptStorage) == 0 {
		return 0, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT rowid, repo_id, storage_name, snapshot_id, revision FROM snapshot_files_cache`)
	if err != nil {
		return 0, fmt.Errorf("reconcile scan: %w", err)
	}
	defer rows.Close()

	var victims []int64
	for rows.Next() {
		var rid int64
		var repo, storage, sid string
		var rev int
		if err := rows.Scan(&rid, &repo, &storage, &sid, &rev); err != nil {
			return 0, fmt.Errorf("reconcile row: %w", err)
		}
		if !st.judges(repo, storage) {
			continue
		}
		if _, ok := st.keep[storageSnapRevKey(storage, sid, rev)]; !ok {
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
