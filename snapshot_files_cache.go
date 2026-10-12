package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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

	// pendingRepos are repos a running warm sweep has not finished with: until
	// a repo's listing succeeds its outcome is unknown, so size eviction (which
	// every put triggers) must not touch the rows it had when the sweep began
	// (rowid <= pendingMaxRowid). Rows added during the sweep stay evictable, so
	// a sweep cannot grow the cache past the cap by the amount it warms.
	pendingMu       sync.Mutex
	pendingRepos    map[string]struct{}
	pendingMaxRowid int64 // rows at or below this existed when the sweep began
}

func (s *snapshotFilesCacheStore) sweepBegin(ctx context.Context, repoIDs []string) {
	if s == nil {
		return
	}
	var maxRowid int64
	if s.db != nil {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid), 0) FROM snapshot_files_cache`).Scan(&maxRowid); err != nil {
			slog.Warn("list-files sweep: max rowid query failed, no rows exempt", "error", err)
		}
	}
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	s.pendingMaxRowid = maxRowid
	s.pendingRepos = make(map[string]struct{}, len(repoIDs))
	for _, id := range repoIDs {
		s.pendingRepos[id] = struct{}{}
	}
}

func (s *snapshotFilesCacheStore) sweepRepoDone(repoID string) {
	if s == nil {
		return
	}
	s.pendingMu.Lock()
	delete(s.pendingRepos, repoID)
	s.pendingMu.Unlock()
}

func (s *snapshotFilesCacheStore) sweepEnd() {
	if s == nil {
		return
	}
	s.pendingMu.Lock()
	s.pendingRepos = nil
	s.pendingMu.Unlock()
}

// pendingSnapshot returns the sweep's pending repos and the rowid fence.
func (s *snapshotFilesCacheStore) pendingSnapshot() (map[string]struct{}, int64) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if len(s.pendingRepos) == 0 {
		return nil, 0
	}
	out := make(map[string]struct{}, len(s.pendingRepos))
	for k := range s.pendingRepos {
		out[k] = struct{}{}
	}
	return out, s.pendingMaxRowid
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
			cap_bytes     INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (repo_id, storage_name, snapshot_id, revision)
		)`)
	return err
}

const (
	uncacheableRowCap  = "row_cap"
	uncacheableCeiling = "spool_ceiling"
	// uncacheableCapEvicted: the size cap evicted this revision from the warm
	// window; re-listing it would only be evicted again, until the cap changes.
	uncacheableCapEvicted = "cap_evicted"
)

func (s *snapshotFilesCacheStore) markUncacheable(ctx context.Context, repoID, storage, snapshotID string, rev int, reason string, gzBytes int64) error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.retryMissingTable(ctx, func() error {
		_, e := s.db.ExecContext(ctx, `
			INSERT OR REPLACE INTO snapshot_files_uncacheable
				(repo_id, storage_name, snapshot_id, revision, reason, gz_bytes, marked_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			repoID, storage, snapshotID, rev, reason, gzBytes, time.Now().UTC())
		return e
	})
	if err != nil {
		return fmt.Errorf("mark uncacheable: %w", err)
	}
	return nil
}

// uncacheableMarker is a stored negative marker.
type uncacheableMarker struct {
	reason   string
	gzBytes  int64
	capBytes int64 // cache cap when a cap_evicted marker was written
}

// uncacheable reports the marker reason for a revision ("" when unmarked).
func (s *snapshotFilesCacheStore) uncacheable(ctx context.Context, repoID, storage, snapshotID string, rev int) (string, error) {
	m, err := s.uncacheableInfo(ctx, repoID, storage, snapshotID, rev)
	return m.reason, err
}

func (s *snapshotFilesCacheStore) uncacheableInfo(ctx context.Context, repoID, storage, snapshotID string, rev int) (uncacheableMarker, error) {
	var m uncacheableMarker
	if s == nil || s.db == nil {
		return m, nil
	}
	q := func() error {
		return s.db.QueryRowContext(ctx, `
			SELECT reason, gz_bytes, cap_bytes FROM snapshot_files_uncacheable
			 WHERE repo_id = ? AND storage_name = ? AND snapshot_id = ? AND revision = ?`,
			repoID, storage, snapshotID, rev).Scan(&m.reason, &m.gzBytes, &m.capBytes)
	}
	err := s.retryMissingTable(ctx, q)
	if errors.Is(err, sql.ErrNoRows) {
		return uncacheableMarker{}, nil
	}
	if err != nil {
		return uncacheableMarker{}, fmt.Errorf("uncacheable lookup: %w", err)
	}
	return m, nil
}

// markCapEvicted records rows the size cap just evicted from the warm window.
func (s *snapshotFilesCacheStore) markCapEvicted(ctx context.Context, items []evictedRow) {
	for _, it := range items {
		err := s.retryMissingTable(ctx, func() error {
			_, e := s.db.ExecContext(ctx, `
				INSERT OR REPLACE INTO snapshot_files_uncacheable
					(repo_id, storage_name, snapshot_id, revision, reason, gz_bytes, marked_at, cap_bytes)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
				it.repo, it.storage, it.snapshot, it.rev, uncacheableCapEvicted, it.gz, time.Now().UTC(), s.maxBytes)
			return e
		})
		if err != nil {
			slog.Warn("list-files: recording cap eviction failed", "error", err)
			return
		}
	}
}

type evictedRow struct {
	repo, storage, snapshot string
	rev                     int
	gz                      int64
}

// retryMissingTable runs fn; if the marker table is missing it is created on
// demand and fn runs once more, so a lost table never silently disables the
// warmer or the reconcile.
func (s *snapshotFilesCacheStore) retryMissingTable(ctx context.Context, fn func() error) error {
	err := fn()
	if err != nil && strings.Contains(err.Error(), "no such table") {
		slog.Warn("list-files marker table missing, recreating", "error", err)
		if serr := s.ensureSchema(ctx); serr != nil {
			return serr
		}
		return fn()
	}
	return err
}

// pinPolicy is the ONE rule for how many newest revisions per (snapshot,
// storage) are warmed and protected from eviction: the largest k (not below
// maxPinRelaxedKeep, not above warmN) such that the rows of rank <= k, summed
// over every snapshot, fit the cap. perRank[i] is the cached gz bytes of rank
// i+1. The warmer warms ranks below k and the evictor protects ranks <= k, so
// they never disagree; evicting deeper ranks leaves every prefix sum unchanged,
// so the answer cannot flap.
func pinPolicy(perRank []int64, warmN int, maxBytes int64) int {
	if maxBytes <= 0 {
		return warmN
	}
	floor := min(maxPinRelaxedKeep, warmN)
	best := floor
	var run int64
	for k := 1; k <= warmN; k++ {
		if k <= len(perRank) {
			run += perRank[k-1]
		}
		if k >= floor && run <= maxBytes {
			best = k
		}
	}
	return best
}

// effectivePinN applies pinPolicy to the current cache contents.
func (s *snapshotFilesCacheStore) effectivePinN(ctx context.Context) int {
	if s == nil || s.db == nil || s.maxBytes <= 0 || s.warmKeepN <= maxPinRelaxedKeep {
		return s.warmN()
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT rn, COALESCE(SUM(gz_bytes), 0) FROM (
			SELECT gz_bytes, ROW_NUMBER() OVER (
				PARTITION BY snapshot_id, storage_name ORDER BY revision DESC) AS rn
			FROM snapshot_files_cache) WHERE rn <= ? GROUP BY rn ORDER BY rn`, s.warmKeepN)
	if err != nil {
		slog.Warn("list-files pin policy query failed, using configured N", "error", err)
		return s.warmKeepN
	}
	defer rows.Close()
	perRank := make([]int64, s.warmKeepN)
	for rows.Next() {
		var rn int
		var b int64
		if err := rows.Scan(&rn, &b); err != nil || rn < 1 || rn > len(perRank) {
			slog.Warn("list-files pin policy row failed, using configured N", "error", err)
			return s.warmKeepN
		}
		perRank[rn-1] = b
	}
	if err := rows.Err(); err != nil {
		return s.warmKeepN
	}
	return pinPolicy(perRank, s.warmKeepN, s.maxBytes)
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
// pinPolicy-many per (snapshot_id, storage_name) (that prefix fits the cap by
// construction, or is the maxPinRelaxedKeep floor). Rows of exemptRepos (a repo whose listing failed this sweep) are
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
	pending, fence := s.pendingSnapshot()
	pinN := s.effectivePinN(ctx)

	rows, err := s.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT rowid AS rid, gz_bytes, last_access, repo_id, storage_name, snapshot_id, revision,
			       ROW_NUMBER() OVER (
			           PARTITION BY snapshot_id, storage_name
			           ORDER BY revision DESC
			       ) AS rn
			FROM snapshot_files_cache
		)
		SELECT rid, gz_bytes, repo_id, storage_name, snapshot_id, revision, rn FROM ranked
		 WHERE rn > ? ORDER BY rn DESC, last_access ASC`,
		pinN)
	if err != nil {
		return fmt.Errorf("cache evict scan: %w", err)
	}
	defer rows.Close()

	var victims []int64
	var evicted []evictedRow
	var freed int64
	for rows.Next() {
		var rid, gzBytes int64
		var repo, storage, snapshot string
		var rev, rn int
		if err := rows.Scan(&rid, &gzBytes, &repo, &storage, &snapshot, &rev, &rn); err != nil {
			return fmt.Errorf("cache evict row: %w", err)
		}
		if _, ok := exemptRepos[repo]; ok {
			continue
		}
		if _, ok := pending[repo]; ok && rid <= fence {
			continue
		}
		victims = append(victims, rid)
		if rn <= s.warmKeepN { // inside the warm window: the warmer would re-list it
			evicted = append(evicted, evictedRow{repo, storage, snapshot, rev, gzBytes})
		}
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
	if pinN < s.warmKeepN {
		slog.Warn("list-files cache: pinned set exceeds cap, pin relaxed",
			"max_bytes", s.maxBytes, "total_bytes", total, "configured_n", s.warmKeepN, "kept_per_snapshot", pinN)
	}
	if err := s.deleteByRowID(ctx, victims); err != nil {
		return err
	}
	s.markCapEvicted(ctx, evicted)
	return nil
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
		if strings.Contains(err.Error(), "no such table") {
			return s.ensureSchema(ctx) // nothing to reconcile yet
		}
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
	// A marker problem must never skip the row reconcile.
	merr := s.reconcileMarkers(ctx, st)
	if merr != nil {
		slog.Warn("list-files marker reconcile failed", "error", merr)
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
