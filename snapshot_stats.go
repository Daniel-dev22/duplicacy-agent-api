package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

// snapshotStatsStore is the read/write layer over the snapshot_stats SQLite
// table. The table lives in the same events.sqlite file the event-buffer
// uses; we share the *sql.DB to avoid a second Open + WAL file.
type snapshotStatsStore struct {
	db *sql.DB
}

func newSnapshotStatsStore(db *sql.DB) *snapshotStatsStore { return &snapshotStatsStore{db: db} }

// upsertCheckRun writes all rows from a single `check -tabular` run in one
// transaction, sharing a single capturedAt across every row so the chart
// rollup can group by timestamp cleanly.
//
// repoID + storageName + destination identity travel from the job context
// (the agent already knows them when it spawns the check); the parser only
// produces the snapshot-id/revision-keyed numeric rows.
func (s *snapshotStatsStore) upsertCheckRun(ctx context.Context, repoID, storageName, destKey, destLabel string, capturedAt time.Time, poolBytes int64, poolChunks int, rows []*snapshotStatRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO snapshot_stats (
			snapshot_id, revision, repo_id, storage_name,
			destination_key, destination_label,
			files, bytes, bytes_pretty,
			total_chunks, total_bytes, total_bytes_pretty,
			uniq_chunks, uniq_bytes, uniq_bytes_pretty,
			new_chunks, new_bytes, new_bytes_pretty,
			pool_bytes, pool_chunks,
			captured_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(snapshot_id, revision, storage_name) DO UPDATE SET
			repo_id            = excluded.repo_id,
			destination_key    = excluded.destination_key,
			destination_label  = excluded.destination_label,
			files              = excluded.files,
			bytes              = excluded.bytes,
			bytes_pretty       = excluded.bytes_pretty,
			total_chunks       = excluded.total_chunks,
			total_bytes        = excluded.total_bytes,
			total_bytes_pretty = excluded.total_bytes_pretty,
			uniq_chunks        = excluded.uniq_chunks,
			uniq_bytes         = excluded.uniq_bytes,
			uniq_bytes_pretty  = excluded.uniq_bytes_pretty,
			new_chunks         = excluded.new_chunks,
			new_bytes          = excluded.new_bytes,
			new_bytes_pretty   = excluded.new_bytes_pretty,
			pool_bytes         = excluded.pool_bytes,
			pool_chunks        = excluded.pool_chunks,
			captured_at        = excluded.captured_at
	`)
	if err != nil {
		return fmt.Errorf("prepare: %w", err)
	}
	defer stmt.Close()

	for _, r := range rows {
		if _, err := stmt.ExecContext(ctx,
			r.SnapshotID, r.Revision, repoID, storageName,
			destKey, destLabel,
			r.Files, r.Bytes, r.BytesPretty,
			r.TotalChunks, r.TotalBytes, r.TotalBytesPretty,
			r.UniqChunks, r.UniqBytes, r.UniqBytesPretty,
			r.NewChunks, r.NewBytes, r.NewBytesPretty,
			poolBytes, poolChunks,
			capturedAt,
		); err != nil {
			return fmt.Errorf("upsert row %s/r%d: %w", r.SnapshotID, r.Revision, err)
		}
	}
	return tx.Commit()
}

// SnapshotStatPublic is the JSON-serializable per-snapshot row served by
// /repos/:id/snapshot-stats. Field names match the frontend SnapshotStat
// TypeScript interface.
type SnapshotStatPublic struct {
	SnapshotID       string    `json:"snapshot_id"`
	Revision         int       `json:"revision"`
	StorageName      string    `json:"storage_name"`
	DestinationKey   string    `json:"destination_key"`
	DestinationLabel string    `json:"destination_label"`
	Files            int       `json:"files,omitempty"`
	Bytes            int64     `json:"bytes,omitempty"`
	BytesPretty      string    `json:"bytes_pretty,omitempty"`
	TotalChunks      int       `json:"total_chunks,omitempty"`
	TotalBytes       int64     `json:"total_bytes,omitempty"`
	TotalBytesPretty string    `json:"total_bytes_pretty,omitempty"`
	UniqChunks       int       `json:"uniq_chunks,omitempty"`
	UniqBytes        int64     `json:"uniq_bytes,omitempty"`
	UniqBytesPretty  string    `json:"uniq_bytes_pretty,omitempty"`
	NewChunks        int       `json:"new_chunks,omitempty"`
	NewBytes         int64     `json:"new_bytes,omitempty"`
	NewBytesPretty   string    `json:"new_bytes_pretty,omitempty"`
	CapturedAt       time.Time `json:"captured_at"`
}

// listByRepo returns all snapshot-stats rows for a repo, optionally filtered
// to one storage_name. Ordering: revision DESC so the most recent shows first.
func (s *snapshotStatsStore) listByRepo(ctx context.Context, repoID, storageName string) ([]SnapshotStatPublic, error) {
	q := `SELECT snapshot_id, revision, storage_name, destination_key, destination_label,
		files, bytes, bytes_pretty, total_chunks, total_bytes, total_bytes_pretty,
		uniq_chunks, uniq_bytes, uniq_bytes_pretty, new_chunks, new_bytes, new_bytes_pretty,
		captured_at
		FROM snapshot_stats WHERE repo_id = ?`
	args := []any{repoID}
	if storageName != "" {
		q += " AND storage_name = ?"
		args = append(args, storageName)
	}
	q += " ORDER BY revision DESC, storage_name ASC"

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list snapshot stats: %w", err)
	}
	defer rows.Close()

	var out []SnapshotStatPublic
	for rows.Next() {
		var r SnapshotStatPublic
		if err := rows.Scan(
			&r.SnapshotID, &r.Revision, &r.StorageName, &r.DestinationKey, &r.DestinationLabel,
			&r.Files, &r.Bytes, &r.BytesPretty,
			&r.TotalChunks, &r.TotalBytes, &r.TotalBytesPretty,
			&r.UniqChunks, &r.UniqBytes, &r.UniqBytesPretty,
			&r.NewChunks, &r.NewBytes, &r.NewBytesPretty,
			&r.CapturedAt,
		); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// StorageDestination is one entry on the rollup's summary-card row.
//
// Field semantics:
//   - PoolBytes / PoolBytesPretty: actual deduplicated disk usage on the
//     destination (parsed from "Total chunk size is X in N chunks"). This
//     is what the destination is physically storing — the operator-meaningful
//     "how much storage am I using" number. Same across all agents that share
//     a chunk pool.
//   - ReferencedBytes / ReferencedBytesPretty: SUM(total_bytes) across the
//     latest revision of each snapshot in the pool — i.e. "if every snapshot
//     stored its content alone with no sharing". Always >= PoolBytes; the
//     gap is the dedup savings.
//   - UniqBytes / UniqBytesPretty: SUM(uniq_bytes) — the floor of bytes that
//     CANNOT be removed without losing data (chunks unique to one snapshot).
//   - DedupPctPretty: human-readable percent string like "57%", ready to render.
//   - SnapshotCount: distinct snapshot ids in the pool.
type StorageDestination struct {
	Key                   string    `json:"key"`
	Label                 string    `json:"label"`
	PoolBytes             int64     `json:"pool_bytes"`
	PoolBytesPretty       string    `json:"pool_bytes_pretty,omitempty"`
	PoolChunks            int       `json:"pool_chunks,omitempty"`
	ReferencedBytes       int64     `json:"referenced_bytes"`
	ReferencedBytesPretty string    `json:"referenced_bytes_pretty,omitempty"`
	UniqBytes             int64     `json:"uniq_bytes"`
	UniqBytesPretty       string    `json:"uniq_bytes_pretty,omitempty"`
	DedupPct              float64   `json:"dedup_pct"`
	DedupPctPretty        string    `json:"dedup_pct_pretty,omitempty"`
	LastCheckAt           time.Time `json:"last_check_at"`
	SnapshotCount         int       `json:"snapshot_count"`
	RevisionCount         int       `json:"revision_count"`

	// Back-compat: keep CurrentBytes/CurrentBytesPretty for clients that
	// haven't upgraded to read the new fields. Mirrors ReferencedBytes.
	CurrentBytes       int64  `json:"current_bytes"`
	CurrentBytesPretty string `json:"current_bytes_pretty,omitempty"`
}

// StorageSeriesPoint is one (timestamp, total bytes) sample for the chart.
type StorageSeriesPoint struct {
	TS    time.Time `json:"ts"`
	Bytes int64     `json:"bytes"`
}

// StorageSeries is one chart line — one per destination.
type StorageSeries struct {
	Key    string               `json:"key"`
	Label  string               `json:"label"`
	Points []StorageSeriesPoint `json:"points"`
}

// StorageRollup is the response body for fleet/node/repo storage-rollup.
type StorageRollup struct {
	Destinations []StorageDestination `json:"destinations"`
	Series       []StorageSeries      `json:"series"`
}

// RepoDestinationRow is one (repo, destination) tuple — answers "what does
// THIS repo cost on THAT destination?". Sorted by uniq_bytes DESC on the
// frontend so the highest-floor repos surface first ("cheapest to delete
// for the most savings"). Same Repo can appear N times if it backs up to N
// destinations (typical for source repos that copy to local NAS + remote
// NAS + Storj).
type RepoDestinationRow struct {
	RepoID                string    `json:"repo_id"`
	RepoSnapshotID        string    `json:"snapshot_id"` // duplicacy's snapshot id — distinguishes repos within one pool
	Node                  string    `json:"node"`        // bare hostname so the frontend can build links
	DestinationKey        string    `json:"destination_key"`
	DestinationLabel      string    `json:"destination_label"`
	ReferencedBytes       int64     `json:"referenced_bytes"`
	ReferencedBytesPretty string    `json:"referenced_bytes_pretty,omitempty"`
	UniqBytes             int64     `json:"uniq_bytes"`
	UniqBytesPretty       string    `json:"uniq_bytes_pretty,omitempty"`
	SnapshotCount         int       `json:"snapshot_count"`
	LastCheckAt           time.Time `json:"last_check_at"`
}

// rollup computes the destination summary cards + time-series for a repo
// scope (or repo+storage scope when storageName != ""). The node-wide scope
// is just rollup with repoIDs containing every repo on the agent.
//
// repoIDs empty means "all repos on this agent" (node-level).
//
// since bounds the time-series points returned; pass time.Time{} to disable.
func (s *snapshotStatsStore) rollup(ctx context.Context, repoIDs []string, storageName string, since time.Time) (*StorageRollup, error) {
	dest, err := s.queryDestinations(ctx, repoIDs, storageName)
	if err != nil {
		return nil, err
	}
	series, err := s.querySeries(ctx, repoIDs, storageName, since)
	if err != nil {
		return nil, err
	}
	return &StorageRollup{Destinations: dest, Series: series}, nil
}

// queryDestinations returns one summary card per destination. CurrentBytes
// uses the most recent captured_at per destination (so a stale repo not
// re-checked since yesterday doesn't dilute today's number from another
// repo on the same destination — we sum the latest snapshot per
// snapshot_id+storage_name).
func (s *snapshotStatsStore) queryDestinations(ctx context.Context, repoIDs []string, storageName string) ([]StorageDestination, error) {
	// "Current" per snapshot = the highest revision at the latest captured_at.
	// We take the highest revision per snapshot_id+storage_name and SUM its
	// total_bytes / uniq_bytes per destination.
	whereRepo, args := repoIDFilter(repoIDs)
	storageFilter := ""
	if storageName != "" {
		storageFilter = " AND storage_name = ?"
		args = append(args, storageName)
	}

	q := `
WITH latest AS (
    SELECT snapshot_id, storage_name, destination_key, destination_label,
           total_bytes, uniq_bytes, pool_bytes, pool_chunks, captured_at,
           ROW_NUMBER() OVER (
               PARTITION BY snapshot_id, storage_name
               ORDER BY revision DESC, captured_at DESC
           ) AS rn
    FROM snapshot_stats
    WHERE 1=1 ` + whereRepo + storageFilter + `
)
SELECT destination_key, destination_label,
       COALESCE(MAX(pool_bytes),  0)  AS pool_bytes,
       COALESCE(MAX(pool_chunks), 0)  AS pool_chunks,
       COALESCE(SUM(total_bytes), 0)  AS referenced_bytes,
       COALESCE(SUM(uniq_bytes),  0)  AS uniq_bytes,
       MAX(captured_at)               AS last_check_at,
       COUNT(DISTINCT snapshot_id)    AS snapshot_count,
       COUNT(*)                       AS revision_count
FROM latest
WHERE rn = 1
GROUP BY destination_key, destination_label
ORDER BY destination_label`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("dest query: %w", err)
	}
	defer rows.Close()

	var out []StorageDestination
	for rows.Next() {
		var d StorageDestination
		// MAX(captured_at) comes back as a string from modernc.org/sqlite —
		// the aggregate strips the column-type info that would otherwise let
		// the driver auto-decode to time.Time. Scan as string and parse.
		var lastCheckRaw sql.NullString
		if err := rows.Scan(&d.Key, &d.Label, &d.PoolBytes, &d.PoolChunks, &d.ReferencedBytes, &d.UniqBytes, &lastCheckRaw, &d.SnapshotCount, &d.RevisionCount); err != nil {
			return nil, fmt.Errorf("dest scan: %w", err)
		}
		if lastCheckRaw.Valid {
			d.LastCheckAt = parseSQLiteTime(lastCheckRaw.String)
		}
		d.PoolBytesPretty = formatPrettyBytes(d.PoolBytes)
		d.ReferencedBytesPretty = formatPrettyBytes(d.ReferencedBytes)
		d.UniqBytesPretty = formatPrettyBytes(d.UniqBytes)
		// Back-compat mirrors (pre-1.0.70 frontend reads these).
		d.CurrentBytes = d.ReferencedBytes
		d.CurrentBytesPretty = d.ReferencedBytesPretty
		// Dedup ratio: how much of the referenced bytes overlap with at least
		// one other snapshot. (1 - uniq/referenced) * 100.
		if d.ReferencedBytes > 0 {
			d.DedupPct = (1 - float64(d.UniqBytes)/float64(d.ReferencedBytes)) * 100
			if d.DedupPct < 0 {
				d.DedupPct = 0
			}
		}
		d.DedupPctPretty = fmt.Sprintf("%.0f%%", d.DedupPct)
		out = append(out, d)
	}
	return out, rows.Err()
}

// parseSQLiteTime decodes the few formats modernc.org/sqlite returns when
// type metadata is missing (aggregates, JOINs, GROUP BY). Falls back to
// time.Time{} on parse failure — caller should treat a zero value as "no
// last-check timestamp known".
func parseSQLiteTime(s string) time.Time {
	formats := []string{
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05-07:00",
		"2006-01-02 15:04:05",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

// querySeries returns one point per (destination, captured_at) using the
// destination's actual pool_bytes — the deduplicated disk usage observed at
// that check run. MAX (not SUM) because every snapshot row from one check
// run carries the same pool_bytes value (denormalised), so SUM would
// N-times-inflate the chart. Falls back to SUM(total_bytes) for old rows
// where pool_bytes is NULL (pre-1.0.70 data).
func (s *snapshotStatsStore) querySeries(ctx context.Context, repoIDs []string, storageName string, since time.Time) ([]StorageSeries, error) {
	whereRepo, args := repoIDFilter(repoIDs)
	storageFilter := ""
	if storageName != "" {
		storageFilter = " AND storage_name = ?"
		args = append(args, storageName)
	}
	sinceFilter := ""
	if !since.IsZero() {
		sinceFilter = " AND captured_at >= ?"
		args = append(args, since)
	}

	q := `
SELECT destination_key, destination_label, captured_at,
       COALESCE(MAX(pool_bytes), SUM(total_bytes), 0)
FROM snapshot_stats
WHERE 1=1 ` + whereRepo + storageFilter + sinceFilter + `
GROUP BY destination_key, destination_label, captured_at
ORDER BY destination_label, captured_at`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("series query: %w", err)
	}
	defer rows.Close()

	byKey := map[string]*StorageSeries{}
	var order []string
	for rows.Next() {
		var key, label string
		var tsRaw string
		var bytes int64
		// captured_at is the raw column here (not an aggregate) so modernc
		// usually decodes to time.Time directly — but the GROUP BY can drop
		// the type metadata depending on the driver version. Scan as string
		// for stability.
		if err := rows.Scan(&key, &label, &tsRaw, &bytes); err != nil {
			return nil, fmt.Errorf("series scan: %w", err)
		}
		ts := parseSQLiteTime(tsRaw)
		ser, ok := byKey[key]
		if !ok {
			ser = &StorageSeries{Key: key, Label: label}
			byKey[key] = ser
			order = append(order, key)
		}
		ser.Points = append(ser.Points, StorageSeriesPoint{TS: ts, Bytes: bytes})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]StorageSeries, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out, nil
}

// listRepoDestinations returns one row per (snapshot_id, storage_name) tuple,
// aggregating across the latest revision per snapshot. This is the data
// behind /storage's "Repos by usage" table — every source repo contributing
// chunks to a destination shows up as one row per destination it backs up
// to. Same as queryDestinations except the GROUP BY also includes
// snapshot_id, so repos are kept separate rather than collapsed into the
// destination total.
//
// Returned rows are sorted by uniq_bytes DESC so the "biggest standalone
// cost" repos surface first. Node is filled in from a caller-supplied map
// (snapshot_id → node name) since the agent doesn't always know which
// originating node a snapshot came from on its own.
func (s *snapshotStatsStore) listRepoDestinations(ctx context.Context, node string) ([]RepoDestinationRow, error) {
	q := `
WITH latest AS (
    SELECT snapshot_id, storage_name, repo_id, destination_key, destination_label,
           total_bytes, uniq_bytes, captured_at,
           ROW_NUMBER() OVER (
               PARTITION BY snapshot_id, storage_name
               ORDER BY revision DESC, captured_at DESC
           ) AS rn
    FROM snapshot_stats
)
SELECT snapshot_id, repo_id, destination_key, destination_label,
       COALESCE(SUM(total_bytes), 0)  AS referenced_bytes,
       COALESCE(SUM(uniq_bytes),  0)  AS uniq_bytes,
       MAX(captured_at)               AS last_check_at,
       COUNT(*)                       AS revision_count
FROM latest
WHERE rn = 1
GROUP BY snapshot_id, repo_id, destination_key, destination_label
ORDER BY uniq_bytes DESC`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("listRepoDestinations: %w", err)
	}
	defer rows.Close()

	var out []RepoDestinationRow
	for rows.Next() {
		var r RepoDestinationRow
		var lastCheckRaw sql.NullString
		if err := rows.Scan(&r.RepoSnapshotID, &r.RepoID, &r.DestinationKey, &r.DestinationLabel,
			&r.ReferencedBytes, &r.UniqBytes, &lastCheckRaw, &r.SnapshotCount); err != nil {
			return nil, fmt.Errorf("listRepoDestinations scan: %w", err)
		}
		if lastCheckRaw.Valid {
			r.LastCheckAt = parseSQLiteTime(lastCheckRaw.String)
		}
		r.ReferencedBytesPretty = formatPrettyBytes(r.ReferencedBytes)
		r.UniqBytesPretty = formatPrettyBytes(r.UniqBytes)
		r.Node = node
		out = append(out, r)
	}
	return out, rows.Err()
}

// repoIDFilter builds "AND repo_id IN (?, ?, …)" with the corresponding args.
// Empty repoIDs returns "" + nil (no filter — all rows).
func repoIDFilter(repoIDs []string) (string, []any) {
	if len(repoIDs) == 0 {
		return "", nil
	}
	args := make([]any, 0, len(repoIDs))
	placeholders := make([]byte, 0, 2*len(repoIDs))
	for i, id := range repoIDs {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	return " AND repo_id IN (" + string(placeholders) + ")", args
}

// formatPrettyBytes is the inverse of parsePrettyBytes — emits "1.2G" /
// "8.3M" style for the JSON response so the UI can render without re-parsing
// numeric.
func formatPrettyBytes(b int64) string {
	const (
		k = 1 << 10
		m = 1 << 20
		g = 1 << 30
		t = 1 << 40
		p = 1 << 50
	)
	switch {
	case b >= p:
		return fmt.Sprintf("%.1fP", float64(b)/float64(p))
	case b >= t:
		return fmt.Sprintf("%.1fT", float64(b)/float64(t))
	case b >= g:
		return fmt.Sprintf("%.1fG", float64(b)/float64(g))
	case b >= m:
		return fmt.Sprintf("%.1fM", float64(b)/float64(m))
	case b >= k:
		return fmt.Sprintf("%.1fK", float64(b)/float64(k))
	default:
		return fmt.Sprintf("%d", b)
	}
}

// flushSnapshotStats takes the rows the tabular parser collected during a
// check job, derives destination_key/label from the repo's storage URL, and
// UPSERTs everything in one transaction with a shared captured_at. Logged
// at warn on failure — the operator still has the raw job log to diagnose
// from, and the next successful check will UPSERT fresh rows.
func (a *app) flushSnapshotStats(j *Job) {
	rows := j.takeTabularRows()
	if len(rows) == 0 {
		return
	}
	snap := j.snapshot()

	repo, ok := a.repos.get(snap.RepoID)
	if !ok {
		slog.Warn("snapshot stats: repo not found at flush time", "repo_id", snap.RepoID, "job", snap.ID)
		return
	}
	var storageURL string
	for _, s := range repo.Storages {
		if s.Name == snap.StorageName || (snap.StorageName == "" && s.Name == "default") {
			storageURL = s.URL
			break
		}
	}
	if storageURL == "" {
		slog.Warn("snapshot stats: no storage URL for job",
			"repo_id", snap.RepoID, "storage", snap.StorageName, "job", snap.ID)
		return
	}
	destKey, destLabel := DestinationKey(storageURL)
	storageName := snap.StorageName
	if storageName == "" {
		storageName = "default"
	}

	// Pull the destination pool size (actual deduplicated disk usage) the
	// check parser captured from "Total chunk size is X in N chunks". Zero
	// when the line never appeared (very small/empty pools, or duplicacy
	// versions that don't emit it) — still UPSERT, just leaves pool_bytes
	// NULL in the row.
	var poolBytes int64
	var poolChunks int
	if snap.Progress != nil {
		poolBytes = snap.Progress.CheckPoolBytes
		poolChunks = snap.Progress.CheckPoolChunks
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := a.snapshotStats.upsertCheckRun(ctx, snap.RepoID, storageName, destKey, destLabel, time.Now().UTC(), poolBytes, poolChunks, rows); err != nil {
		slog.Warn("snapshot stats upsert failed", "error", err, "job", snap.ID, "rows", len(rows))
		return
	}
	slog.Info("snapshot stats written",
		"job", snap.ID,
		"repo_id", snap.RepoID,
		"storage", storageName,
		"destination", destLabel,
		"rows", len(rows),
	)
}

// handleListSnapshotStats GET /repos/:id/snapshot-stats[?storage=…]
// Returns every per-revision dedup row for a repo (optionally one storage).
// Sourced from snapshot_stats SQLite table; rows are written on every
// successful `check -tabular` run via flushSnapshotStats.
func (a *app) handleListSnapshotStats(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	stats, err := a.snapshotStats.listByRepo(c.Request.Context(), repo.ID, c.Query("storage"))
	if err != nil {
		slog.Warn("list snapshot stats failed", "error", err, "repo", repo.ID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "list snapshot stats: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"stats": stats})
}

// handleRepoStorageRollup GET /repos/:id/storage-rollup[?storage=…&since=…]
// Returns the storage-destinations panel data scoped to one repo.
// since accepts an RFC3339 timestamp or "30d"/"7d"/"24h" relative form;
// omitted → no time bound.
func (a *app) handleRepoStorageRollup(c *gin.Context) {
	repo, ok := a.repos.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "repo not found"})
		return
	}
	since := parseSinceParam(c.Query("since"))
	rollup, err := a.snapshotStats.rollup(c.Request.Context(), []string{repo.ID}, c.Query("storage"), since)
	if err != nil {
		slog.Warn("repo storage rollup failed", "error", err, "repo", repo.ID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rollup: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, rollup)
}

// handleNodeStorageRollup GET /storage-rollup[?since=…]
// Node-wide: aggregates across every repo registered on this agent.
func (a *app) handleNodeStorageRollup(c *gin.Context) {
	since := parseSinceParam(c.Query("since"))
	rollup, err := a.snapshotStats.rollup(c.Request.Context(), nil, "", since)
	if err != nil {
		slog.Warn("node storage rollup failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rollup: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, rollup)
}

// handleStorageReposBreakdown GET /storage-rollup/repos
// Returns one row per (snapshot_id, destination_key) — the data behind the
// "Repos by usage" table on /duplicacy/storage. Sorted by uniq_bytes DESC.
// Node name comes from the agent config so the frontend can render links
// back to the originating node/repo.
func (a *app) handleStorageReposBreakdown(c *gin.Context) {
	rows, err := a.snapshotStats.listRepoDestinations(c.Request.Context(), a.cfg.NodeName)
	if err != nil {
		slog.Warn("storage repos breakdown failed", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "repos breakdown: " + err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"rows": rows})
}

// parseSinceParam accepts an RFC3339 timestamp or a relative form like
// "30d", "7d", "24h", "60m". Empty string → zero time (no bound). Unparseable
// → also zero time (caller treats as "all"; matches the empty-string case so
// a typo doesn't error the whole request).
func parseSinceParam(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t
	}
	// Relative: <N>(d|h|m). "d" isn't a stdlib unit so handle it manually.
	if n := len(s); n >= 2 {
		unit := s[n-1]
		num, err := strconv.Atoi(s[:n-1])
		if err == nil && num > 0 {
			switch unit {
			case 'd':
				return time.Now().UTC().Add(-time.Duration(num) * 24 * time.Hour)
			case 'h':
				return time.Now().UTC().Add(-time.Duration(num) * time.Hour)
			case 'm':
				return time.Now().UTC().Add(-time.Duration(num) * time.Minute)
			}
		}
	}
	return time.Time{}
}
