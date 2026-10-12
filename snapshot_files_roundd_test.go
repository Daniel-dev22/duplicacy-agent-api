package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// perRankSizes seeds one snapshot with revisions 8..1 (rank 1 = rev 8) of the
// given per-rank sizes.
func seedRanks(t *testing.T, c *snapshotFilesCacheStore, snap string, sizes []int) {
	t.Helper()
	for i, n := range sizes {
		if err := c.putGzipped(context.Background(), snap, 8-i, "default", "repoA", make([]byte, n), 1); err != nil {
			t.Fatal(err)
		}
	}
}

// Older revisions larger than the newest: the [5 1 1 1 1 1] shape from review.
func TestWarmerAndEvictorSettleWhenOlderRevisionsAreLarger(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 6500, 5)
	a := &app{filesCache: c}
	size := func(rev int) int {
		if rev == 8 {
			return 1000
		}
		return 1500
	}
	var live []Snapshot
	for rev := 1; rev <= 8; rev++ {
		live = append(live, Snapshot{SnapshotID: "snapA", Revision: rev})
	}
	var per []int
	for sweep := 0; sweep < 6; sweep++ {
		n := 0
		ranks := map[string]int{}
		for _, rev := range newestRevisionsPerSnapshot(live, 5) {
			want, err := a.warmWanted(ctx, ranks, rev, "repoA", "default")
			if err != nil {
				t.Fatal(err)
			}
			if !want {
				continue
			}
			n++
			if err := c.putGzipped(ctx, "snapA", rev.Revision, "default", "repoA", make([]byte, size(rev.Revision)), 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.evictBySize(ctx, nil); err != nil {
			t.Fatal(err)
		}
		per = append(per, n)
	}
	for i := 3; i < len(per); i++ {
		if per[i] != 0 {
			t.Fatalf("never settles: listings per sweep %v", per)
		}
	}
	_, gz, _ := c.stats(ctx)
	if gz > 6500 {
		t.Fatalf("cache %d over the cap", gz)
	}
}

func TestEvictorProtectsExactlyThePinnedRanks(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	seedRanks(t, c, "snapA", []int{1000, 1500, 1500, 1500, 1500, 1500})
	c.maxBytes = 5600 // prefix sums: 1000 2500 4000 5500 7000 -> pinN 4
	if n := c.effectivePinN(ctx); n != 4 {
		t.Fatalf("pinN = %d, want 4", n)
	}
	if err := c.evictBySize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for rev := 8; rev >= 5; rev-- { // ranks 1..4
		if ok, _ := c.has(ctx, "snapA", rev, "default"); !ok {
			t.Fatalf("pinned rev%d evicted", rev)
		}
	}
	for rev := 4; rev >= 3; rev-- {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); ok {
			t.Fatalf("unpinned rev%d kept over the cap", rev)
		}
	}
}

func TestSweepOnASingleRepoStillRespectsTheCap(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 2)
	row := make([]byte, 1000)
	for rev := 4; rev >= 1; rev-- { // existed before the sweep; rev 1 (rank 4) has the MAX rowid
		c.putGzipped(ctx, "old", rev, "default", "repoA", row, 1)
	}
	c.maxBytes = 3000
	c.sweepBegin(ctx, []string{"repoA"})
	defer c.sweepEnd()
	for rev := 1; rev <= 20; rev++ {
		if err := c.putGzipped(ctx, "new", rev, "default", "repoA", row, 1); err != nil {
			t.Fatal(err)
		}
	}
	// Rows the pending repo had when the sweep began are untouchable...
	for rev := 1; rev <= 4; rev++ {
		if ok, _ := c.has(ctx, "old", rev, "default"); !ok {
			t.Fatalf("pre-sweep row old rev%d evicted mid-sweep", rev)
		}
	}
	// ...but what the sweep itself warms is bounded by the cap (floor: newest 2).
	var n int
	for rev := 1; rev <= 20; rev++ {
		if ok, _ := c.has(ctx, "new", rev, "default"); ok {
			n++
		}
	}
	if n > 2 {
		t.Fatalf("sweep grew the cache past the cap: %d new rows kept", n)
	}
}

func TestMarkerProblemsNeverSilenceWarmerOrReconcile(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	a := &app{filesCache: c}
	putRaw(c, ctx, "snapA", 9, "default", "repoA", []byte("pruned\n"))
	if _, err := c.db.Exec(`DROP TABLE snapshot_files_uncacheable`); err != nil {
		t.Fatal(err)
	}
	want, err := a.warmWanted(ctx, map[string]int{}, Snapshot{SnapshotID: "snapA", Revision: 1}, "repoA", "default")
	if err != nil || !want {
		t.Fatalf("warmer silenced by a missing marker table: want=%v err=%v", want, err)
	}
	if err := c.markUncacheable(ctx, "repoA", "default", "snapA", 2, uncacheableRowCap, 5, 0); err != nil {
		t.Fatalf("marker table must be recreated on demand: %v", err)
	}
	if r, _ := c.uncacheable(ctx, "repoA", "default", "snapA", 2); r != uncacheableRowCap {
		t.Fatal("marker not stored after recreate")
	}
	c.db.Exec(`DROP TABLE snapshot_files_uncacheable`)
	st := newSweepState()
	st.known["repoA"] = struct{}{}
	st.swept[repoStorage{"repoA", "default"}] = struct{}{}
	st.sweptStorage["default"] = struct{}{}
	n, err := c.reconcileAgainstKeep(ctx, st)
	if err != nil || n != 1 {
		t.Fatalf("row reconcile skipped because of the marker table: n=%d err=%v", n, err)
	}
}

func TestBrokenMarkerTableNeverSkipsRowReconcile(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	putRaw(c, ctx, "snapA", 9, "default", "repoA", []byte("pruned\n"))
	c.db.Exec(`DROP TABLE snapshot_files_uncacheable`)
	c.db.Exec(`CREATE TABLE snapshot_files_uncacheable (x INTEGER)`) // wrong shape: scans fail, not "no such table"
	st := newSweepState()
	st.known["repoA"] = struct{}{}
	st.swept[repoStorage{"repoA", "default"}] = struct{}{}
	st.sweptStorage["default"] = struct{}{}
	n, err := c.reconcileAgainstKeep(ctx, st)
	if err != nil || n != 1 {
		t.Fatalf("a broken marker table skipped the row reconcile: n=%d err=%v", n, err)
	}
}

func TestRowCapMarkerIsReevaluatedWhenTheCapChanges(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	a := &app{filesCache: c, cfg: Config{ListCacheRowMaxBytes: 1 << 20}}
	c.markUncacheable(ctx, "repoA", "default", "snapA", 3, uncacheableRowCap, 5<<20, 0)
	c.markUncacheable(ctx, "repoA", "default", "snapA", 4, uncacheableCeiling, 0, listSpoolFileMax)
	rev := func(n int) Snapshot { return Snapshot{SnapshotID: "snapA", Revision: n} }
	if w, _ := a.warmWanted(ctx, map[string]int{}, rev(3), "repoA", "default"); w {
		t.Fatal("marker still valid at the old cap")
	}
	a.cfg.ListCacheRowMaxBytes = 8 << 20 // operator raised the cap
	if w, _ := a.warmWanted(ctx, map[string]int{}, rev(3), "repoA", "default"); !w {
		t.Fatal("raised cap must re-admit a row_cap marker")
	}
	if w, _ := a.warmWanted(ctx, map[string]int{}, rev(4), "repoA", "default"); w {
		t.Fatal("a spool-ceiling marker is not about the row cap")
	}
}

func TestMarkerReconcileOnlyJudgesReposThatListedTheStorage(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	c.markUncacheable(ctx, "repoX", "s1", "snapX", 1, uncacheableRowCap, 9, 0)
	st := newSweepState()
	st.known["repoX"], st.known["repoY"] = struct{}{}, struct{}{}
	st.swept[repoStorage{"repoY", "s1"}] = struct{}{} // only Y listed s1
	st.sweptStorage["s1"] = struct{}{}
	if _, err := c.reconcileAgainstKeep(ctx, st); err != nil {
		t.Fatal(err)
	}
	if r, _ := c.uncacheable(ctx, "repoX", "s1", "snapX", 1); r == "" {
		t.Fatal("repo X never listed s1: its marker must survive Y's listing")
	}
}

func TestTailRingIgnoresEmptyWrites(t *testing.T) {
	var r tailRing
	r.write(nil)
	r.write([]byte{})
	r.write([]byte("abc"))
	if r.String() != "abc" {
		t.Fatalf("empty write corrupted the ring: %d bytes", len(r.String()))
	}
}

func TestChildRSSUnitsAreKiB(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	serve(a, repo, 1, "", []string{"LISTSZ=100"}, nil)
	kb := childRSS.snapshot()["list-files"]
	// Linux seeds a child's ru_maxrss with the PARENT's RSS at fork, so the
	// ceiling is the parent's own high-water mark, not a few MB.
	var hwm int64
	st, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(st), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "VmHWM:" {
			hwm, _ = strconv.ParseInt(f[1], 10, 64)
		}
	}
	if kb < 100 || kb > 2*hwm+100_000 {
		t.Fatalf("peak rss %d KiB is outside a plausible range (parent VmHWM %d KiB): units wrong?", kb, hwm)
	}
}

func TestGOMEMLIMITStrippedWithoutEnvAdds(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	cmd := cliInvocation{}.command(context.Background(), "x")
	if cmd.Env == nil {
		t.Fatal("child env must be set explicitly")
	}
	for _, kv := range cmd.Env {
		if strings.HasPrefix(kv, "GOMEMLIMIT=") {
			t.Fatal("GOMEMLIMIT leaked into a child that has no EnvAdds")
		}
	}
}

func TestWarmerWritesTheCeilingMarker(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listSpoolFileMax, int64(2<<10))
	setVar(t, &listSpoolMemMax, int64(1<<10))
	a, repo := newListTestApp(t, bin, 1, time.Second)
	err := a.warmOneListing(context.Background(), repo, "default", "", "snapW", 6, []string{"LISTSEQ=300000"})
	if !errors.Is(err, errSpoolCeiling) {
		t.Fatalf("err = %v", err)
	}
	if r, _ := a.filesCache.uncacheable(context.Background(), "repoA", "default", "snapW", 6); r != uncacheableCeiling {
		t.Fatalf("warmer did not record the ceiling: %q", r)
	}
}

func TestRefusedHitDoesNotLoadTheRow(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, 100*time.Millisecond)
	a.hitGate = newListGate(1, 100*time.Millisecond)
	if w := serve(a, repo, 1, "", []string{"LISTSZ=4096"}, nil); w.status != 200 {
		t.Fatal("prime")
	}
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	setLastAccess(t, a.filesCache, "snapA", 1, "default", old)
	stalled := newBlockRW()
	done := make(chan struct{})
	go func() { serveTo(a, repo, 1, stalled, nil); close(done) }()
	<-stalled.started
	// open() touches last_access; the stalled hit already did, so reset it.
	setLastAccess(t, a.filesCache, "snapA", 1, "default", old)
	if w := serve(a, repo, 1, "", nil, nil); w.status != http.StatusServiceUnavailable {
		t.Fatalf("status %d", w.status)
	}
	var la time.Time
	if err := a.filesCache.db.QueryRow(`SELECT last_access FROM snapshot_files_cache WHERE snapshot_id='snapA'`).Scan(&la); err != nil {
		t.Fatal(err)
	}
	if !la.Equal(old) {
		t.Fatal("a hit refused by the hit gate still loaded the row (open ran before the gate)")
	}
	close(stalled.unblock)
	<-done
}

func TestRowIsCachedBeforeTheClientWriteStarts(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	w := newHashRW()
	var cachedAtFirstWrite, seen bool
	w.onWrite = func() {
		if !seen {
			seen = true
			cachedAtFirstWrite, _ = a.filesCache.has(context.Background(), "snapA", 4, "default")
		}
	}
	serveTo(a, repo, 4, w, []string{"LISTSZ=3000"})
	if !seen || !cachedAtFirstWrite {
		t.Fatal("the row must be cached before the response write starts (a queued duplicate would re-run)")
	}
}

// A duplicate open that finds the row after queueing must not hold the listing
// slot while it streams.
func TestQueuedDuplicateHitDoesNotHoldTheListingSlot(t *testing.T) {
	bin := writeFakeList(t)
	log := t.TempDir() + "/calls.log"
	a, repo := newListTestApp(t, bin, 1, 3*time.Second)
	env := []string{"LISTSZ=4096", "LISTSLEEP=0.5", "LISTLOG=" + log}
	first := make(chan *hashRW, 1)
	go func() { first <- serve(a, repo, 3, "", env, nil) }()
	for i := 0; i < 300; i++ {
		if b, _ := os.ReadFile(log); len(b) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stalled := newBlockRW()
	done := make(chan struct{})
	go func() { serveTo(a, repo, 3, stalled, env); close(done) }() // queues, then hits the cache
	select {
	case <-stalled.started:
	case <-time.After(10 * time.Second):
		t.Fatal("duplicate never reached its write")
	}
	t0 := time.Now()
	w := serve(a, repo, 9, "", []string{"LISTSZ=100"}, nil) // unrelated miss
	if w.status != 200 || time.Since(t0) > 2*time.Second {
		t.Fatalf("unrelated miss blocked behind a stalled duplicate hit: status %d after %v", w.status, time.Since(t0))
	}
	close(stalled.unblock)
	<-done
	<-first
}

// --- write deadline (real server: the deadline only exists on a real conn) ---

func newListServer(t *testing.T, a *app, repo *Repo) *httptest.Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/files", func(c *gin.Context) {
		a.serveSnapshotFiles(c, repo, 3, func() ([]string, map[string]string, func(), error) {
			return nil, map[string]string{}, func() {}, errors.New("must be a cache hit")
		})
	})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func seedBigHit(t *testing.T, a *app, rawBytes int) {
	t.Helper()
	raw := bytes.Repeat([]byte("0123456789abcdef /some/path/in/the/listing\n"), rawBytes/43)
	if err := putRaw(a.filesCache, context.Background(), "snapA", 3, "default", "repoA", raw); err != nil {
		t.Fatal(err)
	}
}

func TestWriteDeadlineSlidesWithClientProgress(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listWriteTimeout, 500*time.Millisecond)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	seedBigHit(t, a, 24<<20)
	srv := newListServer(t, a, repo)

	resp, err := http.Get(srv.URL + "/files")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 512<<10)
	var got int64
	t0 := time.Now()
	for {
		n, err := resp.Body.Read(buf)
		got += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("a client making steady progress was cut off after %d of %d bytes: %v", got, resp.ContentLength, err)
		}
		time.Sleep(40 * time.Millisecond)
	}
	if got != resp.ContentLength {
		t.Fatalf("short read %d of %d", got, resp.ContentLength)
	}
	if time.Since(t0) < 2*listWriteTimeout {
		t.Fatalf("test did not outlast the deadline (%v): it proves nothing", time.Since(t0))
	}
}

func TestStalledClientIsCutOffAndFreesItsHitSlot(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listWriteTimeout, 400*time.Millisecond)
	a, repo := newListTestApp(t, bin, 1, 100*time.Millisecond)
	a.hitGate = newListGate(1, 100*time.Millisecond)
	seedBigHit(t, a, 48<<20) // far beyond the socket buffers
	srv := newListServer(t, a, repo)

	resp, err := http.Get(srv.URL + "/files")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	time.Sleep(1500 * time.Millisecond) // the client stops reading
	rel, aerr := a.hitGate.acquire(context.Background(), true)
	if aerr != nil {
		t.Fatalf("the stalled client still holds the hit slot after 1.5s: %v", aerr)
	}
	rel()
}

func TestCapEvictedMarkerIsClearedWhenThePinDepthGrows(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	a := &app{filesCache: c}
	for rev := 1; rev <= 5; rev++ {
		putRaw(c, ctx, "snapA", rev, "default", "repoA", make([]byte, 1000))
	}
	// Re-write as exact-size gz rows so the arithmetic below is exact.
	c.db.Exec(`UPDATE snapshot_files_cache SET gz_bytes = 1000`)
	if err := c.putGzipped(ctx, "snapB", 1, "default", "repoB", make([]byte, 2000), 1); err != nil {
		t.Fatal(err)
	}
	c.db.Exec(`UPDATE snapshot_files_cache SET gz_bytes = 2000 WHERE snapshot_id = 'snapB'`)
	c.maxBytes = 6000
	if err := c.evictBySize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.has(ctx, "snapA", 1, "default"); ok {
		t.Fatal("setup: rank-5 revision should have been evicted under pinN=4")
	}
	want := func() bool {
		w, _ := a.warmWanted(ctx, map[string]int{"snapA": 4}, Snapshot{SnapshotID: "snapA", Revision: 1}, "repoA", "default")
		return w
	}
	if want() {
		t.Fatal("marker must hold while the pressure that wrote it persists")
	}
	// snapB is pruned: the pin depth recovers, so the marked revision is a hole.
	c.db.Exec(`DELETE FROM snapshot_files_cache WHERE snapshot_id = 'snapB'`)
	if !want() {
		t.Fatal("cap_evicted marker outlived the pressure: permanent hole in the warm window")
	}
}

func TestMarkerTableMigratesFromOldSchema(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	c.db.Exec(`DROP TABLE snapshot_files_uncacheable`)
	if _, err := c.db.Exec(`CREATE TABLE snapshot_files_uncacheable (
		repo_id TEXT NOT NULL, storage_name TEXT NOT NULL, snapshot_id TEXT NOT NULL, revision INTEGER NOT NULL,
		reason TEXT NOT NULL, gz_bytes INTEGER NOT NULL, marked_at TIMESTAMP NOT NULL,
		PRIMARY KEY (repo_id, storage_name, snapshot_id, revision))`); err != nil {
		t.Fatal(err)
	}
	c.db.Exec(`INSERT INTO snapshot_files_uncacheable VALUES ('repoA','default','snapA',1,'row_cap',77,'2026-01-01 00:00:00')`)
	for i := 0; i < 2; i++ { // idempotent
		if err := c.ensureSchema(ctx); err != nil {
			t.Fatalf("migration run %d: %v", i, err)
		}
	}
	m, err := c.uncacheableInfo(ctx, "repoA", "default", "snapA", 1)
	if err != nil || m.reason != uncacheableRowCap || m.gzBytes != 77 {
		t.Fatalf("old row not readable after migration: %+v err=%v", m, err)
	}
	c.markCapEvicted(ctx, []evictedRow{{"repoA", "default", "snapA", 2, 5}})
	m, err = c.uncacheableInfo(ctx, "repoA", "default", "snapA", 2)
	if err != nil || m.reason != uncacheableCapEvicted {
		t.Fatalf("new-column marker not stored: %+v err=%v", m, err)
	}
}

func TestCapEvictedMarkerIsReadmittedWhenTheCapChanges(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 3000, 5)
	a := &app{filesCache: c}
	c.putGzipped(ctx, "other", 1, "default", "repoB", make([]byte, 500), 1)
	c.markCapEvicted(ctx, []evictedRow{{"repoA", "default", "snapA", 3, 3000}})
	want := func() bool {
		w, _ := a.warmWanted(ctx, map[string]int{}, Snapshot{SnapshotID: "snapA", Revision: 3}, "repoA", "default")
		return w
	}
	if want() {
		t.Fatal("same cap, no room: must stay marked")
	}
	c.maxBytes = 9000
	if !want() {
		t.Fatal("a changed cap must re-admit a cap_evicted marker")
	}
}

func TestSpoolCeilingMarkerIsVoidWhenTheCeilingChanges(t *testing.T) {
	bin := writeFakeList(t)
	ctx := context.Background()
	a, repo := newListTestApp(t, bin, 1, time.Second)
	a.filesCache.markUncacheable(ctx, "repoA", "default", "snapA", 8, uncacheableCeiling, 0, listSpoolFileMax)
	w := func() bool {
		ok, _ := a.warmWanted(ctx, map[string]int{}, Snapshot{SnapshotID: "snapA", Revision: 8}, "repoA", "default")
		return ok
	}
	if w() {
		t.Fatal("marker valid at the same ceiling")
	}
	if got := serve(a, repo, 8, "", []string{"LISTSZ=500"}, nil); got.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("handler should short-circuit 413, got %d", got.status)
	}
	setVar(t, &listSpoolFileMax, listSpoolFileMax*2) // a later release raised the ceiling
	if !w() {
		t.Fatal("warmer must retry once the ceiling was raised")
	}
	if got := serve(a, repo, 8, "", []string{"LISTSZ=500"}, nil); got.status != 200 {
		t.Fatalf("handler must serve again after the ceiling was raised, got %d", got.status)
	}
}

func TestPolicyBoundaries(t *testing.T) {
	// a prefix sum exactly equal to the cap fits
	if got := pinPolicy([]int64{300, 300, 300, 300, 300}, 5, 1500); got != 5 {
		t.Fatalf("prefix == cap must fit: %d", got)
	}
	// a row_cap marker whose gz equals the (raised) cap is admitted: the cache
	// rule is "gz > cap is uncacheable"
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 5)
	a := &app{filesCache: c, cfg: Config{ListCacheRowMaxBytes: 1 << 20}}
	c.markUncacheable(ctx, "repoA", "default", "snapA", 3, uncacheableRowCap, 1<<20, 0)
	if w, _ := a.warmWanted(ctx, map[string]int{}, Snapshot{SnapshotID: "snapA", Revision: 3}, "repoA", "default"); !w {
		t.Fatal("gz exactly equal to the cap is cacheable and must be re-admitted")
	}
}
