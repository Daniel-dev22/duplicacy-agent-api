package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"go/ast"
	"go/parser"
	"go/token"
	"hash"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// fakeListScript stands in for the duplicacy binary. It interleaves stderr with
// stdout (so a split pipe would reorder bytes) and is driven through env:
//
//	LISTSZ    bytes of repeated listing text to emit (default 1024)
//	LISTSEQ   emit `seq 1 N` instead
//	LISTRAND  emit ~N bytes of deterministic pseudo-random digits (poorly compressible)
//	LISTIGNOREPIPE survive a closed output pipe (so only a kill ends the run)
//	LISTFD    print which pipes stdout and stderr are
//	LISTLOG   append "<files> <id> <storage>" per invocation
//	LISTREVS  discovery `list` prints revisions 1..N for -id (FAILSTORAGE fails one storage)
//	LISTSLEEP seconds to sleep after the first output
//	LISTEXIT  exit status (default 0)
const fakeListScript = `#!/bin/sh
files=0; id=""; st=""; prev=""
for a in "$@"; do
  [ "$a" = "-files" ] && files=1
  [ "$prev" = "-id" ] && id="$a"
  [ "$prev" = "-storage" ] && st="$a"
  prev="$a"
done
[ -n "$LISTLOG" ] && echo "$files $id $st" >> "$LISTLOG"
if [ "$files" = 0 ] && [ -n "$LISTREVS" ]; then
  if [ -n "$FAILSTORAGE" ] && [ "$st" = "$FAILSTORAGE" ]; then echo "storage unreachable" >&2; exit 1; fi
  i=1
  while [ "$i" -le "$LISTREVS" ]; do echo "Snapshot ${id:-snapX} revision $i created at 2026-06-02 12:00 -hash"; i=$((i+1)); done
  exit 0
fi
[ -n "$LISTIGNOREPIPE" ] && trap '' PIPE
[ -n "$LISTFD" ] && echo "fd1=$(readlink /proc/$$/fd/1) fd2=$(readlink /proc/$$/fd/2)"
echo "first-line-on-stderr" >&2
if [ -n "$LISTRAND" ]; then
  awk 'BEGIN{srand(42); n=int(ENVIRON["LISTRAND"]/10); for(i=0;i<n;i++) printf "%d\n", rand()*999999999}'
elif [ -n "$LISTSEQ" ]; then
  seq 1 "$LISTSEQ"
else
  yes "1234 2026-06-02 12:00:00 0123456789abcdef /some/path/to/a/file/in/the/revision" | head -c "${LISTSZ:-1024}"
fi
echo "mid-line-on-stderr" >&2
if [ -n "$LISTSLEEP" ]; then sleep "$LISTSLEEP"; fi
echo "last-line-on-stderr" >&2
exit "${LISTEXIT:-0}"
`

func writeFakeList(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fake-duplicacy")
	if err := os.WriteFile(p, []byte(fakeListScript), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

// hashRW is a ResponseWriter that retains nothing: SHA-256 and a byte count.
type hashRW struct {
	h       hash.Hash
	hdr     http.Header
	status  int
	n       int64
	body    bytes.Buffer // kept for non-200 (small JSON errors), or when keep is set
	keep    bool
	onWrite func()
}

func newHashRW() *hashRW { return &hashRW{h: sha256.New(), hdr: http.Header{}} }

func (w *hashRW) Header() http.Header { return w.hdr }
func (w *hashRW) WriteHeader(s int)   { w.status = s }
func (w *hashRW) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	w.n += int64(len(p))
	if w.onWrite != nil {
		w.onWrite()
	}
	if w.status == 200 {
		w.h.Write(p)
		if w.keep {
			w.body.Write(p)
		}
	} else {
		w.body.Write(p)
	}
	return len(p), nil
}
func (w *hashRW) sum() string { return hex.EncodeToString(w.h.Sum(nil)) }

// referenceSum runs the fake the way CombinedOutput would (one shared writer)
// and hashes the result without retaining it.
func referenceSum(t *testing.T, bin string, env ...string) (string, int64) {
	t.Helper()
	cmd := exec.Command(bin, "list", "-files")
	cmd.Env = append(os.Environ(), env...)
	h := sha256.New()
	cw := &countW{w: h}
	cmd.Stdout, cmd.Stderr = cw, cw
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil)), cw.n
}

type countW struct {
	w interface{ Write([]byte) (int, error) }
	n int64
}

func (c *countW) Write(p []byte) (int, error) { c.n += int64(len(p)); return c.w.Write(p) }

func newListTestApp(t *testing.T, bin string, slots int, wait time.Duration) (*app, *Repo) {
	t.Helper()
	a := &app{
		cfg: Config{
			DuplicacyBinary:      bin,
			ConfigDir:            t.TempDir(),
			ListCacheRowMaxBytes: 256 << 20,
			ListQueueWait:        wait,
		},
		jobs:       newJobRegistry(1, 1),
		listGate:   newListGate(slots, wait),
		hitGate:    newListGate(4, wait),
		filesCache: newTestFilesCache(t, 0, 5),
	}
	return a, &Repo{ID: "repoA", Path: t.TempDir(), SnapshotID: "snapA"}
}

func serve(a *app, repo *Repo, rev int, query string, env []string, onVend func() func()) *hashRW {
	w := newHashRW()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/files?"+query, nil)
	a.serveSnapshotFiles(c, repo, rev, func() ([]string, map[string]string, func(), error) {
		done := func() {}
		if onVend != nil {
			done = onVend()
		}
		return env, map[string]string{}, done, nil
	})
	return w
}

// peakHeap samples HeapAlloc until stop() and returns the peak growth over the
// baseline taken after a GC.
func peakHeap() (stop func() uint64) {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var peak atomic.Uint64
	quit, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-quit:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	return func() uint64 {
		close(quit)
		<-done
		if peak.Load() < base {
			return 0
		}
		return peak.Load() - base
	}
}

func TestListingMemoryIndependentOfSize(t *testing.T) {
	bin := writeFakeList(t)
	const bound = 24 << 20
	// Poorly compressible input, with the RAM spool lowered so the gz really
	// spills to disk (compressible text never reaches the spill path).
	old := listSpoolMemMax
	listSpoolMemMax = 1 << 20
	t.Cleanup(func() { listSpoolMemMax = old })

	var deltas []uint64
	for _, tc := range []struct {
		size   int
		cached bool
	}{{8 << 20, true}, {48 << 20, false}} {
		env := []string{"LISTRAND=" + strconv.Itoa(tc.size)}
		wantSum, wantN := referenceSum(t, bin, env...)
		a, repo := newListTestApp(t, bin, 1, time.Second)
		a.cfg.ListCacheRowMaxBytes = 6 << 20 // 8MB fits (gz ~3.5MB), 48MB does not

		stop := peakHeap()
		w := serve(a, repo, 3, "", env, nil)
		delta := stop()

		if w.status != 200 {
			t.Fatalf("size %d: status %d body %s", tc.size, w.status, w.body.String())
		}
		if w.sum() != wantSum {
			t.Fatalf("size %d: response sha != raw output sha", tc.size)
		}
		if got := w.hdr.Get("Content-Length"); got != strconv.FormatInt(wantN, 10) || w.n != wantN {
			t.Fatalf("size %d: Content-Length %s, body %d, want %d", tc.size, got, w.n, wantN)
		}
		if delta > bound {
			t.Fatalf("size %d: miss path peak heap growth %d MB > %d MB", tc.size, delta>>20, bound>>20)
		}
		deltas = append(deltas, delta)
		// The spill really happened (the dir exists) and was cleaned up.
		ents, err := os.ReadDir(a.listSpoolDir())
		if err != nil || len(ents) != 0 {
			t.Fatalf("size %d: spill dir must exist (spill exercised) and be empty after: err=%v files=%d", tc.size, err, len(ents))
		}
		has, _ := a.filesCache.has(context.Background(), "snapA", 3, "default")
		if has != tc.cached {
			t.Fatalf("size %d: cached=%v, want %v", tc.size, has, tc.cached)
		}
		if !tc.cached {
			continue
		}
		// A cache hit: identical bytes, still bounded.
		stop = peakHeap()
		w2 := serve(a, repo, 3, "", []string{"LISTEXIT=9"}, nil) // would fail if it ran
		hitDelta := stop()
		if w2.status != 200 || w2.sum() != wantSum || w2.n != wantN || w2.hdr.Get("Content-Length") != strconv.FormatInt(wantN, 10) {
			t.Fatalf("size %d: hit path mismatch (status %d)", tc.size, w2.status)
		}
		if hitDelta > bound {
			t.Fatalf("size %d: hit path peak heap growth %d MB > %d MB", tc.size, hitDelta>>20, bound>>20)
		}
	}
	diff := int64(deltas[1]) - int64(deltas[0])
	if diff > 32<<20 || diff < -(32<<20) {
		t.Fatalf("heap growth depends on listing size: 8MB=%d 48MB=%d", deltas[0], deltas[1])
	}
}

func TestKilledOrFailedListingNeverCached(t *testing.T) {
	bin := writeFakeList(t)
	old := listFilesTimeout
	listFilesTimeout = 400 * time.Millisecond
	t.Cleanup(func() { listFilesTimeout = old })
	setVar(t, &listWaitDelay, 300*time.Millisecond)

	a, repo := newListTestApp(t, bin, 1, time.Second)
	// Killed mid-run (timeout): partial output was produced, must not be a 200.
	w := serve(a, repo, 5, "", []string{"LISTSZ=100000", "LISTSLEEP=30"}, nil)
	if w.status != http.StatusInternalServerError {
		t.Fatalf("killed listing status %d, want 500", w.status)
	}
	if ok, _ := a.filesCache.has(context.Background(), "snapA", 5, "default"); ok {
		t.Fatal("killed listing was cached")
	}
	// Non-zero exit with full output.
	w = serve(a, repo, 6, "", []string{"LISTSZ=100000", "LISTEXIT=3"}, nil)
	if w.status != http.StatusInternalServerError {
		t.Fatalf("failed listing status %d, want 500", w.status)
	}
	if ok, _ := a.filesCache.has(context.Background(), "snapA", 6, "default"); ok {
		t.Fatal("failed listing was cached")
	}
	if !strings.Contains(w.body.String(), "last-line-on-stderr") {
		t.Fatalf("error body lacks the output tail: %s", w.body.String())
	}
	if ents, _ := os.ReadDir(a.listSpoolDir()); len(ents) != 0 {
		t.Fatalf("spool files left behind: %d", len(ents))
	}
}

func TestListGateBoundsConcurrentMisses(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 2, 10*time.Second)
	var cur, peak atomic.Int32
	var wg sync.WaitGroup
	statuses := make([]int, 8)
	for i := range statuses {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := serve(a, repo, 10+i, "", []string{"LISTSZ=2048", "LISTSLEEP=0.2"}, func() func() {
				n := cur.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				return func() { cur.Add(-1) }
			})
			statuses[i] = w.status
		}()
	}
	wg.Wait()
	for i, s := range statuses {
		if s != 200 {
			t.Fatalf("request %d status %d", i, s)
		}
	}
	if p := peak.Load(); p > 2 || p < 1 {
		t.Fatalf("peak concurrent listings %d, want 1..2", p)
	}
}

func TestListGateQueueTimeoutIs503AndHitsAreUngated(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, 150*time.Millisecond)

	// Prime a cached revision, then occupy the only slot with a slow miss.
	if w := serve(a, repo, 1, "", []string{"LISTSZ=4096"}, nil); w.status != 200 {
		t.Fatalf("prime status %d", w.status)
	}
	started := make(chan struct{})
	var once sync.Once
	slow := make(chan *hashRW, 1)
	go func() {
		slow <- serve(a, repo, 2, "", []string{"LISTSZ=4096", "LISTSLEEP=1"}, func() func() {
			once.Do(func() { close(started) })
			return func() {}
		})
	}()
	<-started

	w := serve(a, repo, 3, "", []string{"LISTSZ=4096"}, nil)
	if w.status != http.StatusServiceUnavailable {
		t.Fatalf("queued miss status %d, want 503", w.status)
	}
	if w.hdr.Get("Retry-After") == "" {
		t.Fatal("503 lacks Retry-After")
	}
	t0 := time.Now()
	if hit := serve(a, repo, 1, "", nil, nil); hit.status != 200 {
		t.Fatalf("cache hit blocked behind the gate: status %d", hit.status)
	}
	if time.Since(t0) > 500*time.Millisecond {
		t.Fatal("cache hit waited on the gate")
	}
	if s := <-slow; s.status != 200 {
		t.Fatalf("slow listing status %d", s.status)
	}
}

func TestRowCapSpillsToConfigDirAndIsNotCached(t *testing.T) {
	bin := writeFakeList(t)
	env := []string{"LISTSEQ=1500000"}
	wantSum, wantN := referenceSum(t, bin, env...)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	a.cfg.ListCacheRowMaxBytes = 256 << 10 // gz of the listing is a few MB

	w := serve(a, repo, 4, "", env, nil)
	if w.status != 200 || w.sum() != wantSum || w.n != wantN {
		t.Fatalf("spilled response mismatch: status %d n %d want %d", w.status, w.n, wantN)
	}
	if ok, _ := a.filesCache.has(context.Background(), "snapA", 4, "default"); ok {
		t.Fatal("over-cap listing was cached")
	}
	if ents, _ := os.ReadDir(a.listSpoolDir()); len(ents) != 0 {
		t.Fatalf("spill files not deleted: %d", len(ents))
	}
}

func TestListSpoolSpillsUnderDirAndFailureIs503(t *testing.T) {
	dir := filepath.Join(t.TempDir(), listSpoolSubdir)
	s := newListSpool(dir, 16)
	if _, err := s.Write(bytes.Repeat([]byte("a"), 10)); err != nil || s.f != nil {
		t.Fatalf("small write must stay in RAM (err %v)", err)
	}
	if _, err := s.Write(bytes.Repeat([]byte("b"), 10)); err != nil || s.f == nil {
		t.Fatalf("overflow must spill to a file (err %v)", err)
	}
	if filepath.Dir(s.f.Name()) != dir {
		t.Fatalf("spill file %s not under %s", s.f.Name(), dir)
	}
	if err := s.seal(); err != nil {
		t.Fatal(err)
	}
	got, _ := s.bytes()
	if string(got) != strings.Repeat("a", 10)+strings.Repeat("b", 10) {
		t.Fatalf("spill content wrong: %q", got)
	}
	name := s.f.Name()
	s.close()
	if _, err := os.Stat(name); !os.IsNotExist(err) {
		t.Fatal("spill file not removed on close")
	}

	// A spool dir that cannot be created -> 503, never an unbounded buffer.
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	blocker := filepath.Join(t.TempDir(), "file")
	os.WriteFile(blocker, nil, 0o600)
	a.cfg.ConfigDir = blocker // MkdirAll(<file>/list-spool) fails
	a.cfg.ListCacheRowMaxBytes = 1 << 10
	w := serve(a, repo, 7, "", []string{"LISTSEQ=500000"}, nil)
	if w.status != http.StatusServiceUnavailable {
		t.Fatalf("spill failure status %d, want 503", w.status)
	}
	if ok, _ := a.filesCache.has(context.Background(), "snapA", 7, "default"); ok {
		t.Fatal("listing cached despite spill failure")
	}
}

func TestListSpoolFileCeiling(t *testing.T) {
	s := newListSpool(filepath.Join(t.TempDir(), "sp"), 4)
	s.fileMax = 32
	defer s.close()
	if _, err := s.Write(bytes.Repeat([]byte("x"), 20)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(bytes.Repeat([]byte("x"), 20)); err == nil {
		t.Fatal("spill beyond the hard ceiling must fail")
	}
}

func TestTailRingKeepsLastBytes(t *testing.T) {
	var r tailRing
	r.write([]byte("hello "))
	if r.String() != "hello " {
		t.Fatalf("short: %q", r.String())
	}
	big := bytes.Repeat([]byte("z"), listTailBytes-3)
	r.write(big)
	r.write([]byte("END"))
	got := r.String()
	if len(got) != listTailBytes || !strings.HasSuffix(got, "zzzEND") {
		t.Fatalf("ring wrong: len %d tail %q", len(got), got[len(got)-8:])
	}
	r.write(bytes.Repeat([]byte("q"), 3*listTailBytes))
	if r.String() != strings.Repeat("q", listTailBytes) {
		t.Fatal("oversized write must keep only the last window")
	}

	// A single write >= the window after earlier bytes must still be in order.
	var r2 tailRing
	r2.write([]byte("abc"))
	big2 := make([]byte, listTailBytes+10)
	for i := range big2 {
		big2[i] = byte('a' + i%26)
	}
	r2.write(big2)
	if r2.String() != string(big2[10:]) {
		t.Fatal("large write after small garbled the tail")
	}
	r2.write([]byte("XY"))
	want := string(big2[12:]) + "XY"
	if r2.String() != want {
		t.Fatal("write after a large write garbled the tail")
	}
}

// --- D3 / D6 ---

func TestReconcileSparesRepoWhoseVendFailed(t *testing.T) {
	c := newTestFilesCache(t, 0, 5)
	ctx := context.Background()
	// Both repos' rows live on the shared "default" storage.
	putRaw(c, ctx, "snapA", 1, "default", "repoA", []byte("a1\n"))
	putRaw(c, ctx, "snapA", 2, "default", "repoA", []byte("a2\n"))
	putRaw(c, ctx, "snapB", 1, "default", "repoB", []byte("b1\n"))
	putRaw(c, ctx, "snapB", 9, "default", "repoB", []byte("b9 pruned\n"))

	// repoA's vend timed out; repoB listed default successfully.
	st := newSweepState()
	st.known["repoA"], st.known["repoB"] = struct{}{}, struct{}{}
	st.failed["repoA"] = struct{}{}
	st.swept[repoStorage{"repoB", "default"}] = struct{}{}
	st.sweptStorage["default"] = struct{}{}
	st.keep[storageSnapRevKey("default", "snapB", 1)] = struct{}{}

	n, err := c.reconcileAgainstKeep(ctx, st)
	if err != nil || n != 1 {
		t.Fatalf("reconcile n=%d err=%v, want exactly repoB's pruned rev", n, err)
	}
	for _, rev := range []int{1, 2} {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); !ok {
			t.Fatalf("repoA rev%d purged after a transient vend failure", rev)
		}
	}
	if ok, _ := c.has(ctx, "snapB", 9, "default"); ok {
		t.Fatal("repoB's pruned revision should be reconciled away")
	}

	// A known repo that simply did not list this storage is also left alone.
	st2 := newSweepState()
	st2.known["repoA"], st2.known["repoB"] = struct{}{}, struct{}{}
	st2.swept[repoStorage{"repoB", "default"}] = struct{}{}
	st2.sweptStorage["default"] = struct{}{}
	if _, err := c.reconcileAgainstKeep(ctx, st2); err != nil {
		t.Fatal(err)
	}
	for _, rev := range []int{1, 2} {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); !ok {
			t.Fatalf("repoA rev%d purged though repoA never listed the storage", rev)
		}
	}
	// Rows of a repo that no longer exists are cleaned by any storage sweep.
	putRaw(c, ctx, "gone", 1, "default", "repoGone", []byte("g\n"))
	st3 := newSweepState()
	st3.known["repoB"] = struct{}{}
	st3.swept[repoStorage{"repoB", "default"}] = struct{}{}
	st3.sweptStorage["default"] = struct{}{}
	st3.keep[storageSnapRevKey("default", "snapB", 1)] = struct{}{}
	st3.keep[storageSnapRevKey("default", "snapA", 1)] = struct{}{}
	st3.keep[storageSnapRevKey("default", "snapA", 2)] = struct{}{}
	if _, err := c.reconcileAgainstKeep(ctx, st3); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.has(ctx, "gone", 1, "default"); ok {
		t.Fatal("orphaned repo's row should be reconciled")
	}
}

func TestEvictBySizeExemptsFailedRepoAndRelaxesPin(t *testing.T) {
	ctx := context.Background()
	body := bytes.Repeat([]byte("line /with/some/path\n"), 4000)
	fill := func(c *snapshotFilesCacheStore, repo, snap string) {
		for rev := 1; rev <= 4; rev++ {
			putRaw(c, ctx, snap, rev, "default", repo, append([]byte(strconv.Itoa(rev)+"\n"), body...))
		}
	}

	// warmKeepN=5 pins all four revisions; the cap must still bind (newest 2 kept).
	c := newTestFilesCache(t, 0, 5)
	fill(c, "repoB", "snapB")
	c.maxBytes = 1
	if err := c.evictBySize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var have []int
	for rev := 1; rev <= 4; rev++ {
		if ok, _ := c.has(ctx, "snapB", rev, "default"); ok {
			have = append(have, rev)
		}
	}
	if len(have) != 2 || have[0] != 3 || have[1] != 4 {
		t.Fatalf("pinned set not bounded to the newest 2: kept %v", have)
	}

	// A failed repo's rows survive size eviction even over the cap.
	c = newTestFilesCache(t, 0, 5)
	fill(c, "repoA", "snapA")
	fill(c, "repoB", "snapB")
	c.maxBytes = 1
	if err := c.evictBySize(ctx, map[string]struct{}{"repoA": {}}); err != nil {
		t.Fatal(err)
	}
	for rev := 1; rev <= 4; rev++ {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); !ok {
			t.Fatalf("failed repo's rev%d evicted by size", rev)
		}
	}
}

// blockRW stalls the first body write until unblock is closed (a client that
// stopped reading). started closes once the response write begins.
type blockRW struct {
	*hashRW
	started chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func newBlockRW() *blockRW {
	return &blockRW{hashRW: newHashRW(), started: make(chan struct{}), unblock: make(chan struct{})}
}

func (w *blockRW) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.unblock
	return w.hashRW.Write(p)
}

func serveTo(a *app, repo *Repo, rev int, w http.ResponseWriter, env []string) {
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/files", nil)
	a.serveSnapshotFiles(c, repo, rev, func() ([]string, map[string]string, func(), error) {
		return env, map[string]string{}, func() {}, nil
	})
}

func TestStalledClientDoesNotHoldListingGateOrWarmer(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, 300*time.Millisecond)
	env := []string{"LISTSZ=100000"}

	stalled := newBlockRW()
	done := make(chan struct{})
	go func() { serveTo(a, repo, 1, stalled, env); close(done) }()
	select {
	case <-stalled.started:
	case <-time.After(10 * time.Second):
		t.Fatal("listing never reached the response write")
	}

	// The only listing slot must be free while the client is stalled.
	if w := serve(a, repo, 2, "", env, nil); w.status != 200 {
		t.Fatalf("second miss blocked by a stalled reader: status %d", w.status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.warmOneListing(ctx, repo, "default", "", "snapW", 20, env); err != nil {
		t.Fatalf("warmer blocked by a stalled reader: %v", err)
	}
	// ...and so must the maintenance slot the warmer took.
	rel, ok := a.jobs.acquireMaint(ctx)
	if !ok {
		t.Fatal("maintenance slot still held")
	}
	rel()
	close(stalled.unblock)
	<-done
	if stalled.status != 200 {
		t.Fatalf("stalled request finally got %d", stalled.status)
	}
}

func TestWarmerNeverHoldsMaintWhileWaitingOnGate(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, 100*time.Millisecond)
	relGate, err := a.listGate.acquire(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan error, 1)
	go func() {
		_, e := a.runWarmListing(ctx, repo, "default", "snapW", 1, []string{"list", "-files"}, []string{"LISTSZ=10"})
		res <- e
	}()
	time.Sleep(450 * time.Millisecond) // several bounded gate waits
	// check/prune must be able to take the maintenance slot meanwhile.
	got := make(chan bool, 1)
	go func() {
		rel, ok := a.jobs.acquireMaint(context.Background())
		if ok {
			rel()
		}
		got <- ok
	}()
	select {
	case ok := <-got:
		if !ok {
			t.Fatal("acquireMaint failed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("maintenance slot held while the warmer waits for the listing gate")
	}
	relGate()
	if e := <-res; e != nil {
		t.Fatalf("warm listing after gate freed: %v", e)
	}
	cancel()
}

func TestHitsAreBoundedByHitGate(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, 150*time.Millisecond)
	a.hitGate = newListGate(1, 150*time.Millisecond)
	if w := serve(a, repo, 1, "", []string{"LISTSZ=4096"}, nil); w.status != 200 {
		t.Fatalf("prime %d", w.status)
	}
	stalled := newBlockRW()
	done := make(chan struct{})
	go func() { serveTo(a, repo, 1, stalled, nil); close(done) }()
	<-stalled.started
	w := serve(a, repo, 1, "", nil, nil)
	if w.status != http.StatusServiceUnavailable || w.hdr.Get("Retry-After") == "" {
		t.Fatalf("second concurrent hit status %d, want bounded 503", w.status)
	}
	close(stalled.unblock)
	<-done
}

func TestSpoolCeilingIs413MarkedAndWarmerSkips(t *testing.T) {
	bin := writeFakeList(t)
	oldMax := listSpoolFileMax
	listSpoolFileMax = 2 << 10
	t.Cleanup(func() { listSpoolFileMax = oldMax })
	a, repo := newListTestApp(t, bin, 1, time.Second)
	a.cfg.ListCacheRowMaxBytes = 1 << 10
	ctx := context.Background()

	w := serve(a, repo, 8, "", []string{"LISTSEQ=300000"}, nil)
	if w.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("ceiling status %d, want 413", w.status)
	}
	if r, _ := a.filesCache.uncacheable(ctx, "repoA", "default", "snapA", 8); r != uncacheableCeiling {
		t.Fatalf("ceiling marker = %q", r)
	}
	// Second request is refused immediately, without running the child.
	t0 := time.Now()
	w = serve(a, repo, 8, "", []string{"LISTEXIT=9"}, nil)
	if w.status != http.StatusRequestEntityTooLarge || time.Since(t0) > time.Second {
		t.Fatalf("marked ceiling not short-circuited: %d", w.status)
	}
	ranks := map[string]int{}
	if want, _ := a.warmWanted(ctx, ranks, Snapshot{SnapshotID: "snapA", Revision: 8}, "repoA", "default"); want {
		t.Fatal("warmer would re-run a marked revision")
	}
}

func TestRowCapMarkerSurvivesAndReconcileDropsIt(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	a.cfg.ListCacheRowMaxBytes = 64 << 10
	ctx := context.Background()
	if w := serve(a, repo, 4, "", []string{"LISTSEQ=1500000"}, nil); w.status != 200 {
		t.Fatalf("status %d", w.status)
	}
	if r, _ := a.filesCache.uncacheable(ctx, "repoA", "default", "snapA", 4); r != uncacheableRowCap {
		t.Fatalf("row-cap marker = %q", r)
	}
	if want, _ := a.warmWanted(ctx, map[string]int{}, Snapshot{SnapshotID: "snapA", Revision: 4}, "repoA", "default"); want {
		t.Fatal("warmer would re-list an over-cap revision")
	}

	// Revision still live: marker stays. Pruned: marker goes.
	st := newSweepState()
	st.known["repoA"] = struct{}{}
	st.swept[repoStorage{"repoA", "default"}] = struct{}{}
	st.sweptStorage["default"] = struct{}{}
	st.keep[storageSnapRevKey("default", "snapA", 4)] = struct{}{}
	if _, err := a.filesCache.reconcileAgainstKeep(ctx, st); err != nil {
		t.Fatal(err)
	}
	if r, _ := a.filesCache.uncacheable(ctx, "repoA", "default", "snapA", 4); r == "" {
		t.Fatal("marker dropped while its revision is live")
	}
	delete(st.keep, storageSnapRevKey("default", "snapA", 4))
	if _, err := a.filesCache.reconcileAgainstKeep(ctx, st); err != nil {
		t.Fatal(err)
	}
	if r, _ := a.filesCache.uncacheable(ctx, "repoA", "default", "snapA", 4); r != "" {
		t.Fatal("marker survived its revision being pruned")
	}
	// A failed repo's markers are exempt, like its rows.
	a.filesCache.markUncacheable(ctx, "repoA", "default", "snapA", 5, uncacheableRowCap, 1)
	st.failed["repoA"] = struct{}{}
	a.filesCache.reconcileAgainstKeep(ctx, st)
	if r, _ := a.filesCache.uncacheable(ctx, "repoA", "default", "snapA", 5); r == "" {
		t.Fatal("failed repo's marker was reconciled away")
	}
}

// Probe-P3 style: N=5 pinned, cap fits fewer. Warmer and evictor share one
// policy, so after the first sweep nothing is re-listed or re-evicted.
func TestWarmerAndEvictorSettle(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 6500, 5)
	a := &app{filesCache: c}
	row := bytes.Repeat([]byte{7}, 1000)
	snaps := []string{"snapA", "snapB", "snapC"}
	var live []Snapshot
	for _, sn := range snaps {
		for rev := 1; rev <= 8; rev++ {
			live = append(live, Snapshot{SnapshotID: sn, Revision: rev})
		}
	}
	var relisted []int
	for sweep := 0; sweep < 5; sweep++ {
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
			if err := c.putGzipped(ctx, rev.SnapshotID, rev.Revision, "default", "repoA", row, 1); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.evictBySize(ctx, nil); err != nil {
			t.Fatal(err)
		}
		relisted = append(relisted, n)
	}
	for i := 2; i < len(relisted); i++ {
		if relisted[i] != 0 {
			t.Fatalf("sweeps never settle, listings per sweep: %v", relisted)
		}
	}
	// What remains is the NEWEST revisions (oldest-first eviction), within the cap.
	_, gz, _ := c.stats(ctx)
	if gz > 6500 {
		t.Fatalf("cache %d bytes over the cap", gz)
	}
	for _, sn := range snaps {
		for _, rev := range []int{7, 8} {
			if ok, _ := c.has(ctx, sn, rev, "default"); !ok {
				t.Fatalf("%s rev%d (newest) was evicted", sn, rev)
			}
		}
	}
}

func TestPinPolicy(t *testing.T) {
	if got := pinPolicy(100, 5, 1000); got != 5 {
		t.Fatalf("fits: %d", got)
	}
	if got := pinPolicy(300, 5, 1000); got != maxPinRelaxedKeep {
		t.Fatalf("over cap: %d", got)
	}
	if got := pinPolicy(300, 1, 10); got != 1 {
		t.Fatalf("warmN below relaxed keep: %d", got)
	}
	if got := pinPolicy(300, 5, 0); got != 5 {
		t.Fatalf("cap disabled: %d", got)
	}
}

func TestResetListSpoolRemovesLeftovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), listSpoolSubdir)
	os.MkdirAll(dir, 0o700)
	os.WriteFile(filepath.Join(dir, "list-1.gz"), []byte("x"), 0o600) // brand new: no age guard
	resetListSpool(dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("spool dir survived reset")
	}
}

func TestChildRSSRecordedAndExposed(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	if w := serve(a, repo, 1, "", []string{"LISTSZ=4096"}, nil); w.status != 200 {
		t.Fatalf("status %d", w.status)
	}
	if childRSS.snapshot()["list-files"] <= 0 {
		t.Fatalf("no list-files RSS recorded: %v", childRSS.snapshot())
	}
	t.Setenv("LISTSZ", "10")
	if _, err := runSync(context.Background(), bin, cliInvocation{RepoRoot: t.TempDir(), Args: []string{"list"}}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if childRSS.snapshot()["sync:list"] <= 0 {
		t.Fatalf("no sync RSS recorded: %v", childRSS.snapshot())
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	a.handleReadiness(c)
	if !strings.Contains(rec.Body.String(), `"child_peak_rss_kb"`) || !strings.Contains(rec.Body.String(), "list-files") {
		t.Fatalf("readiness lacks child RSS: %s", rec.Body.String())
	}
}

func TestChildEnvDropsGOMEMLIMIT(t *testing.T) {
	t.Setenv("GOMEMLIMIT", "1GiB")
	t.Setenv("KEEP_ME", "1")
	cmd := cliInvocation{EnvAdds: []string{"ADDED=1"}}.command(context.Background(), "x")
	var keep, added bool
	for _, kv := range cmd.Env {
		if strings.HasPrefix(kv, "GOMEMLIMIT=") {
			t.Fatal("GOMEMLIMIT leaked into a duplicacy child")
		}
		keep = keep || kv == "KEEP_ME=1"
		added = added || kv == "ADDED=1"
	}
	if !keep || !added {
		t.Fatal("child env lost entries")
	}
}

// --- wiring (AST) ---
//
// A bare "call appears in the body" check is satisfied by `if false { f() }`.
// These helpers only count calls that actually run: statements of the function
// body itself (topLevelCalls), or anywhere except under a literal `if false`
// (liveCalls).

func callName(ce *ast.CallExpr) string {
	switch fn := ce.Fun.(type) {
	case *ast.SelectorExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			return id.Name + "." + fn.Sel.Name
		}
		return fn.Sel.Name
	case *ast.Ident:
		return fn.Name
	}
	return ""
}

// topLevelCalls returns calls made by the body's own statements, in order,
// without descending into any nested block or closure.
func topLevelCalls(body *ast.BlockStmt) []*ast.CallExpr {
	var out []*ast.CallExpr
	collect := func(n ast.Node) {
		ast.Inspect(n, func(n ast.Node) bool {
			if _, ok := n.(*ast.FuncLit); ok {
				return false
			}
			if ce, ok := n.(*ast.CallExpr); ok {
				out = append(out, ce)
			}
			return true
		})
	}
	for _, st := range body.List {
		// `if x := f(); ...` runs f unconditionally; only its branches are guarded.
		if is, ok := st.(*ast.IfStmt); ok && is.Init != nil {
			collect(is.Init)
			continue
		}
		switch st.(type) {
		case *ast.IfStmt, *ast.ForStmt, *ast.RangeStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt,
			*ast.SelectStmt, *ast.BlockStmt, *ast.GoStmt, *ast.DeferStmt, *ast.LabeledStmt:
			continue
		}
		collect(st)
	}
	return out
}

// liveCalls returns every call name in body except those under `if false`.
func liveCalls(body *ast.BlockStmt) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if is, ok := n.(*ast.IfStmt); ok {
			if id, ok := is.Cond.(*ast.Ident); ok && id.Name == "false" {
				if is.Else != nil {
					for k := range liveCalls(&ast.BlockStmt{List: []ast.Stmt{is.Else}}) {
						out[k] = true
					}
				}
				return false
			}
		}
		if ce, ok := n.(*ast.CallExpr); ok {
			out[callName(ce)] = true
			switch fn := ce.Fun.(type) {
			case *ast.SelectorExpr:
				out[fn.Sel.Name] = true
			}
		}
		return true
	})
	return out
}

func parseBody(t *testing.T, src, fn string) *ast.BlockStmt {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), "x.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == fn {
			return fd.Body
		}
	}
	t.Fatalf("%s not found", fn)
	return nil
}

func funcBody(t *testing.T, file, name string) *ast.BlockStmt {
	t.Helper()
	b, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	return parseBody(t, string(b), name)
}

func hasTopLevelCall(body *ast.BlockStmt, name string) bool {
	for _, ce := range topLevelCalls(body) {
		if callName(ce) == name {
			return true
		}
	}
	return false
}

func TestWiringHelpersRejectDeadBranches(t *testing.T) {
	src := `package main
func main() {
	if false { memlimit.Apply(); resetListSpool(d) }
	if cond { other() }
	func() { closure() }()
	x := newApp()
	_ = x
}`
	body := parseBody(t, src, "main")
	for _, dead := range []string{"memlimit.Apply", "resetListSpool", "other", "closure"} {
		if hasTopLevelCall(body, dead) {
			t.Fatalf("negative control: %s is not a top-level statement call", dead)
		}
	}
	if !hasTopLevelCall(body, "newApp") {
		t.Fatal("positive control: newApp is top level")
	}
	live := liveCalls(body)
	if live["memlimit.Apply"] || live["resetListSpool"] {
		t.Fatal("negative control: calls under `if false` must not count")
	}
	if !live["other"] || !live["newApp"] {
		t.Fatal("positive control: live calls missing")
	}
}

func TestListingWiring(t *testing.T) {
	h := liveCalls(funcBody(t, "duplicacy.go", "handleSnapshotFiles"))
	if !h["serveSnapshotFiles"] {
		t.Fatal("route handler must go through serveSnapshotFiles")
	}
	s := liveCalls(funcBody(t, "duplicacy.go", "serveSnapshotFiles"))
	for _, want := range []string{"acquire", "runListFiles", "writeListing", "storeListing", "open", "release", "markUncacheable"} {
		if !s[want] {
			t.Fatalf("serveSnapshotFiles must call %s", want)
		}
	}
	for _, banned := range []string{"acquireMaint", "runSync", "ReadAll", "CombinedOutput", "gunzip", "gzipBytes"} {
		if s[banned] {
			t.Fatalf("serveSnapshotFiles must not call %s", banned)
		}
	}
	w := liveCalls(funcBody(t, "snapshot_files_warm.go", "warmOneListing"))
	if !w["runWarmListing"] || !w["markUncacheable"] || !w["storeListing"] || w["runSync"] {
		t.Fatalf("warm path wiring: %v", w)
	}
	rw := liveCalls(funcBody(t, "snapshot_files_warm.go", "runWarmListing"))
	if !rw["acquireMaint"] || !rw["acquire"] || !rw["runListFiles"] {
		t.Fatalf("runWarmListing must hold maint slot AND listing gate: %v", rw)
	}
	sweep := liveCalls(funcBody(t, "snapshot_files_warm.go", "warmListCacheSweep"))
	if !sweep["newSweepState"] || !sweep["warmWanted"] {
		t.Fatal("sweep must use the sweep state and the shared pin policy via warmWanted")
	}
	if !liveCalls(funcBody(t, "snapshot_files_warm.go", "warmWanted"))["effectivePinN"] {
		t.Fatal("warmWanted must use effectivePinN")
	}
	if !liveCalls(funcBody(t, "snapshot_files_cache.go", "evictBySize"))["effectivePinN"] {
		t.Fatal("evictBySize must use effectivePinN")
	}
	if !hasTopLevelCall(funcBody(t, "app.go", "newApp"), "resetListSpool") {
		t.Fatal("newApp must reset the spool as a top-level statement, before anything serves")
	}
	r, err := os.ReadFile("snapshot_files_stream.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(r), "cmd.Stdout = sink") || !strings.Contains(string(r), "cmd.Stderr = sink") {
		t.Fatal("stdout and stderr must be the same writer value")
	}
	if !liveCalls(funcBody(t, "jobs.go", "spawn"))["record"] {
		t.Fatal("job runner must record child RSS")
	}
}
