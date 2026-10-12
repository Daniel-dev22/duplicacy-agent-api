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
//	LISTSEQ   emit `seq 1 N` instead (incompressible-ish)
//	LISTSLEEP seconds to sleep after the first output
//	LISTEXIT  exit status (default 0)
const fakeListScript = `#!/bin/sh
echo "first-line-on-stderr" >&2
if [ -n "$LISTSEQ" ]; then
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
	h      hash.Hash
	hdr    http.Header
	status int
	n      int64
	body   bytes.Buffer // only kept for non-200 (small JSON errors)
}

func newHashRW() *hashRW { return &hashRW{h: sha256.New(), hdr: http.Header{}} }

func (w *hashRW) Header() http.Header { return w.hdr }
func (w *hashRW) WriteHeader(s int)   { w.status = s }
func (w *hashRW) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	w.n += int64(len(p))
	if w.status == 200 {
		w.h.Write(p)
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
	var deltas []uint64
	for _, size := range []int{8 << 20, 64 << 20} {
		env := []string{"LISTSZ=" + strconv.Itoa(size)}
		wantSum, wantN := referenceSum(t, bin, env...)
		a, repo := newListTestApp(t, bin, 1, time.Second)

		stop := peakHeap()
		w := serve(a, repo, 3, "", env, nil)
		delta := stop()

		if w.status != 200 {
			t.Fatalf("size %d: status %d body %s", size, w.status, w.body.String())
		}
		if w.sum() != wantSum {
			t.Fatalf("size %d: response sha != raw output sha", size)
		}
		if got := w.hdr.Get("Content-Length"); got != strconv.FormatInt(wantN, 10) || w.n != wantN {
			t.Fatalf("size %d: Content-Length %s, body %d, want %d", size, got, w.n, wantN)
		}
		if delta > bound {
			t.Fatalf("size %d: miss path peak heap growth %d MB > %d MB", size, delta>>20, bound>>20)
		}
		deltas = append(deltas, delta)

		// The same listing again is a cache hit: identical bytes, still bounded.
		stop = peakHeap()
		w2 := serve(a, repo, 3, "", []string{"LISTEXIT=9"}, nil) // would fail if it ran
		hitDelta := stop()
		if w2.status != 200 || w2.sum() != wantSum || w2.n != wantN || w2.hdr.Get("Content-Length") != strconv.FormatInt(wantN, 10) {
			t.Fatalf("size %d: hit path mismatch (status %d)", size, w2.status)
		}
		if hitDelta > bound {
			t.Fatalf("size %d: hit path peak heap growth %d MB > %d MB", size, hitDelta>>20, bound>>20)
		}
	}
	diff := int64(deltas[1]) - int64(deltas[0])
	if diff > 32<<20 || diff < -(32<<20) {
		t.Fatalf("heap growth depends on listing size: 8MB=%d 64MB=%d", deltas[0], deltas[1])
	}
}

func TestKilledOrFailedListingNeverCached(t *testing.T) {
	bin := writeFakeList(t)
	old := listFilesTimeout
	listFilesTimeout = 400 * time.Millisecond
	t.Cleanup(func() { listFilesTimeout = old })

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

// --- wiring (AST, like the route-level guards elsewhere in this repo) ---

func funcBody(t *testing.T, file, name string) *ast.BlockStmt {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == name {
			return fd.Body
		}
	}
	t.Fatalf("%s not found in %s", name, file)
	return nil
}

func callsIn(body *ast.BlockStmt) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		if ce, ok := n.(*ast.CallExpr); ok {
			switch fn := ce.Fun.(type) {
			case *ast.SelectorExpr:
				out[fn.Sel.Name] = true
			case *ast.Ident:
				out[fn.Name] = true
			}
		}
		return true
	})
	return out
}

func TestListingWiring(t *testing.T) {
	h := callsIn(funcBody(t, "duplicacy.go", "handleSnapshotFiles"))
	if !h["serveSnapshotFiles"] {
		t.Fatal("route handler must go through serveSnapshotFiles")
	}
	s := callsIn(funcBody(t, "duplicacy.go", "serveSnapshotFiles"))
	for _, want := range []string{"acquire", "runListFiles", "writeListing", "storeListing", "open"} {
		if !s[want] {
			t.Fatalf("serveSnapshotFiles must call %s", want)
		}
	}
	for _, banned := range []string{"acquireMaint", "runSync", "ReadAll", "CombinedOutput", "gunzip", "gzipBytes"} {
		if s[banned] {
			t.Fatalf("serveSnapshotFiles must not call %s", banned)
		}
	}
	w := callsIn(funcBody(t, "snapshot_files_warm.go", "warmOneListing"))
	if !w["acquireMaint"] || !w["acquire"] || !w["runListFiles"] || w["runSync"] {
		t.Fatalf("warm path must hold maint slot AND listing gate and stream: %v", w)
	}
	r, err := os.ReadFile("snapshot_files_stream.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(r), "cmd.Stdout = sink") || !strings.Contains(string(r), "cmd.Stderr = sink") {
		t.Fatal("stdout and stderr must be the same writer value")
	}
	if !callsIn(funcBody(t, "snapshot_files_warm.go", "warmListCacheSweep"))["newSweepState"] {
		t.Fatal("sweep must use the (repo, storage) sweep state")
	}
}
