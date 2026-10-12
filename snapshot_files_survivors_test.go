package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// These tests exist because a mutation run showed the earlier suite let real
// regressions through (spool policy, pipe sharing, warm sweep, corrupt rows,
// error-body size...). Each one asserts behaviour, not source text.

func setVar[T any](t *testing.T, p *T, v T) {
	t.Helper()
	old := *p
	*p = v
	t.Cleanup(func() { *p = old })
}

func spoolDirState(t *testing.T, a *app) (exists bool, files int) {
	t.Helper()
	ents, err := os.ReadDir(a.listSpoolDir())
	if err != nil {
		return false, 0
	}
	return true, len(ents)
}

func TestSpoolRAMPolicyDefaults(t *testing.T) {
	a := &app{cfg: Config{ListCacheRowMaxBytes: 256 << 20}}
	if got := a.listSpoolMemMax(); got > 32<<20 || got <= 0 {
		t.Fatalf("RAM spool for a large row cap = %d, want <= 32MiB", got)
	}
	a.cfg.ListCacheRowMaxBytes = 1 << 20
	if got := a.listSpoolMemMax(); got != 1<<20 {
		t.Fatalf("RAM spool must not exceed the row cap: %d", got)
	}
}

func TestStdoutAndStderrShareOnePipe(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	w := newHashRW()
	w.keep = true
	serveTo(a, repo, 1, w, []string{"LISTFD=1", "LISTSZ=100"})
	if w.status != 200 {
		t.Fatalf("status %d", w.status)
	}
	line := strings.SplitN(w.body.String(), "\n", 2)[0]
	var fd1, fd2 string
	for _, f := range strings.Fields(line) {
		if v, ok := strings.CutPrefix(f, "fd1="); ok {
			fd1 = v
		}
		if v, ok := strings.CutPrefix(f, "fd2="); ok {
			fd2 = v
		}
	}
	if !strings.HasPrefix(fd1, "pipe:") || fd1 != fd2 {
		t.Fatalf("stdout and stderr must be the SAME pipe, got %q and %q", fd1, fd2)
	}
}

func TestErrorBodyCarriesBoundedTail(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	w := serve(a, repo, 1, "", []string{"LISTRAND=400000", "LISTEXIT=3"}, nil)
	if w.status != http.StatusInternalServerError {
		t.Fatalf("status %d", w.status)
	}
	var body struct {
		Error  string  `json:"error"`
		Output *string `json:"output"`
	}
	if err := json.Unmarshal(w.body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Output == nil {
		t.Fatal(`error JSON lost its "output" field`)
	}
	if len(*body.Output) != listTailBytes {
		t.Fatalf("output is %d bytes, want exactly the %d-byte tail", len(*body.Output), listTailBytes)
	}
	if !strings.HasSuffix(*body.Output, "last-line-on-stderr\n") {
		t.Fatal("output is not the END of the process output")
	}
	if len(body.Error) > 1024 {
		t.Fatalf("error message carries %d bytes of output", len(body.Error))
	}
}

func TestKilledSpilledListingLeavesNoSpoolAndReturnsPromptly(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listSpoolMemMax, int64(64<<10))
	setVar(t, &listFilesTimeout, 400*time.Millisecond)
	setVar(t, &listWaitDelay, 300*time.Millisecond)
	a, repo := newListTestApp(t, bin, 1, time.Second)

	t0 := time.Now()
	// Output spills, then the process idles; the sleep child keeps the pipe
	// open for 4s after the shell is killed. WaitDelay must cut that short.
	w := serve(a, repo, 1, "", []string{"LISTRAND=3000000", "LISTSLEEP=4"}, nil)
	if el := time.Since(t0); el > 2500*time.Millisecond {
		t.Fatalf("killed listing took %v to return (WaitDelay not bounding)", el)
	}
	if w.status != http.StatusInternalServerError {
		t.Fatalf("status %d", w.status)
	}
	if exists, n := spoolDirState(t, a); !exists || n != 0 {
		t.Fatalf("spill must have happened (dir exists) and been removed: exists=%v files=%d", exists, n)
	}
}

func TestSpoolWriteFailureStopsTheProcess(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listSpoolMemMax, int64(1<<10))
	setVar(t, &listSpoolFileMax, int64(4<<10))
	setVar(t, &listWaitDelay, 300*time.Millisecond)
	dir := filepath.Join(t.TempDir(), "sp")
	inv := cliInvocation{RepoRoot: t.TempDir(), Args: []string{"list", "-files"}, EnvAdds: []string{"LISTRAND=3000000", "LISTSLEEP=20"}}
	t0 := time.Now()
	_, _, err := runListFiles(context.Background(), bin, inv, time.Minute, dir, int(listSpoolMemMax))
	if el := time.Since(t0); el > 6*time.Second {
		t.Fatalf("a failed spool left the child running for %v", el)
	}
	if err == nil || !strings.Contains(err.Error(), errSpoolCeiling.Error()) {
		t.Fatalf("err = %v, want the spool ceiling", err)
	}
	if ents, _ := os.ReadDir(dir); len(ents) != 0 {
		t.Fatalf("spill left behind: %d", len(ents))
	}
}

func TestQueuedDuplicateMissIsServedFromCache(t *testing.T) {
	bin := writeFakeList(t)
	log := filepath.Join(t.TempDir(), "calls.log")
	a, repo := newListTestApp(t, bin, 1, 10*time.Second)
	env := []string{"LISTSZ=4096", "LISTSLEEP=0.6", "LISTLOG=" + log}
	var wg sync.WaitGroup
	res := make([]*hashRW, 2)
	wg.Add(1)
	go func() { defer wg.Done(); res[0] = serve(a, repo, 3, "", env, nil) }()
	for i := 0; i < 200; i++ { // wait until the first child is running
		if b, _ := os.ReadFile(log); len(b) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	wg.Add(1)
	go func() { defer wg.Done(); res[1] = serve(a, repo, 3, "", env, nil) }()
	wg.Wait()
	for i, r := range res {
		if r.status != 200 {
			t.Fatalf("request %d status %d", i, r.status)
		}
	}
	if res[0].sum() != res[1].sum() {
		t.Fatal("duplicate requests returned different bytes")
	}
	b, _ := os.ReadFile(log)
	if n := strings.Count(string(b), "1 "); n != 1 {
		t.Fatalf("the queued duplicate re-ran the listing: %d runs\n%s", n, b)
	}
}

func gzOf(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(raw)
	zw.Close()
	return buf.Bytes()
}

func TestCorruptCacheRowsAreDropped(t *testing.T) {
	bin := writeFakeList(t)
	ctx := context.Background()
	raw := bytes.Repeat([]byte("0123456789 /path/to/file\n"), 400)
	good := gzOf(t, raw)

	cases := []struct {
		name string
		gz   []byte
		rawN int64
	}{
		{"truncated stream", good[:len(good)-12], int64(len(raw))},
		{"short body (raw_bytes too large)", good, int64(len(raw)) + 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, repo := newListTestApp(t, bin, 1, time.Second)
			if err := a.filesCache.putGzipped(ctx, "snapA", 2, "default", "repoA", tc.gz, tc.rawN); err != nil {
				t.Fatal(err)
			}
			serve(a, repo, 2, "", nil, nil)
			if ok, _ := a.filesCache.has(ctx, "snapA", 2, "default"); ok {
				t.Fatal("bad row was served and kept")
			}
		})
	}

	t.Run("not gzip at all is re-listed and replaced", func(t *testing.T) {
		a, repo := newListTestApp(t, bin, 1, time.Second)
		a.filesCache.putGzipped(ctx, "snapA", 2, "default", "repoA", []byte("garbage, not gzip"), 17)
		env := []string{"LISTSZ=2000"}
		wantSum, wantN := referenceSum(t, bin, env...)
		w := serve(a, repo, 2, "", env, nil)
		if w.status != 200 || w.sum() != wantSum || w.n != wantN {
			t.Fatalf("status %d, body not the fresh listing", w.status)
		}
		got, hit, err := getAll(a.filesCache, ctx, "snapA", 2, "default")
		if err != nil || !hit || int64(len(got)) != wantN {
			t.Fatalf("row was not replaced by a valid one: hit=%v err=%v", hit, err)
		}
	})
}

func TestListingIsCachedEvenIfTheClientLeft(t *testing.T) {
	bin := writeFakeList(t)
	a, repo := newListTestApp(t, bin, 1, time.Second)
	reqCtx, cancel := context.WithCancel(context.Background())
	w := newHashRW()
	w.onWrite = cancel // the client disconnects as the body starts
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/files", nil).WithContext(reqCtx)
	a.serveSnapshotFiles(c, repo, 4, func() ([]string, map[string]string, func(), error) {
		return []string{"LISTSZ=3000"}, map[string]string{}, func() {}, nil
	})
	if ok, _ := a.filesCache.has(context.Background(), "snapA", 4, "default"); !ok {
		t.Fatal("complete immutable listing was not cached after the client left")
	}
}

func TestEvictionTakesOldestRevisionFirst(t *testing.T) {
	ctx := context.Background()
	c := newTestFilesCache(t, 0, 1)
	row := bytes.Repeat([]byte{1}, 1000)
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	// Access times run OPPOSITE to revision age: rev 2 was touched longest ago.
	access := map[int]time.Duration{1: 5, 2: 1, 3: 2, 4: 3, 5: 4}
	for rev := 1; rev <= 5; rev++ {
		if err := c.putGzipped(ctx, "snapA", rev, "default", "repoA", row, 1); err != nil {
			t.Fatal(err)
		}
		setLastAccess(t, c, "snapA", rev, "default", base.Add(access[rev]*time.Hour))
	}
	c.maxBytes = 4500 // exactly one row must go
	if err := c.evictBySize(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.has(ctx, "snapA", 1, "default"); ok {
		t.Fatal("the OLDEST revision must be evicted first")
	}
	for rev := 2; rev <= 5; rev++ {
		if ok, _ := c.has(ctx, "snapA", rev, "default"); !ok {
			t.Fatalf("rev%d evicted ahead of the oldest revision", rev)
		}
	}
}

// --- real warm sweeps against a fake storage ---

type sweepRepo struct {
	id, dir, snap string
	storages      []string
	vend          bool // has a controller mapping (vend succeeds)
}

func newSweepApp(t *testing.T, bin string, maxBytes int64, repos []sweepRepo) *app {
	t.Helper()
	base := t.TempDir()
	a := &app{
		cfg: Config{
			DuplicacyBinary:      bin,
			ConfigDir:            t.TempDir(),
			ListCacheWarmN:       2,
			ListCacheRowMaxBytes: 64 << 20,
			ListQueueWait:        200 * time.Millisecond,
		},
		jobs:       newJobRegistry(1, 1),
		listGate:   newListGate(1, 200*time.Millisecond),
		hitGate:    newListGate(2, time.Second),
		filesCache: newTestFilesCache(t, maxBytes, 2),
		secrets:    newSecretCache(),
		mapping:    &repoMappingStore{byID: map[string]*RepoMapping{}},
		repos:      &repoIndex{repos: map[string]*Repo{}},
	}
	for _, sr := range repos {
		dir := filepath.Join(base, sr.dir)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		r := &Repo{ID: sr.id, Path: dir, SnapshotID: sr.snap}
		var ms []RepoStorageMapping
		for i, st := range sr.storages {
			r.Storages = append(r.Storages, Storage{Name: st})
			cred := sr.id + "-" + st
			ms = append(ms, RepoStorageMapping{StorageAlias: st, CredentialID: cred, StorageType: "b2", IsPrimary: i == 0})
			a.secrets.put(cred, SecretsBundle{StorageType: "b2", EncryptionPassword: "pw", Backend: map[string]string{"b2_id": "i", "b2_key": "k"}})
		}
		a.repos.repos[sr.id] = r
		if sr.vend {
			a.mapping.byID[repoIDFromPath(dir)] = &RepoMapping{RepoPath: dir, RepoID: sr.snap, Storages: ms}
		}
	}
	return a
}

func countLog(t *testing.T, path, prefix string) int {
	t.Helper()
	b, _ := os.ReadFile(path)
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func TestWarmSweepProtectsRowsOfReposThatDidNotFullyList(t *testing.T) {
	bin := writeFakeList(t)
	log := filepath.Join(t.TempDir(), "calls.log")
	t.Setenv("LISTREVS", "2")
	t.Setenv("FAILSTORAGE", "s2")
	t.Setenv("LISTSZ", "2048")
	t.Setenv("LISTLOG", log)
	ctx := context.Background()

	a := newSweepApp(t, bin, 0, []sweepRepo{
		{"repoA", "a", "snapA", []string{"default", "s2"}, true}, // default lists, s2 FAILS
		{"repoB", "b", "snapB", []string{"default", "sB"}, true}, // lists everything
		{"repoC", "c", "snapC", []string{"default"}, false},      // vend FAILS
		{"repoD", "d", "snapD", []string{"default"}, true},       // lists, but never sB
	})
	big := bytes.Repeat([]byte("seeded row text /a/b/c\n"), 300)
	seed := func(snap string, rev int, storage, repo string) {
		if err := putRaw(a.filesCache, ctx, snap, rev, storage, repo, big); err != nil {
			t.Fatal(err)
		}
	}
	seed("snapA", 9, "default", "repoA") // pruned, but A did not fully list -> must stay
	for rev := 5; rev <= 9; rev++ {
		seed("snapC", rev, "default", "repoC") // vend failed; over cap and rn>2 -> must stay
	}
	seed("snapD", 3, "sB", "repoD") // D never listed sB (B did) -> must stay
	seed("snapB", 9, "default", "repoB")

	a.filesCache.maxBytes = 1 // cap armed only now: every put in the sweep wants to evict
	a.warmListCacheSweep(ctx)

	has := func(snap string, rev int, storage string) bool {
		ok, _ := a.filesCache.has(ctx, snap, rev, storage)
		return ok
	}
	if !has("snapA", 9, "default") {
		t.Fatal("repo A listed one storage and failed another: its rows must not be reconciled or evicted")
	}
	for rev := 5; rev <= 9; rev++ {
		if !has("snapC", rev, "default") {
			t.Fatalf("repo C (vend failed) lost snapC rev%d", rev)
		}
	}
	if !has("snapD", 3, "sB") {
		t.Fatal("repo D's row on a storage it did not list was reconciled away")
	}
	if has("snapB", 9, "default") {
		t.Fatal("repo B listed its storage fully: its pruned revision should be reconciled away (reconcile did not run)")
	}
	if !has("snapB", 2, "default") {
		t.Fatal("repo B's newest live revision should have been warmed and kept")
	}
	if countLog(t, log, "1 snapC") != 0 {
		t.Fatal("a repo whose vend failed must not be listed")
	}
	if countLog(t, log, "1 snapB") == 0 {
		t.Fatal("the sweep did not warm anything for repo B")
	}
}

func TestWarmerSharesTheListingBudgetWithTheHandler(t *testing.T) {
	bin := writeFakeList(t)
	log := filepath.Join(t.TempDir(), "calls.log")
	a, repo := newListTestApp(t, bin, 1, 100*time.Millisecond)
	rel, err := a.listGate.acquire(context.Background(), true) // an interactive miss holds the only slot
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	werr := a.warmOneListing(ctx, repo, "default", "", "snapW", 5, []string{"LISTSZ=100", "LISTLOG=" + log})
	if werr == nil {
		t.Fatal("warm listing ran although the listing budget was exhausted")
	}
	if b, _ := os.ReadFile(log); len(b) != 0 {
		t.Fatalf("the warm child was started without a listing slot: %s", b)
	}
	rel()
	if werr := a.warmOneListing(context.Background(), repo, "default", "", "snapW", 5, []string{"LISTSZ=100"}); werr != nil {
		t.Fatalf("warm listing with a free slot: %v", werr)
	}
}

func TestWarmListingLeavesNoSpoolFile(t *testing.T) {
	bin := writeFakeList(t)
	setVar(t, &listSpoolMemMax, int64(64<<10))
	a, repo := newListTestApp(t, bin, 1, time.Second)
	a.cfg.ListCacheRowMaxBytes = 256 << 10 // the 2MB listing is over the row cap
	err := a.warmOneListing(context.Background(), repo, "default", "", "snapW", 5, []string{"LISTRAND=2000000"})
	if err != nil {
		t.Fatal(err)
	}
	if exists, n := spoolDirState(t, a); !exists || n != 0 {
		t.Fatalf("warm path must have spilled (dir exists) and removed it: exists=%v files=%d", exists, n)
	}
	if r, _ := a.filesCache.uncacheable(context.Background(), "repoA", "default", "snapW", 5); r != uncacheableRowCap {
		t.Fatalf("over-cap warm listing not marked: %q", r)
	}
}

func TestResetListSpoolWiredAtStartup(t *testing.T) {
	// behaviour of the reset itself; the call site is pinned in TestListingWiring
	dir := filepath.Join(t.TempDir(), listSpoolSubdir)
	os.MkdirAll(dir, 0o700)
	for i := 0; i < 3; i++ {
		os.WriteFile(filepath.Join(dir, "list-"+strconv.Itoa(i)+".gz"), []byte("x"), 0o600)
	}
	resetListSpool(dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("leftover spool files survived startup reset")
	}
}
