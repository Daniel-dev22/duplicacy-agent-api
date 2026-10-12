package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

// A `duplicacy list -files` listing can be hundreds of MB of text. Nothing here
// ever holds one raw: output is gzipped as it is produced into a spool (RAM up
// to listSpoolMemMax, then a temp file under CONFIG_DIR), and the response is
// inflated back out of the spool only after the process exited 0.

const (
	listTailBytes   = 64 << 10 // output tail kept for error messages
	listSpoolSubdir = "list-spool"
)

var (
	// listSpoolMemMax is how many gz bytes stay in RAM before a listing spills
	// to disk.
	listSpoolMemMax int64 = 32 << 20
	// listWaitDelay bounds how long Run waits for output pipes after the child
	// was killed (a grandchild may still hold them).
	listWaitDelay = 5 * time.Second

	// listSpoolFileMax is the hard ceiling on one spilled gz listing.
	listSpoolFileMax int64 = 2 << 30
	// listFilesTimeout bounds one `list -files` run (var so tests can shrink it).
	listFilesTimeout = 5 * time.Minute

	errListQueueTimeout = errors.New("list-files queue wait exceeded")
	errSpoolFailed      = errors.New("list-files spool failed")
	// errSpoolCeiling is permanent for that revision (it will exceed the
	// ceiling every time), unlike errSpoolFailed which is an I/O fault.
	errSpoolCeiling = errors.New("list-files listing exceeds spool ceiling")

	// listWriteTimeout bounds how long a client may take to read a response.
	listWriteTimeout = 5 * time.Minute
)

// listGate bounds concurrent cache-MISSING `list -files` runs. Deliberately not
// jobRegistry.maintSem: that slot is held for the whole of a check/prune, so an
// interactive restore-drawer open would sit behind a multi-hour maintenance job.
type listGate struct {
	sem  chan struct{}
	wait time.Duration
}

func newListGate(slots int, wait time.Duration) *listGate {
	if slots < 1 {
		slots = 1
	}
	return &listGate{sem: make(chan struct{}, slots), wait: wait}
}

// acquire takes a slot. bounded=true gives up after the gate's wait with
// errListQueueTimeout (interactive callers); bounded=false waits for ctx only
// (the background warmer).
func (g *listGate) acquire(ctx context.Context, bounded bool) (func(), error) {
	if g == nil { // unwired (tests / disabled): unbounded, like a nil maintSem
		return func() {}, nil
	}
	var once sync.Once
	release := func() { once.Do(func() { <-g.sem }) }
	select {
	case g.sem <- struct{}{}:
		return release, nil
	default:
	}
	var timeout <-chan time.Time
	if bounded {
		t := time.NewTimer(g.wait)
		defer t.Stop()
		timeout = t.C
	}
	select {
	case g.sem <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timeout:
		return nil, errListQueueTimeout
	}
}

// listSpool accumulates gzip bytes: RAM first, then a temp file.
type listSpool struct {
	dir     string
	memMax  int
	fileMax int64
	mem     bytes.Buffer
	f       *os.File
	bw      *bufio.Writer
	n       int64
}

func newListSpool(dir string, memMax int) *listSpool {
	return &listSpool{dir: dir, memMax: memMax, fileMax: listSpoolFileMax}
}

func (s *listSpool) Write(p []byte) (int, error) {
	if s.f == nil {
		if s.mem.Len()+len(p) <= s.memMax {
			s.n += int64(len(p))
			return s.mem.Write(p)
		}
		if err := s.spill(); err != nil {
			return 0, err
		}
	}
	if s.n+int64(len(p)) > s.fileMax {
		return 0, fmt.Errorf("%w: exceeds %d bytes", errSpoolCeiling, s.fileMax)
	}
	if _, err := s.bw.Write(p); err != nil {
		return 0, fmt.Errorf("%w: %v", errSpoolFailed, err)
	}
	s.n += int64(len(p))
	return len(p), nil
}

func (s *listSpool) spill() error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return fmt.Errorf("%w: %v", errSpoolFailed, err)
	}
	f, err := os.CreateTemp(s.dir, "list-*.gz")
	if err != nil {
		return fmt.Errorf("%w: %v", errSpoolFailed, err)
	}
	s.f = f
	s.bw = bufio.NewWriterSize(f, 256<<10)
	if _, err := s.bw.Write(s.mem.Bytes()); err != nil {
		return fmt.Errorf("%w: %v", errSpoolFailed, err)
	}
	s.mem = bytes.Buffer{}
	return nil
}

// seal flushes buffered file bytes; call once after the last Write.
func (s *listSpool) seal() error {
	if s.bw == nil {
		return nil
	}
	if err := s.bw.Flush(); err != nil {
		return fmt.Errorf("%w: %v", errSpoolFailed, err)
	}
	return nil
}

func (s *listSpool) reader() io.Reader {
	if s.f != nil {
		return io.NewSectionReader(s.f, 0, s.n)
	}
	return bytes.NewReader(s.mem.Bytes())
}

// bytes returns the whole gz stream (bounded by the caller's row cap).
func (s *listSpool) bytes() ([]byte, error) {
	if s.f == nil {
		return s.mem.Bytes(), nil
	}
	return io.ReadAll(s.reader())
}

func (s *listSpool) close() {
	if s.f != nil {
		name := s.f.Name()
		s.f.Close()
		os.Remove(name)
		s.f = nil
	}
	s.mem = bytes.Buffer{}
}

// resetListSpool deletes the spool directory. Called once at startup, before
// the listener opens, so no live spool can exist: whatever is there is a
// leftover of a crash or OOM-kill.
func resetListSpool(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		slog.Warn("list-files spool reset failed", "dir", dir, "error", err)
	}
}

// tailRing keeps the last listTailBytes of output.
type tailRing struct {
	buf  [listTailBytes]byte
	pos  int // next write index
	full bool
}

func (r *tailRing) write(p []byte) {
	if len(p) >= len(r.buf) {
		copy(r.buf[:], p[len(p)-len(r.buf):])
		r.pos, r.full = 0, true
		return
	}
	n := copy(r.buf[r.pos:], p)
	if n < len(p) {
		copy(r.buf[:], p[n:])
		r.full = true
	}
	r.pos = (r.pos + len(p)) % len(r.buf)
	if r.pos == 0 {
		r.full = true
	}
}

func (r *tailRing) String() string {
	if !r.full {
		return string(r.buf[:r.pos])
	}
	return string(r.buf[r.pos:]) + string(r.buf[:r.pos])
}

// listSink is the single writer behind both cmd.Stdout and cmd.Stderr. Being the
// SAME value makes os/exec share one pipe, so the bytes are interleaved exactly
// as CombinedOutput would have.
type listSink struct {
	zw     *gzip.Writer
	raw    int64
	tail   tailRing
	err    error
	cancel context.CancelFunc
}

func (k *listSink) Write(p []byte) (int, error) {
	k.tail.write(p)
	if _, err := k.zw.Write(p); err != nil {
		k.err = err
		k.cancel() // stop the process; do not let it block on a full pipe
		return 0, err
	}
	k.raw += int64(len(p))
	return len(p), nil
}

// listResult is a COMPLETE (exit 0) listing, gzipped in its spool.
type listResult struct {
	spool *listSpool
	raw   int64 // uncompressed byte count == Content-Length
}

func (r *listResult) close() { r.spool.close() }

// runListFiles runs a listing to completion. A killed, timed-out or failed run
// returns an error and nothing reusable, so a partial listing can never reach
// the cache or a client.
func runListFiles(ctx context.Context, binary string, inv cliInvocation, timeout time.Duration, spoolDir string, memMax int) (*listResult, string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if timeout > 0 {
		var tc context.CancelFunc
		ctx, tc = context.WithTimeout(ctx, timeout)
		defer tc()
	}
	spool := newListSpool(spoolDir, memMax)
	sink := &listSink{zw: gzip.NewWriter(spool), cancel: cancel}

	cmd := inv.command(ctx, binary)
	cmd.WaitDelay = listWaitDelay
	cmd.Stdout = sink
	cmd.Stderr = sink
	err := cmd.Run()
	childRSS.record("list-files", inv.RepoRoot, cmd.ProcessState, true)
	if sink.err != nil {
		err = sink.err
	}
	if err == nil {
		if err = sink.zw.Close(); err == nil {
			err = spool.seal()
		}
		if err != nil && !errors.Is(err, errSpoolFailed) {
			err = fmt.Errorf("%w: %v", errSpoolFailed, err)
		}
	}
	if err != nil {
		spool.close()
		tail := sink.tail.String()
		return nil, tail, fmt.Errorf("duplicacy %s: %w (output: %s)", strings.Join(inv.Args, " "), err, lastN(tail, 400))
	}
	return &listResult{spool: spool, raw: sink.raw}, "", nil
}

func lastN(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return "..." + s[len(s)-n:]
}

func (a *app) listSpoolDir() string { return filepath.Join(a.cfg.ConfigDir, listSpoolSubdir) }

// listSpoolMemMax is how much gz stays in RAM before a listing spills to disk.
func (a *app) listSpoolMemMax() int {
	if m := a.cfg.ListCacheRowMaxBytes; m > 0 && m < listSpoolMemMax {
		return int(m)
	}
	return int(listSpoolMemMax)
}

// storeListing caches a COMPLETE listing unless its gz exceeds the row cap,
// which is recorded as a permanent marker (served, never cached, never re-run by
// the warmer). The SQLite bind needs the gz as one []byte, so a put briefly holds
// up to the row cap (default 64 MiB) twice; rows beyond it are never read back
// from the spill file.
func (a *app) storeListing(ctx context.Context, snapID string, rev int, storage, repoID string, res *listResult) error {
	if a.filesCache == nil || snapID == "" {
		return nil
	}
	if m := a.cfg.ListCacheRowMaxBytes; m > 0 && res.spool.n > m {
		slog.Info("list-files listing exceeds row cap, not cached",
			"repo", repoID, "snapshot_id", snapID, "rev", rev, "storage", storage, "gz_bytes", res.spool.n, "cap", m)
		return a.filesCache.markUncacheable(ctx, repoID, storage, snapID, rev, uncacheableRowCap, res.spool.n)
	}
	gz, err := res.spool.bytes()
	if err != nil {
		return err
	}
	return a.filesCache.putGzipped(ctx, snapID, rev, storage, repoID, gz, res.raw)
}

// writeListing inflates a gz stream to w with an exact Content-Length. started
// reports whether headers went out (after that a failure can only be signalled
// by the short body closing the connection).
func writeListing(w gin.ResponseWriter, gz io.Reader, rawBytes int64) (started bool, err error) {
	zr, err := gzip.NewReader(gz)
	if err != nil {
		return false, err
	}
	defer zr.Close()
	// A reader that stalls must not pin a hit slot or its row forever.
	if rerr := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(listWriteTimeout)); rerr != nil && !errors.Is(rerr, http.ErrNotSupported) {
		slog.Debug("list-files write deadline not set", "error", rerr)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.FormatInt(rawBytes, 10))
	w.WriteHeader(http.StatusOK)
	n, err := io.Copy(w, zr)
	if err == nil && n != rawBytes {
		err = fmt.Errorf("inflated %d bytes, expected %d", n, rawBytes)
	}
	return true, err
}
