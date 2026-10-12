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
	"time"

	"github.com/gin-gonic/gin"
)

// A `duplicacy list -files` listing can be hundreds of MB of text. Nothing here
// ever holds one raw: output is gzipped as it is produced into a spool (RAM up
// to listSpoolMemMax, then a temp file under CONFIG_DIR), and the response is
// inflated back out of the spool only after the process exited 0.

const (
	listSpoolMemMax = 32 << 20 // gz bytes kept in RAM before spilling to disk
	listTailBytes   = 64 << 10 // output tail kept for error messages
	listSpoolSubdir = "list-spool"
	listWaitDelay   = 5 * time.Second
)

var (
	// listSpoolFileMax is the hard ceiling on one spilled gz listing.
	listSpoolFileMax int64 = 2 << 30
	// listFilesTimeout bounds one `list -files` run (var so tests can shrink it).
	listFilesTimeout = 5 * time.Minute

	errListQueueTimeout = errors.New("list-files queue wait exceeded")
	errSpoolFailed      = errors.New("list-files spool failed")
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
	release := func() { <-g.sem }
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
		return 0, fmt.Errorf("%w: exceeds %d bytes", errSpoolFailed, s.fileMax)
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

// cleanStaleSpool removes spill files left by a crashed process. Only files
// older than maxAge go, so a live spool of a concurrent request is never hit.
func cleanStaleSpool(dir string, maxAge time.Duration) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range ents {
		info, err := e.Info()
		if err == nil && !e.IsDir() && time.Since(info.ModTime()) > maxAge {
			os.Remove(filepath.Join(dir, e.Name()))
		}
	}
}

// tailRing keeps the last listTailBytes of output.
type tailRing struct {
	buf   [listTailBytes]byte
	total int64
}

func (r *tailRing) write(p []byte) {
	if len(p) >= len(r.buf) {
		copy(r.buf[:], p[len(p)-len(r.buf):])
		r.total += int64(len(p))
		return
	}
	pos := int(r.total % int64(len(r.buf)))
	n := copy(r.buf[pos:], p)
	copy(r.buf[:], p[n:])
	r.total += int64(len(p))
}

func (r *tailRing) String() string {
	if r.total <= int64(len(r.buf)) {
		return string(r.buf[:r.total])
	}
	pos := int(r.total % int64(len(r.buf)))
	return string(r.buf[pos:]) + string(r.buf[:pos])
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
	return listSpoolMemMax
}

// storeListing caches a COMPLETE listing unless its gz exceeds the row cap
// (served but not cached). Detached context: the request context may already be
// cancelled by the time the response is written.
func (a *app) storeListing(ctx context.Context, snapID string, rev int, storage, repoID string, res *listResult) error {
	if a.filesCache == nil || snapID == "" {
		return nil
	}
	if m := a.cfg.ListCacheRowMaxBytes; m > 0 && res.spool.n > m {
		slog.Info("list-files listing exceeds row cap, not cached",
			"repo", repoID, "snapshot_id", snapID, "rev", rev, "storage", storage, "gz_bytes", res.spool.n, "cap", m)
		return nil
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
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.FormatInt(rawBytes, 10))
	w.WriteHeader(http.StatusOK)
	n, err := io.Copy(w, zr)
	if err == nil && n != rawBytes {
		err = fmt.Errorf("inflated %d bytes, expected %d", n, rawBytes)
	}
	return true, err
}
