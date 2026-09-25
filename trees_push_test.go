package main

// The manifest-first tree push against a stub controller that enforces what
// the router enforces: manifest entries need every key field and a 64-hex
// sha256; a PUT needs its key in the query and a gzip body whose uncompressed
// bytes hash to the declared sha256 — anything else is a 400, exactly as the
// router answers. The stub stores the hash it COMPUTED, so if the agent ever
// declared a hash other than that of the bytes it sends, the PUT would be
// refused and the second-cycle assertions would fail.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type stubController struct {
	mu        sync.Mutex
	keyField  string
	stored    map[string]string // key -> sha256 hex of the stored bytes
	manifests int
	puts      []string // keys PUT, in order
	refused   int
	down      bool
}

func (s *stubController) handler(t *testing.T) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.down {
			http.Error(w, "down", http.StatusNotFound)
			return
		}
		refuse := func(msg string) { s.refused++; http.Error(w, msg, http.StatusBadRequest) }
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/manifest"):
			s.manifests++
			var req struct {
				Trees []map[string]string `json:"trees"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				refuse("manifest decode")
				return
			}
			need := []map[string]string{}
			for _, e := range req.Trees {
				for _, f := range []string{"node", "site", s.keyField, "sha256"} {
					if e[f] == "" {
						refuse("missing " + f)
						return
					}
				}
				if s.keyField == "repo_id" && e["source_path"] == "" {
					refuse("missing source_path")
					return
				}
				if b, err := hex.DecodeString(e["sha256"]); err != nil || len(b) != 32 {
					refuse("bad sha256")
					return
				}
				if _, has := e["tree"]; has {
					t.Error("the manifest carried a tree")
				}
				if s.stored[e[s.keyField]] != e["sha256"] {
					need = append(need, e)
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"need": need})
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/tree"):
			q := r.URL.Query()
			names := []string{"site", "node", s.keyField, "sha256"}
			if s.keyField == "repo_id" {
				names = append(names, "source_path")
			}
			for _, n := range names {
				if q.Get(n) == "" {
					refuse("missing " + n)
					return
				}
			}
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				refuse("not gzip")
				return
			}
			raw, err := io.ReadAll(zr)
			if err != nil {
				refuse("bad gzip")
				return
			}
			if !json.Valid(raw) || !bytes.HasPrefix(raw, []byte("{")) {
				refuse("not a JSON object")
				return
			}
			sum := sha256.Sum256(raw)
			if hex.EncodeToString(sum[:]) != q.Get("sha256") {
				refuse("body does not hash to the declared sha256")
				return
			}
			s.stored[q.Get(s.keyField)] = hex.EncodeToString(sum[:])
			s.puts = append(s.puts, q.Get(s.keyField))
			_, _ = w.Write([]byte(`{"changed":true}`))
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.Error(w, "no route", http.StatusNotFound)
		}
	})
}

func newStubWalker(t *testing.T, cfg Config, repos *repoIndex, keyField string) (*treeWalker, *stubController) {
	t.Helper()
	stub := &stubController{keyField: keyField, stored: map[string]string{}}
	srv := httptest.NewServer(stub.handler(t))
	t.Cleanup(srv.Close)
	cfg.NodeName, cfg.SiteID, cfg.ControlCenterURL = "host-a", "site-a", srv.URL
	return newTreeWalker(cfg, repos, &app{controlCenterClient: srv.Client()}), stub
}

func mkTree(t *testing.T, files ...string) string {
	t.Helper()
	dir := t.TempDir()
	for _, f := range files {
		p := filepath.Join(dir, f)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestNodeTreePushSendsOnlyWhatTheControllerLacks(t *testing.T) {
	a := mkTree(t, "one/a.txt", "two/b.txt")
	b := mkTree(t, "three/c.txt")
	w, stub := newStubWalker(t, Config{BackupRoots: []string{a, b}}, nil, "root_path")
	ctx := context.Background()

	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if len(stub.puts) != 2 || stub.refused != 0 {
		t.Fatalf("first cycle: put %v, refused %d — want both trees once", stub.puts, stub.refused)
	}

	// Nothing changed: the manifest still goes (it keeps the rows alive for the
	// controller's reaper), and no tree does.
	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("second push: %v", err)
	}
	if stub.manifests != 2 || len(stub.puts) != 2 {
		t.Fatalf("unchanged cycle: manifests=%d puts=%v — the agent's hash does not "+
			"match the bytes it sends", stub.manifests, stub.puts)
	}

	// One tree changes. The directory's mtime moves, so the walker re-reads it.
	if err := os.WriteFile(filepath.Join(b, "new.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	stub.puts = nil
	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("third push: %v", err)
	}
	if len(stub.puts) != 1 || stub.puts[0] != b {
		t.Fatalf("one changed tree: put %v, want just %s", stub.puts, b)
	}
}

func TestRepoTreePushSendsOnlyWhatTheControllerLacks(t *testing.T) {
	src := mkTree(t, "data/a.txt")
	repos := &repoIndex{repos: map[string]*Repo{
		"short": {ID: "short", SnapshotID: "host-a-data", SourcePath: src, Path: src},
	}}
	w, stub := newStubWalker(t, Config{}, repos, "repo_id")
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if err := w.pushRepoTrees(ctx); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if stub.refused != 0 || len(stub.puts) != 1 || stub.puts[0] != "host-a-data" || stub.manifests != 2 {
		t.Fatalf("repo lane: manifests=%d puts=%v refused=%d, want one PUT keyed by snapshot id",
			stub.manifests, stub.puts, stub.refused)
	}
}

func TestTreePushSendsNothingWhenTheManifestFails(t *testing.T) {
	w, stub := newStubWalker(t, Config{BackupRoots: []string{mkTree(t, "a.txt")}}, nil, "root_path")
	stub.down = true
	if err := w.pushNodeTrees(context.Background()); err == nil {
		t.Fatal("a failed manifest was not reported")
	}
	if len(stub.puts) != 0 {
		t.Errorf("sent %d trees after the manifest failed", len(stub.puts))
	}
}

// pushTreeLane in isolation: the reply names keys the agent did not offer, or
// names one twice — neither may send a tree twice or a tree it never had; one
// failing PUT does not stop the others.
func TestPushTreeLaneSendsOnlyOfferedItemsOnce(t *testing.T) {
	items := []laneItem[nodeTreeEntry]{
		{entry: nodeTreeEntry{Node: "n", Site: "s", RootPath: "/a", SHA256: "1"}, tree: &treeNode{Name: "a"}},
		{entry: nodeTreeEntry{Node: "n", Site: "s", RootPath: "/b", SHA256: "2"}, tree: &treeNode{Name: "b"}},
		{entry: nodeTreeEntry{Node: "n", Site: "s", RootPath: "/c", SHA256: "3"}, tree: &treeNode{Name: "c"}},
	}
	var put []string
	post := func(_ context.Context, method, path string, body any, out any) error {
		if method == http.MethodPost {
			return json.Unmarshal([]byte(`{"need":[{"root_path":"/b"},{"root_path":"/b"},
				{"root_path":"/c"},{"root_path":"/never-offered"}]}`), out)
		}
		if _, ok := body.([]byte); !ok {
			t.Errorf("PUT body is %T, want the gzip bytes", body)
		}
		u, _ := url.Parse(path)
		put = append(put, u.Query().Get("root_path"))
		if u.Query().Get("root_path") == "/b" {
			return errors.New("HTTP 500")
		}
		return nil
	}
	err := pushTreeLane(context.Background(), post, "/lane", items,
		func(e nodeTreeEntry) string { return e.RootPath },
		func(e nodeTreeEntry) url.Values { return url.Values{"root_path": {e.RootPath}} })
	if err == nil {
		t.Error("a failed PUT was not reported")
	}
	if strings.Join(put, ",") != "/b,/c" {
		t.Fatalf("put %v, want /b then /c, once each", put)
	}
}

// The hash declared in the manifest is the hash of exactly the bytes gzipped
// into the PUT — the whole contract, pinned without a stub.
func TestTreeSumIsTheHashOfThePutBody(t *testing.T) {
	tree := &treeNode{Name: "<a&b>", Path: "/x/ é", Type: "directory",
		Children: []*treeNode{{Name: "f", Path: "/x/f", Type: "file", Size: 3}}}
	sum, err := treeSum(tree)
	if err != nil {
		t.Fatal(err)
	}
	gz, err := gzipTree(tree)
	if err != nil {
		t.Fatal(err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(zr)
	got := sha256.Sum256(raw)
	if hex.EncodeToString(got[:]) != sum {
		t.Fatal("the manifest hash is not the hash of the PUT body")
	}
}
