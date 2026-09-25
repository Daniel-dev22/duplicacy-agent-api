package main

// The manifest-first tree push against a stub controller that implements the
// controller's contract the way the router does: the manifest answers the rows
// whose stored hash differs, and the ingest stores SHA-256 of the tree bytes
// exactly as they arrive in the JSON body. If the agent hashed anything other
// than the bytes it sends, the stub — like the router — would ask for every
// tree on every cycle, and the second-cycle assertion below would fail.

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type stubController struct {
	mu        sync.Mutex
	stored    map[string]string // key -> sha256 hex of the stored tree bytes
	manifests int
	bodies    int
	sent      []string // keys received in bodies, in order
	fail      bool
}

func (s *stubController) handler(t *testing.T, keyField string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.fail {
			http.Error(w, "down", http.StatusNotFound)
			return
		}
		var body io.Reader = r.Body
		if strings.HasSuffix(r.URL.Path, "/manifest") {
			if r.Header.Get("Content-Encoding") != "" {
				t.Errorf("manifest was encoded %q; it is small and sent plain", r.Header.Get("Content-Encoding"))
			}
			s.manifests++
			var req struct {
				Trees []map[string]json.RawMessage `json:"trees"`
			}
			if err := json.NewDecoder(body).Decode(&req); err != nil {
				t.Errorf("manifest decode: %v", err)
				return
			}
			need := []map[string]json.RawMessage{}
			for _, e := range req.Trees {
				if _, has := e["tree"]; has {
					t.Error("the manifest carried a tree")
				}
				var k, sum string
				_ = json.Unmarshal(e[keyField], &k)
				_ = json.Unmarshal(e["sha256"], &sum)
				if s.stored[k] != sum {
					need = append(need, e)
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"need": need})
			return
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Errorf("tree body sent without gzip")
		} else {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				t.Errorf("body is not gzip: %v", err)
				return
			}
			body = zr
		}
		s.bodies++
		var req struct {
			Trees []map[string]json.RawMessage `json:"trees"`
		}
		if err := json.NewDecoder(body).Decode(&req); err != nil {
			t.Errorf("body decode: %v", err)
			return
		}
		for _, e := range req.Trees {
			var k string
			_ = json.Unmarshal(e[keyField], &k)
			sum := sha256.Sum256(e["tree"]) // what the router stores
			s.stored[k] = hex.EncodeToString(sum[:])
			s.sent = append(s.sent, k)
		}
		_, _ = w.Write([]byte(`{"ingested":1}`))
	})
}

func newStubWalker(t *testing.T, roots []string, keyField string) (*treeWalker, *stubController) {
	t.Helper()
	stub := &stubController{stored: map[string]string{}}
	srv := httptest.NewServer(stub.handler(t, keyField))
	t.Cleanup(srv.Close)
	w := newTreeWalker(Config{NodeName: "host-a", SiteID: "site-a", ControlCenterURL: srv.URL, BackupRoots: roots},
		nil, &app{controlCenterClient: srv.Client()})
	return w, stub
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
	w, stub := newStubWalker(t, []string{a, b}, "root_path")
	ctx := context.Background()

	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if stub.bodies != 1 || len(stub.sent) != 2 {
		t.Fatalf("first cycle: %d bodies carrying %v, want both trees once", stub.bodies, stub.sent)
	}

	// Nothing changed: the manifest still goes (it keeps the rows alive for
	// the controller's reaper), and no tree does.
	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("second push: %v", err)
	}
	if stub.manifests != 2 || stub.bodies != 1 {
		t.Fatalf("unchanged cycle: manifests=%d bodies=%d, want 2/1 — the agent's hash "+
			"does not match the bytes it sends", stub.manifests, stub.bodies)
	}

	// One tree changes. The directory's mtime moves, so the walker re-reads it.
	if err := os.WriteFile(filepath.Join(b, "new.txt"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	stub.sent = nil
	if err := w.pushNodeTrees(ctx); err != nil {
		t.Fatalf("third push: %v", err)
	}
	if len(stub.sent) != 1 || stub.sent[0] != b {
		t.Fatalf("one changed tree: sent %v, want just %s", stub.sent, b)
	}
}

func TestTreePushSendsNoBodyWhenTheManifestFails(t *testing.T) {
	w, stub := newStubWalker(t, []string{mkTree(t, "a.txt")}, "root_path")
	stub.fail = true
	if err := w.pushNodeTrees(context.Background()); err == nil {
		t.Fatal("a failed manifest was not reported")
	}
	if stub.bodies != 0 {
		t.Errorf("sent %d bodies after the manifest failed", stub.bodies)
	}
}

// pushTreeLane in isolation: the reply names keys the agent did not offer, or
// names one twice — neither may send a tree twice or a tree it never had.
func TestPushTreeLaneSendsOnlyOfferedItemsOnce(t *testing.T) {
	items := []nodeTreeOut{
		{Node: "n", Site: "s", RootPath: "/a", SHA256: "1", Tree: json.RawMessage(`{"a":1}`)},
		{Node: "n", Site: "s", RootPath: "/b", SHA256: "2", Tree: json.RawMessage(`{"b":1}`)},
	}
	var sent []nodeTreeOut
	post := func(_ context.Context, path string, body any, out any, gz bool) error {
		if strings.HasSuffix(path, "/manifest") {
			reply := `{"need":[{"root_path":"/b"},{"root_path":"/b"},{"root_path":"/never-offered"}]}`
			return json.Unmarshal([]byte(reply), out)
		}
		if !gz {
			t.Error("tree body not gzip-flagged")
		}
		b, _ := json.Marshal(body)
		var got struct {
			Trees []nodeTreeOut `json:"trees"`
		}
		_ = json.Unmarshal(b, &got)
		sent = got.Trees
		return nil
	}
	err := pushTreeLane(context.Background(), post, "/lane", items,
		func(t nodeTreeOut) string { return t.RootPath },
		func(t nodeTreeOut) nodeTreeOut { t.Tree = nil; return t })
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0].RootPath != "/b" || !bytes.Equal(sent[0].Tree, items[1].Tree) {
		t.Fatalf("sent %+v, want only /b with its tree", sent)
	}
}
