package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/dirsize"
)

// These three tests exercise trees.go (the structural walk and the size annotation
// that reads the kit's cache); they stayed with the agent when the gatherer moved.

func TestWalkDirFileSizes(t *testing.T) {
	dir := t.TempDir()
	writeFileN(t, filepath.Join(dir, "a.txt"), 10)
	writeFileN(t, filepath.Join(dir, "b.bin"), 250)
	if err := os.MkdirAll(filepath.Join(dir, "sub"), 0700); err != nil {
		t.Fatal(err)
	}

	w := newTreeWalker(Config{}, nil, nil)
	st, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	children := w.walkDir(dir, st.ModTime(), 0)

	got := map[string]*treeNode{}
	for _, c := range children {
		got[c.Name] = c
	}
	if got["a.txt"] == nil || got["a.txt"].Size != 10 {
		t.Errorf("a.txt size = %v, want 10", got["a.txt"])
	}
	if got["b.bin"] == nil || got["b.bin"].Size != 250 {
		t.Errorf("b.bin size = %v, want 250", got["b.bin"])
	}
	// Directory nodes get Size from annotateSizes (the gatherer cache), NOT from
	// walkDir — so the structural walk leaves dir Size at 0.
	if got["sub"] == nil || got["sub"].Type != "directory" || got["sub"].Size != 0 {
		t.Errorf("sub dir from walkDir = %v, want directory with Size 0", got["sub"])
	}
}

func TestWalkDirTruncatedSkipsFileStats(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < treeMaxFilesPerDir+5; i++ {
		writeFileN(t, filepath.Join(dir, fmt.Sprintf("f%04d", i)), 1)
	}
	w := newTreeWalker(Config{}, nil, nil)
	st, err := os.Lstat(dir)
	if err != nil {
		t.Fatal(err)
	}
	children := w.walkDir(dir, st.ModTime(), 0)
	if len(children) != 1 || children[0].Type != "truncated" {
		t.Fatalf("want single truncated marker, got %d children", len(children))
	}
	if children[0].FileCount != treeMaxFilesPerDir+5 {
		t.Errorf("truncated FileCount = %d, want %d", children[0].FileCount, treeMaxFilesPerDir+5)
	}
}

func TestAnnotateSizes(t *testing.T) {
	cache := newSizeCache(Config{ConfigDir: t.TempDir()})
	cache.Put("/x", dirsize.Entry{Bytes: 100})
	cache.Put("/x/d", dirsize.Entry{Bytes: 40})
	// /x/d2 intentionally not cached → Size stays 0 (omitted).

	w := &treeWalker{app: &app{sizes: cache}}
	tree := &treeNode{
		Name: "x", Path: "/x", Type: "directory",
		Children: []*treeNode{
			{Name: "f", Path: "/x/f", Type: "file"},
			{Name: "d", Path: "/x/d", Type: "directory"},
			{Name: "d2", Path: "/x/d2", Type: "directory"},
		},
	}
	w.annotateSizes(tree)

	if tree.Size != 100 {
		t.Errorf("root size = %d, want 100", tree.Size)
	}
	if tree.Children[0].Size != 0 {
		t.Errorf("file node should not be sized, got %d", tree.Children[0].Size)
	}
	if tree.Children[1].Size != 40 {
		t.Errorf("/x/d size = %d, want 40", tree.Children[1].Size)
	}
	if tree.Children[2].Size != 0 {
		t.Errorf("uncached dir should stay 0, got %d", tree.Children[2].Size)
	}
}
