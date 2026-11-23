package main

import (
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

// TestSizeOptionsMapsEveryField pins the whole Config -> dirsize.Options seam. Each
// TreeSize* setting carries a distinct non-zero value so a field mapped to the wrong
// target, or not mapped at all, fails here. The reflect sweep fails when the kit adds
// a scalar Options field that sizeOptions does not populate.
func TestSizeOptionsMapsEveryField(t *testing.T) {
	cfg := Config{
		BackupRoots:                []string{"/r1", "/r2"},
		BackupExcludePaths:         []string{"/bk"},
		TreeSizeEnabled:            true,
		TreeSizeSmallRefresh:       3 * time.Hour,
		TreeSizeLargeRefresh:       25 * time.Hour,
		TreeSizeLargeFileThreshold: 12345,
		TreeSizeSlowWalk:           31 * time.Second,
		TreeSizeWalkTimeout:        29 * time.Minute,
		TreeSizeStepSleep:          7 * time.Millisecond,
		TreeSizeStepEvery:          17,
		TreeSizeKeepDepth:          4,
		TreeSizeKeepMinFiles:       99,
		TreeSizeMaxEntries:         5555,
		TreeSizeExcludePaths:       []string{"/env"},
	}
	a := wiredApp(t, []string{"/pushed"}, &Repo{Path: "/repo/p", SourcePath: "/repo/s"})
	o := sizeOptions(cfg, a)

	checks := []struct {
		name      string
		got, want any
	}{
		{"Enabled", o.Enabled, cfg.TreeSizeEnabled},
		{"SmallRefresh", o.SmallRefresh, cfg.TreeSizeSmallRefresh},
		{"LargeRefresh", o.LargeRefresh, cfg.TreeSizeLargeRefresh},
		{"LargeFileThreshold", o.LargeFileThreshold, cfg.TreeSizeLargeFileThreshold},
		{"SlowWalk", o.SlowWalk, cfg.TreeSizeSlowWalk},
		{"WalkTimeout", o.WalkTimeout, cfg.TreeSizeWalkTimeout},
		{"StepSleep", o.StepSleep, cfg.TreeSizeStepSleep},
		{"StepEvery", o.StepEvery, cfg.TreeSizeStepEvery},
		{"KeepDepth", o.KeepDepth, cfg.TreeSizeKeepDepth},
		{"KeepMinFiles", o.KeepMinFiles, cfg.TreeSizeKeepMinFiles},
		{"MaxEntries", o.MaxEntries, cfg.TreeSizeMaxEntries},
		{"Roots", o.Roots(), cfg.BackupRoots},
		{"ExcludePaths", o.ExcludePaths(), []string{"/bk", "/env", "/pushed"}},
		{"Anchors", o.Anchors(), []string{"/repo/p", "/repo/s"}},
	}
	for _, c := range checks {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if !reflect.DeepEqual(o.ExcludeBasenames, excludeBasenames) {
		t.Errorf("ExcludeBasenames not mapped to excludeBasenames")
	}
	if o.SkipPath == nil || !o.SkipPath(filepath.Join("/x", "cache", "localhost", "y")) || o.SkipPath("/x/keep") {
		t.Errorf("SkipPath is not the Duplicacy Web cache rule")
	}
	if o.Stop == nil {
		t.Errorf("Stop channel not wired")
	}

	v := reflect.ValueOf(o)
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).IsZero() {
			t.Errorf("Options.%s is zero: sizeOptions does not populate it", v.Type().Field(i).Name)
		}
	}
}

func TestSizeExcludePathsMergesEnvAndPushed(t *testing.T) {
	cfg := Config{BackupExcludePaths: []string{"/bk"}, TreeSizeExcludePaths: []string{"/env"}}

	if got := sizeExcludePaths(cfg, nil); !slices.Equal(got, []string{"/bk", "/env"}) {
		t.Errorf("nil app = %v, want env lists only", got)
	}
	a := wiredApp(t, []string{"/pushed-a", "/pushed-b"})
	if got := sizeExcludePaths(cfg, a); !slices.Equal(got, []string{"/bk", "/env", "/pushed-a", "/pushed-b"}) {
		t.Errorf("merged = %v, want env then pushed", got)
	}
}

func TestSizeOptionsNilAppDoesNotPanic(t *testing.T) {
	o := sizeOptions(Config{BackupRoots: []string{"/r"}}, nil)
	if o.Stop != nil {
		t.Errorf("nil app must yield a nil Stop channel")
	}
	if got := o.ExcludePaths(); len(got) != 0 {
		t.Errorf("ExcludePaths = %v, want none", got)
	}
	if got := o.Anchors(); got != nil {
		t.Errorf("Anchors = %v, want nil", got)
	}
}

func TestSizeAnchorsNilSafe(t *testing.T) {
	if got := sizeAnchors(nil); got != nil {
		t.Errorf("nil app = %v", got)
	}
	if got := sizeAnchors(&app{}); got != nil {
		t.Errorf("nil repo index = %v", got)
	}
}
