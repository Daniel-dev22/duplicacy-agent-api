package main

import (
	"context"
	"encoding/json"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/Daniel-dev22/agent-kit-go/memlimit"
	"github.com/gin-gonic/gin"
)

// TestMemLimitWiredBeforeApp pins the wiring: main() must call memlimit.Apply
// before newApp opens any database, so the soft limit is in force first.
func TestMemLimitWiredBeforeApp(t *testing.T) {
	body := funcBody(t, "main.go", "main")
	memPos, appPos := token.NoPos, token.NoPos
	for _, ce := range topLevelCalls(body) {
		switch callName(ce) {
		case "memlimit.Apply":
			if memPos == token.NoPos {
				memPos = ce.Pos()
			}
		case "newApp":
			if appPos == token.NoPos {
				appPos = ce.Pos()
			}
		}
	}
	if memPos == token.NoPos {
		t.Fatal("main() never calls memlimit.Apply as a top-level statement (a call under `if` or in a closure does not count)")
	}
	if appPos == token.NoPos {
		t.Fatal("anchor call newApp not found as a top-level statement; update this test")
	}
	if memPos > appPos {
		t.Fatal("memlimit.Apply must run before newApp")
	}
}

func TestMainStartsPeakAnonAndTreatsInvalidConfigAsFatal(t *testing.T) {
	body := funcBody(t, "main.go", "main")
	if !hasTopLevelCall(body, "memlimit.StartPeakAnon") {
		t.Fatal("main must start the peak-anon sampler as a top-level statement")
	}
	live := liveCalls(body)
	if !live["errors.Is"] || !live["Exit"] {
		t.Fatal("main must errors.Is(err, memlimit.ErrInvalidConfig) and exit")
	}
	src, _ := os.ReadFile("main.go")
	if !strings.Contains(string(src), "memlimit.ErrInvalidConfig") {
		t.Fatal("fatal check must test memlimit.ErrInvalidConfig")
	}
	df, _ := os.ReadFile("Dockerfile")
	if !strings.Contains(string(df), `LABEL agent.memlimit="1"`) {
		t.Fatal("Dockerfile must carry LABEL agent.memlimit=1")
	}
}

func TestReadinessExposesAnonPeak(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dir := t.TempDir()
	os.WriteFile(dir+"/self", []byte("0::/\n"), 0o644)
	os.WriteFile(dir+"/memory.stat", []byte("anon 4096\nfile 1\n"), 0o644)
	old := anonPeak
	anonPeak = memlimit.StartPeakAnon(ctx, nil, memlimit.PeakOptions{Root: dir, SelfCgroup: dir + "/self"})
	t.Cleanup(func() { anonPeak = old })
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	(&app{}).handleReadiness(c)
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["anon_bytes"] != float64(4096) || got["anon_peak_bytes"] != float64(4096) {
		t.Fatalf("readiness lacks anon fields: %s", rec.Body.String())
	}
}
