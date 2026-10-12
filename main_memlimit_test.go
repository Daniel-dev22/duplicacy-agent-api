package main

import (
	"go/token"
	"testing"
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
