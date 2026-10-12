package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// TestMemLimitWiredBeforeApp pins the wiring: main() must call memlimit.Apply
// before newApp opens any database, so the soft limit is in force first.
func TestMemLimitWiredBeforeApp(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var memPos, appPos token.Pos
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "main" {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := c.Fun.(type) {
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && id.Name == "memlimit" && fn.Sel.Name == "Apply" && memPos == 0 {
					memPos = c.Pos()
				}
			case *ast.Ident:
				if fn.Name == "newApp" && appPos == 0 {
					appPos = c.Pos()
				}
			}
			return true
		})
	}
	if memPos == 0 {
		t.Fatal("main() never calls memlimit.Apply")
	}
	if appPos == 0 {
		t.Fatal("anchor call newApp not found; update this test")
	}
	if memPos > appPos {
		t.Fatal("memlimit.Apply must run before newApp")
	}
}
