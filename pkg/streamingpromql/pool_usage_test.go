// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPoolsHonourQueryPoisoning enforces that every pool in this engine drops a panicked query's
// memory instead of recycling it (see MemoryConsumptionTracker.Poison). Recovering from evaluation
// panics is only safe because that holds for every pool: a query whose operators were interrupted
// mid-way may still reference a slice it has returned, or return one twice, and recycling it would
// hand that memory to another query, possibly another tenant's. A new pool that bypasses the
// mechanism would reintroduce that risk silently, so this is checked here rather than left to review.
//
// The types package provides the sanctioned pool types and is exempt. Everywhere else:
//   - sync.Pool and zeropool are forbidden; use types.ObjectPool.
//   - pool.NewBucketedPool must be passed directly to types.NewLimitingBucketedPool.
func TestPoolsHonourQueryPoisoning(t *testing.T) {
	var violations []string
	fset := token.NewFileSet()

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if d.Name() == "types" || d.Name() == "testdata" {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		f, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}

		// A NewBucketedPool passed straight to NewLimitingBucketedPool is the sanctioned form, so
		// record those inner calls before looking for violations.
		wrapped := map[ast.Expr]bool{}
		ast.Inspect(f, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && isPackageSelector(call.Fun, "types", "NewLimitingBucketedPool") && len(call.Args) > 0 {
				wrapped[call.Args[0]] = true
			}

			return true
		})

		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SelectorExpr:
				if isPackageSelector(x, "sync", "Pool") {
					violations = append(violations, fmt.Sprintf("%s: sync.Pool is forbidden, use types.ObjectPool", fset.Position(x.Pos())))
				}
			case *ast.CallExpr:
				if isPackageSelector(x.Fun, "zeropool", "New") {
					violations = append(violations, fmt.Sprintf("%s: zeropool is forbidden, use types.ObjectPool", fset.Position(x.Pos())))
				}

				if isPackageSelector(x.Fun, "pool", "NewBucketedPool") && !wrapped[x] {
					violations = append(violations, fmt.Sprintf("%s: pool.NewBucketedPool must be passed directly to types.NewLimitingBucketedPool", fset.Position(x.Pos())))
				}
			}

			return true
		})

		return nil
	})
	require.NoError(t, err)
	require.Emptyf(t, violations, "pools that bypass query poisoning:\n%s", strings.Join(violations, "\n"))
}

// isPackageSelector reports whether e is the expression pkg.name.
func isPackageSelector(e ast.Expr, pkg, name string) bool {
	sel, ok := e.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	id, ok := sel.X.(*ast.Ident)
	return ok && id.Name == pkg && sel.Sel.Name == name
}
