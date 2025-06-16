package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

var Analyzer = &analysis.Analyzer{
	Name: "lintpooledobjects",
	Doc:  "checks for misuse of pooled objects",
	Run:  runAnalysis,
}

const putMethodName = "Put"

func runAnalysis(pass *analysis.Pass) (any, error) {
	var limitingBucketedPoolType types.Type

	// Check the current package for the limiting pool type (in the case where the package being analysed contains the type)...
	limitingBucketedPoolType, err := findLimitingBucketedPoolType(pass.Pkg)
	if err != nil {
		return nil, err
	}

	// ...and if it's not there, check all the imported packages too.
	if limitingBucketedPoolType == nil {
		for _, pkg := range pass.Pkg.Imports() {
			limitingBucketedPoolType, err = findLimitingBucketedPoolType(pkg)
			if err != nil {
				return nil, err
			} else if limitingBucketedPoolType != nil {
				break
			}
		}
	}

	// We couldn't find the limiting pool type in the package or its imports, so we are done.
	if limitingBucketedPoolType == nil {
		// Nothing to do, the 'types' package is not imported.
		return nil, nil
	}

	limitingBucketedPoolTypeName, err := typeNameWithoutGenericParameters(limitingBucketedPoolType)
	if err != nil {
		return nil, err
	}

	for _, file := range pass.Files {
		ast.Inspect(file, func(n ast.Node) bool {
			expr, isExpr := n.(*ast.ExprStmt)
			if !isExpr {
				return true
			}

			call, isCallExpr := expr.X.(*ast.CallExpr)
			if !isCallExpr {
				return true
			}

			selector, isSelector := call.Fun.(*ast.SelectorExpr)
			if !isSelector {
				return true
			}

			if selector.Sel.Name != putMethodName {
				// Call doesn't invoke a method called Put. Nothing more to do.
				return true
			}

			callTargetType, isPointer := pass.TypesInfo.TypeOf(selector.X).(*types.Pointer)
			if !isPointer {
				return true
			}

			callTargetTypeName, err := typeNameWithoutGenericParameters(callTargetType.Elem())
			if err != nil {
				// Call target isn't generic, so can't be a LimitingBucketedPool.
				return true
			}

			if callTargetTypeName != limitingBucketedPoolTypeName {
				return true
			}

			pass.Report(analysis.Diagnostic{
				Pos:     call.Pos(),
				End:     call.End(),
				Message: fmt.Sprintf("return value from '%v' not used", putMethodName),
			})

			return true
		})
	}

	return nil, nil
}

func findLimitingBucketedPoolType(pkg *types.Package) (types.Type, error) {
	if pkg.Path() != "github.com/grafana/mimir/pkg/streamingpromql/types" {
		return nil, nil
	}

	o := pkg.Scope().Lookup("LimitingBucketedPool")
	if o == nil {
		return nil, fmt.Errorf("type LimitingBucketedPool not found in package %v", pkg.Path())
	}

	return o.Type(), nil
}

// HACK: it does not seem to be straightforward to take an instantiated types.Type (eg. LimitingBucketedPool[[]int, int])
// and get the generic type (eg. LimitingBucketedPool[E, T]), or at least I can't find how to do this.
//
// So we instead resort to this, which seems robust enough.
func typeNameWithoutGenericParameters(t types.Type) (string, error) {
	name, _, ok := strings.Cut(t.String(), "[")
	if !ok {
		return "", fmt.Errorf("expected %v type to be generic, but it has no generic parameters", t)
	}

	return name, nil
}
