// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
)

// split-verify parses Go files and outputs a sorted list of top-level
// declarations with their content hashes. This is used to verify that
// file splits preserve every declaration intact.
//
// Usage:
//
//	split-verify <file1.go> [file2.go ...]
//
// Output is TSV: name, sha256 hash (first 16 hex chars).
// Compare the output of the original file against the split files to
// verify nothing was lost or modified.

type decl struct {
	Name string
	Hash string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <file1.go> [file2.go ...]\n", os.Args[0])
		os.Exit(1)
	}

	var decls []decl
	for _, path := range os.Args[1:] {
		fileDecls, err := extractDecls(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing %s: %v\n", path, err)
			os.Exit(1)
		}
		decls = append(decls, fileDecls...)
	}

	sort.SliceStable(decls, func(i, j int) bool {
		if decls[i].Name != decls[j].Name {
			return decls[i].Name < decls[j].Name
		}
		return decls[i].Hash < decls[j].Hash
	})

	for _, d := range decls {
		fmt.Printf("%s\t%s\n", d.Name, d.Hash)
	}
}

func extractDecls(path string) ([]decl, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, err
	}

	var result []decl

	for _, d := range f.Decls {
		switch dt := d.(type) {
		case *ast.FuncDecl:
			name := funcName(dt)
			body := extractSource(src, fset, d)
			result = append(result, decl{
				Name: name,
				Hash: hash(body),
			})

		case *ast.GenDecl:
			switch dt.Tok {
			case token.TYPE:
				for _, spec := range dt.Specs {
					ts := spec.(*ast.TypeSpec)
					body := extractSource(src, fset, spec)
					result = append(result, decl{
						Name: "type " + ts.Name.Name,
						Hash: hash(body),
					})
				}

			case token.VAR, token.CONST:
				names := collectNames(dt)
				body := extractSource(src, fset, d)
				result = append(result, decl{
					Name: dt.Tok.String() + " " + names,
					Hash: hash(body),
				})
			}
		}
	}

	return result, nil
}

func funcName(fd *ast.FuncDecl) string {
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		recv := fd.Recv.List[0].Type
		var typeName string
		switch rt := recv.(type) {
		case *ast.StarExpr:
			if ident, ok := rt.X.(*ast.Ident); ok {
				typeName = ident.Name
			}
		case *ast.Ident:
			typeName = rt.Name
		}
		return fmt.Sprintf("(%s).%s", typeName, fd.Name.Name)
	}
	return fd.Name.Name
}

func collectNames(gd *ast.GenDecl) string {
	var names []string
	for _, spec := range gd.Specs {
		switch s := spec.(type) {
		case *ast.ValueSpec:
			for _, n := range s.Names {
				names = append(names, n.Name)
			}
		}
	}
	if len(names) > 3 {
		return strings.Join(names[:3], ",") + ",..."
	}
	return strings.Join(names, ",")
}

func extractSource(src []byte, fset *token.FileSet, node ast.Node) string {
	start := fset.Position(node.Pos()).Offset
	end := fset.Position(node.End()).Offset
	return string(src[start:end])
}

func hash(s string) string {
	h := sha256.Sum256([]byte(s))
	return fmt.Sprintf("%x", h[:8])
}
