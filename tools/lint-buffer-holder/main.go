// SPDX-License-Identifier: AGPL-3.0-only

// Command lint-buffer-holder checks that types used as gRPC requests and
// contain references to the gRPC buffer from which they were unmarshalled
// embed the mimirpb.BufferHolder type to avoid use-after-release bugs.
package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/grafana/dskit/flagext"
	"golang.org/x/tools/go/packages"
)

func main() {
	args := must(flagext.ParseFlagsAndArguments(new(flag.FlagSet)))
	ok := new(linter).run(args...)
	if !ok {
		os.Exit(1)
	}
}

func (l *linter) run(dirs ...string) (ok bool) {
	l.fset = new(token.FileSet)

	services := slices.Collect(l.findGRPCServices(dirs...))
	var pkgPaths []string
	for _, s := range services {
		pkgPaths = append(pkgPaths, "./"+filepath.Dir(s.protoPath))
	}

	packages := must(packages.Load(&packages.Config{
		Fset:  l.fset,
		Mode:  packages.LoadAllSyntax,
		Tests: false,
	}, pkgPaths...))
	l.fillPackages(packages)

	l.seen = map[types.Type]bool{}
	for _, p := range packages {
		for _, service := range services {
			if p.Dir != must(filepath.Abs(filepath.Dir(service.protoPath))) {
				continue
			}
			l.analyzeGRPCServer(p, service.name)
		}
	}

	return !l.failed
}

func (l *linter) fillPackages(pkgs []*packages.Package) {
	l.packages = make(map[string]*packages.Package)

	var visit func(*packages.Package)
	visit = func(pkg *packages.Package) {
		if _, seen := l.packages[pkg.PkgPath]; seen {
			return
		}
		l.packages[pkg.PkgPath] = pkg

		for _, imp := range pkg.Imports {
			visit(imp)
		}
	}

	for _, pkg := range pkgs {
		visit(pkg)
	}

	l.contextContext = l.packages["context"].Types.Scope().Lookup("Context").(*types.TypeName).Type()
	l.errorType = types.Universe.Lookup("error").(*types.TypeName).Type()
	l.messageWithBufferRef = l.packages["github.com/grafana/mimir/pkg/mimirpb"].Types.Scope().Lookup("MessageWithBufferRef").(*types.TypeName).Type().Underlying().(*types.Interface)
	l.unsafeMutableString = l.packages["github.com/grafana/mimir/pkg/mimirpb"].Types.Scope().Lookup("UnsafeMutableString").(*types.TypeName).Type().(*types.Alias)
}

type grpcService struct {
	protoPath string
	name      string
}

// findGRPCServices finds all gRPC services in .proto files under ./pkg.
// It returns an iterator that yields ServiceInfo for each service found.
func (l *linter) findGRPCServices(dirs ...string) iter.Seq[grpcService] {
	if len(dirs) == 0 {
		dirs = []string{"./pkg"}
	}
	return func(yield func(grpcService) bool) {
		// Walk the directory tree looking for .pb.go files
		for _, dir := range dirs {
			try(filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
				if err != nil {
					return err
				}

				if d.IsDir() || !strings.HasSuffix(d.Name(), ".pb.go") {
					return nil
				}

				// Parse the proto file for service definitions
				services, err := l.parseProtoServices(path)
				if err != nil {
					return fmt.Errorf("failed to parse %s: %w", path, err)
				}

				// Yield each service found
				for _, serviceName := range services {
					if !yield(grpcService{
						protoPath: path,
						name:      serviceName,
					}) {
						return filepath.SkipAll
					}
				}

				return nil
			}))
		}
	}
}

// parseProtoServices extracts service names from a proto generated file.
// It finds all grpc.ServiceDesc.ServiceName.
func (l *linter) parseProtoServices(filePath string) ([]string, error) {
	content := must(os.ReadFile(filePath))
	file := must(parser.ParseFile(l.fset, filePath, content, 0))

	var services []string
	ast.Inspect(file, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := cl.Type.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		ident, ok := sel.X.(*ast.Ident)
		if !ok || ident.Name != "grpc" || sel.Sel.Name != "ServiceDesc" {
			return true
		}

		// Extract ServiceName field
		for _, elt := range cl.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if kv.Key.(*ast.Ident).Name != "ServiceName" {
				continue
			}
			// Unquote the string and split by "."
			fullName := must(strconv.Unquote(kv.Value.(*ast.BasicLit).Value))
			_, name, _ := strings.Cut(fullName, ".")
			services = append(services, name)
		}

		return true
	})

	return services, nil
}

type linter struct {
	fset     *token.FileSet
	packages map[string]*packages.Package

	contextContext       types.Type
	errorType            types.Type
	messageWithBufferRef *types.Interface
	unsafeMutableString  *types.Alias

	seen   map[types.Type]bool
	failed bool
}

func (l *linter) analyzeGRPCServer(pkg *packages.Package, serviceName string) {
	obj := pkg.Types.Scope().Lookup(serviceName + "Server")
	typeName, ok := obj.(*types.TypeName)
	if !ok {
		l.report(obj.Pos(), "expected %s to be a type name, found %T", obj.Name(), obj)
		return
	}
	iface, ok := typeName.Type().Underlying().(*types.Interface)
	if !ok {
		l.report(obj.Pos(), "expected %s to be an interface, found %T", obj.Name(), typeName.Type().Underlying())
		return
	}

	for req := range l.extractRequests(iface) {
		l.analyzeMessage(pkg, serviceName, req)
	}
}

// extractRequests extracts the types used to hold gRPC request messages from a server interface.
func (l *linter) extractRequests(iface *types.Interface) iter.Seq[types.Type] {
	return func(yield func(types.Type) bool) {
		for method := range iface.Methods() {
			for param := range method.Signature().Params().Variables() {
				if param.Type() == l.contextContext {
					continue
				}

				switch ptyp := param.Type().Underlying().(type) {
				case *types.Pointer, *types.Struct:
					if !yield(ptyp) {
						return
					}

				case *types.Interface: // Stream server
					for ptr := range l.extractStreamRequests(ptyp) {
						if !yield(ptr) {
							return
						}
					}
				}
			}
		}
	}
}

func (l *linter) extractStreamRequests(stream *types.Interface) iter.Seq[types.Type] {
	return func(yield func(types.Type) bool) {
		for m := range stream.Methods() {
			if m.Name() != "Recv" {
				continue
			}
			for result := range m.Signature().Results().Variables() {
				if result.Type() == l.errorType {
					continue
				}

				switch rtyp := result.Type().Underlying().(type) {
				case *types.Pointer, *types.Struct:
					if !yield(rtyp) {
						return
					}
				}
			}
		}
	}
}

func (l *linter) analyzeMessage(pkg *packages.Package, serverName string, typ types.Type) {
	isBufferHolder := types.Implements(typ, l.messageWithBufferRef)
	if isBufferHolder {
		return
	}

	named, _ := typ.(*types.Named)
	struc, ok := typ.Underlying().(*types.Struct)
	if !ok {
		if ptr, ok := typ.Underlying().(*types.Pointer); ok {
			named, _ = ptr.Elem().(*types.Named)
			struc, _ = ptr.Elem().Underlying().(*types.Struct)
		}
	}
	if struc == nil || named == nil {
		panic(fmt.Errorf("expected %v to be a struct or pointer to struct, got %T", typ, typ))
	}

	for refPath := range l.referencesToBuffer(named.Obj().Pkg(), struc) {
		l.report(named.Obj().Pos(), "%s used as request in \"%s\".%s should embed mimirpb.BufferHolder because it has a reference to the buffer through:\n\t%v", named.Obj().Name(), pkg.PkgPath, serverName, formatRefPath(refPath))
	}
}

func (l *linter) referencesToBuffer(pkg *types.Package, typ types.Type) iter.Seq[[]string] {
	return func(yield func([]string) bool) {
		if l.seen[typ] {
			return
		}
		l.seen[typ] = true

		switch typ := typ.(type) {
		case *types.Pointer:
			for ref := range l.referencesToBuffer(pkg, typ.Elem()) {
				if !yield(ref) {
					return
				}
			}
		case *types.Named:
			for ref := range l.referencesToBuffer(typ.Obj().Pkg(), typ.Underlying()) {
				if !yield(ref) {
					return
				}
			}
		case *types.Struct:
			for field := range typ.Fields() {
				if strings.HasPrefix(field.Name(), "XXX_") {
					continue
				}
				for ref := range l.referencesToBuffer(pkg, field.Type()) {
					ref := append([]string{field.Name()}, ref...)
					if !yield(ref) {
						return
					}
				}
			}
		case *types.Interface:
			for _, name := range pkg.Scope().Names() {
				decl, ok := pkg.Scope().Lookup(name).(*types.TypeName)
				if !ok {
					continue
				}
				implements := types.Implements(decl.Type(), typ) || types.Implements(types.NewPointer(decl.Type()), typ)
				if !implements {
					continue
				}

				for ref := range l.referencesToBuffer(pkg, decl.Type()) {
					ref := append([]string{"(" + name + ")"}, ref...)
					if !yield(ref) {
						return
					}
				}
			}
		case *types.Alias:
			if typ == l.unsafeMutableString {
				yield([]string{"(mimirpb.UnsafeMutableString)"})
			}
		case *types.Slice:
			for ref := range l.referencesToBuffer(pkg, typ.Elem()) {
				if !yield(ref) {
					return
				}
			}
		}
	}
}

func formatRefPath(refPath []string) string {
	return strings.Join(refPath, ".")
}

func (l *linter) report(pos token.Pos, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s: %s\n", l.fset.Position(pos), fmt.Sprintf(format, args...))
	l.failed = true
}

func must[T any](v T, err error) T {
	try(err)
	return v
}

func try(err error, msgAndArgs ...any) {
	if err != nil {
		if len(msgAndArgs) > 0 {
			panic(fmt.Errorf(msgAndArgs[0].(string), append(msgAndArgs[1:], err)...))
		}
		panic(err)
	}
}
