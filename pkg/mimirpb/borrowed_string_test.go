// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"strings"
	"testing"
	"unsafe"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

func TestBorrowedStringOperations(t *testing.T) {
	s := BorrowString("hello 世界")
	require.Equal(t, 12, s.Len())
	require.Equal(t, 8, s.RuneCount())
	require.True(t, s.ValidUTF8())
	require.True(t, s.EqualString("hello 世界"))
	require.True(t, s.Equal(BorrowString("hello 世界")))
	require.Zero(t, s.Compare(BorrowString("hello 世界")))
	require.Positive(t, s.CompareString("hello"))
	require.True(t, s.HasPrefix("hello"))
	require.True(t, s.HasSuffix("世界"))
	require.True(t, s.Slice(6, s.Len()).EqualString("世界"))
	require.Equal(t, []byte("prefix:hello 世界"), s.AppendTo([]byte("prefix:")))
	require.True(t, BorrowedString{}.IsEmpty())
	require.False(t, BorrowString("\xff").ValidUTF8())
	require.True(t, BorrowString("metric_name").IsValidLabelName(model.LegacyValidation))
	require.False(t, BorrowString("invalid name").IsValidLabelName(model.LegacyValidation))
	require.True(t, BorrowString("metric:name").IsValidMetricName(model.LegacyValidation))
	require.False(t, BorrowString("invalid name").IsValidMetricName(model.LegacyValidation))
	require.True(t, s.InStringSet(map[string]struct{}{"hello 世界": {}}))
	require.False(t, s.InStringSet(nil))
	require.Equal(t, BorrowString("hello 世界").Hash(), s.Hash())
	require.True(t, s.IsValidLabelValue())
	require.False(t, BorrowString("\xff").IsValidLabelValue())
	require.Panics(t, func() { _ = s.Slice(-1, 1) })
}

func TestBorrowedStringCloneOwnsStorage(t *testing.T) {
	buf := []byte("request-label-value")
	borrowed := BorrowString(unsafe.String(unsafe.SliceData(buf), len(buf)))
	owned := borrowed.Clone()
	clear(buf)
	require.Equal(t, "request-label-value", owned)
	require.False(t, borrowed.EqualString(owned))
}

func TestBorrowedStringJSON(t *testing.T) {
	for _, value := range []string{"", "normal", "世界", "a\"b\n", "\xff"} {
		got, err := json.Marshal(BorrowString(value))
		require.NoError(t, err)
		want, err := json.Marshal(value)
		require.NoError(t, err)
		require.Equal(t, want, got)
		var decoded BorrowedString
		require.NoError(t, json.Unmarshal(got, &decoded))
		var expected string
		require.NoError(t, json.Unmarshal(got, &expected))
		require.True(t, decoded.EqualString(expected))
	}
}

func TestBorrowedStringJSONOwnsStorage(t *testing.T) {
	input := []byte(`"request-label-value"`)
	var decoded BorrowedString
	require.NoError(t, json.Unmarshal(input, &decoded))
	clear(input)
	require.True(t, decoded.EqualString("request-label-value"))

	buf := []byte("request-label-value")
	borrowed := BorrowString(unsafe.String(unsafe.SliceData(buf), len(buf)))
	encoded, err := json.Marshal(borrowed)
	require.NoError(t, err)
	clear(buf)
	require.Equal(t, `"request-label-value"`, string(encoded))
}

func TestBorrowedStringReadOperationsDoNotAllocate(t *testing.T) {
	s := BorrowString("request-label-value")
	set := map[string]struct{}{"request-label-value": {}}
	dst := make([]byte, 0, s.Len())
	require.Zero(t, testing.AllocsPerRun(100, func() {
		if !s.EqualString("request-label-value") || !s.InStringSet(set) || !s.ValidUTF8() {
			panic("unexpected read result")
		}
		dst = s.AppendTo(dst[:0])
	}))
	require.Equal(t, "request-label-value", string(dst))
}

func TestBorrowedStringHashMutation(t *testing.T) {
	for _, size := range []int{71, 256, 4096} {
		buf := []byte(strings.Repeat("a", size))
		src := BorrowString(unsafe.String(unsafe.SliceData(buf), len(buf)))
		hash := blake2b.Sum256(buf)
		expected := "(hash:" + hex.EncodeToString(hash[:]) + ")"
		got := UnsafeHashLabelValueInto(src, src)
		require.True(t, got.EqualString(expected))
		require.Equal(t, expected, string(buf[:len(expected)]))
	}
	require.Panics(t, func() { UnsafeHashLabelValueInto(BorrowString("short"), BorrowString("source")) })
}

// TestBorrowedStringRestrictedAPI checks the actual exported type declaration:
// ordinary safe Go cannot unwrap its private storage or convert it to string.
func TestBorrowedStringRestrictedAPI(t *testing.T) {
	fset := token.NewFileSet()
	source, err := os.ReadFile("borrowed_string.go")
	require.NoError(t, err)
	file, err := parser.ParseFile(fset, "borrowed_string.go", source, 0)
	require.NoError(t, err)
	var declaration *ast.TypeSpec
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range gen.Specs {
			if typ, ok := spec.(*ast.TypeSpec); ok && typ.Name.Name == "BorrowedString" {
				declaration = typ
			}
		}
	}
	require.NotNil(t, declaration)
	declFile := &ast.File{Name: ast.NewIdent("mimirpb"), Decls: []ast.Decl{&ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{declaration}}}}
	pkg, err := (&types.Config{}).Check("example.com/mimirpb", fset, []*ast.File{declFile}, nil)
	require.NoError(t, err)
	for _, expression := range []string{"string(s)", "s.value", "s.String()", "func() string { return s }()"} {
		t.Run(expression, func(t *testing.T) {
			client := "package client; import p \"example.com/mimirpb\"; var s p.BorrowedString; var _ = " + expression
			f, err := parser.ParseFile(fset, "client.go", client, 0)
			require.NoError(t, err)
			_, err = (&types.Config{Importer: borrowedStringTestImporter{pkg}}).Check("client", fset, []*ast.File{f}, nil)
			require.Error(t, err)
		})
	}
	typ := types.Unalias(pkg.Scope().Lookup("BorrowedString").Type())
	struc, ok := typ.Underlying().(*types.Struct)
	require.True(t, ok)
	for field := range struc.Fields() {
		require.False(t, field.Exported(), "borrowed storage must not be public")
	}
	for _, decl := range file.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok || method.Recv == nil {
			continue
		}
		require.NotEqual(t, "String", method.Name.Name, "String would silently expose borrowed storage")
	}
}

type borrowedStringTestImporter struct{ pkg *types.Package }

func (i borrowedStringTestImporter) Import(path string) (*types.Package, error) {
	if path == i.pkg.Path() {
		return i.pkg, nil
	}
	return nil, fmt.Errorf("unexpected import %q", path)
}

func BenchmarkBorrowedStringEqual(b *testing.B) {
	left, right := "request-label-value", "request-label-value"
	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		var equal bool
		for b.Loop() {
			equal = left == right
		}
		if !equal {
			b.Fatal("comparison mismatch")
		}
	})
	b.Run("borrowed", func(b *testing.B) {
		s := BorrowString(left)
		b.ReportAllocs()
		var equal bool
		for b.Loop() {
			equal = s.EqualString(right)
		}
		if !equal {
			b.Fatal("comparison mismatch")
		}
	})
}
