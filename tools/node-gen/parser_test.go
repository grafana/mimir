// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParser_NodeStructs(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		expected []string
	}{
		{
			name: "no annotation",
			source: `package core
					 type Plain struct{ X int }`,
			expected: nil,
		},
		{
			name: "single annotated struct",
			source: `package core
					 //node:generate
					 type S struct{ X int }`,
			expected: []string{"S"},
		},
		{
			name: "annotated and unannotated mixed, declaration order preserved",
			source: `package core
					 type Helper struct{}
					 //node:generate
					 type A struct{ X int }
					 type Other struct{}
					 //node:generate
					 type B struct{ Y int }`,
			expected: []string{"A", "B"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg, err := parseFiles(t, map[string]string{"fixture.go": tc.source})
			require.NoError(t, err)
			require.Equal(t, tc.expected, nodeStructNames(pkg))
		})
	}
}

func nodeStructNames(pkg *Package) []string {
	if len(pkg.NodeStructs) == 0 {
		return nil
	}
	out := make([]string, len(pkg.NodeStructs))
	for i, s := range pkg.NodeStructs {
		out[i] = s.Name
	}
	return out
}

func TestParser_FieldTags(t *testing.T) {
	pkg, err := parseFiles(t, map[string]string{"fixture.go": `package core
		//node:generate
		type S struct {
			Inner    planning.Node   ` + "`" + `node:"child"` + "`" + `
			Param    planning.Node   ` + "`" + `node:"child,nilable"` + "`" + `
			Labeled  planning.Node   ` + "`" + `node:"child,label=LHS"` + "`" + `
			Args     []planning.Node ` + "`" + `node:"children"` + "`" + `
			Bounded  []planning.Node ` + "`" + `node:"children,min=1"` + "`" + `
			Fmtd     []planning.Node ` + "`" + `node:"children,labelfmt=node %d"` + "`" + `
			Untagged int
			*Embedded
		}`})
	require.NoError(t, err)

	require.Len(t, pkg.NodeStructs, 1)
	fields := pkg.NodeStructs[0].Fields
	require.Len(t, fields, 8)

	require.Equal(t, "Inner", fields[0].Name)
	require.Equal(t, &NodeTag{IsChild: true}, fields[0].Tag)

	require.Equal(t, "Param", fields[1].Name)
	require.Equal(t, &NodeTag{IsChild: true, Nilable: true}, fields[1].Tag)

	require.Equal(t, "Labeled", fields[2].Name)
	label := "LHS"
	require.Equal(t, &NodeTag{IsChild: true, Label: &label}, fields[2].Tag)

	require.Equal(t, "Args", fields[3].Name)
	require.Equal(t, &NodeTag{IsChildren: true}, fields[3].Tag)

	require.Equal(t, "Bounded", fields[4].Name)
	require.Equal(t, &NodeTag{IsChildren: true, Min: 1}, fields[4].Tag)

	require.Equal(t, "Fmtd", fields[5].Name)
	require.Equal(t, &NodeTag{IsChildren: true, LabelFmt: "node %d"}, fields[5].Tag)

	require.Equal(t, "Untagged", fields[6].Name)
	require.Nil(t, fields[6].Tag)

	require.True(t, fields[7].Embedded)
	require.Equal(t, "", fields[7].Name)
}

func TestParser_BadTag(t *testing.T) {
	cases := []struct {
		name        string
		source      string
		errContains string
	}{
		{
			name: "unknown kind",
			source: `package core
					 //node:generate
					 type S struct{ X int ` + "`" + `node:"wibble"` + "`" + ` }`,
			errContains: `unknown node tag kind "wibble"`,
		},
		{
			name: "unknown child option",
			source: `package core
					 //node:generate
					 type S struct{ X int ` + "`" + `node:"child,bogus"` + "`" + ` }`,
			errContains: `unknown child tag option "bogus"`,
		},
		{
			name: "unknown children option",
			source: `package core
					 //node:generate
					 type S struct{ X []int ` + "`" + `node:"children,bogus"` + "`" + ` }`,
			errContains: `unknown children tag option "bogus"`,
		},
		{
			name: "invalid children min",
			source: `package core
					 //node:generate
					 type S struct{ X []int ` + "`" + `node:"children,min=abc"` + "`" + ` }`,
			errContains: `invalid children min value "abc"`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFiles(t, map[string]string{"fixture.go": tc.source})
			require.ErrorContains(t, err, tc.errContains)
		})
	}
}

func parseFiles(t *testing.T, files map[string]string) (*Package, error) {
	t.Helper()
	fset := token.NewFileSet()
	asts := make([]*ast.File, 0, len(files))
	for name, src := range files {
		f, err := parser.ParseFile(fset, name, src, parser.ParseComments)
		require.NoError(t, err)
		asts = append(asts, f)
	}
	return buildPackage(asts)
}
