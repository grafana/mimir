// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"go/token"
	"go/types"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferencesToBorrowedString(t *testing.T) {
	pkg := types.NewPackage("example.com/borrowed", "borrowed")
	borrowed := types.NewNamed(types.NewTypeName(token.NoPos, pkg, "BorrowedString", nil),
		types.NewStruct([]*types.Var{types.NewField(token.NoPos, pkg, "value", types.Typ[types.String], false)}, nil), nil)
	alias := types.NewAlias(types.NewTypeName(token.NoPos, pkg, "UnsafeMutableString", nil), borrowed)
	for _, tc := range []struct {
		name string
		typ  types.Type
		want [][]string
	}{
		{"named", borrowed, [][]string{{"(mimirpb.BorrowedString)"}}},
		{"alias", alias, [][]string{{"(mimirpb.BorrowedString)"}}},
		{"slice", types.NewSlice(borrowed), [][]string{{"(mimirpb.BorrowedString)"}}},
		{"pointer", types.NewPointer(borrowed), [][]string{{"(mimirpb.BorrowedString)"}}},
		{"ordinary string", types.Typ[types.String], nil},
		{"field", types.NewStruct([]*types.Var{types.NewField(token.NoPos, pkg, "Label", borrowed, false)}, nil), [][]string{{"Label", "(mimirpb.BorrowedString)"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &linter{borrowedString: borrowed, seen: map[types.Type]bool{}}
			require.Equal(t, tc.want, slices.Collect(l.referencesToBuffer(pkg, tc.typ)))
		})
	}
}
