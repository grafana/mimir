// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChildMethod(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		expected string
	}{
		{
			name: "no children",
			source: `package core
					 //node:generate
 					 type NumberLiteral struct{ Value float64 }`,
			expected: `func (n *NumberLiteral) Child(idx int) planning.Node {
						 panic(fmt.Sprintf("node of type NumberLiteral has no children, but attempted to get child at index %d", idx))
					   }`,
		},
		{
			name: "single non-nilable child",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (s *Subquery) Child(idx int) planning.Node {
                         if idx != 0 {
						   panic(fmt.Sprintf("node of type Subquery supports 1 child, but attempted to get child at index %d", idx))
					     }
					     return s.Inner
					   }`,
		},
		{
			name: "single nilable child",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child,nilable"` + "`" + ` }
					`,
			expected: `func (s *Subquery) Child(idx int) planning.Node {
                         if idx != 0 {
						   panic(fmt.Sprintf("node of type Subquery supports 1 child, but attempted to get child at index %d", idx))
					     }
                         if s.Inner == nil {
							panic(fmt.Sprintf("cannot get Subquery child at index %d if Inner is nil", idx))
						 }
					     return s.Inner
					   }`,
		},
		{
			name: "non-nilable children",
			source: `package core
                     import "planning"
                     //node:generate
                     type BinaryExpression struct {
                     	LHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     	RHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     }`,
			expected: `func (b *BinaryExpression) Child(idx int) planning.Node {
					  	switch idx {
					  	case 0:
					  		return b.LHS
					  	case 1:
					  		return b.RHS
					  	default:
					  		panic(fmt.Sprintf("node of type BinaryExpression supports 2 children, but attempted to get child at index %d", idx))
					  	}
					  }`,
		},
		{
			name: "non-nilable plus nilable child",
			source: `package core
                     import "planning"
                     //node:generate
                     type AggregateExpression struct {
                     	Inner planning.Node ` + "`" + `node:"child"` + "`" + `
                     	Param planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
                     }`,
			expected: `func (a *AggregateExpression) Child(idx int) planning.Node {
					  	switch idx {
					  	case 0:
					  		return a.Inner
					  	case 1:
					  		if a.Param == nil {
					  			panic("cannot get AggregateExpression child at index 1 if Param is nil")
					  		}
					  		return a.Param
					  	default:
					  		panic(fmt.Sprintf("node of type AggregateExpression supports 2 children, but attempted to get child at index %d", idx))
					  	}
					  }`,
		},
		{
			name: "children slice",
			source: `package core
                     import "planning"
                     //node:generate
                     type FunctionCall struct {
                     	Args []planning.Node ` + "`" + `node:"children"` + "`" + `
                     }`,
			expected: `func (f *FunctionCall) Child(idx int) planning.Node {
					  	  if idx >= len(f.Args) {
					  		panic(fmt.Sprintf("this FunctionCall node has %d children, but attempted to get child at index %d", len(f.Args), idx))
					  	  }
					  	  return f.Args[idx]
			           }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := ChildMethod.Generate(s, imports)
			require.NoError(t, err)

			expected := gofmt(t, tc.expected)

			require.Equal(t, expected, actual)
		})
	}
}

func TestChildMethod_Unsupported(t *testing.T) {
	cases := []struct {
		name        string
		source      string
		expectedErr string
	}{
		{
			name: "child and children mixed",
			source: `package core
                     import "planning"
                     //node:generate
                     type Mixed struct {
                     	Inner planning.Node   ` + "`" + `node:"child"` + "`" + `
                     	Args  []planning.Node ` + "`" + `node:"children"` + "`" + `
                     }`,
			expectedErr: `mixing node:"child" and node:"children" is not supported`,
		},
		{
			name: "multiple children slices",
			source: `package core
                     import "planning"
                     //node:generate
                     type Multi struct {
                     	A []planning.Node ` + "`" + `node:"children"` + "`" + `
                     	B []planning.Node ` + "`" + `node:"children"` + "`" + `
                     }`,
			expectedErr: `multiple node:"children" fields are not supported`,
		},
		{
			name: "multiple nilable children",
			source: `package core
                     import "planning"
                     //node:generate
                     type Multi struct {
                     	A planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
                     	B planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
                     }`,
			expectedErr: `only one node:"child,nilable" field is supported`,
		},
		{
			name: "nilable child not last",
			source: `package core
                     import "planning"
                     //node:generate
                     type Misordered struct {
                     	A planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
                     	B planning.Node ` + "`" + `node:"child"` + "`" + `
                     }`,
			expectedErr: `a node:"child,nilable" field must be the last node:"child" field`,
		},
		{
			name: "child tag on embedded field",
			source: `package core
                     import "planning"
                     type Embedded struct { FieldA string }
                     //node:generate
					 type NodeX struct {
                     	*Embedded ` + "`" + `node:"child"` + "`" + `
                     }`,
			expectedErr: `node:"child" and node:"children" tags on embedded fields are not supported`,
		},
		{
			name: "children tag on embedded field",
			source: `package core
                     import "planning"
                     type Embedded struct { 
						FieldA string
						FieldB int
					 }
                     //node:generate
					 type NodeX struct {
                     	*Embedded ` + "`" + `node:"children"` + "`" + `
                     }`,
			expectedErr: `node:"child" and node:"children" tags on embedded fields are not supported`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			out, err := ChildMethod.Generate(s, imports)
			require.EqualError(t, err, tc.expectedErr)
			require.Empty(t, out)
			require.Empty(t, imports.Paths(), "no imports should be registered for an unsupported struct")
		})
	}
}

func TestChildCountMethod(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		expected string
	}{
		{
			name: "no children",
			source: `package core
					//node:generate
					type NumberLiteral struct{ Value float64 }`,
			expected: `func (n *NumberLiteral) ChildCount() int {
                          return 0
                       }`,
		},
		{
			name: "all non-nilable children",
			source: `package core
					import "planning"
					//node:generate
					type BinaryExpression struct {
						LHS planning.Node ` + "`" + `node:"child"` + "`" + `
						RHS planning.Node ` + "`" + `node:"child"` + "`" + `
					}`,
			expected: `func (b *BinaryExpression) ChildCount() int {
                          return 2
                       }`,
		},
		{
			name: "non-nilable plus nilable child",
			source: `package core
					import "planning"
					//node:generate
					type AggregateExpression struct {
						Inner planning.Node ` + "`" + `node:"child"` + "`" + `
						Param planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
					}`,
			expected: `func (a *AggregateExpression) ChildCount() int {
						if a.Param == nil {
							return 1
						}
						return 2
					  }`,
		},
		{
			name: "2 non-nilable plus nilable child",
			source: `package core
					import "planning"
					//node:generate
					type AggregateExpression struct {
						Inner 	 planning.Node ` + "`" + `node:"child"` + "`" + `
						InnerTwo planning.Node ` + "`" + `node:"child"` + "`" + `
						Param 	 planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
					}`,
			expected: `func (a *AggregateExpression) ChildCount() int {
						   if a.Param == nil {
							   return 2
						   }
						   return 3
                       }`,
		},
		{
			name: "children slice",
			source: `package core
					import "planning"
					//node:generate
					type FunctionCall struct {
						Args []planning.Node ` + "`" + `node:"children"` + "`" + `
					}`,
			expected: `func (f *FunctionCall) ChildCount() int {
						   return len(f.Args)
                       }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := ChildCountMethod.Generate(s, imports)
			require.NoError(t, err)

			expected := gofmt(t, tc.expected)

			require.Equal(t, expected, actual)
		})
	}
}
