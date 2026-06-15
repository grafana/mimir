// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChildrenLabelsMethod(t *testing.T) {
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
			expected: `func (n *NumberLiteral) ChildrenLabels() []string {
						 return nil
					   }`,
		},
		{
			name: "single child is unlabeled",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (s *Subquery) ChildrenLabels() []string {
						 return []string{""}
					   }`,
		},
		{
			name: "multiple children use labels",
			source: `package core
                     import "planning"
                     //node:generate
                     type BinaryExpression struct {
                     	LHS planning.Node ` + "`" + `node:"child,label=LHS"` + "`" + `
                     	RHS planning.Node ` + "`" + `node:"child,label=RHS"` + "`" + `
                     }`,
			expected: `func (b *BinaryExpression) ChildrenLabels() []string {
					  	return []string{"LHS", "RHS"}
					  }`,
		},
		{
			name: "nilable last child collapses to unlabeled when nil",
			source: `package core
                     import "planning"
                     //node:generate
                     type AggregateExpression struct {
                     	Inner planning.Node ` + "`" + `node:"child,label=expression"` + "`" + `
                     	Param planning.Node ` + "`" + `node:"child,nilable,label=parameter"` + "`" + `
                     }`,
			expected: `func (a *AggregateExpression) ChildrenLabels() []string {
					  	if a.Param == nil {
					  		return []string{""}
					  	}
					  	return []string{"expression", "parameter"}
					  }`,
		},
		{
			name: "children slice with labelfmt",
			source: `package core
                     import "planning"
                     //node:generate
                     type FunctionCall struct {
                     	Args []planning.Node ` + "`" + `node:"children,labelfmt=param %d"` + "`" + `
                     }`,
			expected: `func (f *FunctionCall) ChildrenLabels() []string {
					  	switch len(f.Args) {
					  	case 0:
					  		return nil
					  	case 1:
					  		return []string{""}
					  	default:
					  		labels := make([]string, len(f.Args))
					  		for i := range labels {
					  			labels[i] = fmt.Sprintf("param %d", i)
					  		}
					  		return labels
					  	}
					  }`,
		},
		{
			name: "children slice with nocollapse labels a single child",
			source: `package core
                     import "planning"
                     //node:generate
                     type RemoteExecutionGroup struct {
                     	Nodes []planning.Node ` + "`" + `node:"children,min=1,labelfmt=node %d,nocollapse"` + "`" + `
                     }`,
			expected: `func (r *RemoteExecutionGroup) ChildrenLabels() []string {
					  	switch len(r.Nodes) {
					  	case 0:
					  		return nil
					  	default:
					  		labels := make([]string, len(r.Nodes))
					  		for i := range labels {
					  			labels[i] = fmt.Sprintf("node %d", i)
					  		}
					  		return labels
					  	}
					  }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := ChildrenLabelsMethod.Generate(s, imports)
			require.NoError(t, err)

			expected := gofmt(t, tc.expected)

			require.Equal(t, expected, actual)
		})
	}
}

func TestChildrenLabelsMethod_Unsupported(t *testing.T) {
	cases := []struct {
		name        string
		source      string
		expectedErr string
	}{
		{
			name: "multiple children without labels collide as empty",
			source: `package core
                     import "planning"
                     //node:generate
                     type BinaryExpression struct {
                     	LHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     	RHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     }`,
			expectedErr: `child field "RHS" has a duplicate label ""`,
		},
		{
			name: "duplicate labels",
			source: `package core
                     import "planning"
                     //node:generate
                     type BinaryExpression struct {
                     	LHS planning.Node ` + "`" + `node:"child,label=same"` + "`" + `
                     	RHS planning.Node ` + "`" + `node:"child,label=same"` + "`" + `
                     }`,
			expectedErr: `child field "RHS" has a duplicate label "same"`,
		},
		{
			name: "children without labelfmt",
			source: `package core
                     import "planning"
                     //node:generate
                     type FunctionCall struct {
                     	Args []planning.Node ` + "`" + `node:"children"` + "`" + `
                     }`,
			expectedErr: `node:"children" requires a labelfmt option`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			out, err := ChildrenLabelsMethod.Generate(s, imports)
			require.EqualError(t, err, tc.expectedErr)
			require.Empty(t, out)
		})
	}
}
