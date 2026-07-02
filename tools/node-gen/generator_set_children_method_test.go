// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetChildrenMethod(t *testing.T) {
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
			expected: `func (n *NumberLiteral) SetChildren(children []planning.Node) error {
						 if len(children) != 0 {
							 return fmt.Errorf("node of type NumberLiteral expects 0 children, but got %d", len(children))
						 }
						 return nil
					   }`,
		},
		{
			name: "single non-nilable child",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (s *Subquery) SetChildren(children []planning.Node) error {
						 if len(children) != 1 {
							 return fmt.Errorf("node of type Subquery expects one child, but got %d", len(children))
						 }
						 s.Inner = children[0]
						 return nil
					   }`,
		},
		{
			name: "single non-nilable child from another package",
			source: `package core
				     import "planning"
					 import "github.com/grafana/mimir/pkg/streamingpromql/planning/core"
					 //node:generate
					 type Subquery struct { Inner *core.FunctionCall ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (s *Subquery) SetChildren(children []planning.Node) error {
						 if len(children) != 1 {
							 return fmt.Errorf("node of type Subquery expects one child, but got %d", len(children))
						 }
						 child0, ok := children[0].(*core.FunctionCall)
  						 if !ok {
							return fmt.Errorf("node of type Subquery expects child Inner to be of type *core.FunctionCall, but got %T", children[0])
                         }
	                     s.Inner = child0
						 return nil
					   }`,
		},
		{
			name: "single nilable child",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child,nilable"` + "`" + ` }
					`,
			expected: `func (s *Subquery) SetChildren(children []planning.Node) error {
						 switch len(children) {
						 case 0:
							 s.Inner = nil
						 case 1:
							 s.Inner = children[0]
						 default:
							 return fmt.Errorf("node of type Subquery expects 0 or 1 children, but got %d", len(children))
						 }
						 return nil
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
			expected: `func (b *BinaryExpression) SetChildren(children []planning.Node) error {
					  	if len(children) != 2 {
					  		return fmt.Errorf("node of type BinaryExpression expects 2 children, but got %d", len(children))
					  	}
					  	b.LHS = children[0]
					  	b.RHS = children[1]
					  	return nil
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
			expected: `func (a *AggregateExpression) SetChildren(children []planning.Node) error {
					  	switch len(children) {
					  	case 1:
					  		a.Inner = children[0]
					  		a.Param = nil
					  	case 2:
					  		a.Inner = children[0]
					  		a.Param = children[1]
					  	default:
					  		return fmt.Errorf("node of type AggregateExpression expects 1 or 2 children, but got %d", len(children))
					  	}
					  	return nil
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
			expected: `func (f *FunctionCall) SetChildren(children []planning.Node) error {
					  	  f.Args = children
					  	  return nil
			           }`,
		},
		{
			name: "children slice with min=1",
			source: `package core
                     import "planning"
                     //node:generate
                     type RemoteExecutionGroup struct {
                     	Nodes []planning.Node ` + "`" + `node:"children,min=1"` + "`" + `
                     }`,
			expected: `func (r *RemoteExecutionGroup) SetChildren(children []planning.Node) error {
					  	  if len(children) < 1 {
					  		  return fmt.Errorf("node of type RemoteExecutionGroup expects at least one child, but got %d", len(children))
					  	  }

					  	  r.Nodes = children
					  	  return nil
			           }`,
		},
		{
			name: "children slice with min=2",
			source: `package core
                     import "planning"
                     //node:generate
                     type ManyChildren struct {
                     	Nodes []planning.Node ` + "`" + `node:"children,min=2"` + "`" + `
                     }`,
			expected: `func (m *ManyChildren) SetChildren(children []planning.Node) error {
					  	  if len(children) < 2 {
					  		  return fmt.Errorf("node of type ManyChildren expects at least 2 children, but got %d", len(children))
					  	  }

					  	  m.Nodes = children
					  	  return nil
			           }`,
		},
		{
			name: "typed single child",
			source: `package core
                     import "planning"
                     //node:generate
                     type RemoteExecutionConsumer struct {
                     	Group *RemoteExecutionGroup ` + "`" + `node:"child"` + "`" + `
                     }`,
			expected: `func (r *RemoteExecutionConsumer) SetChildren(children []planning.Node) error {
					  	if len(children) != 1 {
					  		return fmt.Errorf("node of type RemoteExecutionConsumer expects one child, but got %d", len(children))
					  	}
					  	child0, ok := children[0].(*RemoteExecutionGroup)
					  	if !ok {
					  		return fmt.Errorf("node of type RemoteExecutionConsumer expects child Group to be of type *RemoteExecutionGroup, but got %T", children[0])
					  	}
					  	r.Group = child0
					  	return nil
					  }`,
		},
		{
			name: "typed non-nilable plus typed nilable child",
			source: `package core
                     import "planning"
                     //node:generate
                     type Wrapper struct {
                     	Inner planning.Node ` + "`" + `node:"child"` + "`" + `
                     	Opt   *Foo          ` + "`" + `node:"child,nilable"` + "`" + `
                     }`,
			expected: `func (w *Wrapper) SetChildren(children []planning.Node) error {
					  	switch len(children) {
					  	case 1:
					  		w.Inner = children[0]
					  		w.Opt = nil
					  	case 2:
					  		w.Inner = children[0]
					  		child1, ok := children[1].(*Foo)
					  		if !ok {
					  			return fmt.Errorf("node of type Wrapper expects child Opt to be of type *Foo, but got %T", children[1])
					  		}
					  		w.Opt = child1
					  	default:
					  		return fmt.Errorf("node of type Wrapper expects 1 or 2 children, but got %d", len(children))
					  	}
					  	return nil
					  }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := SetChildrenMethod.Generate(s, imports)
			require.NoError(t, err)

			expected := gofmt(t, tc.expected)

			require.Equal(t, expected, actual)
		})
	}
}

func TestSetChildrenMethod_RegistersChildTypeImport(t *testing.T) {
	pkg := parseFixture(t, `package rangevectorsplitting
		import "github.com/grafana/mimir/pkg/streamingpromql/planning"
		import "github.com/grafana/mimir/pkg/streamingpromql/planning/core"
		//node:generate
		type SplitFunctionCall struct {
			Inner *core.FunctionCall `+"`"+`node:"child"`+"`"+`
		}`)
	s := resolveAnnotatedStruct(t, pkg)
	imports := newImportsCollector()

	_, err := SetChildrenMethod.Generate(s, imports)

	require.NoError(t, err)
	require.Contains(t, imports.Paths(), "github.com/grafana/mimir/pkg/streamingpromql/planning/core")
}
