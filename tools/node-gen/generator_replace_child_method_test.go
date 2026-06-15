// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplaceChildMethod(t *testing.T) {
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
			expected: `func (n *NumberLiteral) ReplaceChild(idx int, _ planning.Node) error {
						 return fmt.Errorf("node of type NumberLiteral has no children, but attempted to replace child at index %d", idx)
					   }`,
		},
		{
			name: "single non-nilable child",
			source: `package core
				     import "planning"
					 //node:generate
					 type Subquery struct { Inner planning.Node ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (s *Subquery) ReplaceChild(idx int, node planning.Node) error {
						 if idx != 0 {
							 return fmt.Errorf("node of type Subquery supports 1 child, but attempted to replace child at index %d", idx)
						 }
						 s.Inner = node
						 return nil
					   }`,
		},
		{
			name: "single typed child",
			source: `package core
				     import "planning"
					 //node:generate
					 type RemoteExecutionConsumer struct { Group *RemoteExecutionGroup ` + "`" + `node:"child"` + "`" + ` }
					`,
			expected: `func (r *RemoteExecutionConsumer) ReplaceChild(idx int, node planning.Node) error {
						 if idx != 0 {
							 return fmt.Errorf("node of type RemoteExecutionConsumer supports 1 child, but attempted to replace child at index %d", idx)
						 }
						 child, ok := node.(*RemoteExecutionGroup)
						 if !ok {
							 return fmt.Errorf("node of type RemoteExecutionConsumer expects child Group to be of type *RemoteExecutionGroup, but got %T", node)
						 }
						 r.Group = child
						 return nil
					   }`,
		},
		{
			name: "multiple children",
			source: `package core
                     import "planning"
                     //node:generate
                     type BinaryExpression struct {
                     	LHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     	RHS planning.Node ` + "`" + `node:"child"` + "`" + `
                     }`,
			expected: `func (b *BinaryExpression) ReplaceChild(idx int, node planning.Node) error {
					  	switch idx {
					  	case 0:
					  		b.LHS = node
					  		return nil
					  	case 1:
					  		b.RHS = node
					  		return nil
					  	default:
					  		return fmt.Errorf("node of type BinaryExpression supports 2 children, but attempted to replace child at index %d", idx)
					  	}
					  }`,
		},
		{
			name: "nilable field",
			source: `package core
                     import "planning"
                     //node:generate
                     type AggregateExpression struct {
                     	Inner planning.Node ` + "`" + `node:"child"` + "`" + `
                     	Param planning.Node ` + "`" + `node:"child,nilable"` + "`" + `
                     }`,
			expected: `func (a *AggregateExpression) ReplaceChild(idx int, node planning.Node) error {
					  	switch idx {
					  	case 0:
					  		a.Inner = node
					  		return nil
					  	case 1:
					  		a.Param = node
					  		return nil
					  	default:
					  		return fmt.Errorf("node of type AggregateExpression supports 2 children, but attempted to replace child at index %d", idx)
					  	}
					  }`,
		},
		{
			name: "multiple children with a typed field",
			source: `package core
                     import "planning"
                     //node:generate
                     type TwoTyped struct {
                     	A *Foo          ` + "`" + `node:"child"` + "`" + `
                     	B planning.Node ` + "`" + `node:"child"` + "`" + `
                     }`,
			expected: `func (t *TwoTyped) ReplaceChild(idx int, node planning.Node) error {
					  	switch idx {
					  	case 0:
					  		child, ok := node.(*Foo)
					  		if !ok {
					  			return fmt.Errorf("node of type TwoTyped expects child A to be of type *Foo, but got %T", node)
					  		}
					  		t.A = child
					  		return nil
					  	case 1:
					  		t.B = node
					  		return nil
					  	default:
					  		return fmt.Errorf("node of type TwoTyped supports 2 children, but attempted to replace child at index %d", idx)
					  	}
					  }`,
		},
		{
			name: "multiple typed children",
			source: `package core
                     import "planning"
					 import "github.com/grafana/mimir/pkg/streamingpromql/planning/core"
					 import "github.com/grafana/mimir/pkg/streamingpromql/planning/extra"
                     //node:generate
                     type NodeWithManyChildFields struct {
                     	Child1 planning.Node ` + "`" + `node:"child"` + "`" + `
                     	Child2 *core.FunctionCall ` + "`" + `node:"child"` + "`" + `
                     	Child3 planning.Node ` + "`" + `node:"child"` + "`" + `
                     	Child4 extra.ExtraFunctionCall ` + "`" + `node:"child"` + "`" + `
                     }`,
			expected: `func (n *NodeWithManyChildFields) ReplaceChild(idx int, node planning.Node) error {
                          switch idx {
				          case 0:
	                          n.Child1 = node
	                      	  return nil
	                      case 1:
	                      	  child, ok := node.(*core.FunctionCall)
	                      	  if !ok {
	                      		return fmt.Errorf("node of type NodeWithManyChildFields expects child Child2 to be of type *core.FunctionCall, but got %T", node)
	                      	  } 
	                      	  n.Child2 = child
	                      	  return nil
	                      case 2:
	                      	  n.Child3 = node
	                      	  return nil
	                      case 3:
	                      	  child, ok := node.(extra.ExtraFunctionCall)
	                      	  if !ok {
	                      	      return fmt.Errorf("node of type NodeWithManyChildFields expects child Child4 to be of type extra.ExtraFunctionCall, but got %T", node)
	                      	  }
	                      	  n.Child4 = child
	                      	  return nil
	                      default:
	                      	  return fmt.Errorf("node of type NodeWithManyChildFields supports 4 children, but attempted to replace child at index %d", idx)
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
			expected: `func (f *FunctionCall) ReplaceChild(idx int, node planning.Node) error {
					  	  if idx >= len(f.Args) {
					  		  return fmt.Errorf("this FunctionCall node has %d children, but attempted to replace child at index %d", len(f.Args), idx)
					  	  }
					  	  f.Args[idx] = node
					  	  return nil
			           }`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := ReplaceChildMethod.Generate(s, imports)
			require.NoError(t, err)

			expected := gofmt(t, tc.expected)

			require.Equal(t, expected, actual)
		})
	}
}

func TestReplaceChildMethod_RegistersChildTypeImport(t *testing.T) {
	pkg := parseFixture(t, `package rangevectorsplitting
		import "github.com/grafana/mimir/pkg/streamingpromql/planning"
		import "github.com/grafana/mimir/pkg/streamingpromql/planning/core"
		//node:generate
		type SplitFunctionCall struct {
			Inner *core.FunctionCall `+"`"+`node:"child"`+"`"+`
		}`)
	s := resolveAnnotatedStruct(t, pkg)
	imports := newImportsCollector()

	actual, err := ReplaceChildMethod.Generate(s, imports)

	require.NoError(t, err)
	require.Contains(t, actual, "node.(*core.FunctionCall)")
	require.Contains(t, imports.Paths(), "github.com/grafana/mimir/pkg/streamingpromql/planning/core")
}
