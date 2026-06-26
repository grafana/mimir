// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEquivalentMethod(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		expected string
	}{
		{
			name: "no comparable fields",
			source: `package core
				import "github.com/grafana/mimir/pkg/streamingpromql/planning"
				type DeduplicateAndMergeDetails struct{}
				//node:generate
				type DeduplicateAndMerge struct {
					*DeduplicateAndMergeDetails
					Inner planning.Node ` + "`" + `node:"child"` + "`" + `
				}`,
			expected: `func (d *DeduplicateAndMerge) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
				_, ok := other.(*DeduplicateAndMerge)
				return ok
			}`,
		},
		{
			name: "scalar fields, position range and child skipped",
			source: `package core
				import "github.com/grafana/mimir/pkg/streamingpromql/planning"
				type PositionRange struct{ Start int }
				type UnaryOperation int32
				type UnaryExpressionDetails struct {
					Op                 UnaryOperation
					ExpressionPosition PositionRange
				}
				//node:generate
				type UnaryExpression struct {
					*UnaryExpressionDetails
					Inner planning.Node ` + "`" + `node:"child"` + "`" + `
				}`,
			expected: `func (u *UnaryExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
				oi, ok := other.(*UnaryExpression)
				return ok &&
					u.Op == oi.Op
			}`,
		},
		{
			name: "hints skipped and struct compared via helper",
			source: `package core
				import "github.com/grafana/mimir/pkg/streamingpromql/planning"
				type PositionRange struct{ Start int }
				type BinaryOperation int32
				type VectorMatching struct {
					On             bool
					MatchingLabels []string
				}
				type BinaryExpressionHints struct{ Foo int }
				type BinaryExpressionDetails struct {
					Op                 BinaryOperation
					VectorMatching     *VectorMatching
					ReturnBool         bool
					ExpressionPosition PositionRange
					Hints              *BinaryExpressionHints
				}
				//node:generate
				type BinaryExpression struct {
					*BinaryExpressionDetails ` + "`" + `node:"hints=Hints"` + "`" + `
					LHS planning.Node ` + "`" + `node:"child,label=LHS"` + "`" + `
					RHS planning.Node ` + "`" + `node:"child,label=RHS"` + "`" + `
				}`,
			expected: `func (b *BinaryExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
				oi, ok := other.(*BinaryExpression)
				return ok &&
					b.Op == oi.Op &&
					b.ReturnBool == oi.ReturnBool &&
					((b.VectorMatching == nil && oi.VectorMatching == nil) || (b.VectorMatching != nil && oi.VectorMatching != nil && genEqualsVectorMatching(*b.VectorMatching, *oi.VectorMatching)))
			}

			func genEqualsVectorMatching(a, b VectorMatching) bool {
				return slices.Equal(a.MatchingLabels, b.MatchingLabels) &&
					a.On == b.On
			}`,
		},
		{
			name: "slice of comparable uses slices.Equal, slice of struct passes the helper directly",
			source: `package core
				import "github.com/grafana/mimir/pkg/streamingpromql/planning"
				type Inner struct{ X int }
				type Details struct {
					Names  []string
					Values []Inner
				}
				//node:generate
				type Node struct {
					*Details
				}`,
			expected: `func (n *Node) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
				oi, ok := other.(*Node)
				return ok &&
					slices.Equal(n.Names, oi.Names) &&
					slices.EqualFunc(n.Values, oi.Values, genEqualsInner)
			}

			func genEqualsInner(a, b Inner) bool {
				return a.X == b.X
			}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)
			imports := newImportsCollector()

			actual, err := newEquivalentMethod().Generate(s, imports, pkg.Registry)
			require.NoError(t, err)

			require.Equal(t, gofmt(t, tc.expected), actual)
		})
	}
}

func TestEquivalentMethod_Unsupported(t *testing.T) {
	cases := []struct {
		name        string
		source      string
		expectedErr string
	}{
		{
			name: "field with an unsupported type",
			source: `package core
				type Details struct {
					Handler func(int) error
				}
				//node:generate
				type Node struct {
					*Details
				}`,
			expectedErr: "Node.Handler: unsupported field type",
		},
		{
			name: "field from a type that is neither struct nor alias",
			source: `package core
				type Details struct {
					Values map[string]int
				}
				//node:generate
				type Node struct {
					*Details
				}`,
			expectedErr: "Node.Values: unsupported field type",
		},
		{
			name: "hints names a field that does not exist",
			source: `package core
				type Details struct {
					Foo int
				}
				//node:generate
				type Node struct {
					*Details ` + "`" + `node:"hints=Bar"` + "`" + `
				}`,
			expectedErr: "hints tag names unknown field Bar",
		},
		{
			name: "hints with a trailing empty field name",
			source: `package core
				type Details struct {
					Foo int
				}
				//node:generate
				type Node struct {
					*Details ` + "`" + `node:"hints=Foo;"` + "`" + `
				}`,
			expectedErr: "hints tag contains an empty field name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pkg := parseFixture(t, tc.source)
			s := resolveAnnotatedStruct(t, pkg)

			_, err := newEquivalentMethod().Generate(s, newImportsCollector(), pkg.Registry)
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
