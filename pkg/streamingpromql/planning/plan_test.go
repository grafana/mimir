// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestQueryPlan_String(t *testing.T) {
	sharedNode1 := &testNode{
		description: "first shared node",
	}

	sharedNode2 := &testNode{
		description: "second shared node",
		children: []Node{
			sharedNode1,
			sharedNode1,
		},
	}

	plan := &QueryPlan{
		Root: &testNode{
			description: "node with many children",
			children: []Node{
				sharedNode2,
				&testNode{
					description: "node with no children",
				},
				&testNode{
					description: "node with a child",
					children: []Node{
						&testNode{
							// Test node with no description.
						},
					},
				},
				sharedNode2,
			},
		},
	}

	expected := `
- testNode: node with many children
	- child 1: ref#2 testNode: second shared node
		- child 1: ref#1 testNode: first shared node
		- child 2: ref#1 testNode ...
	- child 2: testNode: node with no children
	- child 3: testNode: node with a child
		- testNode
	- child 4: ref#2 testNode ...
`
	require.Equal(t, strings.TrimSpace(expected), plan.String())
}

type testNode struct {
	children    []Node
	description string
}

func (t *testNode) Describe() string {
	return t.description
}

func (t *testNode) ChildrenLabels() []string {
	switch len(t.children) {
	case 0:
		return nil
	case 1:
		return []string{""}
	default:
		l := make([]string, len(t.children))

		for i := range t.children {
			l[i] = fmt.Sprintf("child %v", i+1)
		}

		return l
	}
}

func (t *testNode) Details() proto.Message {
	panic("not supported")
}

func (t *testNode) NodeType() NodeType {
	panic("not supported")
}

func (t *testNode) Children() []Node {
	return t.children
}

func (t *testNode) SetChildren(_ []Node) error {
	panic("not supported")
}

func (t *testNode) EquivalentTo(_ Node) bool {
	panic("not supported")
}

func (t *testNode) ChildrenTimeRange(_ types.QueryTimeRange) types.QueryTimeRange {
	panic("not supported")
}

func (t *testNode) OperatorFactory(_ *Materializer, _ types.QueryTimeRange, _ *OperatorParameters) (OperatorFactory, error) {
	panic("not supported")
}

func (t *testNode) ResultType() (parser.ValueType, error) {
	panic("not supported")
}
