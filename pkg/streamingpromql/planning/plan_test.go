// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
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

func (t *testNode) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) QueriedTimeRange {
	panic("not supported")
}

func TestQueriedTimeRange_Union(t *testing.T) {
	testCases := map[string]struct {
		first    QueriedTimeRange
		second   QueriedTimeRange
		expected QueriedTimeRange
	}{
		"neither queries any data": {
			first:    QueriedTimeRange{AnyDataQueried: false},
			second:   QueriedTimeRange{AnyDataQueried: false},
			expected: QueriedTimeRange{AnyDataQueried: false},
		},
		"only one queries any data": {
			first:    QueriedTimeRange{AnyDataQueried: false},
			second:   QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
		},
		"both query data and are the same": {
			first:    QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			second:   QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
		},
		"both query data and don't overlap": {
			first:    QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			second:   QueriedTimeRange{MinT: timestamp.Time(4000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
		},
		"both query data and don't overlap, but the end of one aligns with the start of the other": {
			first:    QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			second:   QueriedTimeRange{MinT: timestamp.Time(3000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
		},
		"both query data and one is entirely contained by the other": {
			first:    QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			second:   QueriedTimeRange{MinT: timestamp.Time(2000), MaxT: timestamp.Time(2500), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
		},
		"both query data and overlap": {
			first:    QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(3000), AnyDataQueried: true},
			second:   QueriedTimeRange{MinT: timestamp.Time(2000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
			expected: QueriedTimeRange{MinT: timestamp.Time(1000), MaxT: timestamp.Time(5000), AnyDataQueried: true},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.first.Union(testCase.second)
			require.Equal(t, testCase.expected, result)

			// Swapping the order should produce the same result.
			result = testCase.second.Union(testCase.first)
			require.Equal(t, testCase.expected, result)
		})
	}
}
