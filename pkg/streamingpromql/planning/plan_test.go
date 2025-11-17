// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
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
	children                   []Node
	description                string
	minimumRequiredPlanVersion QueryPlanVersion
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

func (t *testNode) Child(idx int) Node {
	return t.children[idx]
}

func (t *testNode) ChildCount() int {
	return len(t.children)
}

func (t *testNode) SetChildren(_ []Node) error {
	panic("not supported")
}

func (t *testNode) ReplaceChild(_ int, _ Node) error {
	panic("not supported")
}

func (t *testNode) EquivalentToIgnoringHintsAndChildren(_ Node) bool {
	panic("not supported")
}

func (t *testNode) MergeHints(_ Node) error { panic("not supported") }

func (t *testNode) ChildrenTimeRange(_ types.QueryTimeRange) types.QueryTimeRange {
	panic("not supported")
}

func (t *testNode) ResultType() (parser.ValueType, error) {
	panic("not supported")
}

func (t *testNode) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) QueriedTimeRange {
	panic("not supported")
}

func (t *testNode) ExpressionPosition() posrange.PositionRange {
	panic("not supported")
}

func (t *testNode) MinimumRequiredPlanVersion() QueryPlanVersion {
	return t.minimumRequiredPlanVersion
}

func TestQueryPlanVersion(t *testing.T) {
	v0 := QueryPlanVersion(0)
	v1 := QueryPlanVersion(1)
	v2 := QueryPlanVersion(2)

	testCases := map[string]struct {
		plan            QueryPlan
		expectedVersion QueryPlanVersion
		expectedError   error
	}{
		"no root node": {
			plan:            QueryPlan{},
			expectedVersion: v0,
			expectedError:   errors.New("query plan version can not be determined without a root node"),
		},
		"single root node": {
			plan: QueryPlan{
				Root: &testNode{minimumRequiredPlanVersion: v1},
			},
			expectedVersion: v1,
		},
		"node with children": {
			plan: QueryPlan{
				Root: &testNode{
					minimumRequiredPlanVersion: v1,
					children: []Node{
						&testNode{
							minimumRequiredPlanVersion: v2,
						},
						&testNode{
							minimumRequiredPlanVersion: v1,
						},
					},
				},
			},
			expectedVersion: v2,
		},
		"node with deep children": {
			plan: QueryPlan{
				Root: &testNode{
					minimumRequiredPlanVersion: v0,
					children: []Node{
						&testNode{
							minimumRequiredPlanVersion: v1,
							children: []Node{
								&testNode{
									minimumRequiredPlanVersion: v0,
									children: []Node{
										&testNode{
											minimumRequiredPlanVersion: v2,
										},
									},
								},
							},
						},
					},
				},
			},
			expectedVersion: v2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			err := testCase.plan.DeterminePlanVersion()
			if err != nil {
				require.Equal(t, testCase.expectedError, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedVersion, testCase.plan.Version)
			}
		})
	}
}

func TestQueriedTimeRange_Union(t *testing.T) {
	testCases := map[string]struct {
		first    QueriedTimeRange
		second   QueriedTimeRange
		expected QueriedTimeRange
	}{
		"neither queries any data": {
			first:    NoDataQueried(),
			second:   NoDataQueried(),
			expected: NoDataQueried(),
		},
		"only one queries any data": {
			first:    NoDataQueried(),
			second:   NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
		},
		"both query data and are the same": {
			first:    NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			second:   NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
		},
		"both query data and don't overlap": {
			first:    NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			second:   NewQueriedTimeRange(timestamp.Time(4000), timestamp.Time(5000)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(5000)),
		},
		"both query data and don't overlap, but the end of one aligns with the start of the other": {
			first:    NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			second:   NewQueriedTimeRange(timestamp.Time(3000), timestamp.Time(5000)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(5000)),
		},
		"both query data and one is entirely contained by the other": {
			first:    NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			second:   NewQueriedTimeRange(timestamp.Time(2000), timestamp.Time(2500)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
		},
		"both query data and overlap": {
			first:    NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(3000)),
			second:   NewQueriedTimeRange(timestamp.Time(2000), timestamp.Time(5000)),
			expected: NewQueriedTimeRange(timestamp.Time(1000), timestamp.Time(5000)),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			result := testCase.first.Union(testCase.second)
			require.Equal(t, testCase.expected, result)

			// Swapping the order should produce the same result.
			// This doesn't hold true if either is invalid with AnyDataQueried=false and non-zero MinT or MaxT,
			// but if either is invalid then garbage in-garbage out applies.
			result = testCase.second.Union(testCase.first)
			require.Equal(t, testCase.expected, result)
		})
	}
}
