// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func marshalDetails(m proto.Message) []byte {
	b, err := proto.Marshal(m)

	if err != nil {
		panic(err)
	}

	return b
}

func TestPlanCreationEncodingAndDecoding(t *testing.T) {
	instantQuery := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	instantQueryEncodedTimeRange := planning.EncodedQueryTimeRange{StartT: 1000, EndT: 1000, IntervalMilliseconds: 1, IsInstant: true}
	rangeQuery := types.NewRangeQueryTimeRange(timestamp.Time(3000), timestamp.Time(5000), time.Second)
	rangeQueryEncodedTimeRange := planning.EncodedQueryTimeRange{StartT: 3000, EndT: 5000, IntervalMilliseconds: 1000}

	testCases := map[string]struct {
		expr                     string
		timeRange                types.QueryTimeRange
		enableDelayedNameRemoval bool

		expectedPlan *planning.EncodedQueryPlan
	}{
		"instant query with vector selector": {
			expr:      `some_metric{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*"}`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "env", Value: "prod"},
								{Type: 1, Name: "cluster", Value: "cluster-2"},
								{Type: 2, Name: "name", Value: "foo.*"},
								{Type: 3, Name: "node", Value: "small-nodes-.*"},
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 84},
						}),
						Type:        "VectorSelector",
						Description: `{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*", __name__="some_metric"}`,
					},
				},
			},
		},
		"range query with vector selector": {
			expr:      `some_metric{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*"}`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "env", Value: "prod"},
								{Type: 1, Name: "cluster", Value: "cluster-2"},
								{Type: 2, Name: "name", Value: "foo.*"},
								{Type: 3, Name: "node", Value: "small-nodes-.*"},
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 84},
						}),
						Type:        "VectorSelector",
						Description: `{env="prod", cluster!="cluster-2", name=~"foo.*", node!~"small-nodes-.*", __name__="some_metric"}`,
					},
				},
			},
		},
		"vector selector with '@ 0' instant query": {
			expr:      `some_metric @ 0`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Version:   0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          timestampOf(0),
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} @ 0 (1970-01-01T00:00:00Z)`,
					},
				},
			},
		},
		"vector selector with '@ 0' range query": {
			expr:      `some_metric @ 0`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  1,
				Version:   1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          timestampOf(0),
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} @ 0 (1970-01-01T00:00:00Z)`,
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						ChildrenLabels: []string{""},
						Children:       []int64{0},
						Type:           "StepInvariantExpression",
					},
				},
			},
		},
		"vector selector with '@ start()'": {
			expr:      `some_metric @ start()`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  1,
				Version:   planning.QueryPlanV1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          timestampOf(3000),
							ExpressionPosition: core.PositionRange{Start: 0, End: 21},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} @ 3000 (1970-01-01T00:00:03Z)`,
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						ChildrenLabels: []string{""},
						Children:       []int64{0},
						Type:           "StepInvariantExpression",
					},
				},
			},
		},
		"vector selector with '@ end()'": {
			expr:      `some_metric @ end()`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  1,
				Version:   planning.QueryPlanV1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          timestampOf(5000),
							ExpressionPosition: core.PositionRange{Start: 0, End: 19},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} @ 5000 (1970-01-01T00:00:05Z)`,
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						ChildrenLabels: []string{""},
						Children:       []int64{0},
						Type:           "StepInvariantExpression",
					},
				},
			},
		},
		"vector selector with offset": {
			expr:      `some_metric offset 30s`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Offset:             30 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 22},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} offset 30s`,
					},
				},
			},
		},
		"matrix selector": {
			expr:      `some_metric[1m]`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s]`,
					},
				},
			},
		},
		"matrix selector with '@ 0'": {
			expr:      `some_metric[1m] @ 0`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          timestampOf(0),
							ExpressionPosition: core.PositionRange{Start: 0, End: 19},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] @ 0 (1970-01-01T00:00:00Z)`,
					},
				},
			},
		},
		"matrix selector with '@ start()'": {
			expr:      `rate(some_metric[1m] @ start())`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  3,
				Version:   1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          timestampOf(3000),
							ExpressionPosition: core.PositionRange{Start: 5, End: 30},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] @ 3000 (1970-01-01T00:00:03Z)`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_RATE,
							ExpressionPosition: core.PositionRange{Start: 0, End: 31},
						}),
						Type:           "FunctionCall",
						Description:    `rate(...)`,
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{1},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						Type:           "StepInvariantExpression",
						Children:       []int64{2},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"matrix selector with '@ end()'": {
			expr:      `rate(some_metric[1m] @ end())`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  3,
				Version:   1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          timestampOf(5000),
							ExpressionPosition: core.PositionRange{Start: 5, End: 28},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] @ 5000 (1970-01-01T00:00:05Z)`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_RATE,
							ExpressionPosition: core.PositionRange{Start: 0, End: 29},
						}),
						Type:           "FunctionCall",
						Description:    `rate(...)`,
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{1},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						Type:           "StepInvariantExpression",
						Children:       []int64{2},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"expression with parenthesis": {
			expr:      `(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 1, End: 12},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
				},
			},
		},
		"number literal": {
			expr:      `12`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              12,
							ExpressionPosition: core.PositionRange{Start: 0, End: 2},
						}),
						Type:        "NumberLiteral",
						Description: `12`,
					},
				},
			},
		},
		"string literal": {
			expr:      `"abc"`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_STRING_LITERAL,
						Details: marshalDetails(&core.StringLiteralDetails{
							Value:              "abc",
							ExpressionPosition: core.PositionRange{Start: 0, End: 5},
						}),
						Type:        "StringLiteral",
						Description: `"abc"`,
					},
				},
			},
		},
		"function call with no arguments": {
			expr:      `time()`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_TIME,
							ExpressionPosition: core.PositionRange{Start: 0, End: 6},
						}),
						Type:        "FunctionCall",
						Description: `time(...)`,
					},
				},
			},
		},
		"function call with optional arguments omitted": {
			expr:      `year()`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_YEAR,
							ExpressionPosition: core.PositionRange{Start: 0, End: 6},
						}),
						Type:        "FunctionCall",
						Description: `year(...)`,
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{0},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"function call with optional arguments provided": {
			expr:      `year(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 5, End: 16},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_YEAR,
							ExpressionPosition: core.PositionRange{Start: 0, End: 17},
						}),
						Type:           "FunctionCall",
						Children:       []int64{0},
						Description:    `year(...)`,
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{1},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},

		"basic aggregation": {
			expr:      `sum(some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 4, End: 15},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_AGGREGATE_EXPRESSION,
						Details: marshalDetails(&core.AggregateExpressionDetails{
							Op:                 core.AGGREGATION_SUM,
							ExpressionPosition: core.PositionRange{Start: 0, End: 16},
						}),
						Type:           "AggregateExpression",
						Children:       []int64{0},
						Description:    `sum`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"aggregation with grouping": {
			expr:      `sum by (foo) (some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 14, End: 25},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_AGGREGATE_EXPRESSION,
						Details: marshalDetails(&core.AggregateExpressionDetails{
							Op:                 core.AGGREGATION_SUM,
							Grouping:           []string{"foo"},
							ExpressionPosition: core.PositionRange{Start: 0, End: 26},
						}),
						Type:           "AggregateExpression",
						Children:       []int64{0},
						Description:    `sum by (foo)`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"aggregation with 'without'": {
			expr:      `sum without (foo) (some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 19, End: 30},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_AGGREGATE_EXPRESSION,
						Details: marshalDetails(&core.AggregateExpressionDetails{
							Op:                 core.AGGREGATION_SUM,
							Grouping:           []string{"foo"},
							Without:            true,
							ExpressionPosition: core.PositionRange{Start: 0, End: 31},
						}),
						Type:           "AggregateExpression",
						Children:       []int64{0},
						Description:    `sum without (foo)`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"aggregation with parameter": {
			expr:      `topk(3, some_metric)`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 8, End: 19},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              3,
							ExpressionPosition: core.PositionRange{Start: 5, End: 6},
						}),
						Type:        "NumberLiteral",
						Description: `3`,
					},
					{
						NodeType: planning.NODE_TYPE_AGGREGATE_EXPRESSION,
						Details: marshalDetails(&core.AggregateExpressionDetails{
							Op:                 core.AGGREGATION_TOPK,
							ExpressionPosition: core.PositionRange{Start: 0, End: 20},
						}),
						Type:           "AggregateExpression",
						Children:       []int64{0, 1},
						Description:    `topk`,
						ChildrenLabels: []string{"expression", "parameter"},
					},
				},
			},
		},
		"binary expression with two scalars instant query": {
			expr:      `2 + 3`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Version:   0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 0, End: 1},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              3,
							ExpressionPosition: core.PositionRange{Start: 4, End: 5},
						}),
						Type:        "NumberLiteral",
						Description: `3`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op:                 core.BINARY_ADD,
							ExpressionPosition: core.PositionRange{Start: 0, End: 5},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS + RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
				},
			},
		},
		"binary expression with two scalars range query": {
			expr:      `2 + 3`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  3,
				Version:   planning.QueryPlanV1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 0, End: 1},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              3,
							ExpressionPosition: core.PositionRange{Start: 4, End: 5},
						}),
						Type:        "NumberLiteral",
						Description: `3`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op:                 core.BINARY_ADD,
							ExpressionPosition: core.PositionRange{Start: 0, End: 5},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS + RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						ChildrenLabels: []string{""},
						Children:       []int64{2},
						Type:           "StepInvariantExpression",
					},
				},
			},
		},
		"binary expression with vector and scalar": {
			expr:      `2 * some_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  3,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 0, End: 1},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 4, End: 15},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op:                 core.BINARY_MUL,
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS * RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{2},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"binary expression with 'bool'": {
			expr:      `some_metric > bool 2`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  3,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 19, End: 20},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op:                 core.BINARY_GTR,
							ReturnBool:         true,
							ExpressionPosition: core.PositionRange{Start: 0, End: 20},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS > bool RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{2},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"binary expression with two vectors": {
			expr:      `some_metric * some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_other_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 14, End: 31},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_other_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op:                 core.BINARY_MUL,
							VectorMatching:     &core.VectorMatching{},
							ExpressionPosition: core.PositionRange{Start: 0, End: 31},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS * RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
				},
			},
		},
		"binary expression with 'on'": {
			expr:      `some_metric * on (foo) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_other_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 23, End: 40},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_other_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op: core.BINARY_MUL,
							VectorMatching: &core.VectorMatching{
								MatchingLabels: []string{"foo"},
								On:             true,
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 40},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS * on (foo) RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
				},
			},
		},
		"binary expression with 'ignoring'": {
			expr:      `some_metric * ignoring (foo) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_other_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 29, End: 46},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_other_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op: core.BINARY_MUL,
							VectorMatching: &core.VectorMatching{
								MatchingLabels: []string{"foo"},
								On:             false,
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 46},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS * ignoring (foo) RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
				},
			},
		},
		"binary expression with 'group_left'": {
			expr:      `some_metric * ignoring (foo) group_left (bar) some_other_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_other_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 46, End: 63},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_other_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op: core.BINARY_MUL,
							VectorMatching: &core.VectorMatching{
								Card:           parser.CardManyToOne,
								MatchingLabels: []string{"foo"},
								Include:        []string{"bar"},
								On:             false,
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 63},
						}),
						Type:           "BinaryExpression",
						Children:       []int64{0, 1},
						Description:    `LHS * ignoring (foo) group_left (bar) RHS`,
						ChildrenLabels: []string{"LHS", "RHS"},
					},
				},
			},
		},
		"subquery": {
			expr:      `(some_metric)[1m:1s]`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 1, End: 12},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_SUBQUERY,
						Details: marshalDetails(&core.SubqueryDetails{
							Range:              time.Minute,
							Step:               time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 20},
						}),
						Type:           "Subquery",
						Children:       []int64{0},
						Description:    `[1m0s:1s]`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"subquery without explicit step": {
			expr:      `(some_metric)[1m:]`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 1, End: 12},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_SUBQUERY,
						Details: marshalDetails(&core.SubqueryDetails{
							Range:              time.Minute,
							Step:               23 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 18},
						}),
						Type:           "Subquery",
						Children:       []int64{0},
						Description:    `[1m0s:23s]`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"subquery with offset": {
			expr:      `(some_metric)[1m:1s] offset 3s`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 1, End: 12},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_SUBQUERY,
						Details: marshalDetails(&core.SubqueryDetails{
							Range:              time.Minute,
							Step:               time.Second,
							Offset:             3 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 30},
						}),
						Type:           "Subquery",
						Children:       []int64{0},
						Description:    `[1m0s:1s] offset 3s`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"subquery with '@' instant query": {
			expr:      `(some_metric)[1m:1s] @ 0`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  1,
				Version:   0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 1, End: 12},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType: planning.NODE_TYPE_SUBQUERY,
						Details: marshalDetails(&core.SubqueryDetails{
							Range:              time.Minute,
							Step:               time.Second,
							Timestamp:          timestampOf(0),
							ExpressionPosition: core.PositionRange{Start: 0, End: 24},
						}),
						Type:           "Subquery",
						Children:       []int64{0},
						Description:    `[1m0s:1s] @ 0 (1970-01-01T00:00:00Z)`,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"query with delayed name removal enabled": {
			expr:                     `some_metric`,
			timeRange:                instantQuery,
			enableDelayedNameRemoval: true,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange:                instantQueryEncodedTimeRange,
				RootNode:                 2,
				EnableDelayedNameRemoval: true,
				Version:                  planning.QueryPlanV1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							ExpressionPosition: core.PositionRange{Start: 0, End: 11},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"}`,
					},
					{
						NodeType:       planning.NODE_TYPE_DROP_NAME,
						Details:        marshalDetails(&core.DropNameDetails{}),
						Type:           "DropName",
						Description:    "",
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Description:    "",
						Children:       []int64{1},
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"timestamp not step invariant": {
			expr:                     `timestamp(metric)`,
			timeRange:                rangeQuery,
			enableDelayedNameRemoval: true,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange:                rangeQueryEncodedTimeRange,
				RootNode:                 3,
				Version:                  planning.QueryPlanV1,
				EnableDelayedNameRemoval: true,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "metric"},
							},
							Offset:                 0,
							ExpressionPosition:     core.PositionRange{Start: 10, End: 16},
							ReturnSampleTimestamps: true,
						}),
						Type:        "VectorSelector",
						Description: `{__name__="metric"}, return sample timestamps`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_TIMESTAMP,
							ExpressionPosition: core.PositionRange{Start: 0, End: 17},
						}),
						Type:           "FunctionCall",
						Description:    `timestamp(...)`,
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DROP_NAME,
						Details:        marshalDetails(&core.DropNameDetails{}),
						Type:           "DropName",
						Description:    "",
						Children:       []int64{1},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{2},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"timestamp with step invariant": {
			expr:                     `timestamp(metric @ 1)`,
			timeRange:                rangeQuery,
			enableDelayedNameRemoval: false,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange:                rangeQueryEncodedTimeRange,
				RootNode:                 3,
				Version:                  planning.QueryPlanV1,
				EnableDelayedNameRemoval: false,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "metric"},
							},
							Timestamp:              timestampOf(1000),
							Offset:                 0,
							ExpressionPosition:     core.PositionRange{Start: 10, End: 20},
							ReturnSampleTimestamps: true,
						}),
						Type:        "VectorSelector",
						Description: `{__name__="metric"} @ 1000 (1970-01-01T00:00:01Z), return sample timestamps`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_TIMESTAMP,
							ExpressionPosition: core.PositionRange{Start: 0, End: 21},
						}),
						Type:           "FunctionCall",
						Description:    `timestamp(...)`,
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{1},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						Type:           "StepInvariantExpression",
						Children:       []int64{2},
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"timestamp with unsafe step invariant": {
			expr:                     `timestamp(abs(metric @ 1))`,
			timeRange:                rangeQuery,
			enableDelayedNameRemoval: false,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange:                rangeQueryEncodedTimeRange,
				RootNode:                 5,
				Version:                  planning.QueryPlanV1,
				EnableDelayedNameRemoval: false,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_VECTOR_SELECTOR,
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "metric"},
							},
							Timestamp:              timestampOf(1000),
							Offset:                 0,
							ExpressionPosition:     core.PositionRange{Start: 14, End: 24},
							ReturnSampleTimestamps: false,
						}),
						Type:        "VectorSelector",
						Description: `{__name__="metric"} @ 1000 (1970-01-01T00:00:01Z)`,
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_ABS,
							ExpressionPosition: core.PositionRange{Start: 10, End: 25},
						}),
						Type:           "FunctionCall",
						Description:    `abs(...)`,
						Children:       []int64{0},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{1},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_STEP_INVARIANT_EXPRESSION,
						Details:        marshalDetails(&core.StepInvariantExpressionDetails{}),
						Type:           "StepInvariantExpression",
						Children:       []int64{2},
						ChildrenLabels: []string{""},
					},
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function:           functions.FUNCTION_TIMESTAMP,
							ExpressionPosition: core.PositionRange{Start: 0, End: 26},
						}),
						Type:           "FunctionCall",
						Description:    `timestamp(...)`,
						Children:       []int64{3},
						ChildrenLabels: []string{""},
					},
					{
						NodeType:       planning.NODE_TYPE_DEDUPLICATE_AND_MERGE,
						Details:        marshalDetails(&core.DeduplicateAndMergeDetails{}),
						Type:           "DeduplicateAndMerge",
						Children:       []int64{4},
						Description:    ``,
						ChildrenLabels: []string{""},
					},
				},
			},
		},
		"matrix anchored selector": {
			expr:      `some_metric[1m] anchored`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Version:   planning.QueryPlanV4,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
							Anchored:           true,
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] anchored`,
					},
				},
			},
		},
		"matrix smoothed selector": {
			expr:      `some_metric[1m] smoothed`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  0,
				Version:   planning.QueryPlanV4,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_MATRIX_SELECTOR,
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
							Smoothed:           true,
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] smoothed`,
					},
				},
			},
		},
	}

	ctx := context.Background()

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			testCase.expectedPlan.OriginalExpression = testCase.expr

			reg := prometheus.NewPedanticRegistry()
			opts := NewTestEngineOpts()
			opts.CommonOpts.NoStepSubqueryIntervalFn = func(_ int64) int64 {
				return (23 * time.Second).Milliseconds()
			}
			opts.CommonOpts.Reg = reg
			opts.CommonOpts.EnableDelayedNameRemoval = testCase.enableDelayedNameRemoval
			planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			originalPlan, err := planner.NewQueryPlan(ctx, testCase.expr, testCase.timeRange, NoopPlanningObserver{})
			require.NoError(t, err)

			requireHistogramCounts(t, reg, "cortex_mimir_query_engine_plan_stage_latency_seconds", `
{stage="Original plan", stage_type="Plan"} 1
{stage="Parsing", stage_type="AST"} 1
{stage="Pre-processing", stage_type="AST"} 1
			`)

			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_mimir_query_engine_plans_generated_total Total number of query plans generated.
				# TYPE cortex_mimir_query_engine_plans_generated_total counter
				cortex_mimir_query_engine_plans_generated_total{version="%d"} 1
			`, originalPlan.Version)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_mimir_query_engine_plans_generated_total"))

			// Encode plan, confirm it matches what we expect
			encoded, nodeIndices, err := originalPlan.ToEncodedPlan(true, true)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedPlan, encoded)
			require.Equal(t, []int64{testCase.expectedPlan.RootNode}, nodeIndices)

			// Decode plan tree from root node, confirm it matches the original plan
			nodes, err := encoded.DecodeNodes(encoded.RootNode)
			require.NoError(t, err)
			require.Len(t, nodes, 1)
			require.Equal(t, originalPlan.Root, nodes[0])

			require.Equal(t, originalPlan.Parameters, encoded.DecodeParameters())
		})
	}
}

func TestToEncodedPlan_SpecificNodesRequested(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	expr := `topk(5, foo)`
	ctx := context.Background()
	plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(time.Now()), NoopPlanningObserver{})
	require.NoError(t, err)

	aggregationNode := plan.Root.(*core.AggregateExpression)
	numberLiteralNode := aggregationNode.Param
	vectorSelectorNode := aggregationNode.Inner

	encoded, nodes, err := plan.ToEncodedPlan(false, true, numberLiteralNode, vectorSelectorNode)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Len(t, encoded.Nodes, 2)
	require.Equal(t, planning.NODE_TYPE_NUMBER_LITERAL, encoded.Nodes[nodes[0]].NodeType)
	require.Equal(t, planning.NODE_TYPE_VECTOR_SELECTOR, encoded.Nodes[nodes[1]].NodeType)
}

func TestToEncodedPlan_SameNodeProvidedMultipleTimes(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	expr := `sum(foo)`
	ctx := context.Background()
	plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(time.Now()), NoopPlanningObserver{})
	require.NoError(t, err)

	encoded, nodes, err := plan.ToEncodedPlan(false, true, plan.Root, plan.Root)
	require.NoError(t, err)
	require.Len(t, encoded.Nodes, 2)
	require.Equal(t, []int64{1, 1}, nodes)
	require.Equal(t, planning.NODE_TYPE_VECTOR_SELECTOR, encoded.Nodes[0].NodeType)
	require.Equal(t, planning.NODE_TYPE_AGGREGATE_EXPRESSION, encoded.Nodes[1].NodeType)
}

func TestPlanCreation_OptimisationPassGeneratesPlanWithHigherVersionThanAllowed(t *testing.T) {
	opts := NewTestEngineOpts()
	planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewStaticQueryPlanVersionProvider(12))
	require.NoError(t, err)

	planner.RegisterQueryPlanOptimizationPass(&optimizationPassThatGeneratesHigherVersionPlanThanAllowed{})

	plan, err := planner.NewQueryPlan(context.Background(), "foo", types.NewInstantQueryTimeRange(time.Now()), NoopPlanningObserver{})
	require.EqualError(t, err, "maximum supported query plan version is 12, but generated plan version is 13 - this is a bug")
	require.Nil(t, plan)
}

type optimizationPassThatGeneratesHigherVersionPlanThanAllowed struct{}

func (o *optimizationPassThatGeneratesHigherVersionPlanThanAllowed) Name() string {
	return "test optimization pass"
}

func (o *optimizationPassThatGeneratesHigherVersionPlanThanAllowed) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	plan.Root = newTestNode(maximumSupportedQueryPlanVersion + 1)
	return plan, nil
}

func TestPlanVersioning(t *testing.T) {
	planning.RegisterNodeFactory(func() planning.Node {
		return &versioningTestNode{NumberLiteralDetails: &core.NumberLiteralDetails{}}
	})

	originalMaximumPlanVersion := planning.MaximumSupportedQueryPlanVersion
	planning.MaximumSupportedQueryPlanVersion = 9001
	t.Cleanup(func() { planning.MaximumSupportedQueryPlanVersion = originalMaximumPlanVersion })

	// Plan has a node which has a min required plan version of 9000
	plan := &planning.QueryPlan{
		Root: newTestNode(9000),
		Parameters: &planning.QueryParameters{
			TimeRange:          types.NewInstantQueryTimeRange(time.Now()),
			OriginalExpression: "123",
		},
	}

	err := plan.DeterminePlanVersion()
	require.NoError(t, err)

	encoded, _, err := plan.ToEncodedPlan(false, true)
	require.NoError(t, err)
	require.Equal(t, planning.QueryPlanVersion(9000), encoded.Version)

	nodes, err := encoded.DecodeNodes(encoded.RootNode)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, plan.Root, nodes[0])

	require.Equal(t, plan.Parameters, encoded.DecodeParameters())
}

func TestDeduplicateAndMergePlanning(t *testing.T) {
	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"unary negation - should deduplicate and merge": {
			expr: `-some_metric`,
			expectedPlan: `
				- DeduplicateAndMerge
					- UnaryExpression: -
						- VectorSelector: {__name__="some_metric"}
			`,
		},
		"OR binary operation - should deduplicate and merge": {
			expr: `metric_a or metric_b`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: VectorSelector: {__name__="metric_a"}
						- RHS: VectorSelector: {__name__="metric_b"}
			`,
		},
		"time transformation - should deduplicate and merge": {
			expr: `hour(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: hour(...)
						- VectorSelector: {__name__="some_metric"}
			`,
		},
		"range vector function which drops __name__ - should deduplicate and merge": {
			expr: `rate(some_metric[5m])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="some_metric"}[5m0s]
			`,
		},
		"instant vector function which drops __name__ - should deduplicate and merge": {
			expr: `abs(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: abs(...)
						- VectorSelector: {__name__="some_metric"}
			`,
		},
		"label_join - should deduplicate and merge": {
			expr: `label_join(some_metric, "new_label", "-", "label1", "label2")`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: label_join(...)
						- param 0: VectorSelector: {__name__="some_metric"}
						- param 1: StringLiteral: "new_label"
						- param 2: StringLiteral: "-"
						- param 3: StringLiteral: "label1"
						- param 4: StringLiteral: "label2"
			`,
		},
		"label_replace - should deduplicate and merge": {
			expr: `label_replace(some_metric, "dst", "$1", "src", "(.+)")`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: label_replace(...)
						- param 0: VectorSelector: {__name__="some_metric"}
						- param 1: StringLiteral: "dst"
						- param 2: StringLiteral: "$1"
						- param 3: StringLiteral: "src"
						- param 4: StringLiteral: "(.+)"
			`,
		},
		"instant vector function which doesn't drop __name__  - should NOT deduplicate and merge": {
			expr: `sort(some_metric)`,
			expectedPlan: `
				- FunctionCall: sort(...)
					- VectorSelector: {__name__="some_metric"}
			`,
		},
		"range  vector function which doesn't drop __name__- should NOT deduplicate and merge": {
			expr: `absent_over_time(some_metric[5m])`,
			expectedPlan: `
				- FunctionCall: absent_over_time(...)
					- MatrixSelector: {__name__="some_metric"}[5m0s]
			`,
		},
		"aritmetic vector-scalar operation - should deduplicate and merge": {
			expr: `some_metric * 2`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS * RHS
						- LHS: VectorSelector: {__name__="some_metric"}
						- RHS: NumberLiteral: 2
			`,
		},
		"comparison vector-scalar operation - should NOT deduplicate and merge": {
			expr: `some_metric > 2`,
			expectedPlan: `
				- BinaryExpression: LHS > RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: NumberLiteral: 2
			`,
		},
		"comparison vector-scalar operation with bool modifier - should deduplicate and merge": {
			expr: `some_metric > bool 2`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS > bool RHS
						- LHS: VectorSelector: {__name__="some_metric"}
						- RHS: NumberLiteral: 2
			`,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	observer := NoopPlanningObserver{}

	opts := NewTestEngineOpts()
	planner, err := NewQueryPlannerWithoutOptimizationPasses(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}

func BenchmarkPlanEncodingAndDecoding(b *testing.B) {
	testCases := []string{
		`foo`,
		`foo{env="test", region="us"}`,
		`sum(foo)`,
		`sum by (env) (foo)`,
		`sum(foo) + sum(foo)`,
		`foo + foo + foo + bar + foo`,
		`a + b + c + d + e`,
		`(a - a) + (a - a) + (a * b) + (a * b)`,
		`(a - b) + (c - d) + (e * f) + (g * h)`,
		`123`,
		`sum(rate(foo[5m]))`,
		`sum by (zone) (label_replace(rate(foo[5m]), "zone", "$1", "pod", "ingester-zone-(.*)-\\d+"))`,
		`some_metric * on (foo) some_other_metric`,
		`some_metric * ignoring (foo) group_left (bar) some_other_metric`,
		`(some_metric)[1m:1s] offset 3s`,
		`max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(bar[5m:])[10m:])`,
		`label_join(foo, "abc", "-") + label_join(bar, "def", ",")`,
	}

	opts := NewTestEngineOpts()
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(b, err)
	ctx := context.Background()

	for _, expr := range testCases {
		b.Run(expr, func(b *testing.B) {
			plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(timestamp.Time(0)), NoopPlanningObserver{})
			require.NoError(b, err)

			b.Run("encode", func(b *testing.B) {
				var marshalled []byte

				for b.Loop() {
					encoded, _, err := plan.ToEncodedPlan(false, true)
					if err != nil {
						require.NoError(b, err)
					}

					marshalled, err = encoded.Marshal()
					if err != nil {
						require.NoError(b, err)
					}
				}

				b.ReportMetric(float64(len(marshalled)), "B")
			})

			b.Run("decode", func(b *testing.B) {
				encoded, _, err := plan.ToEncodedPlan(false, true)
				require.NoError(b, err)

				marshalled, err := encoded.Marshal()
				require.NoError(b, err)

				for b.Loop() {
					unmarshalled := &planning.EncodedQueryPlan{}
					err := unmarshalled.Unmarshal(marshalled)
					if err != nil {
						require.NoError(b, err)
					}

					_, err = unmarshalled.DecodeNodes(unmarshalled.RootNode)
					if err != nil {
						require.NoError(b, err)
					}
				}
			})
		})
	}
}

func TestQueryPlanner_ActivityTracking(t *testing.T) {
	opts := NewTestEngineOpts()
	tracker := &testQueryTracker{}
	opts.ActiveQueryTracker = tracker
	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	expr := "test"
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	_, err = planner.NewQueryPlan(context.Background(), expr, timeRange, NoopPlanningObserver{})
	require.NoError(t, err)

	expectedPlanningActivities := []trackedQuery{
		{expr: "test", stage: "planning", timeRange: timeRange, deleted: true},
	}

	require.Equal(t, expectedPlanningActivities, tracker.queries)
}

func TestDecodingInvalidPlan(t *testing.T) {
	testCases := map[string]struct {
		input         *planning.EncodedQueryPlan
		expectedError string
	}{
		"unknown node type": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: 12345,
						Details:  []byte("foo"),
					},
				},
			},
			expectedError: "unknown node type: 12345",
		},
		"root node index out of range": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
				},
			},
			expectedError: "node index 1 out of range with 1 nodes in plan",
		},
		"negative root node index": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           -1,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
				},
			},
			expectedError: "node index -1 out of range with 1 nodes in plan",
		},
		"child node index out of range": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function: functions.FUNCTION_ABS,
						}),
						Children: []int64{1},
					},
				},
			},
			expectedError: "node index 1 out of range with 1 nodes in plan",
		},
		"negative child node index": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_FUNCTION_CALL,
						Details: marshalDetails(&core.FunctionCallDetails{
							Function: functions.FUNCTION_ABS,
						}),
						Children: []int64{-1},
					},
				},
			},
			expectedError: "node index -1 out of range with 1 nodes in plan",
		},
		"too many children for node": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op: core.BINARY_ADD,
						}),
						Children: []int64{1, 2, 3},
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
				},
			},
			expectedError: "node of type BinaryExpression expects 2 children, but got 3",
		},
		"not enough children for node": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "foo",
				RootNode:           0,
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_BINARY_EXPRESSION,
						Details: marshalDetails(&core.BinaryExpressionDetails{
							Op: core.BINARY_ADD,
						}),
						Children: []int64{1},
					},
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 5,
						}),
					},
				},
			},
			expectedError: "node of type BinaryExpression expects 2 children, but got 1",
		},
		"query plan version is too high": {
			input: &planning.EncodedQueryPlan{
				OriginalExpression: "123",
				Nodes: []*planning.EncodedNode{
					{
						NodeType: planning.NODE_TYPE_NUMBER_LITERAL,
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value: 123,
						}),
					},
				},
				Version: planning.MaximumSupportedQueryPlanVersion + 1,
			},
			expectedError: fmt.Sprintf("query plan has version %v, but the maximum supported query plan version is %v", planning.MaximumSupportedQueryPlanVersion+1, planning.MaximumSupportedQueryPlanVersion),
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			_, err := testCase.input.DecodeNodes(testCase.input.RootNode)
			require.EqualError(t, err, testCase.expectedError)
		})
	}
}

func requireHistogramCounts(t *testing.T, reg *prometheus.Registry, name string, expected string) {
	metrics := getMetrics(t, reg, name)
	builder := &strings.Builder{}

	for i, m := range metrics {
		if i > 0 {
			builder.WriteRune('\n')
		}

		require.NotNilf(t, m.Histogram, "expected series %v to be a histogram", m.Label)
		builder.WriteRune('{')

		for i, l := range m.Label {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(*l.Name)
			builder.WriteString(`="`)
			builder.WriteString(*l.Value)
			builder.WriteRune('"')
		}

		builder.WriteString("} ")
		builder.WriteString(strconv.FormatUint(*m.Histogram.SampleCount, 10))
	}

	require.Equal(t, strings.TrimSpace(expected), builder.String())
}

func timestampOf(ts int64) *time.Time {
	return core.TimeFromTimestamp(&ts)
}

func TestFunctionNeedsDeduplicationHandlesAllKnownFunctions(t *testing.T) {
	for fnc, name := range functions.Function_name {
		t.Run(name, func(t *testing.T) {
			require.NotPanics(t, func() {
				functionNeedsDeduplication(functions.Function(fnc))
			}, "functionNeedsDeduplication should handle %s", name)
		})
	}
}

// versioningTestNode is a node for use with TestPlanVersioning.
// It uses the NumberLiteralDetails to encode an arbitrary minimumRequiredPlanVersion
// Note that most of the Node interface functions return dummy values, and it does not support children.
type versioningTestNode struct {
	*core.NumberLiteralDetails
}

func newTestNode(minimumRequiredPlanVersion planning.QueryPlanVersion) *versioningTestNode {
	return &versioningTestNode{
		NumberLiteralDetails: &core.NumberLiteralDetails{Value: float64(minimumRequiredPlanVersion)},
	}
}

func (t *versioningTestNode) Describe() string {
	return ""
}

func (t *versioningTestNode) ChildrenLabels() []string {
	return []string{}
}

func (t *versioningTestNode) Details() proto.Message {
	return t.NumberLiteralDetails
}

func (t *versioningTestNode) NodeType() planning.NodeType {
	return planning.NODE_TYPE_TEST
}

func (t *versioningTestNode) Child(idx int) planning.Node {
	panic("this test node has no children")
}

func (t *versioningTestNode) ChildCount() int {
	return 0
}

func (t *versioningTestNode) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		panic("not supported")
	}
	return nil
}

func (t *versioningTestNode) ReplaceChild(_ int, _ planning.Node) error {
	panic("not supported")
}

func (t *versioningTestNode) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherTestNode, ok := other.(*versioningTestNode)
	return ok && t.NumberLiteralDetails == otherTestNode.NumberLiteralDetails
}

func (t *versioningTestNode) MergeHints(_ planning.Node) error {
	panic("not supported")
}

func (t *versioningTestNode) ChildrenTimeRange(_ types.QueryTimeRange) types.QueryTimeRange {
	return types.QueryTimeRange{}
}

func (t *versioningTestNode) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeScalar, nil
}

func (t *versioningTestNode) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return planning.NoDataQueried(), nil
}

func (t *versioningTestNode) ExpressionPosition() (posrange.PositionRange, error) {
	return posrange.PositionRange{}, nil
}

func (t *versioningTestNode) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersion(t.Value)
}
