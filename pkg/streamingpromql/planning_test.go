// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/engineopts"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
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
		expr      string
		timeRange types.QueryTimeRange

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
		"vector selector with '@ 0'": {
			expr:      `some_metric @ 0`,
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
							Timestamp:          timestampOf(0),
							ExpressionPosition: core.PositionRange{Start: 0, End: 15},
						}),
						Type:        "VectorSelector",
						Description: `{__name__="some_metric"} @ 0 (1970-01-01T00:00:00Z)`,
					},
				},
			},
		},
		"vector selector with '@ start()'": {
			expr:      `some_metric @ start()`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  0,
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
				},
			},
		},
		"vector selector with '@ end()'": {
			expr:      `some_metric @ end()`,
			timeRange: rangeQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: rangeQueryEncodedTimeRange,
				RootNode:  0,
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
				RootNode:  1,
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
							ExpressionPosition: core.PositionRange{Start: 0, End: 30},
						}),
						Type:           "FunctionCall",
						Description:    `rate(...)`,
						Children:       []int64{0},
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
				RootNode:  1,
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
							ExpressionPosition: core.PositionRange{Start: 0, End: 28},
						}),
						Type:           "FunctionCall",
						Description:    `rate(...)`,
						Children:       []int64{0},
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
				RootNode:  0,
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
				},
			},
		},
		"function call with optional arguments provided": {
			expr:      `year(some_metric)`,
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
				},
			},
		},
		"unary expression": {
			expr:      `-some_metric`,
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
						NodeType: planning.NODE_TYPE_UNARY_EXPRESSION,
						Details: marshalDetails(&core.UnaryExpressionDetails{
							Op:                 core.UNARY_SUB,
							ExpressionPosition: core.PositionRange{Start: 0, End: 12},
						}),
						Type:           "UnaryExpression",
						Children:       []int64{0},
						Description:    `-`,
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
		"binary expression with two scalars": {
			expr:      `2 + 3`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
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
		"binary expression with vector and scalar": {
			expr:      `2 * some_metric`,
			timeRange: instantQuery,

			expectedPlan: &planning.EncodedQueryPlan{
				TimeRange: instantQueryEncodedTimeRange,
				RootNode:  2,
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
				},
			},
		},
		"binary expression with 'bool'": {
			expr:      `some_metric > bool 2`,
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
		"subquery with '@'": {
			expr:      `(some_metric)[1m:1s] @ 0`,
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
	}

	ctx := context.Background()

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			testCase.expectedPlan.OriginalExpression = testCase.expr

			reg := prometheus.NewPedanticRegistry()
			opts := engineopts.NewTestEngineOpts()
			opts.CommonOpts.NoStepSubqueryIntervalFn = func(_ int64) int64 {
				return (23 * time.Second).Milliseconds()
			}
			opts.CommonOpts.Reg = reg
			planner := NewQueryPlannerWithoutOptimizationPasses(opts)

			originalPlan, err := planner.NewQueryPlan(ctx, testCase.expr, testCase.timeRange, NoopPlanningObserver{})
			require.NoError(t, err)

			requireHistogramCounts(t, reg, "cortex_mimir_query_engine_plan_stage_latency_seconds", `
{stage="Original plan", stage_type="Plan"} 1
{stage="Parsing", stage_type="AST"} 1
{stage="Pre-processing", stage_type="AST"} 1
			`)

			// Encode plan, confirm it matches what we expect
			encoded, err := originalPlan.ToEncodedPlan(true, true)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedPlan, encoded)

			// Decode plan, confirm it matches the original plan
			decodedPlan, err := encoded.ToDecodedPlan()
			require.NoError(t, err)
			require.Equal(t, originalPlan, decodedPlan)
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

	opts := engineopts.NewTestEngineOpts()
	planner := NewQueryPlanner(opts)
	ctx := context.Background()

	for _, expr := range testCases {
		b.Run(expr, func(b *testing.B) {
			plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(timestamp.Time(0)), NoopPlanningObserver{})
			require.NoError(b, err)

			b.Run("encode", func(b *testing.B) {
				var marshalled []byte

				for b.Loop() {
					encoded, err := plan.ToEncodedPlan(false, true)
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
				encoded, err := plan.ToEncodedPlan(false, true)
				require.NoError(b, err)

				marshalled, err := encoded.Marshal()
				require.NoError(b, err)

				for b.Loop() {
					unmarshalled := &planning.EncodedQueryPlan{}
					err := unmarshalled.Unmarshal(marshalled)
					if err != nil {
						require.NoError(b, err)
					}

					_, err = unmarshalled.ToDecodedPlan()
					if err != nil {
						require.NoError(b, err)
					}
				}
			})
		})
	}
}

func TestQueryPlanner_ActivityTracking(t *testing.T) {
	opts := engineopts.NewTestEngineOpts()
	tracker := &testQueryTracker{}
	opts.CommonOpts.ActiveQueryTracker = tracker
	planner := NewQueryPlanner(opts)

	expr := "test"
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	_, err := planner.NewQueryPlan(context.Background(), expr, timeRange, NoopPlanningObserver{})
	require.NoError(t, err)

	expectedPlanningActivities := []trackedQuery{
		{expr: "test # (planning)", deleted: true},
	}

	require.Equal(t, expectedPlanningActivities, tracker.queries)
}

func TestAnalysisHandler(t *testing.T) {
	originalTimeSince := timeSince
	timeSince = func(_ time.Time) time.Duration { return 1234 * time.Millisecond }
	t.Cleanup(func() { timeSince = originalTimeSince })

	testCases := map[string]struct {
		params url.Values

		expectedResponse   string
		expectedStatusCode int
	}{
		"valid request for instant query": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
			},
			expectedResponse: `{
			  "originalExpression": "up",
			  "timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Final expression", "duration": null, "outputExpression": "up"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up"
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640995200000, "intervalMilliseconds": 1, "isInstant": true},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up"
				  }
				}
			  ]
			}`,
			expectedStatusCode: http.StatusOK,
		},

		"valid request for range query": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"10"},
			},
			expectedResponse: `{
			  "originalExpression": "up",
			  "timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
			  "astStages": [
				{"name": "Parsing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Pre-processing", "duration": 1234000000, "outputExpression": "up"},
				{"name": "Final expression", "duration": null, "outputExpression": "up"}
			  ],
			  "planningStages": [
				{
				  "name": "Original plan",
				  "duration": 1234000000,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up"
				  }
				},
				{
				  "name": "Final plan",
				  "duration": null,
				  "outputPlan": {
					"timeRange": {"startT": 1640995200000, "endT": 1640998800000, "intervalMilliseconds": 10000},
					"nodes": [
					  {"type": "VectorSelector", "description": "{__name__=\"up\"}"}
					],
					"originalExpression": "up"
				  }
				}
			  ]
			}`,
			expectedStatusCode: http.StatusOK,
		},

		"no params": {
			expectedResponse:   `missing 'query' parameter`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"no time range": {
			params: url.Values{
				"query": []string{`up`},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"foo"},
			},
			expectedResponse:   `could not parse 'time' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid start time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"foo"},
				"end":   []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10"},
			},
			expectedResponse:   `could not parse 'start' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid end time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"foo"},
				"step":  []string{"10"},
			},
			expectedResponse:   `could not parse 'end' parameter: cannot parse "foo" to a valid timestamp`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"invalid step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"foo"},
			},
			expectedResponse:   `could not parse 'step' parameter: cannot parse "foo" to a valid duration`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"0 step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"0"},
			},
			expectedResponse:   `step must be greater than 0`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"negative step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"-10"},
			},
			expectedResponse:   `step must be greater than 0`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"end before start": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T01:00:00Z"},
				"end":   []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `end time must be not be before start time`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing start time": {
			params: url.Values{
				"query": []string{`up`},
				"end":   []string{"2022-01-01T01:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing end time": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"missing step": {
			params: url.Values{
				"query": []string{`up`},
				"start": []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `missing 'time' parameter for instant query or 'start', 'end' and 'step' parameters for range query`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query start time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"start": []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query end time": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"end":   []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},
		"have both instant query time and range query step": {
			params: url.Values{
				"query": []string{`up`},
				"time":  []string{"2022-01-01T00:00:00Z"},
				"step":  []string{"10s"},
			},
			expectedResponse:   `cannot provide a mixture of parameters for instant query ('time') and range query ('start', 'end' and 'step')`,
			expectedStatusCode: http.StatusBadRequest,
		},

		"invalid expression": {
			params: url.Values{
				"query": []string{`-`},
				"time":  []string{"2022-01-01T01:00:00Z"},
			},
			expectedResponse:   `parsing expression failed: 1:2: parse error: unexpected end of input`,
			expectedStatusCode: http.StatusBadRequest,
		},
	}

	planner := NewQueryPlannerWithoutOptimizationPasses(engineopts.NewTestEngineOpts())
	handler := AnalysisHandler(planner)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.URL.RawQuery = testCase.params.Encode()
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, req)

			body := resp.Body.String()

			if testCase.expectedStatusCode == http.StatusOK {
				require.JSONEq(t, testCase.expectedResponse, body)
				require.Equal(t, "application/json", resp.Header().Get("Content-Type"))
			} else {
				require.Equal(t, testCase.expectedResponse, body)
				require.Equal(t, "text/plain", resp.Header().Get("Content-Type"))
			}

			require.Equal(t, testCase.expectedStatusCode, resp.Code)
			require.Equal(t, strconv.Itoa(len(body)), resp.Header().Get("Content-Length"))
		})
	}
}

func TestAnalysisHandler_PlanningDisabled(t *testing.T) {
	handler := AnalysisHandler(nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	body := resp.Body.String()

	require.Equal(t, "query planning is disabled, analysis is not available", body)
	require.Equal(t, "text/plain", resp.Header().Get("Content-Type"))
	require.Equal(t, http.StatusNotFound, resp.Code)
	require.Equal(t, strconv.Itoa(len(body)), resp.Header().Get("Content-Length"))
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
			expectedError: "root node index 1 out of range with 1 nodes in plan",
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
			expectedError: "root node index -1 out of range with 1 nodes in plan",
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
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			output, err := testCase.input.ToDecodedPlan()
			require.EqualError(t, err, testCase.expectedError)
			require.Nil(t, output)
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
