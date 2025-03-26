// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	prototypes "github.com/gogo/protobuf/types"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestPlanCreationEncodingAndDecoding(t *testing.T) {
	opts := NewTestEngineOpts()
	opts.CommonOpts.NoStepSubqueryIntervalFn = func(_ int64) int64 {
		return (23 * time.Second).Milliseconds()
	}

	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), log.NewNopLogger())
	require.NoError(t, err)

	instantQuery := types.NewInstantQueryTimeRange(timestamp.Time(1000))
	instantQueryEncodedTimeRange := planning.EncodedQueryTimeRange{StartT: 1000, EndT: 1000, IntervalMilliseconds: 1}
	rangeQuery := types.NewRangeQueryTimeRange(timestamp.Time(3000), timestamp.Time(5000), time.Second)
	rangeQueryEncodedTimeRange := planning.EncodedQueryTimeRange{StartT: 3000, EndT: 5000, IntervalMilliseconds: 1000}

	marshalDetails := func(m proto.Message) *prototypes.Any {
		a, err := prototypes.MarshalAny(m)
		require.NoError(t, err)
		return a
	}

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
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          &core.Timestamp{Timestamp: 0},
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
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          &core.Timestamp{Timestamp: 3000},
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
						Details: marshalDetails(&core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Timestamp:          &core.Timestamp{Timestamp: 5000},
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
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          &core.Timestamp{Timestamp: 0},
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
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          &core.Timestamp{Timestamp: 3000},
							ExpressionPosition: core.PositionRange{Start: 5, End: 30},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] @ 3000 (1970-01-01T00:00:03Z)`,
					},
					{
						Details: marshalDetails(&core.FunctionCallDetails{
							FunctionName:       "rate",
							ExpressionPosition: core.PositionRange{Start: 0, End: 31},
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
						Details: marshalDetails(&core.MatrixSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Type: 0, Name: "__name__", Value: "some_metric"},
							},
							Range:              60 * time.Second,
							Timestamp:          &core.Timestamp{Timestamp: 5000},
							ExpressionPosition: core.PositionRange{Start: 5, End: 28},
						}),
						Type:        "MatrixSelector",
						Description: `{__name__="some_metric"}[1m0s] @ 5000 (1970-01-01T00:00:05Z)`,
					},
					{
						Details: marshalDetails(&core.FunctionCallDetails{
							FunctionName:       "rate",
							ExpressionPosition: core.PositionRange{Start: 0, End: 29},
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
						Details: marshalDetails(&core.FunctionCallDetails{
							FunctionName:       "time",
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
						Details: marshalDetails(&core.FunctionCallDetails{
							FunctionName:       "year",
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
						Details: marshalDetails(&core.FunctionCallDetails{
							FunctionName:       "year",
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
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              3,
							ExpressionPosition: core.PositionRange{Start: 5, End: 6},
						}),
						Type:        "NumberLiteral",
						Description: `3`,
					},
					{
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
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 0, End: 1},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              3,
							ExpressionPosition: core.PositionRange{Start: 4, End: 5},
						}),
						Type:        "NumberLiteral",
						Description: `3`,
					},
					{
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
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 0, End: 1},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
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
						Details: marshalDetails(&core.NumberLiteralDetails{
							Value:              2,
							ExpressionPosition: core.PositionRange{Start: 19, End: 20},
						}),
						Type:        "NumberLiteral",
						Description: `2`,
					},
					{
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
						Details: marshalDetails(&core.SubqueryDetails{
							Range:              time.Minute,
							Step:               time.Second,
							Timestamp:          &core.Timestamp{Timestamp: 0},
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

			originalPlan, err := engine.NewQueryPlan(ctx, testCase.expr, testCase.timeRange, NoopPlanningObserver{})
			require.NoError(t, err)

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
