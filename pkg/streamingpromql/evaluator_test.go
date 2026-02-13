// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestEvaluator(t *testing.T) {
	opts := NewTestEngineOpts()
	opts.CommonOpts.Reg = prometheus.NewPedanticRegistry()
	planner, err := NewQueryPlanner(opts, NewStaticQueryPlanVersionProvider(planning.MaximumSupportedQueryPlanVersion))
	require.NoError(t, err)
	engine, err := NewEngine(opts, NewStaticQueryLimitsProvider(0, false), stats.NewQueryMetrics(opts.CommonOpts.Reg), planner)
	require.NoError(t, err)

	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(0).Add(2*time.Minute), time.Minute)
	stats := types.NewQueryStats()
	lookbackDelta := 5 * time.Minute

	storage := promqltest.LoadedStorage(t, `
	load 10s
		some_metric{group="group-1", idx="0"} 1+1x20
		some_metric{group="group-1", idx="1"} 1+2x20
		some_metric{group="group-1", idx="2"} 1+3x20
		some_metric{group="group-1", idx="3"} 1+4x20
		some_metric{group="group-2", idx="0"} 2+1x20
		some_metric{group="group-2", idx="1"} 3+2x20
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	scalarNode := &core.NumberLiteral{NumberLiteralDetails: &core.NumberLiteralDetails{Value: 12}}
	scalarOperator := scalars.NewScalarConstant(scalarNode.Value, timeRange, memoryConsumptionTracker, scalarNode.GetExpressionPosition().ToPrometheusType())

	stringNode := &core.StringLiteral{StringLiteralDetails: &core.StringLiteralDetails{Value: "foo"}}
	stringOperator := operators.NewStringLiteral(stringNode.Value, stringNode.GetExpressionPosition().ToPrometheusType())

	instantVectorNode := &core.VectorSelector{VectorSelectorDetails: &core.VectorSelectorDetails{
		Matchers: []*core.LabelMatcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "some_metric"},
			{Type: labels.MatchEqual, Name: "group", Value: "group-1"},
		},
	}}
	instantVectorSelector := &selectors.Selector{
		Queryable:                storage,
		TimeRange:                timeRange,
		Matchers:                 core.LabelMatchersToOperatorType(instantVectorNode.Matchers),
		LookbackDelta:            lookbackDelta,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}
	instantVectorOperator := selectors.NewInstantVectorSelector(instantVectorSelector, memoryConsumptionTracker, stats, false, false)

	rangeVectorNode := &core.MatrixSelector{MatrixSelectorDetails: &core.MatrixSelectorDetails{
		Matchers: []*core.LabelMatcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "some_metric"},
			{Type: labels.MatchEqual, Name: "group", Value: "group-2"},
		},
		Range: 30 * time.Second,
	}}
	rangeVectorSelector := &selectors.Selector{
		Queryable:                storage,
		TimeRange:                timeRange,
		Matchers:                 core.LabelMatchersToOperatorType(rangeVectorNode.Matchers),
		Range:                    rangeVectorNode.Range,
		MemoryConsumptionTracker: memoryConsumptionTracker,
	}
	rangeVectorOperator := selectors.NewRangeVectorSelector(rangeVectorSelector, memoryConsumptionTracker, stats)

	nodeRequests := []NodeEvaluationRequest{
		{
			Node:      scalarNode,
			TimeRange: timeRange,
			operator:  scalarOperator,
		},
		{
			Node:      stringNode,
			TimeRange: timeRange,
			operator:  stringOperator,
		},
		{
			Node:      instantVectorNode,
			TimeRange: timeRange,
			operator:  instantVectorOperator,
		},
		{
			Node:      rangeVectorNode,
			TimeRange: timeRange,
			operator:  rangeVectorOperator,
		},
	}

	params := &planning.OperatorParameters{
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Annotations:              annotations.New(),
		QueryStats:               stats,
	}

	evaluator, err := NewEvaluator(nodeRequests, params, engine, "this expression is not used")
	require.NoError(t, err)

	ctx := context.Background()
	observer := &loggingEvaluationObserver{}
	require.NoError(t, evaluator.Evaluate(ctx, observer))

	expectedObserverEvents := []evaluationObserverEvent{
		{node: scalarNode, event: "ScalarEvaluated", details: `[12 @[0] 12 @[60000] 12 @[120000]]`},
		{node: stringNode, event: "StringEvaluated", details: `foo`},
		{node: instantVectorNode, event: "SeriesMetadataEvaluated", details: `[{{__name__="some_metric", group="group-1", idx="0"} false} {{__name__="some_metric", group="group-1", idx="1"} false} {{__name__="some_metric", group="group-1", idx="2"} false} {{__name__="some_metric", group="group-1", idx="3"} false}]`},
		{node: rangeVectorNode, event: "SeriesMetadataEvaluated", details: `[{{__name__="some_metric", group="group-2", idx="0"} false} {{__name__="some_metric", group="group-2", idx="1"} false}]`},
		{node: instantVectorNode, event: "InstantVectorSeriesDataEvaluated", details: `series: 0, floats: [1 @[0] 7 @[60000] 13 @[120000]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 0, floats: [2 @[0]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 0, floats: [6 @[40000] 7 @[50000] 8 @[60000]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 0, floats: [12 @[100000] 13 @[110000] 14 @[120000]], histograms: []`},
		{node: instantVectorNode, event: "InstantVectorSeriesDataEvaluated", details: `series: 1, floats: [1 @[0] 13 @[60000] 25 @[120000]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 1, floats: [3 @[0]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 1, floats: [11 @[40000] 13 @[50000] 15 @[60000]], histograms: []`},
		{node: rangeVectorNode, event: "RangeVectorStepSamplesEvaluated", details: `series: 1, floats: [23 @[100000] 25 @[110000] 27 @[120000]], histograms: []`},
		{node: instantVectorNode, event: "InstantVectorSeriesDataEvaluated", details: `series: 2, floats: [1 @[0] 19 @[60000] 37 @[120000]], histograms: []`},
		{node: instantVectorNode, event: "InstantVectorSeriesDataEvaluated", details: `series: 3, floats: [1 @[0] 25 @[60000] 49 @[120000]], histograms: []`},
		{event: "EvaluationCompleted", details: `annotations: <nil>, stats: {26}`},
	}

	require.Equal(t, expectedObserverEvents, observer.events)
}

type loggingEvaluationObserver struct {
	events []evaluationObserverEvent
}

type evaluationObserverEvent struct {
	node    planning.Node
	event   string
	details string
}

func (l *loggingEvaluationObserver) SeriesMetadataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, series []types.SeriesMetadata) error {
	l.events = append(l.events, evaluationObserverEvent{node: node, event: "SeriesMetadataEvaluated", details: fmt.Sprintf("%v", series)})
	return nil
}

func (l *loggingEvaluationObserver) InstantVectorSeriesDataEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, seriesCount int, seriesData types.InstantVectorSeriesData) error {
	l.events = append(l.events, evaluationObserverEvent{node: node, event: "InstantVectorSeriesDataEvaluated", details: fmt.Sprintf("series: %v, floats: %v, histograms: %v", seriesIndex, seriesData.Floats, seriesData.Histograms)})
	return nil
}

func (l *loggingEvaluationObserver) RangeVectorStepSamplesEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, seriesIndex int, stepIndex int, stepData *types.RangeVectorStepData) error {
	floats, err := stepData.Floats.CopyPoints()
	if err != nil {
		return err
	}

	histograms, err := stepData.Histograms.CopyPoints()
	if err != nil {
		return err
	}

	l.events = append(l.events, evaluationObserverEvent{node: node, event: "RangeVectorStepSamplesEvaluated", details: fmt.Sprintf("series: %v, floats: %v, histograms: %v", seriesIndex, floats, histograms)})
	return nil
}

func (l *loggingEvaluationObserver) ScalarEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data types.ScalarData) error {
	l.events = append(l.events, evaluationObserverEvent{node: node, event: "ScalarEvaluated", details: fmt.Sprintf("%v", data.Samples)})
	return nil
}

func (l *loggingEvaluationObserver) StringEvaluated(ctx context.Context, evaluator *Evaluator, node planning.Node, data string) error {
	l.events = append(l.events, evaluationObserverEvent{node: node, event: "StringEvaluated", details: data})
	return nil
}

func (l *loggingEvaluationObserver) EvaluationCompleted(ctx context.Context, evaluator *Evaluator, annotations *annotations.Annotations, stats *types.QueryStats) error {
	l.events = append(l.events, evaluationObserverEvent{event: "EvaluationCompleted", details: fmt.Sprintf("annotations: %v, stats: %v", annotations, *stats)})
	return nil
}
