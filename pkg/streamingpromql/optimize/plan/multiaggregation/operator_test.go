// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Most of the operator logic is exercised by the tests in pkg/streamingpromql/testdata/ours/multi_aggregation.test.
// The tests below cover behaviour that is difficult or impossible to exercise through PromQL test scripts.
func TestOperator_FinishedReadingAndCloseBehaviour(t *testing.T) {
	ctx := context.Background()
	inner := &operators.TestOperator{}
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	group := NewMultiAggregatorGroupEvaluator(inner, memoryConsumptionTracker, types.NewInstantQueryTimeRange(time.Now()), log.NewNopLogger())

	instance1 := group.AddInstance()
	instance2 := group.AddInstance()

	require.NoError(t, instance1.FinishedReading(ctx))
	require.False(t, inner.FinishedReadingCalled, "should only call FinishedReading on inner operator after all instances have had FinishedReading called")
	require.NoError(t, instance1.FinishedReading(ctx))
	require.False(t, inner.FinishedReadingCalled, "should ignore second FinishedReading call from instance that already had FinishedReading called")
	require.NoError(t, instance2.FinishedReading(ctx))
	require.True(t, inner.FinishedReadingCalled, "should call FinishedReading on inner operator after all instances have had FinishedReading called")

	instance1.Close()
	require.False(t, inner.Closed, "should only close inner operator after all instances have been closed")
	instance1.Close()
	require.False(t, inner.Closed, "should ignore second Close call from instance already closed")
	instance2.Close()
	require.True(t, inner.Closed, "should close inner operator after all instances have been closed")
}

func TestOperator_Stats(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	storage := promqltest.LoadedStorage(t, `
		load 1m
			metric{env="prod", idx="0"} 1
			metric{env="prod", idx="1"} 2
			metric{env="test", idx="2"} 3
	`)
	t.Cleanup(func() { _ = storage.Close() })

	subset := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "env", "test"),
	}

	timeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))
	selector := selectors.NewInstantVectorSelector(
		&selectors.Selector{
			Queryable:                storage,
			TimeRange:                timeRange,
			LookbackDelta:            5 * time.Minute,
			Matchers:                 types.Matchers{types.Matcher{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			MemoryConsumptionTracker: memoryConsumptionTracker,
			Subsets:                  []selectors.Subset{{Filter: subset}},
		},
		memoryConsumptionTracker,
		false,
		false,
	)

	group := NewMultiAggregatorGroupEvaluator(selector, memoryConsumptionTracker, timeRange, log.NewNopLogger())

	instance1 := group.AddInstance()
	require.NoError(t, instance1.Configure(parser.SUM, nil, false, nil, -1, memoryConsumptionTracker, nil, timeRange, posrange.PositionRange{}))
	instance2 := group.AddInstance()
	require.NoError(t, instance2.Configure(parser.SUM, nil, false, subset, 0, memoryConsumptionTracker, nil, timeRange, posrange.PositionRange{}))

	require.NoError(t, instance1.Prepare(ctx, nil))
	require.NoError(t, instance2.Prepare(ctx, nil))

	// Read the results from both instances.
	metadata, err := instance1.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []types.SeriesMetadata{{Labels: labels.EmptyLabels()}}, metadata, "first consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)

	data, err := instance1.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: float64(6)}}}, data, "first consumer should get expected result")
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)

	metadata, err = instance2.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, []types.SeriesMetadata{{Labels: labels.EmptyLabels()}}, metadata, "second consumer should get expected series metadata")
	types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)

	data, err = instance2.NextSeries(ctx)
	require.NoError(t, err)
	require.Equal(t, types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: float64(3)}}}, data, "second consumer should get expected result")
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)

	// Call FinishedReading on both operators, and check that the statistics are calculated correctly.
	require.NoError(t, instance1.FinishedReading(ctx))
	require.NoError(t, instance2.FinishedReading(ctx))

	requireStats(t, instance1, ctx, 3, 3)
	requireStats(t, instance2, ctx, 1, 1)

	instance1.Close()
	instance2.Close()
	require.Zerof(t, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all instances to be returned to pool, current memory consumption is:\n%v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
}

func requireStats(t *testing.T, o types.Operator, ctx context.Context, expectedProcessed int64, expectedRead int64) {
	operatorStats, err := o.Stats(ctx)
	require.NoError(t, err)

	require.False(t, operatorStats.HasSubsets(), "subsets should not be present in statistics returned by duplication consumer")

	promStats, err := operatorStats.FinalizeAndComputePrometheusStats()
	require.NoError(t, err)

	require.Equal(t, expectedProcessed, promStats.TotalSamples)
	require.Equal(t, []int64{expectedProcessed}, promStats.TotalSamplesPerStep)
	require.Equal(t, expectedRead, promStats.SamplesRead)
	require.Equal(t, []int64{expectedRead}, promStats.SamplesReadPerStep)

	operatorStats.Close()
}
