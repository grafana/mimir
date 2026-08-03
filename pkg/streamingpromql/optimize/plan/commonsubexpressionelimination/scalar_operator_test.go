// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestScalarOperator_Buffering(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	inner, expectedData := createTestScalarOperator(t, memoryConsumptionTracker)

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Read the values from the first consumer. This should trigger reading from the inner operator, and the values
	// should be buffered for the second consumer.
	d, err := consumer1.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData, d)
	require.Equal(t, 1, inner.getValuesCalls, "inner operator should have been read exactly once")
	require.True(t, buffer.valuesLoaded)
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)

	// Read the values from the second consumer. As it is the last consumer, it should receive the buffered values
	// directly, and the buffer should no longer hold any data.
	d, err = consumer2.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData, d)
	require.Equal(t, 1, inner.getValuesCalls, "inner operator should not have been read a second time")
	require.Nil(t, buffer.values.Samples, "buffered values should have been handed to the last consumer")
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)

	// Check that the inner operator hasn't been closed or had FinishedReading called yet.
	require.False(t, inner.finishedReadingCalled)
	require.False(t, inner.closed)

	// Call FinishedReading on each consumer, and check that the inner operator had FinishedReading called only after the last consumer had FinishedReading called.
	require.NoError(t, consumer1.FinishedReading(ctx))
	require.False(t, inner.finishedReadingCalled)
	require.NoError(t, consumer2.FinishedReading(ctx))
	require.True(t, inner.finishedReadingCalled)
	require.NoError(t, consumer1.FinishedReading(ctx), "it should be safe to call FinishedReading on either consumer a second time")
	require.NoError(t, consumer2.FinishedReading(ctx), "it should be safe to call FinishedReading on either consumer a second time")

	// Close both consumers, and check that the inner operator was closed.
	consumer1.Close()
	require.False(t, inner.closed)
	consumer2.Close()
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)

	// Make sure it's safe to close either consumer a second time.
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestScalarOperator_Cloning(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	inner, expectedData := createTestScalarOperator(t, memoryConsumptionTracker)

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Both consumers should get the same values, but not the same underlying slice.
	d1, err := consumer1.GetValues(ctx)
	require.NoError(t, err)
	d2, err := consumer2.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData, d1, "first consumer should get expected values")
	require.Equal(t, expectedData, d2, "second consumer should get expected values")
	require.NotSame(t, &d1.Samples[0], &d2.Samples[0], "consumers should not share sample slices")

	types.FPointSlicePool.Put(&d1.Samples, memoryConsumptionTracker)
	types.FPointSlicePool.Put(&d2.Samples, memoryConsumptionTracker)

	require.NoError(t, consumer1.FinishedReading(ctx))
	require.NoError(t, consumer2.FinishedReading(ctx))
	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestScalarOperator_ClosingConsumerBeforeReading(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	inner, expectedData := createTestScalarOperator(t, memoryConsumptionTracker)

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	// Close the first consumer before it reads anything. The inner operator should not have been read yet.
	consumer1.Close()
	require.Equal(t, 0, inner.getValuesCalls)
	require.False(t, inner.closed, "inner operator should not be closed while the second consumer is still open")

	// The second consumer can still read the values.
	d, err := consumer2.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedData, d)
	require.Equal(t, 1, inner.getValuesCalls)
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)

	require.NoError(t, consumer2.FinishedReading(ctx))
	require.True(t, inner.finishedReadingCalled)

	consumer2.Close()
	require.True(t, inner.closed)
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestScalarOperator_ReadingFails(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	inner := &testScalarOperator{getValuesErr: errors.New("something went wrong reading data")}

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	d, err := consumer1.GetValues(ctx)
	require.EqualError(t, err, "something went wrong reading data")
	require.Equal(t, types.ScalarData{}, d)

	consumer2.Close()
	consumer1.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestScalarOperator_PrepareAndAfterPrepareAreIdempotent(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
	inner := &testScalarOperator{}

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	require.NoError(t, consumer1.Prepare(ctx, nil))
	require.NoError(t, consumer2.Prepare(ctx, nil))
	require.Equal(t, 1, inner.prepareCalls, "inner operator should only be prepared once")

	require.NoError(t, consumer1.AfterPrepare(ctx))
	require.NoError(t, consumer2.AfterPrepare(ctx))
	require.Equal(t, 1, inner.afterPrepareCalls, "inner operator should only have AfterPrepare called once")

	consumer1.Close()
	consumer2.Close()
}

func TestScalarOperator_Finalize(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	storage := promqltest.LoadedStorage(t, `
		load 1m
			metric 4
	`)
	t.Cleanup(func() { _ = storage.Close() })

	timeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))
	selector := selectors.NewInstantVectorSelector(
		&selectors.Selector{
			Queryable:                storage,
			TimeRange:                timeRange,
			LookbackDelta:            5 * time.Minute,
			Matchers:                 types.Matchers{types.Matcher{Type: labels.MatchEqual, Name: model.MetricNameLabel, Value: "metric"}},
			MemoryConsumptionTracker: memoryConsumptionTracker,
		},
		memoryConsumptionTracker,
		false,
		false,
	)

	inner := scalars.NewInstantVectorToScalar(selector, timeRange, memoryConsumptionTracker, posrange.PositionRange{})

	buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
	consumer1 := buffer.AddConsumer()
	consumer2 := buffer.AddConsumer()

	require.NoError(t, consumer1.Prepare(ctx, nil))
	require.NoError(t, consumer2.Prepare(ctx, nil))

	// Read the values from both consumers.
	d, err := consumer1.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, types.ScalarData{Samples: []promql.FPoint{{T: 0, F: 4}}}, d)
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)

	d, err = consumer2.GetValues(ctx)
	require.NoError(t, err)
	require.Equal(t, types.ScalarData{Samples: []promql.FPoint{{T: 0, F: 4}}}, d)
	types.FPointSlicePool.Put(&d.Samples, memoryConsumptionTracker)

	// Call FinishedReading on both consumers, and check that the statistics are calculated correctly.
	require.NoError(t, consumer1.FinishedReading(ctx))
	require.NoError(t, consumer2.FinishedReading(ctx))

	requireStats(t, consumer1, ctx, 1, 1)
	requireStats(t, consumer2, ctx, 1, 1)

	consumer1.Close()
	consumer2.Close()
	requireNoMemoryConsumption(t, memoryConsumptionTracker)
}

func TestScalarOperator_FinalizeErrorCases(t *testing.T) {
	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	t.Run("finalizing before all consumers have finished reading", func(t *testing.T) {
		inner := &testScalarOperator{}
		buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
		consumer1 := buffer.AddConsumer()
		consumer2 := buffer.AddConsumer()

		require.NoError(t, consumer1.FinishedReading(ctx))

		_, _, err := consumer1.Finalize(ctx)
		require.EqualError(t, err, "ScalarDuplicationBuffer: cannot finalize when one or more consumers have not had FinishedReading called")

		consumer1.Close()
		consumer2.Close()
	})

	t.Run("finalizing the same consumer twice", func(t *testing.T) {
		innerStats, err := types.NewOperatorEvaluationStats(ctx, types.NewInstantQueryTimeRange(timestamp.Time(0)), memoryConsumptionTracker, 0)
		require.NoError(t, err)

		inner := &testScalarOperator{stats: innerStats}
		buffer := NewScalarDuplicationBuffer(inner, memoryConsumptionTracker)
		consumer1 := buffer.AddConsumer()
		consumer2 := buffer.AddConsumer()

		require.NoError(t, consumer1.FinishedReading(ctx))
		require.NoError(t, consumer2.FinishedReading(ctx))

		stats, _, err := consumer1.Finalize(ctx)
		require.NoError(t, err)
		stats.Close()

		_, _, err = consumer1.Finalize(ctx)
		require.EqualError(t, err, "ScalarDuplicationBuffer: cannot finalize the same consumer twice")

		stats, _, err = consumer2.Finalize(ctx)
		require.NoError(t, err)
		stats.Close()

		consumer1.Close()
		consumer2.Close()
	})
}

// createTestScalarOperator returns a testScalarOperator whose values use pooled slices, along with an equivalent
// ScalarData that does not use pooled slices, so the returned data can be checked in tests.
func createTestScalarOperator(t *testing.T, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*testScalarOperator, types.ScalarData) {
	samples, err := types.FPointSlicePool.Get(3, memoryConsumptionTracker)
	require.NoError(t, err)

	samples = append(samples,
		promql.FPoint{T: 0, F: 1},
		promql.FPoint{T: 1, F: 2},
		promql.FPoint{T: 2, F: 3},
	)

	expected := types.ScalarData{Samples: []promql.FPoint{
		{T: 0, F: 1},
		{T: 1, F: 2},
		{T: 2, F: 3},
	}}

	return &testScalarOperator{data: types.ScalarData{Samples: samples}}, expected
}

type testScalarOperator struct {
	data         types.ScalarData
	getValuesErr error

	getValuesCalls        int
	prepareCalls          int
	afterPrepareCalls     int
	finishedReadingCalled bool
	closed                bool

	stats *types.OperatorEvaluationStats
	annos annotations.Annotations
}

var _ types.ScalarOperator = &testScalarOperator{}

func (o *testScalarOperator) GetValues(_ context.Context) (types.ScalarData, error) {
	o.getValuesCalls++

	if o.getValuesErr != nil {
		return types.ScalarData{}, o.getValuesErr
	}

	return o.data, nil
}

func (o *testScalarOperator) ExpressionPosition() posrange.PositionRange {
	return posrange.PositionRange{}
}

func (o *testScalarOperator) Prepare(_ context.Context, _ *types.PrepareParams) error {
	o.prepareCalls++
	return nil
}

func (o *testScalarOperator) AfterPrepare(_ context.Context) error {
	o.afterPrepareCalls++
	return nil
}

func (o *testScalarOperator) FinishedReading(_ context.Context) error {
	o.finishedReadingCalled = true
	return nil
}

func (o *testScalarOperator) Finalize(_ context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return o.stats, o.annos, nil
}

func (o *testScalarOperator) Close() {
	o.closed = true
}
