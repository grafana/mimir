// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type MultiAggregatorGroupEvaluator struct {
	inner                    types.InstantVectorOperator
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker

	instances []*MultiAggregatorInstanceOperator

	prepareCalled      bool
	afterPrepareCalled bool
}

func NewMultiAggregatorGroupEvaluator(
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
) *MultiAggregatorGroupEvaluator {
	return &MultiAggregatorGroupEvaluator{
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
	}
}

func (m *MultiAggregatorGroupEvaluator) AddInstance() *MultiAggregatorInstanceOperator {
	instance := &MultiAggregatorInstanceOperator{group: m}
	m.instances = append(m.instances, instance)
	return instance
}

func (m *MultiAggregatorGroupEvaluator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if m.prepareCalled {
		return nil
	}

	m.prepareCalled = true
	return m.inner.Prepare(ctx, params)
}

func (m *MultiAggregatorGroupEvaluator) AfterPrepare(ctx context.Context) error {
	if m.afterPrepareCalled {
		return nil
	}

	m.afterPrepareCalled = true
	return m.inner.AfterPrepare(ctx)
}

func (m *MultiAggregatorGroupEvaluator) ComputeOutputSeriesForAllInstances(ctx context.Context) error {
	// We can't pass any matchers to the inner operator because different consumers may provide different matchers.
	innerSeries, err := m.inner.SeriesMetadata(ctx, types.Matchers{})
	if err != nil {
		return err
	}

	defer types.SeriesMetadataSlicePool.Put(&innerSeries, m.memoryConsumptionTracker)

	for _, instance := range m.instances {
		groups, err := instance.aggregator.ComputeGroups(innerSeries)
		if err != nil {
			return err
		}

		instance.outputSeriesMetadata = groups
	}

	return nil
}

func (m *MultiAggregatorGroupEvaluator) ReadNextSeries(ctx context.Context) error {
	data, err := m.inner.NextSeries(ctx)
	if err != nil {
		return err
	}

	for idx, instance := range m.instances {
		isLastInstance := idx == len(m.instances)-1
		if err := instance.aggregator.AccumulateNextInnerSeries(data, isLastInstance); err != nil {
			return err
		}
	}

	return nil
}

func (m *MultiAggregatorGroupEvaluator) Finalize(ctx context.Context) error {
	// Only finalize the inner operator if all instances have been finalized.
	for _, instance := range m.instances {
		if !instance.finalized {
			return nil
		}
	}

	return m.inner.Finalize(ctx)
}

func (m *MultiAggregatorGroupEvaluator) Close() {
	// Only close the inner operator if all instances have been closed.
	for _, instance := range m.instances {
		if !instance.closed {
			return
		}
	}

	m.inner.Close()
}

type MultiAggregatorInstanceOperator struct {
	group              *MultiAggregatorGroupEvaluator
	expressionPosition posrange.PositionRange
	aggregator         *aggregations.Aggregator

	outputSeriesMetadata []types.SeriesMetadata

	finalized bool
	closed    bool
}

var _ types.InstantVectorOperator = (*MultiAggregatorInstanceOperator)(nil)

func (m *MultiAggregatorInstanceOperator) Configure(
	op parser.ItemType,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
) error {
	var err error
	m.aggregator, err = aggregations.NewAggregator(op, grouping, without, memoryConsumptionTracker, annotations, timeRange, m.group.inner.ExpressionPosition())
	if err != nil {
		return err
	}

	m.expressionPosition = expressionPosition

	return nil
}

func (m *MultiAggregatorInstanceOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return m.group.Prepare(ctx, params)
}

func (m *MultiAggregatorInstanceOperator) AfterPrepare(ctx context.Context) error {
	return m.group.AfterPrepare(ctx)
}

func (m *MultiAggregatorInstanceOperator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if m.outputSeriesMetadata == nil {
		if err := m.group.ComputeOutputSeriesForAllInstances(ctx); err != nil {
			return nil, err
		}
	}

	series := m.outputSeriesMetadata
	m.outputSeriesMetadata = nil
	return series, nil
}

func (m *MultiAggregatorInstanceOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if !m.aggregator.HasMoreOutputSeries() {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	for !m.aggregator.IsNextOutputSeriesComplete() {
		if err := m.group.ReadNextSeries(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	return m.aggregator.ComputeNextOutputSeries()
}

func (m *MultiAggregatorInstanceOperator) Finalize(ctx context.Context) error {
	if m.finalized {
		return nil
	}

	m.finalized = true
	return m.group.Finalize(ctx)
}

func (m *MultiAggregatorInstanceOperator) Close() {
	if m.closed {
		return
	}

	m.closed = true
	m.group.Close()
}

func (m *MultiAggregatorInstanceOperator) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}
