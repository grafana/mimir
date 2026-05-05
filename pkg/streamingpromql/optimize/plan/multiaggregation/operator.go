// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation

import (
	"context"
	"errors"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"
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
	timeRange                types.QueryTimeRange
	logger                   log.Logger

	instances []*MultiAggregatorInstanceOperator

	nextSeriesIndex            int
	haveComputedSeriesMetadata bool
	prepareCalled              bool
	afterPrepareCalled         bool

	cachedQueryStats *types.OperatorEvaluationStats
}

func NewMultiAggregatorGroupEvaluator(
	inner types.InstantVectorOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	logger log.Logger,
) *MultiAggregatorGroupEvaluator {
	return &MultiAggregatorGroupEvaluator{
		inner:                    inner,
		memoryConsumptionTracker: memoryConsumptionTracker,
		timeRange:                timeRange,
		logger:                   logger,
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

	m.haveComputedSeriesMetadata = true
	defer types.SeriesMetadataSlicePool.Put(&innerSeries, m.memoryConsumptionTracker)

	for _, instance := range m.instances {
		if err := instance.computeGroups(innerSeries); err != nil {
			return err
		}
	}

	return nil
}

func (m *MultiAggregatorGroupEvaluator) ReadNextSeries(ctx context.Context) error {
	data, err := m.inner.NextSeries(ctx)
	if err != nil {
		return err
	}

	thisSeriesIndex := m.nextSeriesIndex
	lastInstanceToConsumeSeries := m.findIndexOfLastInstanceToConsumeSeries(thisSeriesIndex)
	m.nextSeriesIndex++

	if lastInstanceToConsumeSeries == -1 {
		types.PutInstantVectorSeriesData(data, m.memoryConsumptionTracker)
		return nil
	}

	for idx, instance := range m.instances {
		if !instance.needToConsumeSeries(thisSeriesIndex) {
			continue
		}

		isLastInstance := idx == lastInstanceToConsumeSeries

		if err := instance.aggregator.AccumulateNextInnerSeries(data, isLastInstance); err != nil {
			return err
		}
	}

	return nil
}

func (m *MultiAggregatorGroupEvaluator) findIndexOfLastInstanceToConsumeSeries(unfilteredSeriesIndex int) int {
	for idx := len(m.instances) - 1; idx >= 0; idx-- {
		if m.instances[idx].needToConsumeSeries(unfilteredSeriesIndex) {
			return idx
		}
	}

	return -1
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

func (m *MultiAggregatorGroupEvaluator) QueryStats(ctx context.Context, instance *MultiAggregatorInstanceOperator) (*types.OperatorEvaluationStats, error) {
	if !m.allInstancesFinalized() {
		return nil, errors.New("MultiAggregatorGroupEvaluator: cannot get stats when one or more instances are not finalized")
	}

	if instance.hasReadStats {
		return nil, errors.New("MultiAggregatorGroupEvaluator: cannot get stats twice for the same instance")
	}

	if m.cachedQueryStats == nil {
		var err error
		m.cachedQueryStats, err = m.inner.Stats(ctx)
		if err != nil {
			return nil, err
		}
	}

	instance.hasReadStats = true
	stats := m.cachedQueryStats

	if m.allInstancesHaveReadStats() {
		// Last call: return stats without cloning, and clear reference to existing stats.
		m.cachedQueryStats = nil
	} else {
		var err error
		stats, err = stats.Clone() // FIXME: this is wasteful, we could just clone the subset needed
		if err != nil {
			return nil, err
		}
	}

	if len(instance.filters) > 0 {
		// If the inner operator was remotely executed on a querier that does not report stats or subset stats,
		// then the subset at the requested index won't be present.
		//
		// For simplicity during upgrades, and for consistency with remote execution's behaviour during the same circumstances,
		// we return an empty set of stats.
		if !stats.HasSubsets() {
			stats.Close()

			level.Warn(m.logger).Log("msg", "MultiAggregatorGroupEvaluator expected subset statistics, but none were present, so returning empty set of statistics. This is expected during an upgrade from queriers without stats support to those with stats support, but a bug otherwise.")
			return types.NewOperatorEvaluationStats(ctx, m.timeRange, m.memoryConsumptionTracker, 0)
		}

		stats.UseSubset(instance.subsetIndex)
	} else {
		stats.RemoveAllSubsets()
	}

	return stats, nil
}

func (m *MultiAggregatorGroupEvaluator) allInstancesFinalized() bool {
	for _, instance := range m.instances {
		if !instance.finalized {
			return false
		}
	}

	return true
}

func (m *MultiAggregatorGroupEvaluator) allInstancesHaveReadStats() bool {
	for _, instance := range m.instances {
		if !instance.hasReadStats {
			return false
		}
	}

	return true
}

func (m *MultiAggregatorGroupEvaluator) Close() {
	// Only close the inner operator if all instances have been closed.
	for _, instance := range m.instances {
		if !instance.closed {
			return
		}
	}

	m.inner.Close()

	if m.cachedQueryStats != nil {
		m.cachedQueryStats.Close()
		m.cachedQueryStats = nil
	}
}

type MultiAggregatorInstanceOperator struct {
	group              *MultiAggregatorGroupEvaluator
	expressionPosition posrange.PositionRange
	aggregator         *aggregations.Aggregator

	filters     []*labels.Matcher
	subsetIndex int // If filters is non-empty, the index in the inner operator's stats that we expect to find the subset statistics for this instance.

	// unfilteredSeriesBitmap contains one entry per unfiltered input series, where true indicates that it passes this instance's filters.
	// If this instance has no filters, this is nil.
	unfilteredSeriesBitmap []bool

	outputSeriesMetadata []types.SeriesMetadata

	finalized    bool
	hasReadStats bool
	closed       bool
}

var _ types.InstantVectorOperator = (*MultiAggregatorInstanceOperator)(nil)

func (m *MultiAggregatorInstanceOperator) Configure(
	op parser.ItemType,
	grouping []string,
	without bool,
	filters []*labels.Matcher,
	subsetIndex int,
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
	m.filters = filters
	m.subsetIndex = subsetIndex

	return nil
}

func (m *MultiAggregatorInstanceOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return m.group.Prepare(ctx, params)
}

func (m *MultiAggregatorInstanceOperator) AfterPrepare(ctx context.Context) error {
	return m.group.AfterPrepare(ctx)
}

func (m *MultiAggregatorInstanceOperator) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	// Note that we deliberately ignore the matchers passed here as we can't use them: there's no
	// guarantee that they apply to other instances in the same group.

	if !m.group.haveComputedSeriesMetadata {
		if err := m.group.ComputeOutputSeriesForAllInstances(ctx); err != nil {
			return nil, err
		}
	}

	series := m.outputSeriesMetadata
	m.outputSeriesMetadata = nil
	return series, nil
}

func (m *MultiAggregatorInstanceOperator) computeGroups(unfilteredSeries []types.SeriesMetadata) error {
	if m.aggregator == nil {
		// Already closed.
		return nil
	}

	var filteredSeries []types.SeriesMetadata

	if len(m.filters) == 0 {
		filteredSeries = unfilteredSeries
	} else {
		var err error
		filteredSeries, err = types.SeriesMetadataSlicePool.Get(len(unfilteredSeries), m.group.memoryConsumptionTracker)
		if err != nil {
			return err
		}

		m.unfilteredSeriesBitmap, err = types.BoolSlicePool.Get(len(unfilteredSeries), m.group.memoryConsumptionTracker)
		if err != nil {
			return err
		}

		for _, series := range unfilteredSeries {
			matches := m.matchesSeries(series.Labels)
			m.unfilteredSeriesBitmap = append(m.unfilteredSeriesBitmap, matches)

			if matches {
				filteredSeries, err = types.AppendSeriesMetadata(m.group.memoryConsumptionTracker, filteredSeries, series)
				if err != nil {
					return err
				}
			}
		}
	}

	var err error
	m.outputSeriesMetadata, err = m.aggregator.ComputeGroups(filteredSeries)
	if err != nil {
		return err
	}

	if len(m.filters) != 0 {
		// If we got a new slice to hold the filtered list of series, return it to the pool now.
		types.SeriesMetadataSlicePool.Put(&filteredSeries, m.group.memoryConsumptionTracker)
	}

	return nil
}

func (m *MultiAggregatorInstanceOperator) matchesSeries(series labels.Labels) bool {
	return types.MatchersMatch(m.filters, series)
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

func (m *MultiAggregatorInstanceOperator) needToConsumeSeries(unfilteredSeriesIndex int) bool {
	if m.aggregator == nil {
		// Closed.
		return false
	}

	if len(m.filters) == 0 {
		return true
	}

	return m.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (m *MultiAggregatorInstanceOperator) Finalize(ctx context.Context) error {
	if m.finalized {
		return nil
	}

	if m.aggregator != nil {
		m.aggregator.Finalize()
		m.aggregator = nil
	}

	types.BoolSlicePool.Put(&m.unfilteredSeriesBitmap, m.group.memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&m.outputSeriesMetadata, m.group.memoryConsumptionTracker)

	m.finalized = true
	return m.group.Finalize(ctx)
}

func (m *MultiAggregatorInstanceOperator) Stats(ctx context.Context) (*types.OperatorEvaluationStats, error) {
	return m.group.QueryStats(ctx, m)
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
