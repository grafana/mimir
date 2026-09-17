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

	cachedStats       *types.OperatorEvaluationStats
	cachedAnnotations annotations.Annotations
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

func (m *MultiAggregatorGroupEvaluator) FinishedReading(ctx context.Context) error {
	// Only call FinishedReading on the inner operator if all instances have had FinishedReading called.
	for _, instance := range m.instances {
		if !instance.finishedReadingCalled {
			return nil
		}
	}

	return m.inner.FinishedReading(ctx)
}

func (m *MultiAggregatorGroupEvaluator) Finalize(ctx context.Context, instance *MultiAggregatorInstanceOperator) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	if !m.allInstancesFinishedReading() {
		return nil, nil, errors.New("MultiAggregatorGroupEvaluator: cannot finalize when one or more instances have not had FinishedReading called")
	}

	if instance.finalized {
		return nil, nil, errors.New("MultiAggregatorGroupEvaluator: cannot finalize the same instance twice")
	}

	if m.cachedStats == nil {
		var err error
		m.cachedStats, m.cachedAnnotations, err = m.inner.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	instance.finalized = true
	stats := m.cachedStats
	annos := m.cachedAnnotations

	if m.allInstancesFinalized() {
		// Last call: return stats without cloning, and clear references to existing stats and annotations.
		m.cachedStats = nil
		m.cachedAnnotations = nil
	} else {
		var err error
		stats, err = stats.Clone() // FIXME: this is wasteful, we could just clone the subset needed
		if err != nil {
			return nil, nil, err
		}

		annos = types.CloneAnnotations(annos)
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
			emptyStats, err := types.NewOperatorEvaluationStats(ctx, m.timeRange, m.memoryConsumptionTracker, 0)
			return emptyStats, annos, err
		}

		stats.UseSubset(instance.subsetIndex)
	} else {
		stats.RemoveAllSubsets()
	}

	return stats, annos, nil
}

func (m *MultiAggregatorGroupEvaluator) allInstancesFinishedReading() bool {
	for _, instance := range m.instances {
		if !instance.finishedReadingCalled {
			return false
		}
	}

	return true
}

func (m *MultiAggregatorGroupEvaluator) allInstancesFinalized() bool {
	for _, instance := range m.instances {
		if !instance.finalized {
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

	if m.cachedStats != nil {
		m.cachedStats.Close()
		m.cachedStats = nil
	}
}

type MultiAggregatorInstanceOperator struct {
	group              *MultiAggregatorGroupEvaluator
	expressionPosition posrange.PositionRange
	aggregator         *aggregations.Aggregator

	filters     []*labels.Matcher
	subsetIndex int // If filters is non-empty, the index in the inner operator's stats that we expect to find the subset statistics for this instance.

	// param is the scalar operator providing the parameter value for parameterized aggregations (eg. quantile).
	// nil for non-parameterized aggregations.
	param types.ScalarOperator

	// unfilteredSeriesBitmap contains one entry per unfiltered input series, where true indicates that it passes this instance's filters.
	// If this instance has no filters, this is nil.
	unfilteredSeriesBitmap []bool

	outputSeriesMetadata []types.SeriesMetadata

	finishedReadingCalled bool
	finalized             bool
	closed                bool
}

var _ types.InstantVectorOperator = (*MultiAggregatorInstanceOperator)(nil)

func (m *MultiAggregatorInstanceOperator) Configure(
	op parser.ItemType,
	grouping []string,
	without bool,
	filters []*labels.Matcher,
	subsetIndex int,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	timeRange types.QueryTimeRange,
	expressionPosition posrange.PositionRange,
	param types.ScalarOperator,
) error {
	var err error
	m.aggregator, err = aggregations.NewAggregator(op, grouping, without, memoryConsumptionTracker, timeRange, m.group.inner.ExpressionPosition())
	if err != nil {
		return err
	}

	m.expressionPosition = expressionPosition
	m.filters = filters
	m.subsetIndex = subsetIndex
	m.param = param

	return nil
}

func (m *MultiAggregatorInstanceOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := m.group.Prepare(ctx, params); err != nil {
		return err
	}

	if m.param != nil {
		return m.param.Prepare(ctx, params)
	}

	return nil
}

func (m *MultiAggregatorInstanceOperator) AfterPrepare(ctx context.Context) error {
	if err := m.group.AfterPrepare(ctx); err != nil {
		return err
	}

	if m.param != nil {
		return m.param.AfterPrepare(ctx)
	}

	return nil
}

func (m *MultiAggregatorInstanceOperator) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	// Note that we deliberately ignore the matchers passed here as we can't use them: there's no
	// guarantee that they apply to other instances in the same group.

	// For parameterized aggregations (eg. quantile), fetch and validate the parameter values
	// before computing output series, so that ParamData is available when ComputeNextOutputSeries is called.
	if m.param != nil {
		paramData, err := m.param.GetValues(ctx)
		if err != nil {
			return nil, err
		}

		aggregations.ValidateQuantileParam(paramData, m.param.ExpressionPosition(), &m.aggregator.Annotations)

		m.aggregator.ParamData = paramData
	}

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
	if m.finishedReadingCalled {
		return false
	}

	if len(m.filters) == 0 {
		return true
	}

	return m.unfilteredSeriesBitmap[unfilteredSeriesIndex]
}

func (m *MultiAggregatorInstanceOperator) FinishedReading(ctx context.Context) error {
	if m.finishedReadingCalled {
		return nil
	}

	m.aggregator.FinishedReading()

	types.BoolSlicePool.Put(&m.unfilteredSeriesBitmap, m.group.memoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&m.outputSeriesMetadata, m.group.memoryConsumptionTracker)

	if m.param != nil {
		if err := m.param.FinishedReading(ctx); err != nil {
			return err
		}
	}

	m.finishedReadingCalled = true
	return m.group.FinishedReading(ctx)
}

func (m *MultiAggregatorInstanceOperator) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := m.group.Finalize(ctx, m)
	if err != nil {
		return nil, nil, err
	}

	m.aggregator.Annotations.Merge(childAnnos)

	if m.param != nil {
		paramStats, paramAnnos, err := m.param.Finalize(ctx)
		if err != nil {
			return nil, nil, err
		}
		if err := stats.Add(paramStats); err != nil {
			return nil, nil, err
		}
		paramStats.Close()
		m.aggregator.Annotations.Merge(paramAnnos)
	}

	return stats, m.aggregator.Annotations, nil
}

func (m *MultiAggregatorInstanceOperator) Close() {
	if m.closed {
		return
	}

	if m.param != nil {
		m.param.Close()
	}

	m.closed = true
	m.group.Close()
}

func (m *MultiAggregatorInstanceOperator) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}
