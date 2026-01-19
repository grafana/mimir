// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitklimitratio

import (
	"context"
	"math"
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type Operator struct {
	stepArg limitArgument

	Param                    types.ScalarOperator
	Inner                    types.InstantVectorOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, New will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations

	// the next output series to be returned in NextSeries()
	nextSeriesIndex int

	// true if this is a limit_ratio operation
	isRatio bool

	// return a new groupLimiter instance
	groupLimiterFactory func() (groupLimiter, error)

	// series idx --> groupLimiter
	seriesToGroups []groupLimiter
}

// initParam validates the given parameter and determines the k or ratio for each step.
// This function will also initialise the groupLimiterFactory() factory func which will return new groupLimiter instances with specific logic for limitk vs limit_ratio.
// The function will return a bool indicating if k/ratio arguments are non-zero, and a second bool which indicates if the groupLimiter requires each series metadata to be initialised/registered with it.
func (o *Operator) initParam(ctx context.Context) (canReturnAnyResults bool, err error) {

	if o.isRatio {
		stepArg, zeros, err := newLimitRatioArgument(ctx, o.annotations, o.MemoryConsumptionTracker, o.TimeRange.StepCount, o.Param)
		if err != nil {
			return false, err
		}
		o.stepArg = stepArg
		o.groupLimiterFactory = func() (groupLimiter, error) {
			return &groupLimiterRatio{stepArg: stepArg}, nil
		}
		return zeros < o.TimeRange.StepCount, nil
	}

	stepArg, zeros, err := newLimitkArgument(ctx, o.MemoryConsumptionTracker, o.TimeRange.StepCount, o.Param)
	if err != nil {
		return false, err
	}
	o.stepArg = stepArg
	o.groupLimiterFactory = func() (groupLimiter, error) {

		counter, err := newStepCounter(o.TimeRange.StepCount, o.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		return &groupLimiterK{
			stepCounter: counter,
			stepArg:     stepArg,
			stepsFilled: zeros,
		}, nil
	}

	return zeros < o.TimeRange.StepCount, nil
}

func (o *Operator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {

	// parse and validate the given input argument for this aggregate (ie k or ratio)
	// also initialises the factory method for creating new groupLimiters
	canReturnAnyResults, err := o.initParam(ctx)
	if err != nil {
		return nil, err
	} else if !canReturnAnyResults {
		// all the k/ratio values are 0 so we will have nothing to return
		return nil, nil
	}

	innerSeries, err := o.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	groupLabelsBytesFunc := aggregations.GroupLabelsBytesFunc(o.Grouping, o.Without)
	groups := map[string]groupLimiter{}

	o.seriesToGroups = make([]groupLimiter, len(innerSeries))

	// Go through each series and determine which group the series will belong to.
	// A limitGroup is created for each unique group, which will be used to assist in tracking the series values across the whole group at each step.
	// Note that we can not discard any series at this point, even if we observe more series then the k limit, as we do not yet know which series have data.
	// A series with no samples will not contribute to increasing k.
	for idx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: do not extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g, err = o.groupLimiterFactory()
			if err != nil {
				return nil, err
			}
			groups[string(groupLabelsString)] = g
		}

		g.increaseSeriesCount()
		o.seriesToGroups[idx] = g
	}

	// Perform a second iteration of all the series to initialize each series into its group.
	// This is done as a second step since now we know how many series are in each group.
	// This is only done for limit_ratio, as this step allows the caching of the series label hash
	for idx, g := range o.seriesToGroups {
		g.initSeries(idx, innerSeries[idx])
	}

	return innerSeries, nil
}

func (o *Operator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {

	if o.nextSeriesIndex >= len(o.seriesToGroups) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	g := o.seriesToGroups[o.nextSeriesIndex]
	o.seriesToGroups[o.nextSeriesIndex] = nil

	thisSeriesIndex := o.nextSeriesIndex
	o.nextSeriesIndex++

	// In the future there could be a fast path here to move the next series iterator if the group has already been filled and avoiding reading the data.
	data, err := o.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Note that the data slices are not reclaimed here unless there is an error or the limited series has no samples.
	retData, err := g.limitSeriesData(thisSeriesIndex, data, o.TimeRange)
	if err != nil {
		types.PutInstantVectorSeriesData(data, o.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, err
	} else if len(retData.Floats) == 0 && len(retData.Histograms) == 0 {
		types.PutInstantVectorSeriesData(data, o.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, nil
	}

	return retData, nil
}

func (o *Operator) ExpressionPosition() posrange.PositionRange {
	return o.expressionPosition
}

func (o *Operator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := o.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	return o.Param.Prepare(ctx, params)
}

func (o *Operator) AfterPrepare(ctx context.Context) error {
	if err := o.Inner.AfterPrepare(ctx); err != nil {
		return err
	}
	return o.Param.AfterPrepare(ctx)
}

func (o *Operator) Finalize(ctx context.Context) error {
	if err := o.Inner.Finalize(ctx); err != nil {
		return err
	}

	return o.Param.Finalize(ctx)
}

func (o *Operator) Close() {
	o.Inner.Close()
	o.Param.Close()

	if o.stepArg != nil {
		o.stepArg.close()
	}

	for _, g := range o.seriesToGroups {
		if g != nil {
			g.close()
		}
	}

}

func NewLimitK(
	inner types.InstantVectorOperator,
	param types.ScalarOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) types.InstantVectorOperator {
	grouping = initGrouping(grouping, without)

	return &Operator{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		annotations:              annotations,
		isRatio:                  false,
	}
}

func NewLimitRatio(
	inner types.InstantVectorOperator,
	param types.ScalarOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) types.InstantVectorOperator {
	grouping = initGrouping(grouping, without)

	return &Operator{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		annotations:              annotations,
		isRatio:                  true,
	}
}

func initGrouping(grouping []string, without bool) []string {
	if without {
		grouping = append(grouping, model.MetricNameLabel)
	}

	slices.Sort(grouping)
	return grouping
}

type limitStepCounter struct {
	// index by step --> number of series (which have a point at this step) we have included for the group
	seriesIncludedAtStep     []int
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
}

func (s *limitStepCounter) close() {
	types.IntSlicePool.Put(&s.seriesIncludedAtStep, s.memoryConsumptionTracker)
}

func (s *limitStepCounter) inc(step int) int {
	s.seriesIncludedAtStep[step]++
	return s.seriesIncludedAtStep[step]
}

func (s *limitStepCounter) count(step int) int {
	return s.seriesIncludedAtStep[step]
}

func newStepCounter(size int, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (*limitStepCounter, error) {
	intSlice, err := types.IntSlicePool.Get(size, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	intSlice = intSlice[:size]
	return &limitStepCounter{
		memoryConsumptionTracker: memoryConsumptionTracker,
		seriesIncludedAtStep:     intSlice,
	}, nil
}

type groupLimiter interface {
	close()

	// increaseSeriesCount tracks the number of series associated with this group
	increaseSeriesCount()

	// initSeries informs the groupLimiter that this series will be included in its group
	initSeries(seriesIndex int, series types.SeriesMetadata)

	// limitSeriesData reduces the series data to meet the k/ratio limit requirements
	limitSeriesData(seriesIndex int, value types.InstantVectorSeriesData, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error)
}

type groupLimiterRatio struct {
	seriesRefCount int // Track how many series are related to this group.
	stepArg        *limitRatioArgument
	seriesHashMap  map[int]float64
}

func (q *groupLimiterRatio) increaseSeriesCount() {
	q.seriesRefCount++
}

func (q *groupLimiterRatio) limitSeriesData(seriesIndex int, value types.InstantVectorSeriesData, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {

	value.Histograms = slices.DeleteFunc(value.Histograms, func(p promql.HPoint) bool {
		step := timeRange.PointIndex(p.T)
		return !q.accumulateSampleAtStep(seriesIndex, int(step))
	})

	value.Floats = slices.DeleteFunc(value.Floats, func(p promql.FPoint) bool {
		step := timeRange.PointIndex(p.T)
		return !q.accumulateSampleAtStep(seriesIndex, int(step))
	})

	return value, nil
}

func (q *groupLimiterRatio) accumulateSampleAtStep(seriesIndex int, step int) bool {
	ratioLimit := q.stepArg.r[step]
	sampleOffset := q.seriesHashMap[seriesIndex]

	// This logic matches the prometheus implementation of promql.HashRatioSampler in engine.go
	// Once https://github.com/prometheus/prometheus/pull/17516 is merged and vendored this code can directly reference the promql.HashRatioSampler
	// Note that the conditional has been kept identical to the promql.HashRatioSampler
	allow := (ratioLimit >= 0 && sampleOffset < ratioLimit) ||
		(ratioLimit < 0 && sampleOffset >= (1.0+ratioLimit))
	return allow
}
func (q *groupLimiterRatio) close() {}

func (q *groupLimiterRatio) initSeries(seriesIndex int, series types.SeriesMetadata) {

	if q.seriesHashMap == nil {
		q.seriesHashMap = make(map[int]float64, q.seriesRefCount)
	}

	// This hash matches the prometheus implementation of promql.HashRatioSampler in engine.go
	// Once https://github.com/prometheus/prometheus/pull/17516 is merged and vendored this code can directly reference the promql.HashRatioSampler
	// The reason for extracting this out is that currently the promql.HashRatioSampler does not expose this hash function directly,
	// which means we need to call both the hash and ratio comparison for every step. This code path allows for the hash to be
	// calculated once per series and referenced in each step comparison.
	const (
		float64MaxUint64 = float64(math.MaxUint64)
	)
	q.seriesHashMap[seriesIndex] = float64(series.Labels.Hash()) / float64MaxUint64
}

type groupLimiterK struct {
	seriesRefCount int
	stepCounter    *limitStepCounter // A utility to track the number of samples at each step which have been accumulated across the observed series
	stepsFilled    int               // Track the number of steps where we have k samples within the group. When stepsFilled == steps then this group is all filled.
	stepArg        *limitkArgument
}

func (q *groupLimiterK) increaseSeriesCount() {
	q.seriesRefCount++
}

func (q *groupLimiterK) initSeries(_ int, _ types.SeriesMetadata) {
	// not used
}

func (q *groupLimiterK) decreaseSeriesCount() {
	q.seriesRefCount--
	if q.seriesRefCount == 0 {
		q.close()
	}
}

func (q *groupLimiterK) accumulateSampleAtStep(step int) bool {
	if int64(q.stepCounter.count(step)) == q.stepArg.k[step] {
		return false
	}

	count := q.stepCounter.inc(step)

	if int64(count) == q.stepArg.k[step] {
		q.stepsFilled++
	}

	return true
}

func (q *groupLimiterK) limitSeriesData(_ int, value types.InstantVectorSeriesData, timeRange types.QueryTimeRange) (types.InstantVectorSeriesData, error) {

	defer func() {
		// Note this will also close() the group if it's series ref count has reached 0
		q.decreaseSeriesCount()
	}()

	if q.stepsFilled == q.stepArg.stepCount {
		return types.InstantVectorSeriesData{}, nil
	}

	value.Histograms = slices.DeleteFunc(value.Histograms, func(p promql.HPoint) bool {
		step := timeRange.PointIndex(p.T)
		return !q.accumulateSampleAtStep(int(step))
	})

	value.Floats = slices.DeleteFunc(value.Floats, func(p promql.FPoint) bool {
		step := timeRange.PointIndex(p.T)
		return !q.accumulateSampleAtStep(int(step))
	})

	return value, nil
}

func (q *groupLimiterK) close() {
	q.stepCounter.close()
}
