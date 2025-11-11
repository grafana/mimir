// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitklimitratio

import (
	"context"
	"math"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type Operator struct {
	stepArg stepArgument

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
func (o *Operator) initParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (ok bool, requiresSeriesInit bool, err error) {

	if o.isRatio {
		stepArg, err := newStepArgumentRatio(ctx, annotations, memoryConsumptionTracker, stepCount, param)
		if err != nil {
			return false, false, err
		}
		o.stepArg = stepArg
		o.groupLimiterFactory = func() (groupLimiter, error) {
			return &groupLimiterRatio{stepArg: stepArg}, nil
		}
		return !stepArg.rAllZero, true, nil
	}

	stepArg, err := newStepArgumentK(ctx, memoryConsumptionTracker, stepCount, param)
	if err != nil {
		return false, false, err
	}
	o.stepArg = stepArg
	o.groupLimiterFactory = func() (groupLimiter, error) {

		counter, err := newStepCounter(stepCount, memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		return &groupLimiterK{
			memoryConsumptionTracker: memoryConsumptionTracker,
			stepCounter:              counter,
			stepArg:                  stepArg,
		}, nil
	}

	return !stepArg.kAllZero, false, nil
}

func (o *Operator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {

	// parse and validate the given input argument for this aggregate (ie k or ratio)
	// also initialises the factory method for creating new groupLimiters
	ok, requiresSeriesInit, err := o.initParam(ctx, o.annotations, o.MemoryConsumptionTracker, o.TimeRange.StepCount, o.Param)
	if err != nil {
		return nil, err
	} else if !ok {
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

		g.incSeriesRefCount()
		o.seriesToGroups[idx] = g
	}

	// Perform a second iteration of all the series to initialize each series into its group.
	// This is done as a second step since now we know how many series are in each group.
	// This is only done for limit_ratio, as this step allows the caching of the series label hash
	if requiresSeriesInit {
		for idx, g := range o.seriesToGroups {
			g.initSeries(idx, innerSeries[idx])
		}
	}

	return innerSeries, nil
}

// accumulateValue walks the given series and modifies it to produce a sparse vector which will meet the group k/ratio limits.
// If the group limits have already been reached, then the series is discarded.
// This function will return false if the given series is empty, can be fully discarded or has no samples which can contribute to the group.
func (o *Operator) accumulateValue(seriesIndex int, value *types.InstantVectorSeriesData, g groupLimiter) (bool, error) {

	iterator := &types.InstantVectorSeriesDataIterator{}
	iterator.Reset(*value)

	// The slices of floats and histograms within the given value will be modified to remove any steps which would exceed the k/ratio limit for the group.
	// Samples are shifted left, overriding samples which are not needed for a given step.
	nextFloatIndex := 0
	nextHistogramIndex := 0

	// read the first point in the series
	ts, f, h, _, ok := iterator.Next()
	if !ok {
		return false, nil
	}

	// If the group is already filled then these samples can be discarded and an empty series returned
	if !g.startAccumulatingSeries() {
		return false, nil
	}

	step := 0
	for pt := o.TimeRange.StartT; pt <= o.TimeRange.EndT; pt += o.TimeRange.IntervalMilliseconds {

		// move the iterator to the next ts
		for ts < pt {
			ts, f, h, _, ok = iterator.Next()
			if !ok {
				break
			}
		}

		if pt != ts || g.discardSampleAtStep(seriesIndex, step) {
			g.onStepAccumulated(step, false)
			step++
			continue
		}

		if h != nil {
			value.Histograms[nextHistogramIndex].T = ts
			value.Histograms[nextHistogramIndex].H = h
			nextHistogramIndex++
		} else {
			value.Floats[nextFloatIndex].T = ts
			value.Floats[nextFloatIndex].F = f
			nextFloatIndex++
		}

		// track that we have accumulated a point in this group at this step
		g.onStepAccumulated(step, true)
		step++
	}

	g.endAccumulatingSeries()

	// prune off excess samples which have been copied left
	value.Histograms = value.Histograms[:nextHistogramIndex]
	value.Floats = value.Floats[:nextFloatIndex]

	return len(value.Histograms)+len(value.Floats) > 0, nil
}

func (o *Operator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {

	if o.nextSeriesIndex >= len(o.seriesToGroups) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	defer func() {
		// Note this will also close() the group if it's series ref count has reached 0
		o.seriesToGroups[o.nextSeriesIndex].releaseSeries(o.nextSeriesIndex)
		// clear these references to allow the GC to clean up sooner
		o.seriesToGroups[o.nextSeriesIndex] = nil
		o.nextSeriesIndex++
	}()

	g := o.seriesToGroups[o.nextSeriesIndex]

	// In the future there could be a fast path here to move the next series iterator if the group has already been filled and avoiding reading the data.
	data, err := o.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Note that the data slices are not reclaimed unless there is an error or the series does not need to be accumulated.
	// The existing slices are used for the accumulation and will be reclaimed by the NextSeries() consumer.
	hasData, err := o.accumulateValue(o.nextSeriesIndex, &data, g)
	if err != nil {
		types.PutInstantVectorSeriesData(data, o.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, err
	} else if !hasData {
		types.PutInstantVectorSeriesData(data, o.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, nil
	}

	return data, nil
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
		grouping = append(grouping, labels.MetricName)
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

	incSeriesRefCount()

	// initSeries informs the groupLimiter that this series will be included in it's grouping
	initSeries(seriesIndex int, series types.SeriesMetadata)

	// releaseSeries informs the groupLimiter that this series has finished being accumulated and any internal resources related to this series can be released
	releaseSeries(seriesIndex int)

	// startAccumulatingSeries is invoked each time we start accumulating data for a series.
	// if false is returned then this series should not be accumulated - it can be discarded
	startAccumulatingSeries() bool

	// onStepAccumulated is invoked for each step in accumulating the data for a series
	onStepAccumulated(step int, hasValue bool)

	// endAccumulatingSeries is invoked once we have finished accumulating the series data
	endAccumulatingSeries()

	// discardSampleAtStep returns true if the sample at this step should be ignored/discarded
	discardSampleAtStep(seriesIndex int, step int) bool
}

type groupLimiterRatio struct {
	seriesRefCount int // Track how many series are related to this group.
	stepArg        *stepArgumentRatio
	seriesHashMap  map[int]float64
}

func (q *groupLimiterRatio) incSeriesRefCount() {
	q.seriesRefCount++
}

func (q *groupLimiterRatio) startAccumulatingSeries() bool   { return true }
func (q *groupLimiterRatio) onStepAccumulated(_ int, _ bool) {}
func (q *groupLimiterRatio) endAccumulatingSeries()          {}
func (q *groupLimiterRatio) close()                          {}

func (q *groupLimiterRatio) initSeries(seriesIndex int, series types.SeriesMetadata) {

	if q.seriesHashMap == nil {
		q.seriesHashMap = make(map[int]float64, q.seriesRefCount)
	}

	// This hash matches the prometheus implementation of promql.HashRatioSampler in engine.go
	// TODO update promql.HashRatioSampler to expose this hash function and call directly into this rather then calculate the hash here
	// The reason for extracting this out is that currently the promql.HashRatioSampler does not expose this hash function directly,
	// which means we need to call both the hash and ratio comparison for every step. This code path allows for the hash to be
	// calculated once per series and referenced in each step comparison.
	const (
		float64MaxUint64 = float64(math.MaxUint64)
	)
	sample := &promql.Sample{Metric: series.Labels}
	q.seriesHashMap[seriesIndex] = float64(sample.Metric.Hash()) / float64MaxUint64
}

func (q *groupLimiterRatio) releaseSeries(seriesIndex int) {
	delete(q.seriesHashMap, seriesIndex)
	q.seriesRefCount--
	if q.seriesRefCount == 0 {
		q.close()
	}
}

// discardSampleAtStep is the function we use in limit_ratio mode to determine if a series should be accumulated for a group at the given step.
// The ratio value for each step may not be step invariant so each series in the group may be sparse.
func (q *groupLimiterRatio) discardSampleAtStep(seriesIndex int, step int) bool {
	ratioLimit := q.stepArg.r[step]
	sampleOffset := q.seriesHashMap[seriesIndex]

	// This logic matches the prometheus implementation of promql.HashRatioSampler in engine.go
	// TODO update promql.HashRatioSampler to expose this test passing in the sampleOffset and call directly rather then calculate here
	// Note that the conditional has been kept identical to the promql.HashRatioSampler
	allow := (ratioLimit >= 0 && sampleOffset < ratioLimit) ||
		(ratioLimit < 0 && sampleOffset >= (1.0+ratioLimit))
	return !allow
}

type groupLimiterK struct {
	seriesRefCount           int
	stepCounter              *limitStepCounter // A utility to track the number of samples at each step which have been accumulated across the observed series
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	filled                   bool // A flag to track if all the required k samples have been filled for this group.
	tmpFilled                bool
	nextExpectedStep         int
	stepArg                  *stepArgumentK
}

func (q *groupLimiterK) incSeriesRefCount() {
	q.seriesRefCount++
}

func (q *groupLimiterK) initSeries(_ int, _ types.SeriesMetadata) {
	// not used
}

func (q *groupLimiterK) releaseSeries(_ int) {
	q.seriesRefCount--
	if q.seriesRefCount == 0 {
		q.close()
	}
}

func (q *groupLimiterK) startAccumulatingSeries() bool {
	q.tmpFilled = true
	q.nextExpectedStep = 0
	return !q.filled
}

func (q *groupLimiterK) onStepAccumulated(step int, hasValue bool) {
	count := 0
	if hasValue {
		count = q.stepCounter.inc(step)
	}
	if int64(count) < q.stepArg.k[step] || q.nextExpectedStep != step {
		q.tmpFilled = false
	}
	q.nextExpectedStep++
}

func (q *groupLimiterK) endAccumulatingSeries() {
	if q.tmpFilled && q.nextExpectedStep == q.stepArg.stepCount {
		q.filled = true
	}
}

func (q *groupLimiterK) close() {
	q.stepCounter.close()
}

// discardSampleAtStep is the function we use in limitk mode to determine when to stop accumulating points for a series in a group.
// Once the number of series at the given step reaches k no more series in this group are considered.
// Note that k may not be step invariant, so we accumulate into a collection of sparse series for each group.
// ie if k is [ 1, 2, 3 ] we would expect to see the series within a group such as;
// {series1}[v, v, v]
// {series2}[_, v, v]
// {series3}[_, _, v]
func (q *groupLimiterK) discardSampleAtStep(_ int, step int) bool {
	return int64(q.stepCounter.count(step)) == q.stepArg.k[step]
}
