// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitklimitratio

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Note - this ratio sampler is used for limit_ratio to determine a hash for a series labels and
// to use this hash for the ratio comparison.
// In its current use, this RatioSampler is called via AddRatioSample(), which passes in the labels for
// each point comparison. This results in the hash for the series being redundantly calculated.
// A future enhancement will be to modify this utility to re-use a hash between ratio comparisons.
var ratiosampler promql.RatioSampler = promql.NewHashRatioSampler()

// discardPointAtStepK is the function we use in limitk mode to determine when to stop accumulating points for a series in a group.
// Once the number of series at the given step reaches k no more series in this group are considered.
// Note that k may not be step invariant, so we accumulate into a collection of sparse series for each group.
// ie if k is [ 1, 2, 3 ] we would expect to see the series within a group such as;
// {series1}[v, v, v]
// {series2}[_, v, v]
// {series3}[_, _, v]
var discardPointAtStepK = func(_ types.SeriesMetadata, step int, g *limitGroup, p stepArgument) bool {
	k := p.(*kParam).k[step]
	return int64(g.countAtStep(step)) == k
}

// discardPointAtStepR is the function we use in limit_ratio mode to determine if a series should be accumulated for a group at the given step.
// As above, the ratio value for each step may not be step invariant so each series in the group may be sparse.
var discardPointAtStepR = func(metadata types.SeriesMetadata, step int, g *limitGroup, p stepArgument) bool {
	r := p.(*ratioParam).r[step]
	return !ratiosampler.AddRatioSample(r, &promql.Sample{Metric: metadata.Labels})
}

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

	// index by output series id - populated via the SeriesMetadata() and then returned in the NextSeries()
	values []*types.InstantVectorSeriesData

	// the next output series to be returned in NextSeries()
	nextSeriesIndex int

	// true if this is a limit_ratio operation
	isRatio bool
}

// isLimitK returns true if this is a limitk operation.
// This is helper function to avoid negatives in limitk related conditionals.
func (t *Operator) isLimitK() bool {
	return !t.isRatio
}

// initParam validates the given parameter and determines the k or ratio for each step.
func (t *Operator) initParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) error {
	var err error

	if t.isRatio {
		t.stepArg, err = newRatioParam(ctx, annotations, memoryConsumptionTracker, stepCount, param)
	} else {
		t.stepArg, err = newKParam(ctx, memoryConsumptionTracker, stepCount, param)
	}
	return err
}

func (t *Operator) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error

	// parse and validate the given input argument for this aggregate (ie k or ratio)
	if err = t.initParam(ctx, t.annotations, t.MemoryConsumptionTracker, t.TimeRange.StepCount, t.Param); err != nil {
		return nil, err
	}
	defer t.stepArg.close()

	// all the k/ratio values are 0 so we will have nothing to return
	if t.stepArg.allZero() {
		return nil, nil
	}

	// all the series metadata we should consider
	innerSeries, err := t.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}
	defer types.SeriesMetadataSlicePool.Put(&innerSeries, t.MemoryConsumptionTracker)

	groupLabelsBytesFunc := aggregations.GroupLabelsBytesFunc(t.Grouping, t.Without)
	groups := map[string]*limitGroup{}
	seriesToGroups := make([]*limitGroup, len(innerSeries))

	defer func() {
		for _, group := range seriesToGroups {
			group.close()
		}
	}()

	// Go through each series and find / create its group, and keep track of how many series contribute to each group.
	// We do this separately to the loop below so that we know the number of series in each group when we allocate
	// each group's `series` slice inside accumulateValue - this allows us to avoid allocating a huge slice if the
	// group only has a few series and `k` is large.
	for idx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			// note - only limitk uses the step counter
			g, err = newLimitGroup(t.TimeRange.StepCount, t.MemoryConsumptionTracker, t.isLimitK())
			if err != nil {
				// in the case of an early exit all the groups can be closed
				return nil, err
			}

			groups[string(groupLabelsString)] = g
		}

		g.seriesCount++
		seriesToGroups[idx] = g
	}

	outputSeriesCount := 0

	// A function which can determine if a point can be discarded at this step
	var discardFunc = discardPointAtStepK

	if t.isRatio {
		discardFunc = discardPointAtStepR
	}

	for idx, series := range innerSeries {
		g := seriesToGroups[idx]

		// In the future there could be a fast path here to move the next series iterator if the group has already been filled and avoiding reading the data.
		data, err := t.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		// Fast path for limitk - no need to consider accumulation if the group is already filled
		if (t.isLimitK() && g.filled) || (len(data.Floats) == 0 && len(data.Histograms) == 0) {
			g.seriesCount-- // Avoid allocating space for this series if we can.
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
			continue
		}

		// Note that the data slices are not reclaimed unless there is an error. The existing slices in this record are used in the accumulation.
		// These slices will be reclaimed by the NextSeries() consumer.
		if addedAdditionalSeriesToOutput, err := t.accumulateValue(series, data, g, discardFunc); err != nil {
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
			return nil, err
		} else if addedAdditionalSeriesToOutput {
			outputSeriesCount++
		} else {
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
		}
	}

	// this slice is returned by this func - SeriesMetadata()
	outputSeries, err := types.SeriesMetadataSlicePool.Get(outputSeriesCount, t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// these slices are returned by NextSeries()
	t.values = make([]*types.InstantVectorSeriesData, outputSeriesCount)

	// ensure that the len(outputSeries) == outputSeriesCount so we can directly by index
	outputSeries = outputSeries[:outputSeriesCount]

	firstOutputSeriesIndexForNextGroup := 0

	// walk all the groups and match up the output series indexes with group series data
	// ie series[0] = groups[0].accumulatedSeries[0], series[1] = groups[0].accumulatedSeries[1], series[2] = groups[1].accumulatedSeries[0] ...
	for _, g := range groups {
		lastOutputSeriesIndexForGroup := firstOutputSeriesIndexForNextGroup + len(g.accumulatedSeries) - 1
		nextOutputSeriesIndex := lastOutputSeriesIndexForGroup
		firstOutputSeriesIndexForNextGroup += len(g.accumulatedSeries)

		for _, series := range g.accumulatedSeries {
			err := t.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(series.metadata.Labels)
			if err != nil {
				return nil, err
			}

			outputSeries[nextOutputSeriesIndex] = series.metadata
			t.values[nextOutputSeriesIndex] = &series.value

			nextOutputSeriesIndex--
		}

		// this close is also registered in a defer, but this allows an early release
		g.close()
	}

	return outputSeries, nil
}

// Returns true if accumulating this series means that the group will return an additional series.
func (t *Operator) accumulateValue(metadata types.SeriesMetadata, value types.InstantVectorSeriesData, g *limitGroup, discardFunc func(types.SeriesMetadata, int, *limitGroup, stepArgument) bool) (bool, error) {

	if g.accumulatedSeries == nil {
		// This is deferred until we know how many series fit within a group.
		// This is a temporary placeholder for the series that will be returned for this group.

		maxPossibleSeries := g.seriesCount

		if t.isLimitK() {
			maxPossibleSeries = int(min(t.stepArg.(*kParam).kMax, int64(g.seriesCount)))
		}

		g.initAccumulatedSeries(maxPossibleSeries)
	}

	iterator := &types.InstantVectorSeriesDataIterator{}
	iterator.Reset(value)

	// timestamps of the points we will discard
	tsNotNeeded := make(map[int64]struct{}, len(value.Floats)+len(value.Histograms))

	// read the first point in the series
	ts, _, _, _, ok := iterator.Next()
	if !ok {
		return false, nil
	}

	// walk all the steps and identify which points should be removed to create sparse vectors which meet the k/ratio limit for each step within the group
	allFilled := true
	step := 0
	for pt := t.TimeRange.StartT; pt <= t.TimeRange.EndT; pt += t.TimeRange.IntervalMilliseconds {

		// move the iterator to the next ts
		for ts < pt {
			ts, _, _, _, ok = iterator.Next()
			if !ok {
				break
			}
		}

		// a point at this step may not be required if there are already k series with a point at this step (limitk) or the ratio for this series at this step is not met (limit_ratio)
		if discardFunc(metadata, step, g, t.stepArg) {
			step++

			if ts == pt {
				tsNotNeeded[ts] = struct{}{}
			}

			continue
		}

		// track that we have accumulated a point in this group at this step
		g.incStepCounter(step)

		// Track if the group has filled each step to k series
		if t.isLimitK() && int64(g.countAtStep(step)) < t.stepArg.(*kParam).k[step] {
			allFilled = false
		}

		step++
	}

	// prune / zero out the discarded points. Note that the slice capacity will remain the same, but the length will be reduced.
	if len(tsNotNeeded) > 0 {
		value.Histograms = slices.DeleteFunc(value.Histograms, func(point promql.HPoint) bool {
			_, ok := tsNotNeeded[point.T]
			return ok
		})

		value.Floats = slices.DeleteFunc(value.Floats, func(point promql.FPoint) bool {
			_, ok := tsNotNeeded[point.T]
			return ok
		})
	}

	// after discarding points there is no data remaining
	if len(value.Floats) == 0 && len(value.Histograms) == 0 {
		return false, nil
	}

	g.accumulatedSeries = append(g.accumulatedSeries, &querySeries{metadata, value})

	// this only applies to limitk. It provides a fast path to block considering additional series for this group
	if t.isLimitK() && allFilled {
		g.filled = true
	}

	return true, nil
}

func (t *Operator) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	// Note that the next series to be returned has already been pre-calculated during SeriesMetadata()
	if t.nextSeriesIndex >= len(t.values) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := t.values[t.nextSeriesIndex]
	t.nextSeriesIndex++

	return *data, nil
}

func (t *Operator) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *Operator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := t.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	return t.Param.Prepare(ctx, params)
}

func (t *Operator) Finalize(ctx context.Context) error {
	if err := t.Inner.Finalize(ctx); err != nil {
		return err
	}

	return t.Param.Finalize(ctx)
}

func (t *Operator) Close() {
	t.Inner.Close()
	t.Param.Close()

	// if NextSeries() has not been called these allocations need to be released
	for i := t.nextSeriesIndex; i < len(t.values); i++ {
		types.FPointSlicePool.Put(&t.values[i].Floats, t.MemoryConsumptionTracker)
		types.HPointSlicePool.Put(&t.values[i].Histograms, t.MemoryConsumptionTracker)
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
	if without {
		grouping = append(grouping, labels.MetricName)
	}

	slices.Sort(grouping)

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
	if without {
		grouping = append(grouping, labels.MetricName)
	}

	slices.Sort(grouping)

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
