// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package limitk

import (
	"context"
	"fmt"
	"math"
	"unsafe"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

// Query implements limitk() for instant and range queries
type Query struct {
	Inner                    types.InstantVectorOperator
	Param                    types.ScalarOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, New will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	k                  []int64 // The k value for each step
	kZero              bool    // True when all k params are 0
	kMax               int64   // The max(k) across all steps

	// index by output series id - populated via the SeriesMetadata() and then returned in the NextSeries()
	values []types.InstantVectorSeriesData

	// the next output series to be returned in NextSeries()
	nextSeriesIndex int
}

var _ types.InstantVectorOperator = &Query{}

func (t *Query) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// determine and validate k for each step.
	if err := t.initK(ctx); err != nil {
		return nil, err
	}

	// all the k values are 0 so we will have nothing to return
	if t.kZero {
		// We can't return any series, so stop now.
		return nil, nil
	}

	// all the series metadata we should consider
	innerSeries, err := t.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	defer types.SeriesMetadataSlicePool.Put(&innerSeries, t.MemoryConsumptionTracker)

	groupLabelsBytesFunc := aggregations.GroupLabelsBytesFunc(t.Grouping, t.Without)
	groups := map[string]*queryGroup{}
	seriesToGroups := make([]*queryGroup, 0, len(innerSeries))

	// Go through each series and find / create its group, and keep track of how many series contribute to each group.
	// We do this separately to the loop below so that we know the number of series in each group when we allocate
	// each group's `series` slice inside accumulateValue - this allows us to avoid allocating a huge slice if the
	// group only has a few series and `k` is large.
	for _, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			intSlice, err := types.IntSlicePool.Get(t.TimeRange.StepCount, t.MemoryConsumptionTracker)
			intSlice = intSlice[:t.TimeRange.StepCount]
			if err != nil {
				return nil, err
			}

			g = &queryGroup{
				stepCounter: &stepCounter{
					seriesIncludedAtStep: intSlice,
				},
			}
			groups[string(groupLabelsString)] = g
		}

		g.seriesCount++
		seriesToGroups = append(seriesToGroups, g)
	}

	outputSeriesCount := 0

	for idx, series := range innerSeries {
		g := seriesToGroups[idx]

		// In the future there could be a fast path here to move the next series iterator if the group has already been filled and avoiding reading the data.
		data, err := t.Inner.NextSeries(ctx)
		if err != nil {
			return nil, err
		}

		// Fast path - if the group is already filled then we do not need to consider any more series. Also ignore series with no data.
		if g.filled || (len(data.Floats) == 0 && len(data.Histograms) == 0) {
			g.seriesCount-- // Avoid allocating space for this series if we can.
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
			continue
		}

		if addedAdditionalSeriesToOutput, err := t.accumulateValue(series, data, g); err != nil {
			types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
			return nil, err
		} else if addedAdditionalSeriesToOutput {
			outputSeriesCount++
		}

		types.PutInstantVectorSeriesData(data, t.MemoryConsumptionTracker)
	}

	// this slice is returned by this func - SeriesMetadata()
	outputSeries, err := types.SeriesMetadataSlicePool.Get(outputSeriesCount, t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	// these slices are returned by NextSeries()
	t.values, err = instantVectorSeriesDataSlicePool.Get(outputSeriesCount, t.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}
	t.values = t.values[:outputSeriesCount]
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
			t.values[nextOutputSeriesIndex] = series.value

			nextOutputSeriesIndex--
		}

		// release the temporary series store and step counter for this group
		instantQuerySeriesSlicePool.Put(&g.accumulatedSeries, t.MemoryConsumptionTracker)
		types.IntSlicePool.Put(&g.stepCounter.seriesIncludedAtStep, t.MemoryConsumptionTracker)
	}

	return outputSeries, nil
}

func (t *Query) validateLimitKParam(v float64) error {
	// These error strings match the prometheus engine.
	if math.IsNaN(v) {
		return fmt.Errorf("Parameter value is NaN")
	}

	if v <= math.MinInt64 {
		return fmt.Errorf("Scalar value %v underflows int64", v)
	}

	if v >= math.MaxInt64 {
		return fmt.Errorf("Scalar value %v overflows int64", v)
	}

	return nil
}

func (t *Query) initK(ctx context.Context) error {
	// note that k can change per step. ie count(limitk(scalar(foo), http_requests))
	paramValues, err := t.Param.GetValues(ctx)
	if err != nil {
		return err
	}

	defer types.FPointSlicePool.Put(&paramValues.Samples, t.MemoryConsumptionTracker)

	t.k, err = types.Int64SlicePool.Get(t.TimeRange.StepCount, t.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	t.k = t.k[:t.TimeRange.StepCount]

	t.kZero = true
	t.kMax = 0

	for stepIdx := range t.TimeRange.StepCount {
		v := paramValues.Samples[stepIdx].F

		if err := t.validateLimitKParam(v); err != nil {
			return err
		}

		t.k[stepIdx] = max(int64(v), 0) // Ignore any negative values.

		if t.k[stepIdx] > 0 {
			t.kZero = false
			t.kMax = max(t.kMax, t.k[stepIdx])
		}
	}

	return nil
}

func (t *Query) functionName() string {
	return "limitk"
}

// Returns true if accumulating this series means that the group will return an additional series.
func (t *Query) accumulateValue(metadata types.SeriesMetadata, value types.InstantVectorSeriesData, g *queryGroup) (bool, error) {
	var err error

	if g.accumulatedSeries == nil {
		// This is deferred until we know how many series fit within a group.
		// Initialise a place to hold the series that will be returned for this group.
		maximumPossibleSeries := min(t.kMax, int64(g.seriesCount))

		g.accumulatedSeries, err = instantQuerySeriesSlicePool.Get(int(maximumPossibleSeries), t.MemoryConsumptionTracker)
		if err != nil {
			return false, err
		}
	}

	// Fast path - every step for this group has k series accumulated
	if g.filled {
		return false, nil
	}

	// Create a new sparse series where each step checks that we only include k points per group
	sparse := types.InstantVectorSeriesData{}

	// Worse case allocations
	sparse.Floats, err = types.FPointSlicePool.Get(len(value.Floats), t.MemoryConsumptionTracker)
	if err != nil {
		return false, err
	}
	sparse.Histograms, err = types.HPointSlicePool.Get(len(value.Histograms), t.MemoryConsumptionTracker)
	if err != nil {
		return false, err
	}

	allFilled := true
	for step := 0; step < t.TimeRange.StepCount; step++ {
		k := t.k[step]

		// track how many series with points at this step that we have accumulated for this group
		if int64(g.stepCounter.seriesIncludedAtStep[step]) < k {

			if len(value.Histograms) > step && value.Histograms[step].H != nil {
				sparse.Histograms = append(sparse.Histograms, promql.HPoint{
					T: value.Histograms[step].T,
					H: value.Histograms[step].H.Copy(),
				})
			} else if len(value.Floats) > step {
				sparse.Floats = append(sparse.Floats, promql.FPoint{
					T: value.Floats[step].T,
					F: value.Floats[step].F,
				})
			}

			g.stepCounter.seriesIncludedAtStep[step]++

			if int64(g.stepCounter.seriesIncludedAtStep[step]) < k {
				allFilled = false
			}
		}
	}

	g.accumulatedSeries = append(g.accumulatedSeries, querySeries{metadata, sparse})

	if allFilled {
		g.filled = true
	}

	return true, nil
}

func (t *Query) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	// Note that the next series to be returned has already been pre-calculated during SeriesMetadata()
	if t.nextSeriesIndex >= len(t.values) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := t.values[t.nextSeriesIndex]
	t.nextSeriesIndex++

	return data, nil
}

func (t *Query) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *Query) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := t.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	return t.Param.Prepare(ctx, params)
}

func (t *Query) Finalize(ctx context.Context) error {
	if err := t.Inner.Finalize(ctx); err != nil {
		return err
	}

	return t.Param.Finalize(ctx)
}

func (t *Query) Close() {
	t.Inner.Close()
	t.Param.Close()

	// reclaim the slice we use to store k
	types.Int64SlicePool.Put(&t.k, t.MemoryConsumptionTracker)

	// reclaim the slice we use to store the accumulated series data
	instantVectorSeriesDataSlicePool.Put(&t.values, t.MemoryConsumptionTracker)
}

type queryGroup struct {
	seriesCount       int           // Total number of series that contribute to this group.
	filled            bool          // Flag to indicate we have identified k series for this group and no more series need to be accumulated
	accumulatedSeries []querySeries // A place to store the series we will return for this group
	stepCounter       *stepCounter  // A utility to track the number of series with points for each step
}

type querySeries struct {
	metadata types.SeriesMetadata
	value    types.InstantVectorSeriesData
}

type stepCounter struct {
	// index by step --> number of series (which have a point at this step) we have included for the group
	seriesIncludedAtStep []int
}

var instantQuerySeriesSlicePool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []querySeries {
		return make([]querySeries, 0, size)
	}),
	limiter.LimitKQuerySeriesSlices,
	uint64(unsafe.Sizeof(querySeries{})),
	true,
	nil,
	nil,
)

var instantVectorSeriesDataSlicePool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(types.MaxExpectedSeriesPerResult, func(size int) []types.InstantVectorSeriesData {
		return make([]types.InstantVectorSeriesData, 0, size)
	}),
	limiter.InstantVectorSeriesDataSlices,
	uint64(unsafe.Sizeof(types.InstantVectorSeriesData{})),
	true,
	nil,
	nil,
)
