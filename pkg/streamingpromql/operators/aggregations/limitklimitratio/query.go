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

var _ types.InstantVectorOperator = (*Query[param])(nil)
var _ queryInternalImpls[param] = (*limitKQueryInternalImpls[kParam])(nil)
var _ queryInternalImpls[param] = (*limitRatioQueryInternalImpls[ratioParam])(nil)

var ratiosampler promql.RatioSampler = promql.NewHashRatioSampler()

// queryInternalImpls defines an interface for implementing internal query logic for limitk and limit_ratio.
// The overall query execution path for limitk and limit_ratio is very similar.
// This abstraction allows for the majority of the implementation to be shared in the Query struct below.
type queryInternalImpls[p param] interface {
	// newLimitParam returns a new typed param which assists in the validation and management of the given input param
	newLimitParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (param, error)

	// discardSeries returns true if this series can be discarded / ignored and not accumulated into the group
	discardSeries(metadata types.SeriesMetadata, data types.InstantVectorSeriesData, g *queryGroup) bool

	// discardPointAtStep returns true if the point at this step should be discarded for this group
	discardPointAtStep(metadata types.SeriesMetadata, step int, g *queryGroup) bool

	// isAllFilled should return true if this group has accumulated the required number of series for a given step
	isAllFilled(step int, g *queryGroup) bool

	// maxPossibleSeries returns the maximum number of series that could be returned for a group
	maxPossibleSeries(g *queryGroup) int

	enableGroupStepCounter() bool
}

type limitKQueryInternalImpls[p kParam] struct {
	param *kParam
}

func (q *limitKQueryInternalImpls[p]) discardSeries(metadata types.SeriesMetadata, data types.InstantVectorSeriesData, g *queryGroup) bool {
	// once the required number of series for the group have been accumulated no further series need to be considered for this group
	return g.filled || (len(data.Floats) == 0 && len(data.Histograms) == 0)
}

func (q *limitKQueryInternalImpls[p]) discardPointAtStep(metadata types.SeriesMetadata, step int, g *queryGroup) bool {
	// in the case of a variable k we will discard points where the k number of series as already been reached for this step
	k := q.param.k[step]
	return int64(g.countAtStep(step)) == k
}

func (q *limitKQueryInternalImpls[p]) isAllFilled(step int, g *queryGroup) bool {
	return int64(g.countAtStep(step)) == q.param.k[step]
}

func (q *limitKQueryInternalImpls[p]) enableGroupStepCounter() bool {
	return true
}

func (q *limitKQueryInternalImpls[p]) newLimitParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (param, error) {
	var err error
	q.param, err = newKParam(ctx, memoryConsumptionTracker, stepCount, param)
	if err != nil {
		return nil, err
	}
	return q.param, nil
}

func (q *limitKQueryInternalImpls[p]) maxPossibleSeries(g *queryGroup) int {
	// consider the largest k vs the number of series observed for this group
	return int(min(q.param.kMax, int64(g.seriesCount)))
}

type limitRatioQueryInternalImpls[p ratioParam] struct {
	param *ratioParam
}

func (q *limitRatioQueryInternalImpls[p]) discardSeries(metadata types.SeriesMetadata, data types.InstantVectorSeriesData, g *queryGroup) bool {
	return q.param.rStepInvariant && !ratiosampler.AddRatioSample(q.param.r[0], &promql.Sample{Metric: metadata.Labels})
}

func (q *limitRatioQueryInternalImpls[p]) discardPointAtStep(metadata types.SeriesMetadata, step int, g *queryGroup) bool {
	r := q.param.r[step]
	return !ratiosampler.AddRatioSample(r, &promql.Sample{Metric: metadata.Labels})
}

func (q *limitRatioQueryInternalImpls[p]) enableGroupStepCounter() bool {
	return false
}

func (q *limitRatioQueryInternalImpls[p]) isAllFilled(step int, g *queryGroup) bool {
	return false
}

func (q *limitRatioQueryInternalImpls[p]) newLimitParam(ctx context.Context, annotations *annotations.Annotations, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, stepCount int, param types.ScalarOperator) (param, error) {
	var err error
	q.param, err = newRatioParam(ctx, annotations, memoryConsumptionTracker, stepCount, param)
	if err != nil {
		return nil, err
	}
	return q.param, nil
}

func (q *limitRatioQueryInternalImpls[p]) maxPossibleSeries(g *queryGroup) int {
	return g.seriesCount
}

type Query[p param] struct {
	impls queryInternalImpls[p]
	param param

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
}

func (t *Query[p]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error
	functionFailedToComplete := true

	// parse and validate the given input argument for this aggregate (ie k or ratio)
	t.param, err = t.impls.newLimitParam(ctx, t.annotations, t.MemoryConsumptionTracker, t.TimeRange.StepCount, t.Param)
	if err != nil {
		return nil, err
	}
	defer t.param.close()

	// all the k/ratio values are 0 so we will have nothing to return
	if t.param.allZero() {
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

	defer func() {
		for _, group := range seriesToGroups {
			group.close()
		}
	}()

	// Go through each series and find / create its group, and keep track of how many series contribute to each group.
	// We do this separately to the loop below so that we know the number of series in each group when we allocate
	// each group's `series` slice inside accumulateValue - this allows us to avoid allocating a huge slice if the
	// group only has a few series and `k` is large.
	for _, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g, err = newQueryGroup(t.TimeRange.StepCount, t.MemoryConsumptionTracker, t.impls.enableGroupStepCounter())
			if err != nil {
				// in the case of an early exit all the groups can be closed
				return nil, err
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

		if t.impls.discardSeries(series, data, g) {
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
	defer func() {
		if functionFailedToComplete {
			types.SeriesMetadataSlicePool.Put(&outputSeries, t.MemoryConsumptionTracker)
		}
	}()

	// these slices are returned by NextSeries()
	t.values = make([]*types.InstantVectorSeriesData, 0, outputSeriesCount)

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

		// this close is also registered in a defer, but this allows an early release
		g.close()
	}

	functionFailedToComplete = false
	return outputSeries, nil
}

// Returns true if accumulating this series means that the group will return an additional series.
func (t *Query[p]) accumulateValue(metadata types.SeriesMetadata, value types.InstantVectorSeriesData, g *queryGroup) (bool, error) {
	var err error

	if g.accumulatedSeries == nil {
		// This is deferred until we know how many series fit within a group.
		// This is a temporary placeholder for the series that will be returned for this group.
		g.initAccumulatedSeries(t.impls.maxPossibleSeries(g))
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
		types.FPointSlicePool.Put(&value.Floats, t.MemoryConsumptionTracker)
		return false, err
	}

	allFilled := true
	for step := 0; step < t.TimeRange.StepCount; step++ {

		if t.impls.discardPointAtStep(metadata, step, g) {
			continue
		}

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

		g.incStepCounter(step)

		if !t.impls.isAllFilled(step, g) {
			allFilled = false
		}
	}

	g.accumulatedSeries = append(g.accumulatedSeries, &querySeries{metadata, &sparse})

	// this only applies to limitk. It provides a fast path to block considering additional series for this group
	if allFilled {
		g.filled = true
	}

	return true, nil
}

func (t *Query[p]) NextSeries(_ context.Context) (types.InstantVectorSeriesData, error) {
	// Note that the next series to be returned has already been pre-calculated during SeriesMetadata()
	if t.nextSeriesIndex >= len(t.values) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	data := t.values[t.nextSeriesIndex]
	t.nextSeriesIndex++

	return *data, nil
}

func (t *Query[p]) ExpressionPosition() posrange.PositionRange {
	return t.expressionPosition
}

func (t *Query[p]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := t.Inner.Prepare(ctx, params); err != nil {
		return err
	}
	return t.Param.Prepare(ctx, params)
}

func (t *Query[p]) Finalize(ctx context.Context) error {
	if err := t.Inner.Finalize(ctx); err != nil {
		return err
	}

	return t.Param.Finalize(ctx)
}

func (t *Query[p]) Close() {
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

	impl := &limitKQueryInternalImpls[kParam]{}
	return &Query[*kParam]{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		annotations:              annotations,
		impls:                    impl,
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

	impl := &limitRatioQueryInternalImpls[ratioParam]{}
	return &Query[*ratioParam]{
		Inner:                    inner,
		Param:                    param,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		annotations:              annotations,
		impls:                    impl,
	}
}
