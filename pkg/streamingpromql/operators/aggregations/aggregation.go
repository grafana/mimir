// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type Aggregation struct {
	Inner types.InstantVectorOperator

	aggregator         *Aggregator
	expressionPosition posrange.PositionRange
}

func NewAggregation(
	inner types.InstantVectorOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	op parser.ItemType,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*Aggregation, error) {
	aggregator, err := NewAggregator(op, grouping, without, memoryConsumptionTracker, annotations, timeRange, inner.ExpressionPosition())
	if err != nil {
		return nil, err
	}

	a := &Aggregation{
		Inner:              inner,
		expressionPosition: expressionPosition,
		aggregator:         aggregator,
	}

	return a, nil
}

type groupWithLabels struct {
	labels   labels.Labels
	group    *group
	dropName bool
}

type group struct {
	// The number of input series that belong to this group that we haven't yet seen.
	remainingSeriesCount uint

	// The index of the last series that contributes to this group.
	// Used to sort groups in the order that they'll be completed in.
	lastSeriesIndex int

	// The aggregation for this group of series.
	aggregation AggregationGroup
}

var _ types.InstantVectorOperator = &Aggregation{}

var groupPool = zeropool.New(func() *group {
	return &group{}
})

func (a *Aggregation) ExpressionPosition() posrange.PositionRange {
	return a.expressionPosition
}

func (a *Aggregation) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// If we've been passed extra matchers at runtime when grouping by a label, make sure
	// the matchers only apply to the fields we are grouping by. It shouldn't be the case
	// that we are given matchers that apply to other labels since the matchers are
	// created based hints applied to binary operations which come from aggregations but
	// this prevents misuse.
	if len(matchers) != 0 && len(a.aggregator.Grouping) != 0 {
		if !a.aggregator.Without {
			matchers = matchers.With(a.aggregator.Grouping...)
		} else {
			// We don't set hints for aggregations using "without" and we don't (yet) support
			// excluding matchers so drop any extra matchers passed at runtime if this is a
			// "without" aggregation.
			matchers = nil
		}
	}

	// Fetch the source series
	innerSeries, err := a.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	defer types.SeriesMetadataSlicePool.Put(&innerSeries, a.aggregator.MemoryConsumptionTracker)

	return a.aggregator.ComputeGroups(innerSeries)
}

func (a *Aggregation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if !a.aggregator.HasMoreOutputSeries() {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	if err := a.accumulateUntilNextGroupComplete(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return a.aggregator.ComputeNextOutputSeries()
}

func (a *Aggregation) accumulateUntilNextGroupComplete(ctx context.Context) error {
	for !a.aggregator.IsNextOutputSeriesComplete() {
		data, err := a.Inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}

			return err
		}

		if err := a.aggregator.AccumulateNextInnerSeries(data, true); err != nil {
			return err
		}
	}

	return nil
}

func (a *Aggregation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	// The wrapping operator (if any) is responsible for calling Prepare() on whatever provides a.ParamData, so we don't need to do it here.
	return a.Inner.Prepare(ctx, params)
}

func (a *Aggregation) AfterPrepare(ctx context.Context) error {
	// The wrapping operator (if any) is responsible for calling AfterPrepare() on whatever provides a.ParamData, so we don't need to do it here.
	return a.Inner.AfterPrepare(ctx)
}

func (a *Aggregation) Finalize(ctx context.Context) error {
	// The wrapping operator (if any) is responsible for calling Finalize() on whatever provides a.ParamData, so we don't need to do it here.
	return a.Inner.Finalize(ctx)
}

func (a *Aggregation) SetParamData(data types.ScalarData) {
	a.aggregator.ParamData = data
}

func (a *Aggregation) Close() {
	a.Inner.Close()
	a.aggregator.Close()
}

type groupSorter struct {
	metadata []types.SeriesMetadata
	groups   []*group
}

func (g groupSorter) Len() int {
	return len(g.metadata)
}

func (g groupSorter) Less(i, j int) bool {
	return g.groups[i].lastSeriesIndex < g.groups[j].lastSeriesIndex
}

func (g groupSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.groups[i], g.groups[j] = g.groups[j], g.groups[i]
}

func newAggregationCounterResetCollisionWarning(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewHistogramCounterResetCollisionWarning(expressionPosition, annotations.HistogramAgg)
}

func newAggregationMismatchedCustomBucketsHistogramInfo(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewMismatchedCustomBucketsHistogramsInfo(expressionPosition, annotations.HistogramAgg)
}

// Aggregator performs aggregations over a set of input data.
//
// It is implemented separately to the Aggregation operator above to allow reuse by the multi-aggregation optimization.
type Aggregator struct {
	Grouping                 []string // If this is a 'without' aggregation, NewAggregation will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Annotations              *annotations.Annotations
	TimeRange                types.QueryTimeRange

	// If the aggregation has a parameter, its values are expected
	// to be filled here by the wrapping operator.
	// Currently only used by the quantile aggregation.
	ParamData types.ScalarData

	metricNames             *operators.MetricNames
	currentSeriesIndex      int
	aggregationGroupFactory AggregationGroupFactory
	emitAnnotationFunc      types.EmitAnnotationFunc
	innerExpressionPosition posrange.PositionRange

	remainingInnerSeriesToGroup []*group // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*group // One entry per group, in the order we want to return them

	haveEmittedMixedFloatsAndHistogramsWarning bool
}

// NewAggregator creates a new Aggregator instance.
//
// It may mutate the grouping slice provided.
func NewAggregator(
	op parser.ItemType,
	grouping []string,
	without bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	timeRange types.QueryTimeRange,
	innerExpressionPosition posrange.PositionRange,
) (*Aggregator, error) {
	opGroupFactory := AggregationGroupFactories[op]
	if opGroupFactory == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("aggregation operation with '%s'", op))
	}

	if without {
		grouping = append(grouping, model.MetricNameLabel)
	}

	slices.Sort(grouping)

	a := &Aggregator{
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Annotations:              annotations,
		TimeRange:                timeRange,

		metricNames:             &operators.MetricNames{},
		aggregationGroupFactory: opGroupFactory,
		innerExpressionPosition: innerExpressionPosition,
	}

	a.emitAnnotationFunc = a.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return a, nil
}

// ComputeGroups determines the groups this Aggregator will produce for the given series.
//
// It is the caller's responsibility to return the provided slice to a pool.
func (a *Aggregator) ComputeGroups(innerSeries []types.SeriesMetadata) ([]types.SeriesMetadata, error) {
	if len(innerSeries) == 0 {
		// No input series == no output series.
		return nil, nil
	}

	a.metricNames.CaptureMetricNames(innerSeries)

	// Determine the groups we'll return.
	// Note that we use a string here to uniquely identify the groups, while Prometheus' engine uses a hash without any handling of hash collisions.
	// While rare, this may cause differences in the results returned by this engine and Prometheus' engine.
	groups := map[string]groupWithLabels{}
	groupLabelsBytesFunc := a.groupLabelsBytesFunc()
	groupLabelsFunc := a.groupLabelsFunc()
	a.remainingInnerSeriesToGroup = make([]*group, 0, len(innerSeries))

	for seriesIdx, series := range innerSeries {
		groupLabelsString := groupLabelsBytesFunc(series.Labels)
		g, groupExists := groups[string(groupLabelsString)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !groupExists {
			g.labels = groupLabelsFunc(series.Labels)
			g.group = groupPool.Get()
			g.group.aggregation = a.aggregationGroupFactory()
			g.group.remainingSeriesCount = 0
			g.dropName = series.DropName

			groups[string(groupLabelsString)] = g
		}

		g.group.remainingSeriesCount++
		g.group.lastSeriesIndex = seriesIdx
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g.group)
		// If at least 1 series drops the name, we drop the name for all series.
		if !g.dropName && series.DropName {
			g.dropName = series.DropName
			groups[string(groupLabelsString)] = g
		}
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(groups), a.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	a.remainingGroups = make([]*group, 0, len(groups))

	for _, g := range groups {
		seriesMetadata, err = types.AppendSeriesMetadata(a.MemoryConsumptionTracker, seriesMetadata, types.SeriesMetadata{Labels: g.labels, DropName: g.dropName})
		if err != nil {
			return nil, err
		}
		a.remainingGroups = append(a.remainingGroups, g.group)
	}

	sort.Sort(groupSorter{seriesMetadata, a.remainingGroups})

	return seriesMetadata, nil
}

func (a *Aggregator) groupLabelsBytesFunc() SeriesToGroupLabelsBytesFunc {
	return GroupLabelsBytesFunc(a.Grouping, a.Without)
}

// seriesToGroupLabelsFunc is a function that returns the output group labels for the given input series.
type seriesToGroupLabelsFunc func(labels.Labels) labels.Labels

func (a *Aggregator) groupLabelsFunc() seriesToGroupLabelsFunc {
	switch {
	case a.Without:
		lb := labels.NewBuilder(labels.EmptyLabels())
		return func(m labels.Labels) labels.Labels {
			lb.Reset(m)
			lb.Del(a.Grouping...) // NewAggregation will add __name__ to Grouping for 'without' aggregations, so no need to add it here.
			l := lb.Labels()
			return l
		}

	case len(a.Grouping) == 0:
		return groupToSingleSeriesLabelsFunc

	default:
		lb := labels.NewBuilder(labels.EmptyLabels())
		return func(m labels.Labels) labels.Labels {
			lb.Reset(m)
			lb.Keep(a.Grouping...)
			l := lb.Labels()
			return l
		}
	}
}

var groupToSingleSeriesLabelsFunc = func(_ labels.Labels) labels.Labels { return labels.EmptyLabels() }

// AccumulateNextInnerSeries accumulates the provided series into its corresponding group.
//
// If takeOwnershipOfData is true, the provided data will be returned to a pool when this function returns,
// and points within the data slices may be mutated. (For example, FloatHistogram instances may be mutated in-place.)
//
// If takeOwnershipOfData is false, the provided data will not be returned to a pool when this function returns,
// and points within the data slices will not be mutated.
func (a *Aggregator) AccumulateNextInnerSeries(data types.InstantVectorSeriesData, takeOwnershipOfData bool) error {
	if takeOwnershipOfData {
		defer types.PutInstantVectorSeriesData(data, a.MemoryConsumptionTracker)
	}

	thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
	a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]
	if err := thisSeriesGroup.aggregation.AccumulateSeries(data, a.TimeRange, a.MemoryConsumptionTracker, a.emitAnnotationFunc, thisSeriesGroup.remainingSeriesCount, takeOwnershipOfData); err != nil {
		return err
	}

	thisSeriesGroup.remainingSeriesCount--
	a.currentSeriesIndex++

	return nil
}

// ComputeNextOutputSeries computes the final result for the next output series from this Aggregator.
func (a *Aggregator) ComputeNextOutputSeries() (types.InstantVectorSeriesData, error) {
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Construct the group and return it
	seriesData, hasMixedData, err := thisGroup.aggregation.ComputeOutputSeries(a.ParamData, a.TimeRange, a.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if hasMixedData && !a.haveEmittedMixedFloatsAndHistogramsWarning {
		a.Annotations.Add(annotations.NewMixedFloatsHistogramsAggWarning(a.innerExpressionPosition))

		// The warning description only varies based on the position of the expression this operator represents, so only emit it
		// once, to avoid unnecessary work if there are many instances of floats and histograms conflicting.
		a.haveEmittedMixedFloatsAndHistogramsWarning = true
	}

	thisGroup.aggregation.Close(a.MemoryConsumptionTracker)
	groupPool.Put(thisGroup)

	return seriesData, nil
}

// IsNextOutputSeriesComplete returns true if all inner series for the next output series have been passed
// to AccumulateNextInnerSeries.
func (a *Aggregator) IsNextOutputSeriesComplete() bool {
	return a.remainingGroups[0].remainingSeriesCount == 0
}

// HasMoreOutputSeries returns true if there are more output series to produce.
func (a *Aggregator) HasMoreOutputSeries() bool {
	return len(a.remainingGroups) > 0
}

func (a *Aggregator) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := a.metricNames.GetMetricNameForSeries(a.currentSeriesIndex)
	a.Annotations.Add(generator(metricName, a.innerExpressionPosition))
}

func (a *Aggregator) Close() {
	if a.ParamData.Samples != nil {
		types.FPointSlicePool.Put(&a.ParamData.Samples, a.MemoryConsumptionTracker)
	}

	for _, g := range a.remainingGroups {
		g.aggregation.Close(a.MemoryConsumptionTracker)
		groupPool.Put(g)
	}

	a.remainingGroups = nil
}
