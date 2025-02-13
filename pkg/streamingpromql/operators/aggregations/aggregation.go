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

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/prometheus/prometheus/util/zeropool"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type Aggregation struct {
	Inner                    types.InstantVectorOperator
	TimeRange                types.QueryTimeRange
	Grouping                 []string // If this is a 'without' aggregation, NewAggregation will ensure that this slice contains __name__.
	Without                  bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	aggregationGroupFactory AggregationGroupFactory

	Annotations *annotations.Annotations

	metricNames        *operators.MetricNames
	currentSeriesIndex int

	expressionPosition posrange.PositionRange
	emitAnnotationFunc types.EmitAnnotationFunc

	remainingInnerSeriesToGroup []*group // One entry per series produced by Inner, value is the group for that series
	remainingGroups             []*group // One entry per group, in the order we want to return them

	haveEmittedMixedFloatsAndHistogramsWarning bool
}

func NewAggregation(
	inner types.InstantVectorOperator,
	timeRange types.QueryTimeRange,
	grouping []string,
	without bool,
	op parser.ItemType,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) (*Aggregation, error) {
	opGroupFactory := AggregationGroupFactories[op]
	if opGroupFactory == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("aggregation operation with '%s'", op))
	}

	if without {
		grouping = append(grouping, labels.MetricName)
	}

	slices.Sort(grouping)

	a := &Aggregation{
		Inner:                    inner,
		TimeRange:                timeRange,
		Grouping:                 grouping,
		Without:                  without,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Annotations:              annotations,
		metricNames:              &operators.MetricNames{},
		expressionPosition:       expressionPosition,
		aggregationGroupFactory:  opGroupFactory,
	}

	a.emitAnnotationFunc = a.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return a, nil
}

type groupWithLabels struct {
	labels labels.Labels
	group  *group
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

func (a *Aggregation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Fetch the source series
	innerSeries, err := a.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	defer types.PutSeriesMetadataSlice(innerSeries)

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

			groups[string(groupLabelsString)] = g
		}

		g.group.remainingSeriesCount++
		g.group.lastSeriesIndex = seriesIdx
		a.remainingInnerSeriesToGroup = append(a.remainingInnerSeriesToGroup, g.group)
	}

	// Sort the list of series we'll return, and maintain the order of the corresponding groups at the same time
	seriesMetadata := types.GetSeriesMetadataSlice(len(groups))
	a.remainingGroups = make([]*group, 0, len(groups))

	for _, g := range groups {
		seriesMetadata = append(seriesMetadata, types.SeriesMetadata{Labels: g.labels})
		a.remainingGroups = append(a.remainingGroups, g.group)
	}

	sort.Sort(groupSorter{seriesMetadata, a.remainingGroups})

	return seriesMetadata, nil
}

func (a *Aggregation) groupLabelsBytesFunc() SeriesToGroupLabelsBytesFunc {
	return GroupLabelsBytesFunc(a.Grouping, a.Without)
}

// seriesToGroupLabelsFunc is a function that returns the output group labels for the given input series.
type seriesToGroupLabelsFunc func(labels.Labels) labels.Labels

func (a *Aggregation) groupLabelsFunc() seriesToGroupLabelsFunc {
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

var groupToSingleSeriesLabelsBytesFunc = func(_ labels.Labels) []byte { return nil }
var groupToSingleSeriesLabelsFunc = func(_ labels.Labels) labels.Labels { return labels.EmptyLabels() }

func (a *Aggregation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(a.remainingGroups) == 0 {
		// No more groups left.
		return types.InstantVectorSeriesData{}, types.EOS
	}

	// Determine next group to return
	thisGroup := a.remainingGroups[0]
	a.remainingGroups = a.remainingGroups[1:]

	// Iterate through inner series until the desired group is complete
	if err := a.accumulateUntilGroupComplete(ctx, thisGroup); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// Construct the group and return it
	seriesData, hasMixedData, err := thisGroup.aggregation.ComputeOutputSeries(a.TimeRange, a.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if hasMixedData && !a.haveEmittedMixedFloatsAndHistogramsWarning {
		a.Annotations.Add(annotations.NewMixedFloatsHistogramsAggWarning(a.Inner.ExpressionPosition()))

		// The warning description only varies based on the position of the expression this operator represents, so only emit it
		// once, to avoid unnecessary work if there are many instances of floats and histograms conflicting.
		a.haveEmittedMixedFloatsAndHistogramsWarning = true
	}

	groupPool.Put(thisGroup)
	return seriesData, nil
}

func (a *Aggregation) accumulateUntilGroupComplete(ctx context.Context, g *group) error {
	for g.remainingSeriesCount > 0 {
		s, err := a.Inner.NextSeries(ctx)
		if err != nil {
			if errors.Is(err, types.EOS) {
				return fmt.Errorf("exhausted series before all groups were completed: %w", err)
			}

			return err
		}

		thisSeriesGroup := a.remainingInnerSeriesToGroup[0]
		a.remainingInnerSeriesToGroup = a.remainingInnerSeriesToGroup[1:]
		if err := thisSeriesGroup.aggregation.AccumulateSeries(s, a.TimeRange, a.MemoryConsumptionTracker, a.emitAnnotationFunc); err != nil {
			return err
		}
		thisSeriesGroup.remainingSeriesCount--

		a.currentSeriesIndex++
	}
	return nil
}

func (a *Aggregation) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := a.metricNames.GetMetricNameForSeries(a.currentSeriesIndex)
	a.Annotations.Add(generator(metricName, a.Inner.ExpressionPosition()))
}

func (a *Aggregation) Close() {
	a.Inner.Close()
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
