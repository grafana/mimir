// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package binops

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var errMultipleMatchesOnManySide = errors.New("multiple matches for labels: grouping labels must ensure unique matches")

// GroupedVectorVectorBinaryOperation represents a one-to-many or many-to-one binary operation between instant vectors such as "<expr> + group_left <expr>" or "<expr> - group_right <expr>".
// One-to-one binary operations between instant vectors are not supported.
type GroupedVectorVectorBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiting.MemoryConsumptionTracker

	VectorMatching parser.VectorMatching

	expressionPosition posrange.PositionRange
	annotations        *annotations.Annotations
	timeRange          types.QueryTimeRange

	evaluator       vectorVectorBinaryOperationEvaluator
	remainingSeries []*groupedBinaryOperationOutputSeries
	oneSide         types.InstantVectorOperator // Either Left or Right
	manySide        types.InstantVectorOperator
	oneSideBuffer   *operators.InstantVectorOperatorBuffer
	manySideBuffer  *operators.InstantVectorOperatorBuffer

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	oneSideMetadata  []types.SeriesMetadata
	manySideMetadata []types.SeriesMetadata
}

var _ types.InstantVectorOperator = &GroupedVectorVectorBinaryOperation{}

type groupedBinaryOperationOutputSeries struct {
	manySide *manySide
	oneSide  *oneSide
}

type groupedBinaryOperationOutputSeriesWithLabels struct {
	labels       labels.Labels
	outputSeries *groupedBinaryOperationOutputSeries
}

type manySide struct {
	// If this side has not been populated, seriesIndices will not be nil and mergedData will be empty.
	// If this side has been populated, seriesIndices will be nil.
	seriesIndices []int
	mergedData    types.InstantVectorSeriesData

	outputSeriesCount int
}

// latestSeriesIndex returns the index of the last series from this side.
//
// It assumes that seriesIndices is sorted in ascending order.
func (s manySide) latestSeriesIndex() int {
	return s.seriesIndices[len(s.seriesIndices)-1]
}

type oneSide struct {
	// If this side has not been populated, seriesIndices will not be nil and mergedData will be empty.
	// If this side has been populated, seriesIndices will be nil.
	seriesIndices []int
	mergedData    types.InstantVectorSeriesData

	outputSeriesCount int // The number of output series that refer to this side.

	matchGroup *matchGroup // nil if this is the only "one" side in this group.
}

// latestSeriesIndex returns the index of the last series from this side.
//
// It assumes that seriesIndices is sorted in ascending order.
func (s oneSide) latestSeriesIndex() int {
	return s.seriesIndices[len(s.seriesIndices)-1]
}

type matchGroup struct {
	// Time steps at which we've seen samples for any "one" side in this group.
	// Each value is the index of the source series of the sample, or -1 if no sample has been seen for this time step yet.
	presence []int

	oneSideCount int
}

func (g *matchGroup) updatePresence(timestampIdx int64, seriesIdx int) int {
	if existing := g.presence[timestampIdx]; existing != -1 {
		return existing
	}

	g.presence[timestampIdx] = seriesIdx
	return -1
}

func NewGroupedVectorVectorBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) (*GroupedVectorVectorBinaryOperation, error) {
	e, err := newVectorVectorBinaryOperationEvaluator(op, returnBool, memoryConsumptionTracker, annotations, expressionPosition)
	if err != nil {
		return nil, err
	}

	g := &GroupedVectorVectorBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		evaluator:          e,
		expressionPosition: expressionPosition,
		annotations:        annotations,
		timeRange:          timeRange,
	}

	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		g.oneSide, g.manySide = g.Left, g.Right
	case parser.CardManyToOne:
		g.manySide, g.oneSide = g.Left, g.Right
	default:
		return nil, fmt.Errorf("unsupported cardinality '%v'", g.VectorMatching.Card)
	}

	slices.Sort(g.VectorMatching.Include)

	return g, nil
}

// SeriesMetadata returns the series expected to be produced by this operator.
//
// Note that it is possible that this method returns a series which will not have any points, as the
// list of possible output series is generated based solely on the series labels, not their data.
//
// For example, if this operator is for a range query with the expression "left_metric + right_metric", but
// left_metric has points at T=0 and T=1 in the query range, and right_metric has points at T=2 and T=3 in the
// query range, then SeriesMetadata will return a series, but NextSeries will return no points for that series.
//
// If this affects many series in the query, this may cause consuming operators to be less efficient, but in
// practice this rarely happens.
//
// (The alternative would be to compute the entire result here in SeriesMetadata and only return the series that
// contain points, but that would mean we'd need to hold the entire result in memory at once, which we want to
// avoid.)
func (g *GroupedVectorVectorBinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if canProduceAnySeries, err := g.loadSeriesMetadata(ctx); err != nil {
		return nil, err
	} else if !canProduceAnySeries {
		return nil, nil
	}

	allMetadata, allSeries, oneSideSeriesUsed, manySideSeriesUsed, err := g.computeOutputSeries()
	if err != nil {
		return nil, err
	}

	g.sortSeries(allMetadata, allSeries)
	g.remainingSeries = allSeries

	g.oneSideBuffer = operators.NewInstantVectorOperatorBuffer(g.oneSide, oneSideSeriesUsed, g.MemoryConsumptionTracker)
	g.manySideBuffer = operators.NewInstantVectorOperatorBuffer(g.manySide, manySideSeriesUsed, g.MemoryConsumptionTracker)

	return allMetadata, nil
}

// loadSeriesMetadata loads series metadata from both sides of this operation.
// It returns false if one side returned no series and that means there is no way for this operation to return any series.
// (eg. if doing A + B and either A or B have no series, then there is no way for this operation to produce any series)
func (g *GroupedVectorVectorBinaryOperation) loadSeriesMetadata(ctx context.Context) (bool, error) {
	// We retain the series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().

	var err error
	g.oneSideMetadata, err = g.oneSide.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(g.oneSideMetadata) == 0 {
		// No series on left-hand side, we'll never have any output series.
		return false, nil
	}

	g.manySideMetadata, err = g.manySide.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(g.manySideMetadata) == 0 {
		// No series on right-hand side, we'll never have any output series.
		return false, nil
	}

	return true, nil
}

// computeOutputSeries determines the possible output series from this operator.
// It assumes oneSideMetadata and manySideMetadata have already been populated.
//
// It returns:
// - a list of all possible series this operator could return
// - a corresponding list of the source series for each output series
// - a list indicating which series from the "one" side are needed to compute the output
// - a list indicating which series from the "many" side are needed to compute the output
func (g *GroupedVectorVectorBinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*groupedBinaryOperationOutputSeries, []bool, []bool, error) {
	groupKeyFunc := vectorMatchingGroupKeyFunc(g.VectorMatching)

	// First, iterate through all the series on the "one" side and determine all the possible groups.
	// For example, if we are matching on the "env" label and "region" is an additional label,
	// oneSideMap would look something like this once we're done:
	// [env=test][region=au]: {...}
	// [env=test][region=eu]: {...}
	// [env=test][region=us]: {...}
	// [env=prod][region=au]: {...}
	// [env=prod][region=eu]: {...}
	// [env=prod][region=us]: {...}
	additionalLabelsKeyFunc := g.additionalLabelsKeyFunc()
	oneSideMap := map[string]map[string]*oneSide{}

	for idx, s := range g.oneSideMetadata {
		groupKey := groupKeyFunc(s.Labels)
		oneSideGroup, exists := oneSideMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			oneSideGroup = map[string]*oneSide{}
			oneSideMap[string(groupKey)] = oneSideGroup
		}

		additionalLabelsKey := additionalLabelsKeyFunc(s.Labels)
		side, exists := oneSideGroup[string(additionalLabelsKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			side = &oneSide{}
			oneSideGroup[string(additionalLabelsKey)] = side
		}

		side.seriesIndices = append(side.seriesIndices, idx)
	}

	// Now iterate through all series on the "many" side and determine all the possible output series, as
	// well as which series from the "many" side we'll actually need.
	outputSeriesMap := map[string]groupedBinaryOperationOutputSeriesWithLabels{} // All output series, keyed by their labels.
	manySideMap := map[string]*manySide{}                                        // Series from the "many" side, grouped by which output series they'll contribute to.
	manySideGroupKeyFunc := g.manySideGroupKeyFunc()
	outputSeriesLabelsFunc := g.outputSeriesLabelsFunc()
	buf := make([]byte, 0, 1024)

	manySideSeriesUsed, err := types.BoolSlicePool.Get(len(g.manySideMetadata), g.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	manySideSeriesUsed = manySideSeriesUsed[:len(g.manySideMetadata)]

	for idx, s := range g.manySideMetadata {
		groupKey := groupKeyFunc(s.Labels)
		oneSideGroup, exists := oneSideMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			// There are no series on the "one" side that match this series, so we'll produce no output series for this series.
			continue
		}

		manySideSeriesUsed[idx] = true
		manySideGroupKey := manySideGroupKeyFunc(s.Labels)
		thisManySide, exists := manySideMap[string(manySideGroupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if exists {
			// There is already at least one other "many" side series that contributes to the same set of output series, so just append this series to the same output series.
			thisManySide.seriesIndices = append(thisManySide.seriesIndices, idx)
			continue
		}

		thisManySide = &manySide{
			seriesIndices: []int{idx},
		}

		manySideMap[string(manySideGroupKey)] = thisManySide

		for _, oneSide := range oneSideGroup {
			// Most of the time, the output series won't already exist (unless we have input series with different metric names),
			// so just create the series labels directly rather than trying to avoid their creation until we know for sure we'll
			// need them.
			l := outputSeriesLabelsFunc(g.oneSideMetadata[oneSide.seriesIndices[0]].Labels, s.Labels)
			_, exists := outputSeriesMap[string(l.Bytes(buf))]

			if !exists {
				oneSide.outputSeriesCount++
				thisManySide.outputSeriesCount++

				outputSeriesMap[string(l.Bytes(buf))] = groupedBinaryOperationOutputSeriesWithLabels{
					labels: l,
					outputSeries: &groupedBinaryOperationOutputSeries{
						manySide: thisManySide,
						oneSide:  oneSide,
					},
				}
			}
		}
	}

	// Next, go through all the "one" side groups again, and determine which of the "one" side series we'll actually need.
	oneSideSeriesUsed, err := types.BoolSlicePool.Get(len(g.oneSideMetadata), g.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	oneSideSeriesUsed = oneSideSeriesUsed[:len(g.oneSideMetadata)]

	for _, oneSideGroup := range oneSideMap {
		var thisMatchGroup *matchGroup

		for _, oneSide := range oneSideGroup {
			if oneSide.outputSeriesCount == 0 {
				// If any part of a group has no output series, then no parts of that group will have output series.
				break
			} else if thisMatchGroup == nil && len(oneSideGroup) > 1 {
				// We only need a matchGroup to detect conflicts between series on the "one" side that have the same grouping labels.
				// So if there is only one "one" side, we don't need to bother with this and can skip creating the matchGroup.
				thisMatchGroup = &matchGroup{oneSideCount: len(oneSideGroup)}
			}

			oneSide.matchGroup = thisMatchGroup

			for _, idx := range oneSide.seriesIndices {
				oneSideSeriesUsed[idx] = true
			}
		}
	}

	// Finally, construct the list of series that this operator will return.
	outputMetadata := types.GetSeriesMetadataSlice(len(outputSeriesMap))
	outputSeries := make([]*groupedBinaryOperationOutputSeries, 0, len(outputSeriesMap))

	for _, o := range outputSeriesMap {
		outputMetadata = append(outputMetadata, types.SeriesMetadata{Labels: o.labels})
		outputSeries = append(outputSeries, o.outputSeries)
	}

	return outputMetadata, outputSeries, oneSideSeriesUsed, manySideSeriesUsed, nil
}

// additionalLabelsKeyFunc returns a function that extracts a key representing the additional labels from a "one" side series that will
// be included in the final output series labels.
func (g *GroupedVectorVectorBinaryOperation) additionalLabelsKeyFunc() func(oneSideLabels labels.Labels) []byte {
	if len(g.VectorMatching.Include) == 0 {
		return func(_ labels.Labels) []byte {
			return nil
		}
	}

	buf := make([]byte, 0, 1024)

	return func(oneSideLabels labels.Labels) []byte {
		buf = oneSideLabels.BytesWithLabels(buf, g.VectorMatching.Include...)
		return buf
	}
}

// manySideGroupKeyFunc returns a function that extracts a key representing the set of labels from the "many" side that will contribute
// to the same set of output series.
func (g *GroupedVectorVectorBinaryOperation) manySideGroupKeyFunc() func(manySideLabels labels.Labels) []byte {
	buf := make([]byte, 0, 1024)

	if !g.shouldRemoveMetricNameFromManySide() && len(g.VectorMatching.Include) == 0 {
		return func(manySideLabels labels.Labels) []byte {
			buf = manySideLabels.Bytes(buf) // FIXME: it'd be nice if we could avoid Bytes() copying the slice here
			return buf
		}
	}

	if len(g.VectorMatching.Include) == 0 {
		return func(manySideLabels labels.Labels) []byte {
			buf = manySideLabels.BytesWithoutLabels(buf, labels.MetricName)
			return buf
		}
	}

	labelsToRemove := g.VectorMatching.Include

	if g.shouldRemoveMetricNameFromManySide() {
		labelsToRemove = append(labelsToRemove, labels.MetricName)
		slices.Sort(labelsToRemove)
	}

	return func(manySideLabels labels.Labels) []byte {
		buf = manySideLabels.BytesWithoutLabels(buf, labelsToRemove...)
		return buf
	}
}

// outputSeriesLabelsFunc returns a function that determines the final output series labels for given series on both sides.
func (g *GroupedVectorVectorBinaryOperation) outputSeriesLabelsFunc() func(oneSideLabels labels.Labels, manySideLabels labels.Labels) labels.Labels {
	if len(g.VectorMatching.Include) == 0 {
		if g.shouldRemoveMetricNameFromManySide() {
			return func(_ labels.Labels, manySideLabels labels.Labels) labels.Labels {
				return manySideLabels.DropMetricName()
			}
		}

		return func(_ labels.Labels, manySideLabels labels.Labels) labels.Labels {
			return manySideLabels
		}
	}

	lb := labels.NewBuilder(labels.EmptyLabels())

	if g.shouldRemoveMetricNameFromManySide() {
		return func(oneSideLabels labels.Labels, manySideLabels labels.Labels) labels.Labels {
			lb.Reset(manySideLabels)
			lb.Del(labels.MetricName)

			for _, l := range g.VectorMatching.Include {
				lb.Set(l, oneSideLabels.Get(l))
			}

			return lb.Labels()
		}
	}

	return func(oneSideLabels labels.Labels, manySideLabels labels.Labels) labels.Labels {
		lb.Reset(manySideLabels)

		for _, l := range g.VectorMatching.Include {
			lb.Set(l, oneSideLabels.Get(l))
		}

		return lb.Labels()
	}
}

func (g *GroupedVectorVectorBinaryOperation) shouldRemoveMetricNameFromManySide() bool {
	if g.Op.IsComparisonOperator() {
		return g.ReturnBool
	}

	return true
}

// sortSeries sorts metadata and series in place to try to minimise the number of input series we'll need to buffer in memory.
//
// This is critical for minimising the memory consumption of this operator: if we choose a poor ordering of series,
// we'll need to buffer many input series in memory.
//
// At present, sortSeries uses a very basic heuristic to guess the best way to sort the output series, but we could make
// this more sophisticated in the future.
func (g *GroupedVectorVectorBinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*groupedBinaryOperationOutputSeries) {
	// Each series from the "many" side is usually used for at most one output series, so sort the output series so that we buffer as little of the
	// "many" side series as possible.
	//
	// This isn't necessarily perfect: it may be that this still requires us to buffer many series from the "many" side if many
	// series from the "many" side map to one output series, but this is expected to be rare.
	sort.Sort(newFavourManySideSorter(metadata, series))
}

type favourManySideSorter struct {
	metadata []types.SeriesMetadata
	series   []*groupedBinaryOperationOutputSeries
}

func newFavourManySideSorter(metadata []types.SeriesMetadata, series []*groupedBinaryOperationOutputSeries) sort.Interface {
	return favourManySideSorter{metadata, series}
}

func (s favourManySideSorter) Len() int {
	return len(s.metadata)
}

func (s favourManySideSorter) Less(i, j int) bool {
	iMany := s.series[i].manySide.latestSeriesIndex()
	jMany := s.series[j].manySide.latestSeriesIndex()
	if iMany != jMany {
		return iMany < jMany
	}

	return s.series[i].oneSide.latestSeriesIndex() < s.series[j].oneSide.latestSeriesIndex()
}

func (s favourManySideSorter) Swap(i, j int) {
	s.metadata[i], s.metadata[j] = s.metadata[j], s.metadata[i]
	s.series[i], s.series[j] = s.series[j], s.series[i]
}

func (g *GroupedVectorVectorBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(g.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := g.remainingSeries[0]
	g.remainingSeries = g.remainingSeries[1:]

	if err := g.ensureOneSidePopulated(ctx, thisSeries.oneSide); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if err := g.ensureManySidePopulated(ctx, thisSeries.manySide); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	thisSeries.oneSide.outputSeriesCount--
	isLastOutputSeriesForOneSide := thisSeries.oneSide.outputSeriesCount == 0

	thisSeries.manySide.outputSeriesCount--
	isLastOutputSeriesForManySide := thisSeries.manySide.outputSeriesCount == 0

	var result types.InstantVectorSeriesData
	var err error

	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		result, err = g.evaluator.computeResult(thisSeries.oneSide.mergedData, thisSeries.manySide.mergedData, isLastOutputSeriesForOneSide, isLastOutputSeriesForManySide)
	case parser.CardManyToOne:
		result, err = g.evaluator.computeResult(thisSeries.manySide.mergedData, thisSeries.oneSide.mergedData, isLastOutputSeriesForManySide, isLastOutputSeriesForOneSide)
	default:
		panic(fmt.Sprintf("unsupported cardinality '%v'", g.VectorMatching.Card))
	}

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return result, nil
}

func (g *GroupedVectorVectorBinaryOperation) ensureOneSidePopulated(ctx context.Context, side *oneSide) error {
	if side.seriesIndices == nil {
		// Already populated.
		return nil
	}

	// First time we've used this "one" side, populate it.
	data, err := g.oneSideBuffer.GetSeries(ctx, side.seriesIndices)
	if err != nil {
		return err
	}

	if err := g.updateOneSidePresence(side, data); err != nil {
		return err
	}

	side.mergedData, err = g.mergeOneSide(data, side.seriesIndices)
	if err != nil {
		return err
	}

	// Clear seriesIndices to indicate that we've populated it.
	side.seriesIndices = nil

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) updateOneSidePresence(side *oneSide, data []types.InstantVectorSeriesData) error {
	matchGroup := side.matchGroup
	if matchGroup == nil {
		// If there is only one set of additional labels for this set of grouping labels, then there's nothing to do.
		return nil
	}

	// If there are multiple sets of additional labels for the same set of grouping labels, check that there is only one series at each
	// time step for each set of grouping labels.

	if matchGroup.presence == nil {
		var err error
		matchGroup.presence, err = types.IntSlicePool.Get(g.timeRange.StepCount, g.MemoryConsumptionTracker)

		if err != nil {
			return err
		}

		matchGroup.presence = matchGroup.presence[:g.timeRange.StepCount]

		for idx := range matchGroup.presence {
			matchGroup.presence[idx] = -1
		}
	}

	for dataIdx, seriesData := range data {
		seriesIdx := side.seriesIndices[dataIdx]

		for _, p := range seriesData.Floats {
			if otherSeriesIdx := matchGroup.updatePresence(g.timeRange.PointIndex(p.T), seriesIdx); otherSeriesIdx != -1 {
				return g.formatConflictError(otherSeriesIdx, seriesIdx, "duplicate series", p.T, g.oneSideMetadata, g.oneSideHandedness())
			}
		}

		for _, p := range seriesData.Histograms {
			if otherSeriesIdx := matchGroup.updatePresence(g.timeRange.PointIndex(p.T), seriesIdx); otherSeriesIdx != -1 {
				return g.formatConflictError(otherSeriesIdx, seriesIdx, "duplicate series", p.T, g.oneSideMetadata, g.oneSideHandedness())
			}
		}
	}

	matchGroup.oneSideCount--

	if matchGroup.oneSideCount == 0 {
		types.IntSlicePool.Put(matchGroup.presence, g.MemoryConsumptionTracker)
	}

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) mergeOneSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := operators.MergeSeries(data, sourceSeriesIndices, g.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, g.formatConflictError(conflict.FirstConflictingSeriesIndex, conflict.SecondConflictingSeriesIndex, conflict.Description, conflict.Timestamp, g.oneSideMetadata, g.oneSideHandedness())
	}

	return merged, nil
}

func (g *GroupedVectorVectorBinaryOperation) ensureManySidePopulated(ctx context.Context, side *manySide) error {
	if side.seriesIndices == nil {
		// Already populated.
		return nil
	}

	// First time we've used this "one" side, populate it.
	data, err := g.manySideBuffer.GetSeries(ctx, side.seriesIndices)
	if err != nil {
		return err
	}

	side.mergedData, err = g.mergeManySide(data, side.seriesIndices)
	if err != nil {
		return err
	}

	// Clear seriesIndices to indicate that we've populated it.
	side.seriesIndices = nil

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) mergeManySide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := operators.MergeSeries(data, sourceSeriesIndices, g.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, errMultipleMatchesOnManySide
	}

	return merged, nil
}

func (g *GroupedVectorVectorBinaryOperation) formatConflictError(
	firstConflictingSeriesIndex int,
	secondConflictingSeriesIndex int,
	description string,
	ts int64,
	sourceSeriesMetadata []types.SeriesMetadata,
	side string,
) error {
	firstConflictingSeriesLabels := sourceSeriesMetadata[firstConflictingSeriesIndex].Labels
	groupLabels := groupLabelsFunc(g.VectorMatching, g.Op, g.ReturnBool)(firstConflictingSeriesLabels)

	if secondConflictingSeriesIndex == -1 {
		return fmt.Errorf(
			"found %s for the match group %s on the %s side of the operation at timestamp %s",
			description,
			groupLabels,
			side,
			timestamp.Time(ts).Format(time.RFC3339Nano),
		)
	}

	secondConflictingSeriesLabels := sourceSeriesMetadata[secondConflictingSeriesIndex].Labels

	return fmt.Errorf(
		"found %s for the match group %s on the %s side of the operation at timestamp %s: %s and %s",
		description,
		groupLabels,
		side,
		timestamp.Time(ts).Format(time.RFC3339Nano),
		firstConflictingSeriesLabels,
		secondConflictingSeriesLabels,
	)
}

func (g *GroupedVectorVectorBinaryOperation) oneSideHandedness() string {
	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		return "left"
	case parser.CardManyToOne:
		return "right"
	default:
		panic(fmt.Sprintf("unsupported cardinality '%v'", g.VectorMatching.Card))
	}
}

func (g *GroupedVectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return g.expressionPosition
}

func (g *GroupedVectorVectorBinaryOperation) Close() {
	g.Left.Close()
	g.Right.Close()
	// We don't need to close g.oneSide or g.manySide, as these are either g.Left or g.Right and so have been closed above.

	if g.oneSideMetadata != nil {
		types.PutSeriesMetadataSlice(g.oneSideMetadata)
	}

	if g.manySideMetadata != nil {
		types.PutSeriesMetadataSlice(g.manySideMetadata)
	}

	if g.oneSideBuffer != nil {
		g.oneSideBuffer.Close()
	}

	if g.manySideBuffer != nil {
		g.manySideBuffer.Close()
	}
}
