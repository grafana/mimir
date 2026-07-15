// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package binops

import (
	"context"
	"sort"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// OneToOneVectorVectorBinaryOperation represents a one-to-one binary operation between instant vectors such as "<expr> + <expr>" or "<expr> - <expr>".
// One-to-many and many-to-one binary operations between instant vectors are not supported.
type OneToOneVectorVectorBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	VectorMatching           parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []types.SeriesMetadata
	rightMetadata []types.SeriesMetadata

	remainingSeries []*oneToOneBinaryOperationOutputSeries
	leftBuffer      *operators.InstantVectorOperatorBuffer
	rightBuffer     *operators.InstantVectorOperatorBuffer
	evaluator       *vectorVectorBinaryOperationEvaluator

	expressionPosition posrange.PositionRange
	timeRange          types.QueryTimeRange
	hints              *Hints
	logger             log.Logger
}

var _ types.InstantVectorOperator = &OneToOneVectorVectorBinaryOperation{}

type oneToOneBinaryOperationOutputSeries struct {
	leftSeriesIndices []int
	rightSide         *oneToOneBinaryOperationRightSide

	// fillMissingLeft is true when this output series has no real left side and the left operand is
	// synthesised from the LHS fill value. leftSeriesIndices is then empty and rightSide is populated.
	fillMissingLeft bool

	// fillMissingRight is true when this output series has no real right side and the right operand is
	// synthesised from the RHS fill value. rightSide is then nil and leftSeriesIndices is populated.
	fillMissingRight bool
}

// latestLeftSeries returns the index of the last series from the left source needed for this output series.
//
// It assumes that leftSeriesIndices is sorted in ascending order.
// It returns -1 for output series that only exist because of a left-side fill, as those have no left series.
func (s oneToOneBinaryOperationOutputSeries) latestLeftSeries() int {
	if len(s.leftSeriesIndices) == 0 {
		return -1
	}

	return s.leftSeriesIndices[len(s.leftSeriesIndices)-1]
}

// latestRightSeries returns the index of the last series from the right source needed for this output series.
//
// It assumes that rightSide.rightSeriesIndices is sorted in ascending order.
// It returns -1 for output series that only exist because of a right-side fill, as those have no right series.
func (s oneToOneBinaryOperationOutputSeries) latestRightSeries() int {
	if s.rightSide == nil {
		return -1
	}

	return s.rightSide.rightSeriesIndices[len(s.rightSide.rightSeriesIndices)-1]
}

type oneToOneBinaryOperationRightSide struct {
	// If this right side is used for multiple output series and has not been populated, rightSeriesIndices will not be nil.
	// If this right side has been populated, rightSeriesIndices will be nil.
	rightSeriesIndices []int
	mergedData         types.InstantVectorSeriesData

	// The number of output series that use the same series from the right side.
	// Will only be greater than 1 for comparison binary operations without the bool modifier
	// where the input series on the left side have different metric names.
	outputSeriesCount int

	// Time steps at which we've seen samples for any left side that matches with this right side.
	// Each value is the index of the source series of the sample, or -1 if no sample has been seen for this time step yet.
	leftSidePresence []int
}

// updatePresence records the presence of a sample from the left side series with index seriesIdx at the timestamp with index timestampIdx.
//
// If there is already a sample present from another series at the same timestamp, updatePresence returns that series' index, or
// -1 if there was no sample present at the same timestamp from another series.
func (g *oneToOneBinaryOperationRightSide) updatePresence(timestampIdx int64, seriesIdx int) int {
	if existing := g.leftSidePresence[timestampIdx]; existing != -1 {
		return existing
	}

	g.leftSidePresence[timestampIdx] = seriesIdx
	return -1
}

// latestSeriesIndex returns the index of the last right series used in this side.
//
// It assumes that rightSeriesIndices is sorted in ascending order.
func (g *oneToOneBinaryOperationRightSide) latestRightSeriesIndex() int {
	return g.rightSeriesIndices[len(g.rightSeriesIndices)-1]
}

func (g *oneToOneBinaryOperationRightSide) FinishedReading(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.IntSlicePool.Put(&g.leftSidePresence, memoryConsumptionTracker)

	// If this right side was used for all of its corresponding output series, then mergedData will have already been returned to the pool by the evaluator's computeResult.
	// However, if the operator is having FinishedReading called early, then we need to return mergedData to the pool.
	types.PutInstantVectorSeriesData(g.mergedData, memoryConsumptionTracker)
	g.mergedData = types.InstantVectorSeriesData{}
}

type oneToOneBinaryOperationOutputSeriesWithLabels struct {
	labels labels.Labels
	series *oneToOneBinaryOperationOutputSeries
}

func NewOneToOneVectorVectorBinaryOperation(
	left types.InstantVectorOperator,
	right types.InstantVectorOperator,
	vectorMatching parser.VectorMatching,
	op parser.ItemType,
	returnBool bool,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	hints *Hints,
	logger log.Logger,
) (*OneToOneVectorVectorBinaryOperation, error) {
	e, err := newVectorVectorBinaryOperationEvaluator(op, returnBool, memoryConsumptionTracker, expressionPosition)
	if err != nil {
		return nil, err
	}

	// The one-to-one operator never swaps operands, so the fill values map directly onto
	// computeResult's left and right arguments.
	e.setFillValues(vectorMatching.FillValues.LHS, vectorMatching.FillValues.RHS)

	b := &OneToOneVectorVectorBinaryOperation{
		Left:                     left,
		Right:                    right,
		VectorMatching:           vectorMatching,
		Op:                       op,
		ReturnBool:               returnBool,
		MemoryConsumptionTracker: memoryConsumptionTracker,

		evaluator:          e,
		expressionPosition: expressionPosition,
		timeRange:          timeRange,
		hints:              hints,
		logger:             logger,
	}

	return b, nil
}

func (b *OneToOneVectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return b.expressionPosition
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
func (b *OneToOneVectorVectorBinaryOperation) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// When a side is filled, the other side's series still produce output, so we cannot short-circuit
	// when the filled side is empty.
	fillLeft := b.VectorMatching.FillValues.LHS != nil
	fillRight := b.VectorMatching.FillValues.RHS != nil

	var err error
	b.leftMetadata, err = b.Left.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	} else if len(b.leftMetadata) == 0 && !fillLeft {
		// No series on left-hand side and not filling it, so we'll never have any output series.
		if err = b.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	// If there are labels that this binary operation selects on or aggregations being done
	// on the LHS, we can use the series and their values for those labels to reduce the amount
	// of data fetched on the RHS.
	// Note we are reassigning `matchers` here before passing to the RHS and dropping any
	// other extra matchers passed to this binary operation. Hints from the optimization
	// pass are set specifically for each binary operation and include only fields that are
	// valid to be passed to its RHS. We drop existing extra matchers since they may refer
	// to labels that don't exist on the RHS of this binary operation.
	//
	// b.hints is nil for fill expressions (the optimisation pass sets no hints for them), so we
	// won't narrow the right side here.
	if b.hints != nil {
		matchers = BuildMatchers(ctx, b.logger, b.leftMetadata, b.hints)
	}

	b.rightMetadata, err = b.Right.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	} else if len(b.rightMetadata) == 0 && !fillRight {
		// No series on right-hand side and not filling it, so we'll never have any output series.
		if err = b.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	if len(b.leftMetadata) == 0 && len(b.rightMetadata) == 0 {
		// Both sides are empty, so there is nothing to fill, so there will never be any output series.
		if err = b.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	allMetadata, allSeries, leftSeriesUsed, lastLeftSeriesUsedIndex, rightSeriesUsed, lastRightSeriesUsedIndex, err := b.computeOutputSeries()
	if err != nil {
		return nil, err
	}

	if len(allMetadata) == 0 {
		types.SeriesMetadataSlicePool.Put(&allMetadata, b.MemoryConsumptionTracker)
		types.BoolSlicePool.Put(&leftSeriesUsed, b.MemoryConsumptionTracker)
		types.BoolSlicePool.Put(&rightSeriesUsed, b.MemoryConsumptionTracker)

		if err := b.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	b.sortSeries(allMetadata, allSeries)
	b.remainingSeries = allSeries

	b.leftBuffer = operators.NewInstantVectorOperatorBuffer(b.Left, leftSeriesUsed, lastLeftSeriesUsedIndex, b.MemoryConsumptionTracker)
	b.rightBuffer = operators.NewInstantVectorOperatorBuffer(b.Right, rightSeriesUsed, lastRightSeriesUsedIndex, b.MemoryConsumptionTracker)

	return allMetadata, nil
}

// computeOutputSeries determines the possible output series from this operator.
// It assumes leftMetadata and rightMetadata have already been populated.
//
// It returns:
// - a list of all possible series this operator could return
// - a corresponding list of the source series for each output series
// - a list indicating which series from the left side are needed to compute the output
// - the index of the last series from the left side that is needed to compute the output
// - a list indicating which series from the right side are needed to compute the output
// - the index of the last series from the right side that is needed to compute the output
func (b *OneToOneVectorVectorBinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*oneToOneBinaryOperationOutputSeries, []bool, int, []bool, int, error) {
	groupKeyFunc := vectorMatchingGroupKeyFunc(b.VectorMatching)

	// When a fill value is set, match groups that exist on only one side must still produce output.
	fillLeft := b.VectorMatching.FillValues.LHS != nil
	fillRight := b.VectorMatching.FillValues.RHS != nil

	// If the left side is smaller than the right, build a map of the possible groups from the left side
	// to allow us to avoid creating unnecessary groups when iterating through the right side in computeRightSideGroups.
	// This optimisation assumes that most series on either side match at most one series on the other side,
	// which is generally true for one-to-one matching.
	// FIXME: a possible improvement would be to only bother with this if the left side is significantly smaller
	var leftSideGroupsMap map[string]struct{}

	// When filling the left side, unmatched right-side groups must still produce output, so we can't
	// prune them via the left-side groups map.
	if !fillLeft && len(b.leftMetadata) < len(b.rightMetadata) {
		leftSideGroupsMap = b.computeLeftSideGroups(groupKeyFunc)
	}

	rightSideGroupsMap := b.computeRightSideGroups(leftSideGroupsMap, groupKeyFunc)

	outputSeriesMap := map[string]oneToOneBinaryOperationOutputSeriesWithLabels{}

	leftSeriesUsed, err := types.BoolSlicePool.Get(len(b.leftMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	rightSeriesUsed, err := types.BoolSlicePool.Get(len(b.rightMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	leftSeriesUsed = leftSeriesUsed[:len(b.leftMetadata)]
	lastLeftSeriesUsedIndex := -1
	rightSeriesUsed = rightSeriesUsed[:len(b.rightMetadata)]
	lastRightSeriesUsedIndex := -1
	labelsFunc := groupLabelsFunc(b.VectorMatching, b.Op, b.ReturnBool)
	fillLabelsFunc := fillGroupLabelsFunc(b.VectorMatching)
	outputSeriesLabelsBytes := make([]byte, 0, 1024)

	// matchedRightGroups records, by group key, which right-side groups have a matching series on
	// the left side. Only tracked (and only needed) when filling the left side.
	var matchedRightGroups map[string]struct{}
	if fillLeft {
		matchedRightGroups = make(map[string]struct{}, len(rightSideGroupsMap))
	}

	for leftSeriesIndex, s := range b.leftMetadata {
		groupKey := groupKeyFunc(s.Labels)

		// Important: don't extract the string(...) call below - passing it directly allows us to avoid allocating it.
		rightSide, rightExists := rightSideGroupsMap[string(groupKey)]

		if !rightExists {
			// No matching series on the right side.
			if !fillRight {
				continue
			}

			// Fill the right side using the same label function as a real match (the result metric is derived from the left operand).
			if err := b.addLeftSeriesToOutput(outputSeriesMap, &outputSeriesLabelsBytes, labelsFunc(s.Labels), nil, true, leftSeriesIndex, leftSeriesUsed); err != nil {
				return nil, nil, nil, -1, nil, -1, err
			}

			lastLeftSeriesUsedIndex = leftSeriesIndex
			continue
		}

		if fillLeft {
			matchedRightGroups[string(groupKey)] = struct{}{}
		}

		outputSeriesLabels := labelsFunc(s.Labels)
		outputSeriesLabelsBytes = outputSeriesLabels.Bytes(outputSeriesLabelsBytes) // FIXME: it'd be better if we could just get the underlying byte slice without copying here
		outputSeries, exists := outputSeriesMap[string(outputSeriesLabelsBytes)]

		// If two left series produce the same output labels (possible for comparison filters, which retain __name__),
		// we merge them into one output series (appending leftSeriesIndex below) and let them compete per-timestep.
		// For arithmetic operators the output labels are 1:1 with the match group key, so this never happens.
		if !exists {
			if rightSide.outputSeriesCount == 0 {
				// First output series the right side has matched to.
				for _, rightSeriesIndex := range rightSide.rightSeriesIndices {
					rightSeriesUsed[rightSeriesIndex] = true
				}

				lastRightSeriesUsedIndex = max(lastRightSeriesUsedIndex, rightSide.latestRightSeriesIndex())
			}

			rightSide.outputSeriesCount++

			// Account for the memory consumption from the labels now. This helps protect against
			// queries that return many series from this operator.
			// All series in outputSeriesMap will be returned, so this doesn't lead to over-counting
			// of memory consumption.
			if err := b.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(outputSeriesLabels); err != nil {
				return nil, nil, nil, -1, nil, -1, err
			}

			outputSeries = oneToOneBinaryOperationOutputSeriesWithLabels{
				labels: outputSeriesLabels,
				series: &oneToOneBinaryOperationOutputSeries{rightSide: rightSide},
			}

			outputSeriesMap[string(outputSeriesLabelsBytes)] = outputSeries
		}

		outputSeries.series.leftSeriesIndices = append(outputSeries.series.leftSeriesIndices, leftSeriesIndex)
		leftSeriesUsed[leftSeriesIndex] = true
		lastLeftSeriesUsedIndex = leftSeriesIndex
	}

	// Fill the left side: emit output driven by the right side alone for any unmatched right group.
	if fillLeft {
		lastRightSeriesUsedIndex, err = b.addFilledLeftSeries(outputSeriesMap, &outputSeriesLabelsBytes, fillLabelsFunc, rightSideGroupsMap, matchedRightGroups, rightSeriesUsed, lastRightSeriesUsedIndex)
		if err != nil {
			return nil, nil, nil, -1, nil, -1, err
		}
	}

	allMetadata, err := types.SeriesMetadataSlicePool.Get(len(outputSeriesMap), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	allSeries := make([]*oneToOneBinaryOperationOutputSeries, 0, len(outputSeriesMap))

	for _, outputSeries := range outputSeriesMap {
		// Note that we deliberately don't use types.AppendSeriesMetadata here as we've already
		// accounted for the memory consumption of every set of labels in outputSeriesMap above.
		allMetadata = append(allMetadata, types.SeriesMetadata{Labels: outputSeries.labels})
		allSeries = append(allSeries, outputSeries.series)
	}

	return allMetadata, allSeries, leftSeriesUsed, lastLeftSeriesUsedIndex, rightSeriesUsed, lastRightSeriesUsedIndex, nil
}

// addLeftSeriesToOutput registers a left-side series into the output series identified by
// outputSeriesLabels. When fillMissingRight is true, the right side is synthesised from the RHS fill
// value at evaluation time. outputSeriesLabelsBytes is a scratch buffer reused across calls.
func (b *OneToOneVectorVectorBinaryOperation) addLeftSeriesToOutput(
	outputSeriesMap map[string]oneToOneBinaryOperationOutputSeriesWithLabels,
	outputSeriesLabelsBytes *[]byte,
	outputSeriesLabels labels.Labels,
	rightSide *oneToOneBinaryOperationRightSide,
	fillMissingRight bool,
	leftSeriesIndex int,
	leftSeriesUsed []bool,
) error {
	*outputSeriesLabelsBytes = outputSeriesLabels.Bytes(*outputSeriesLabelsBytes)
	outputSeries, exists := outputSeriesMap[string(*outputSeriesLabelsBytes)]

	// If an output series with these labels already exists, we merge by appending this left series'
	// index below rather than overwriting (see the matched path in computeOutputSeries).
	if !exists {
		if err := b.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(outputSeriesLabels); err != nil {
			return err
		}

		outputSeries = oneToOneBinaryOperationOutputSeriesWithLabels{
			labels: outputSeriesLabels,
			series: &oneToOneBinaryOperationOutputSeries{rightSide: rightSide, fillMissingRight: fillMissingRight},
		}

		outputSeriesMap[string(*outputSeriesLabelsBytes)] = outputSeries
	}

	outputSeries.series.leftSeriesIndices = append(outputSeries.series.leftSeriesIndices, leftSeriesIndex)
	leftSeriesUsed[leftSeriesIndex] = true

	return nil
}

// addFilledLeftSeries emits output series for right-side groups that have no matching series on the
// left side, using the LHS fill value. The left side of each such output series is synthesised at
// evaluation time. It returns the updated index of the last right series that is needed.
func (b *OneToOneVectorVectorBinaryOperation) addFilledLeftSeries(
	outputSeriesMap map[string]oneToOneBinaryOperationOutputSeriesWithLabels,
	outputSeriesLabelsBytes *[]byte,
	fillLabelsFunc func(labels.Labels) labels.Labels,
	rightSideGroupsMap map[string]*oneToOneBinaryOperationRightSide,
	matchedRightGroups map[string]struct{},
	rightSeriesUsed []bool,
	lastRightSeriesUsedIndex int,
) (int, error) {
	for groupKey, rightSide := range rightSideGroupsMap {
		if _, matched := matchedRightGroups[groupKey]; matched {
			continue
		}

		// Derive the output labels from the first right-side series in the group. All series in the
		// group share the same matching labels, so any of them produces the same filled labels.
		outputSeriesLabels := fillLabelsFunc(b.rightMetadata[rightSide.rightSeriesIndices[0]].Labels)
		*outputSeriesLabelsBytes = outputSeriesLabels.Bytes(*outputSeriesLabelsBytes)

		if _, exists := outputSeriesMap[string(*outputSeriesLabelsBytes)]; exists {
			// Collision with a left-derived output series. A fill-left series can't be merged into it
			// (it has no leftSeriesIndices), and overwriting would discard real data, so we keep the
			// existing series and skip this one. Only reachable in a degenerate empty-__name__ case for
			// comparison filters; impossible for arithmetic operators.
			continue
		}

		for _, rightSeriesIndex := range rightSide.rightSeriesIndices {
			rightSeriesUsed[rightSeriesIndex] = true
		}

		lastRightSeriesUsedIndex = max(lastRightSeriesUsedIndex, rightSide.latestRightSeriesIndex())
		rightSide.outputSeriesCount++

		if err := b.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(outputSeriesLabels); err != nil {
			return -1, err
		}

		outputSeriesMap[string(*outputSeriesLabelsBytes)] = oneToOneBinaryOperationOutputSeriesWithLabels{
			labels: outputSeriesLabels,
			series: &oneToOneBinaryOperationOutputSeries{rightSide: rightSide, fillMissingLeft: true},
		}
	}

	return lastRightSeriesUsedIndex, nil
}

func (b *OneToOneVectorVectorBinaryOperation) computeLeftSideGroups(groupKeyFunc func(labels.Labels) []byte) map[string]struct{} {
	m := map[string]struct{}{}

	for _, s := range b.leftMetadata {
		groupKey := groupKeyFunc(s.Labels)
		if _, exists := m[string(groupKey)]; !exists {
			m[string(groupKey)] = struct{}{}
		}
	}

	return m
}

func (b *OneToOneVectorVectorBinaryOperation) computeRightSideGroups(leftSideGroupsMap map[string]struct{}, groupKeyFunc func(labels.Labels) []byte) map[string]*oneToOneBinaryOperationRightSide {
	m := map[string]*oneToOneBinaryOperationRightSide{}

	for idx, s := range b.rightMetadata {
		groupKey := groupKeyFunc(s.Labels)

		if leftSideGroupsMap != nil {
			// Left side is smaller than the right, check if there's any series on the left that could match this right side series.

			if _, exists := leftSideGroupsMap[string(groupKey)]; !exists {
				continue
			}
		}

		group, exists := m[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			group = &oneToOneBinaryOperationRightSide{}
			m[string(groupKey)] = group
		}

		group.rightSeriesIndices = append(group.rightSeriesIndices, idx)
	}

	return m
}

// sortSeries sorts metadata and series in place to try to minimise the number of input series we'll need to buffer in memory.
//
// This is critical for minimising the memory consumption of this operator: if we choose a poor ordering of series,
// we'll need to buffer many input series in memory.
//
// At present, sortSeries uses a very basic heuristic to guess the best way to sort the output series, but we could make
// this more sophisticated in the future.
func (b *OneToOneVectorVectorBinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) {
	// For one-to-one matching, we assume that each output series takes one series from each side of the operator.
	// If this is true, then the best order is the one in which we read from the highest cardinality side in order.
	// If we do this, then in the worst case, we'll have to buffer the whole of the lower cardinality side.
	// (Compare this with sorting so that we read the lowest cardinality side in order: in the worst case, we'll have
	// to buffer the whole of the higher cardinality side.)
	var sortInterface sort.Interface

	if len(b.leftMetadata) < len(b.rightMetadata) {
		sortInterface = newFavourRightSideSorter(metadata, series)
	} else {
		sortInterface = newFavourLeftSideSorter(metadata, series)
	}

	sort.Sort(sortInterface)
}

type binaryOperationOutputSorter struct {
	metadata []types.SeriesMetadata
	series   []*oneToOneBinaryOperationOutputSeries
}

type favourLeftSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourLeftSideSorter(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) favourLeftSideSorter {
	return favourLeftSideSorter{binaryOperationOutputSorter{metadata, series}}
}

type favourRightSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourRightSideSorter(metadata []types.SeriesMetadata, series []*oneToOneBinaryOperationOutputSeries) favourRightSideSorter {
	return favourRightSideSorter{binaryOperationOutputSorter{metadata, series}}
}

func (g binaryOperationOutputSorter) Len() int {
	return len(g.metadata)
}

func (g binaryOperationOutputSorter) Swap(i, j int) {
	g.metadata[i], g.metadata[j] = g.metadata[j], g.metadata[i]
	g.series[i], g.series[j] = g.series[j], g.series[i]
}

func (g favourLeftSideSorter) Less(i, j int) bool {
	iLeft := g.series[i].latestLeftSeries()
	jLeft := g.series[j].latestLeftSeries()
	if iLeft != jLeft {
		return iLeft < jLeft
	}

	return g.series[i].latestRightSeries() < g.series[j].latestRightSeries()
}

func (g favourRightSideSorter) Less(i, j int) bool {
	iRight := g.series[i].latestRightSeries()
	jRight := g.series[j].latestRightSeries()
	if iRight != jRight {
		return iRight < jRight
	}

	return g.series[i].latestLeftSeries() < g.series[j].latestLeftSeries()
}

func (b *OneToOneVectorVectorBinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	if thisSeries.fillMissingLeft {
		return b.nextFilledLeftSeries(ctx, thisSeries)
	}

	rightSide := thisSeries.rightSide

	if !thisSeries.fillMissingRight && rightSide.rightSeriesIndices != nil {
		// Right side hasn't been populated yet.
		if err := b.populateRightSide(ctx, rightSide); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	// A filled-right output series has no shared right side, so this is always its last use.
	isLastUseOfRightSide := true

	if !thisSeries.fillMissingRight {
		// We don't need to return thisSeries.rightSide.mergedData here - computeResult will return it below if this is the last output series that references this right side.
		rightSide.outputSeriesCount--
		isLastUseOfRightSide = rightSide.outputSeriesCount == 0
	}

	allLeftSeries, err := b.leftBuffer.GetSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// If the right side matches to many output series, check for conflicts between those left side series
	// before we apply any filtering operations (https://github.com/prometheus/prometheus/pull/17668).
	if !thisSeries.fillMissingRight && rightSide.leftSidePresence != nil {
		for i, leftSeries := range allLeftSeries {
			seriesIdx := thisSeries.leftSeriesIndices[i]

			if err := b.updateLeftSidePresence(rightSide, leftSeries, seriesIdx); err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		}
	}

	mergedLeftSide, err := b.mergeSingleSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// For a fillMissingRight series we pass an empty right operand; the evaluator's RHS fill value
	// then produces output at every left timestep, via the same per-timestep fill path used for
	// intermittently matched groups.
	var rightData types.InstantVectorSeriesData
	if !thisSeries.fillMissingRight {
		rightData = rightSide.mergedData
	}

	finalResult, err := b.evaluator.computeResult(mergedLeftSide, rightData, true, isLastUseOfRightSide)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if thisSeries.fillMissingRight {
		// There was no real right side to release.
		return finalResult, nil
	}

	if isLastUseOfRightSide {
		// We've passed ownership of mergedData to the evaluator, so clear it now to avoid returning it to the pool in FinishedReading().
		rightSide.mergedData = types.InstantVectorSeriesData{}

		rightSide.FinishedReading(b.MemoryConsumptionTracker)
	}

	return finalResult, nil
}

// nextFilledLeftSeries produces the output for a series with no real left side, synthesising the left
// operand from the LHS fill value at each timestep the right side has a sample.
func (b *OneToOneVectorVectorBinaryOperation) nextFilledLeftSeries(ctx context.Context, thisSeries *oneToOneBinaryOperationOutputSeries) (types.InstantVectorSeriesData, error) {
	rightSide := thisSeries.rightSide

	if rightSide.rightSeriesIndices != nil {
		// Right side hasn't been populated yet.
		if err := b.populateRightSide(ctx, rightSide); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	// A filled-left output series is the only user of its right side, so this is always the last use.
	rightSide.outputSeriesCount--
	isLastUseOfRightSide := rightSide.outputSeriesCount == 0

	// We pass an empty left operand; the evaluator's LHS fill value then produces output at every
	// right timestep, via the same per-timestep fill path used for intermittently matched groups.
	finalResult, err := b.evaluator.computeResult(types.InstantVectorSeriesData{}, rightSide.mergedData, true, isLastUseOfRightSide)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if isLastUseOfRightSide {
		// We've passed ownership of mergedData to the evaluator, so clear it now to avoid returning it to the pool in FinishedReading().
		rightSide.mergedData = types.InstantVectorSeriesData{}

		rightSide.FinishedReading(b.MemoryConsumptionTracker)
	}

	return finalResult, nil
}

func (b *OneToOneVectorVectorBinaryOperation) populateRightSide(ctx context.Context, rightSide *oneToOneBinaryOperationRightSide) error {
	allRightSeries, err := b.rightBuffer.GetSeries(ctx, rightSide.rightSeriesIndices)
	if err != nil {
		return err
	}

	rightSide.mergedData, err = b.mergeSingleSide(allRightSeries, rightSide.rightSeriesIndices, b.rightMetadata, "right")
	if err != nil {
		return err
	}

	if rightSide.outputSeriesCount > 1 {
		rightSide.leftSidePresence, err = types.IntSlicePool.Get(b.timeRange.StepCount, b.MemoryConsumptionTracker)
		if err != nil {
			return err
		}

		rightSide.leftSidePresence = rightSide.leftSidePresence[:b.timeRange.StepCount]

		for i := range rightSide.leftSidePresence {
			rightSide.leftSidePresence[i] = -1
		}
	}

	// Signal that the right side has been populated.
	rightSide.rightSeriesIndices = nil

	return nil
}

func (b *OneToOneVectorVectorBinaryOperation) updateLeftSidePresence(rightSide *oneToOneBinaryOperationRightSide, leftSideData types.InstantVectorSeriesData, leftSideSeriesIdx int) error {
	for _, p := range leftSideData.Floats {
		if otherSeriesIdx := rightSide.updatePresence(b.timeRange.PointIndex(p.T), leftSideSeriesIdx); otherSeriesIdx != -1 {
			return formatConflictError(otherSeriesIdx, leftSideSeriesIdx, "duplicate series", p.T, b.leftMetadata, "left", b.VectorMatching, b.Op, b.ReturnBool)
		}
	}

	for _, p := range leftSideData.Histograms {
		if otherSeriesIdx := rightSide.updatePresence(b.timeRange.PointIndex(p.T), leftSideSeriesIdx); otherSeriesIdx != -1 {
			return formatConflictError(otherSeriesIdx, leftSideSeriesIdx, "duplicate series", p.T, b.leftMetadata, "left", b.VectorMatching, b.Op, b.ReturnBool)
		}
	}

	return nil
}

// mergeSingleSide exists to handle the case where one side of an output series has different source series at different time steps.
//
// For example, consider the query "left_side + on (env) right_side" with the following source data:
//
//	left_side{env="test", pod="a"} 1 2 _
//	left_side{env="test", pod="b"} _ _ 3
//	right_side{env="test"} 100 200 300
//
// mergeSingleSide will take in both series for left_side and return a single series with the points [1, 2, 3].
//
// mergeSingleSide is optimised for the case where there is only one source series, or the source series do not overlap, as in the example above.
//
// mergeSingleSide has the side effect of re-ordering both data and sourceSeriesIndices.
func (b *OneToOneVectorVectorBinaryOperation) mergeSingleSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []types.SeriesMetadata, side string) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := operators.MergeSeries(data, sourceSeriesIndices, b.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		return types.InstantVectorSeriesData{}, b.mergeConflictToError(conflict, sourceSeriesMetadata, side)
	}

	return merged, nil
}

func (b *OneToOneVectorVectorBinaryOperation) mergeConflictToError(conflict *operators.MergeConflict, sourceSeriesMetadata []types.SeriesMetadata, side string) error {
	return formatConflictError(conflict.FirstConflictingSeriesIndex, conflict.SecondConflictingSeriesIndex, conflict.Description, conflict.Timestamp, sourceSeriesMetadata, side, b.VectorMatching, b.Op, b.ReturnBool)
}

func (b *OneToOneVectorVectorBinaryOperation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := b.Left.Prepare(ctx, params); err != nil {
		return err
	}

	return b.Right.Prepare(ctx, params)
}

func (b *OneToOneVectorVectorBinaryOperation) AfterPrepare(ctx context.Context) error {
	if err := b.Left.AfterPrepare(ctx); err != nil {
		return err
	}

	return b.Right.AfterPrepare(ctx)
}

func (b *OneToOneVectorVectorBinaryOperation) FinishedReading(ctx context.Context) error {
	types.SeriesMetadataSlicePool.Put(&b.leftMetadata, b.MemoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&b.rightMetadata, b.MemoryConsumptionTracker)

	if b.leftBuffer != nil {
		b.leftBuffer.FinishedReading()
		b.leftBuffer = nil
	}

	if b.rightBuffer != nil {
		b.rightBuffer.FinishedReading()
		b.rightBuffer = nil
	}

	for _, s := range b.remainingSeries {
		// Output series that only exist because of a right-side fill have no right side to release.
		if s.rightSide != nil {
			s.rightSide.FinishedReading(b.MemoryConsumptionTracker)
		}
	}

	b.remainingSeries = nil

	if err := b.Left.FinishedReading(ctx); err != nil {
		return err
	}

	return b.Right.FinishedReading(ctx)
}

func (b *OneToOneVectorVectorBinaryOperation) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := types.FinalizeAndCombine(ctx, b.Left, b.Right)
	if err != nil {
		return nil, nil, err
	}

	b.evaluator.annotations.Merge(childAnnos)

	return stats, b.evaluator.annotations, nil
}

func (b *OneToOneVectorVectorBinaryOperation) Close() {
	b.Left.Close()
	b.Right.Close()
}
