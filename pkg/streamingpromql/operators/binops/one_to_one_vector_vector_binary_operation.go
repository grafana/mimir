// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package binops

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
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

	// splitHolder is non-nil for every output series of a name-retaining fill-left split group (see
	// computeOutputSeries). One group has one holder. Every matched output series of the group and the
	// group's single name-dropped sibling share that holder. The last matched read of the group
	// computes the group's fill-left points and stores them in the holder. The sibling then takes
	// them.
	splitHolder *oneToOneBinaryOperationSplitHolder

	// fillLeftCarrier marks the single output series of a split group that emits the group's
	// fill-left points. Those are the points at the steps where no left series of the group has a
	// sample. Upstream Prometheus produces them without a metric name, so they carry the group's match
	// key labels.
	//
	// The carrier is normally the group's name-dropped sibling. That sibling reads no left series of
	// its own, and nameDropped is then also true. The sibling's labels can collide with a matched
	// output series of the group. That matched output series then becomes the carrier instead. It
	// emits the group's fill-left points together with its own name-retaining points, and nameDropped
	// stays false. See addNameDroppedFillLeftSiblings.
	fillLeftCarrier bool

	// nameDropped marks the name-dropped sibling of a split group. The sibling reads no left series
	// and takes its whole result from splitHolder. The matched output series of the group
	// (nameDropped false) hold the name-retaining points.
	nameDropped bool

	// groupLatestLeftSeriesIndex is set only when fillLeftCarrier is true. It holds the highest left
	// series index of the split group, and latestLeftSeries reports it. sortSeries then places the
	// carrier after every other matched output series of its group. The carrier needs that order.
	// Only the last matched read of the group has the complete left-side presence of the group (see
	// NextSeries).
	groupLatestLeftSeriesIndex int
}

// oneToOneBinaryOperationSplitHolder carries the fill-left points of a name-retaining fill-left split
// group. The last matched read of the group puts the points in the holder. The group's name-dropped
// sibling then takes them.
//
// The operator evaluates every matched output series of the group against the same right side. Each
// of those evaluations sees only its own left series. Such an evaluation would treat a step as
// left-absent even when another left series of the group has a sample there. Only the last of those
// reads has the complete left-side presence of the group. Only that read can decide which steps the
// left fill applies to. Every earlier read therefore skips the fill-left branch (see
// fillLeftOptionsFor).
//
// A group whose fill-left carrier is one of its matched output series has no name-dropped sibling.
// That carrier emits the group's fill-left points itself, so the holder stays empty and only marks
// the group's output series as split.
type oneToOneBinaryOperationSplitHolder struct {
	// computed is true once the last matched read of the group has stored fillLeft.
	computed bool

	// fillLeft holds the group's fill-left points until the name-dropped sibling takes them.
	fillLeft types.InstantVectorSeriesData
}

// oneToOneBinaryOperationSplitGroup collects one name-retaining fill-left split group while
// computeOutputSeries builds the group's output series. addNameDroppedFillLeftSiblings then gives
// each group its fill-left carrier.
type oneToOneBinaryOperationSplitGroup struct {
	// holder is the single split holder shared by every output series of the group.
	holder *oneToOneBinaryOperationSplitHolder

	// rightSide is the group's right side, shared by every output series of the group.
	rightSide *oneToOneBinaryOperationRightSide

	// matchedSeriesCount is the number of name-retaining matched output series of the group.
	matchedSeriesCount int

	// latestLeftSeriesIndex is the highest left series index of the group.
	latestLeftSeriesIndex int
}

// latestLeftSeries returns the index of the last series from the left source needed for this output series.
//
// It assumes that leftSeriesIndices is sorted in ascending order.
// It returns -1 for output series that only exist because of a left-side fill, as those have no left series.
//
// For the fill-left carrier of a split group it returns the highest left series index of the group.
// The operator must read the carrier after every other matched output series of its group (see
// groupLatestLeftSeriesIndex).
func (s oneToOneBinaryOperationOutputSeries) latestLeftSeries() int {
	if s.fillLeftCarrier {
		return s.groupLatestLeftSeriesIndex
	}

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
	//
	// The operator does not count the name-dropped sibling of a name-retaining fill-left split group
	// here. The sibling takes its points from the group's split holder and never reads the right side
	// itself (see addNameDroppedFillLeftSiblings). So outputSeriesCount reaches 0 on the last matched
	// read of the group. That read is where the operator computes the group's fill-left points.
	outputSeriesCount int

	// Time steps at which we've seen samples for any left side that matches with this right side.
	// Each value is the index of the source series of the sample, or -1 if no sample has been seen for this time step yet.
	//
	// The operator populates this slice only when more than one output series uses this right side.
	// The operator uses the slice to report duplicate left series. The slice also controls the left
	// fill. The evaluator must not fill the left side at a step where another left series of the same
	// match group has a sample.
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
	// The one-to-one operator never swaps operands, so the fill values map directly onto
	// computeResult's left and right arguments.
	e, err := newVectorVectorBinaryOperationEvaluator(op, returnBool, memoryConsumptionTracker, expressionPosition, timeRange, vectorMatching.FillValues.LHS, vectorMatching.FillValues.RHS)
	if err != nil {
		return nil, err
	}

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

	// splitFillLeftName is true when the operator must split a matched group into its name-retaining
	// output series and one extra name-dropped output series. Upstream Prometheus builds the missing
	// left operand of a left-filled step from the right series' match labels only and drops __name__.
	// So a kept left-filled step for a name-retaining operator has no metric name, but a both-present
	// step keeps the name. The two kinds of step therefore need distinct output series.
	//
	// One match group can hold several left series with different metric names. The group then has
	// several name-retaining output series. The left fill still applies only at a step where no left
	// series of the group has a sample. So the group needs exactly one name-dropped output series.
	//
	// The split matters only when the name-retaining labels (labelsFunc) differ from the name-dropped
	// labels (fillLabelsFunc). This happens when the operator retains __name__ and matching uses
	// ignoring() or without. It does not happen with on(...), which never keeps __name__ in the output
	// labels.
	splitFillLeftName := fillLeft && !b.VectorMatching.On && promqlext.RetainsMetricName(b.Op, b.ReturnBool)

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

	// With fillRight, every left series makes output. The operator fills each unmatched left series
	// and does not discard it. So the operator uses all left series. In that case leave leftSeriesUsed
	// nil. InstantVectorOperatorBuffer reads a nil "used" slice as "the operator needs all series".
	// This is correct and removes the need to get and fill an all-true slice.
	// Do not apply the same optimization to rightSeriesUsed with fillLeft.
	// addUnmatchedRightGroupsWithFilledLeftSides can skip a right group on a filled-labels collision
	// (see the comment there). So a right series can stay unused even with fillLeft set.
	var (
		leftSeriesUsed []bool
		err            error
	)
	lastLeftSeriesUsedIndex := -1

	if !fillRight {
		leftSeriesUsed, err = types.BoolSlicePool.Get(len(b.leftMetadata), b.MemoryConsumptionTracker)
		if err != nil {
			return nil, nil, nil, -1, nil, -1, err
		}

		leftSeriesUsed = leftSeriesUsed[:len(b.leftMetadata)]
	} else {
		lastLeftSeriesUsedIndex = len(b.leftMetadata) - 1
	}

	rightSeriesUsed, err := types.BoolSlicePool.Get(len(b.rightMetadata), b.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	rightSeriesUsed = rightSeriesUsed[:len(b.rightMetadata)]
	lastRightSeriesUsedIndex := -1
	labelsFunc := groupLabelsFunc(b.VectorMatching, b.Op, b.ReturnBool)
	fillLabelsFunc := fillGroupLabelsFunc(b.VectorMatching)
	outputSeriesLabelsBytes := make([]byte, 0, types.LabelBytesBufferSize)

	// matchedRightGroups records, by group key, which right-side groups have a matching series on
	// the left side. Only tracked (and only needed) when filling the left side.
	var matchedRightGroups map[string]struct{}
	if fillLeft {
		matchedRightGroups = make(map[string]struct{}, len(rightSideGroupsMap))
	}

	// unmatchedLeftSeries records the indices of left series with no matching right group. The
	// operator collects these only when it fills the right side. A second pass handles them, after the
	// loop registers all matched series. This order lets a matched output series win a labels collision
	// against a filled-right series (see addUnmatchedLeftSeriesWithFilledRightSides).
	var unmatchedLeftSeries []int

	// splitFillLeftGroups records, by group key, the output series of each name-retaining fill-left
	// split group (see splitFillLeftName). After the loop registers every matched output series, a
	// second pass adds one name-dropped sibling per group (see addNameDroppedFillLeftSiblings).
	var splitFillLeftGroups map[string]*oneToOneBinaryOperationSplitGroup
	if splitFillLeftName {
		splitFillLeftGroups = make(map[string]*oneToOneBinaryOperationSplitGroup, len(rightSideGroupsMap))
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

			// Delay the fill until the loop registers every matched series. This mirrors the fill-left
			// path (addUnmatchedRightGroupsWithFilledLeftSides) and lets a matched output series win a
			// labels collision against a filled-right series.
			unmatchedLeftSeries = append(unmatchedLeftSeries, leftSeriesIndex)
			continue
		}

		if fillLeft {
			matchedRightGroups[string(groupKey)] = struct{}{}
		}

		// All matched output series of one split group share one holder and one name-dropped sibling.
		// The left fill applies only at a step where no left series of the group has a sample.
		var splitGroup *oneToOneBinaryOperationSplitGroup

		if splitFillLeftName {
			// Important: do not extract the string(...) call below - passing it directly allows us to avoid allocating it.
			splitGroup = splitFillLeftGroups[string(groupKey)]

			if splitGroup == nil {
				splitGroup = &oneToOneBinaryOperationSplitGroup{
					holder:    &oneToOneBinaryOperationSplitHolder{},
					rightSide: rightSide,
				}

				splitFillLeftGroups[string(groupKey)] = splitGroup
			}

			// The loop reads b.leftMetadata in ascending index order. So the last value that the loop
			// writes here is the group's highest left series index.
			splitGroup.latestLeftSeriesIndex = leftSeriesIndex
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

			series := &oneToOneBinaryOperationOutputSeries{rightSide: rightSide}

			// A name-retaining fill-left matched group splits into its matched output series and one
			// name-dropped sibling. Attach the group's shared holder now. After the loop registers
			// every matched output series of the group, a second pass
			// (addNameDroppedFillLeftSiblings) adds the sibling.
			if splitFillLeftName {
				series.splitHolder = splitGroup.holder
				splitGroup.matchedSeriesCount++
			}

			outputSeries = oneToOneBinaryOperationOutputSeriesWithLabels{
				labels: outputSeriesLabels,
				series: series,
			}

			outputSeriesMap[string(outputSeriesLabelsBytes)] = outputSeries
		}

		outputSeries.series.leftSeriesIndices = append(outputSeries.series.leftSeriesIndices, leftSeriesIndex)

		// leftSeriesUsed is nil only with fillRight. In that case lastLeftSeriesUsedIndex is already
		// the final index, because the operator uses every left series.
		if leftSeriesUsed != nil {
			leftSeriesUsed[leftSeriesIndex] = true
			lastLeftSeriesUsedIndex = leftSeriesIndex
		}
	}

	// Fill the left side: emit output driven by the right side alone for any unmatched right group.
	if fillLeft {
		lastRightSeriesUsedIndex, err = b.addUnmatchedRightGroupsWithFilledLeftSides(outputSeriesMap, &outputSeriesLabelsBytes, fillLabelsFunc, rightSideGroupsMap, matchedRightGroups, rightSeriesUsed, lastRightSeriesUsedIndex)
		if err != nil {
			return nil, nil, nil, -1, nil, -1, err
		}
	}

	// Fill the right side: emit output driven by the left side alone for any unmatched left group.
	if fillRight {
		if err := b.addUnmatchedLeftSeriesWithFilledRightSides(outputSeriesMap, &outputSeriesLabelsBytes, labelsFunc, unmatchedLeftSeries); err != nil {
			return nil, nil, nil, -1, nil, -1, err
		}
	}

	// Add one name-dropped sibling for each name-retaining fill-left matched group. This pass runs
	// last, so every group already holds all of its matched output series.
	if splitFillLeftName {
		if err := b.addNameDroppedFillLeftSiblings(outputSeriesMap, &outputSeriesLabelsBytes, fillLabelsFunc, splitFillLeftGroups); err != nil {
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

// addUnmatchedLeftSeriesWithFilledRightSides emits one filled-right output series for each left-side
// series that has no matching group on the right side. It uses the RHS fill value. The evaluator
// builds the right side of each such output series at evaluation time.
//
// The caller runs this after it registers every matched series. So an existing output series with the
// same labels is either a matched series or another filled-right series. Only the fillRight path
// reaches this function. With fillRight the operator uses every left series, so this function does not
// track leftSeriesUsed. The caller leaves leftSeriesUsed nil and sets lastLeftSeriesUsedIndex to the
// final index.
//
// outputSeriesLabelsBytes is a scratch buffer reused across calls.
func (b *OneToOneVectorVectorBinaryOperation) addUnmatchedLeftSeriesWithFilledRightSides(
	outputSeriesMap map[string]oneToOneBinaryOperationOutputSeriesWithLabels,
	outputSeriesLabelsBytes *[]byte,
	labelsFunc func(labels.Labels) labels.Labels,
	unmatchedLeftSeries []int,
) error {
	for _, leftSeriesIndex := range unmatchedLeftSeries {
		// Use the same label function as a real match. The result metric comes from the left operand.
		outputSeriesLabels := labelsFunc(b.leftMetadata[leftSeriesIndex].Labels)
		*outputSeriesLabelsBytes = outputSeriesLabels.Bytes(*outputSeriesLabelsBytes)
		outputSeries, exists := outputSeriesMap[string(*outputSeriesLabelsBytes)]

		if exists {
			if !outputSeries.series.fillMissingRight {
				// Collision with a matched (left-derived) output series. Do not merge this filled-right
				// series into it. The matched series has a real right side. Appending this left index
				// would evaluate this unmatched left series against the wrong right side. Keep the matched
				// series and skip this one.
				//
				// Only operators that retain __name__ reach this branch (comparison filters without the
				// bool modifier, or trim operators). A degenerate empty-__name__ case can produce two
				// left-side groups whose filled labels collide, one matched and one unmatched.
				//
				// Operators that do not retain __name__ (arithmetic operators) cannot reach this branch.
				// A collision here means a bug in the query engine.
				if !promqlext.RetainsMetricName(b.Op, b.ReturnBool) {
					return fmt.Errorf("unexpected output series collision during right-side fill for operator %v that does not retain __name__; this indicates a bug in the query engine", b.Op)
				}

				continue
			}

			// Collision with another filled-right series. Both build their right side from the same RHS
			// fill value. Merge by appending this left index and let them compete per-timestep, the same
			// way the matched path does for comparison filters.
			outputSeries.series.leftSeriesIndices = append(outputSeries.series.leftSeriesIndices, leftSeriesIndex)
			continue
		}

		if err := b.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(outputSeriesLabels); err != nil {
			return err
		}

		outputSeriesMap[string(*outputSeriesLabelsBytes)] = oneToOneBinaryOperationOutputSeriesWithLabels{
			labels: outputSeriesLabels,
			series: &oneToOneBinaryOperationOutputSeries{fillMissingRight: true, leftSeriesIndices: []int{leftSeriesIndex}},
		}
	}

	return nil
}

// addNameDroppedFillLeftSiblings gives each name-retaining fill-left match group in
// splitFillLeftGroups one fill-left carrier. The carrier is the output series that emits the group's
// fill-left points (see splitFillLeftName). For nearly every group the carrier is one extra
// name-dropped sibling output series.
//
// One sibling serves the whole group, even when the group has several matched output series. The left
// fill applies only at a step where no left series of the group has a sample. So the group has exactly
// one set of fill-left points. The sibling shares the group's rightSide and split holder. The sibling
// reads no left series of its own. The last matched read of the group computes the group's fill-left
// points and stores them in the holder (see NextSeries).
//
// The sibling's labels are the group's match key labels (fillLabelsFunc). Those labels can collide
// with a matched output series of the same group. That happens when a left series of the group has no
// __name__. labelsFunc then gives that left series the match key labels as well. The group already
// has an output series with the sibling's labels, so it needs no sibling. The colliding matched
// output series becomes the group's fill-left carrier instead. addNameDroppedFillLeftSiblings handles
// the collision in one of two ways:
//
//   - The colliding series is the group's only matched output series. Every left series of the group
//     then merges into that one output series. That series therefore reads the whole left side of
//     the group, and its own left-side presence is the whole group's presence. The group also has no
//     other matched output series to keep the fill-left points out of, so the split serves no
//     purpose. The function clears the group's split holder, and the series emits its fill-left
//     points inline. Without a sibling to take the fill-left points, a split would leak that pooled
//     data.
//   - The group has more than one matched output series. The colliding series then becomes the
//     group's fill-left carrier, and keeps the split. The operator reads it last in the group and
//     merges the group's fill-left points into its own points (see NextSeries). Every other matched
//     output series of the group keeps its fill-left points out of its result, exactly as it does
//     when a sibling exists.
//
// A collision with any other output series is impossible:
//   - The sibling's labels are the group's match key labels. A matched output series of another group
//     carries that other group's match key labels, plus a metric name unless its left series has none.
//     Two different groups never have equal match key labels, because the group key is a canonical
//     encoding of exactly those labels.
//   - addUnmatchedRightGroupsWithFilledLeftSides skips every matched group, so it never creates a
//     series with this group's match key labels.
//   - addUnmatchedLeftSeriesWithFilledRightSides only handles left series whose group has no right
//     side, so its output series belong to other groups.
//
// outputSeriesLabelsBytes is a scratch buffer reused across calls.
func (b *OneToOneVectorVectorBinaryOperation) addNameDroppedFillLeftSiblings(
	outputSeriesMap map[string]oneToOneBinaryOperationOutputSeriesWithLabels,
	outputSeriesLabelsBytes *[]byte,
	fillLabelsFunc func(labels.Labels) labels.Labels,
	splitFillLeftGroups map[string]*oneToOneBinaryOperationSplitGroup,
) error {
	for _, group := range splitFillLeftGroups {
		// Derive the name-dropped labels from one left series of the group. Every left series of the
		// group has the same name-dropped (match key) labels.
		outputSeriesLabels := fillLabelsFunc(b.leftMetadata[group.latestLeftSeriesIndex].Labels)
		*outputSeriesLabelsBytes = outputSeriesLabels.Bytes(*outputSeriesLabelsBytes)

		if existing, exists := outputSeriesMap[string(*outputSeriesLabelsBytes)]; exists {
			// Only a matched output series of this same group can collide here. Every other collision
			// is impossible (see the doc comment), so treat one as a bug.
			if existing.series.splitHolder != group.holder {
				return fmt.Errorf("unexpected output series collision for match group %v while adding the name-dropped fill-left series for operator %v; this indicates a bug in the query engine", outputSeriesLabels, b.Op)
			}

			if group.matchedSeriesCount == 1 {
				// Every left series of the group merges into this one matched output series. Its own
				// left-side presence is therefore the whole group's presence. The group has no other
				// matched output series to keep the fill-left points out of. The split serves no
				// purpose. Clear it and let the series emit its fill-left points inline.
				existing.series.splitHolder = nil
				continue
			}

			// The group has more than one matched output series. So the operator must still keep the
			// fill-left points out of the other matched output series. Make the colliding series the
			// group's fill-left carrier.
			existing.series.fillLeftCarrier = true
			existing.series.groupLatestLeftSeriesIndex = group.latestLeftSeriesIndex
			continue
		}

		if err := b.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(outputSeriesLabels); err != nil {
			return err
		}

		// The sibling does not increment rightSide.outputSeriesCount. It never reads the right side
		// itself, so the group's matched output series already account for every use of the right side.
		// outputSeriesCount therefore reaches 0 on the last matched read of the group. That read is
		// where the operator computes the group's fill-left points.
		outputSeriesMap[string(*outputSeriesLabelsBytes)] = oneToOneBinaryOperationOutputSeriesWithLabels{
			labels: outputSeriesLabels,
			series: &oneToOneBinaryOperationOutputSeries{
				rightSide:                  group.rightSide,
				splitHolder:                group.holder,
				fillLeftCarrier:            true,
				nameDropped:                true,
				groupLatestLeftSeriesIndex: group.latestLeftSeriesIndex,
			},
		}
	}

	return nil
}

// addUnmatchedRightGroupsWithFilledLeftSides emits one output series for each right-side group that
// has no matching series on the left side, using the LHS fill value. The left side of each such
// output series is synthesised at evaluation time. It returns the updated index of the last right
// series that is needed.
func (b *OneToOneVectorVectorBinaryOperation) addUnmatchedRightGroupsWithFilledLeftSides(
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
			// (it has no leftSeriesIndices), and overwriting would discard real data.
			//
			// This is only legitimately reachable for operators that retain __name__ (comparison
			// filters used without the bool modifier, or trim operators): a degenerate empty-__name__
			// case can produce two right-side groups whose filled labels collide. In that case we keep
			// the existing series and intentionally skip this one.
			//
			// For operators that do not retain __name__ (arithmetic operators) this collision is
			// impossible, so reaching it indicates a bug in the query engine.
			if !promqlext.RetainsMetricName(b.Op, b.ReturnBool) {
				return -1, fmt.Errorf("unexpected output series collision during left-side fill for operator %v that does not retain __name__; this indicates a bug in the query engine", b.Op)
			}

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

	iRight := g.series[i].latestRightSeries()
	jRight := g.series[j].latestRightSeries()
	if iRight != jRight {
		return iRight < jRight
	}

	return fillLeftCarrierLast(g.series[i], g.series[j])
}

func (g favourRightSideSorter) Less(i, j int) bool {
	iRight := g.series[i].latestRightSeries()
	jRight := g.series[j].latestRightSeries()
	if iRight != jRight {
		return iRight < jRight
	}

	iLeft := g.series[i].latestLeftSeries()
	jLeft := g.series[j].latestLeftSeries()
	if iLeft != jLeft {
		return iLeft < jLeft
	}

	return fillLeftCarrierLast(g.series[i], g.series[j])
}

// fillLeftCarrierLast is the final tie-break of both sorters. It orders the fill-left carrier of a
// split group after every output series it ties with. Those series include every other matched output
// series of its own group.
//
// The carrier reports the group's highest left series index as its latest left series. So it ties
// with the matched output series that holds that index. The carrier also shares its right side with
// every matched output series of the group. Without this tie-break the two sorters report those
// series as equal. sort.Sort is not stable, so the carrier could then run before the group's
// left-side presence is complete.
func fillLeftCarrierLast(i, j *oneToOneBinaryOperationOutputSeries) bool {
	return !i.fillLeftCarrier && j.fillLeftCarrier
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

	if thisSeries.nameDropped {
		// This is the name-dropped sibling of a name-retaining fill-left split group. sortSeries places
		// the sibling after every matched output series of its group. The last matched read has
		// therefore already computed and stored the group's fill-left points. Take them and clear the
		// holder, so the operator neither returns them twice nor frees them twice.
		holder := thisSeries.splitHolder

		if !holder.computed {
			return types.InstantVectorSeriesData{}, fmt.Errorf("read the name-dropped fill-left series of a match group before the group was evaluated for operator %v; this indicates a bug in the query engine", b.Op)
		}

		result := holder.fillLeft
		holder.fillLeft = types.InstantVectorSeriesData{}

		return result, nil
	}

	rightSide := thisSeries.rightSide

	if !thisSeries.fillMissingRight && rightSide.rightSeriesIndices != nil {
		// Right side hasn't been populated yet.
		if err := b.populateRightSide(ctx, rightSide); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	var isLastUseOfRightSide bool

	if thisSeries.fillMissingRight {
		// A filled-right output series has no shared right side, so this is always its last use.
		isLastUseOfRightSide = true
	} else {
		// We don't need to return thisSeries.rightSide.mergedData here - computeResult will return it below if this is the last output series that references this right side.
		rightSide.outputSeriesCount--
		isLastUseOfRightSide = rightSide.outputSeriesCount == 0
	}

	if thisSeries.fillLeftCarrier && !isLastUseOfRightSide {
		// sortSeries places the fill-left carrier after every other matched output series of its group.
		// The read of the carrier is therefore always the last use of the group's right side.
		return types.InstantVectorSeriesData{}, fmt.Errorf("read the fill-left carrier of a match group before the group was evaluated for operator %v; this indicates a bug in the query engine", b.Op)
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

	fillLeft := b.fillLeftOptionsFor(thisSeries, rightSide, isLastUseOfRightSide)

	finalResult, fillLeftResult, err := b.evaluator.computeResult(mergedLeftSide, rightData, true, isLastUseOfRightSide, fillLeft)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if thisSeries.fillMissingRight {
		// There was no real right side to release.
		return finalResult, nil
	}

	if fillLeft.mode == fillLeftSeparate {
		// This is the last matched read of a split group, so fillLeftResult holds the group's fill-left
		// points.
		if thisSeries.fillLeftCarrier {
			// This matched output series already carries the group's match key labels. So the group has
			// no name-dropped sibling, and this series emits the group's fill-left points itself.
			finalResult, err = b.mergeGroupFillLeftPoints(finalResult, fillLeftResult)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		} else {
			// Keep the group's fill-left points for the name-dropped sibling.
			thisSeries.splitHolder.fillLeft = fillLeftResult
			thisSeries.splitHolder.computed = true
		}
	}

	if isLastUseOfRightSide {
		// We've passed ownership of mergedData to the evaluator, so clear it now to avoid returning it to the pool in FinishedReading().
		rightSide.mergedData = types.InstantVectorSeriesData{}

		rightSide.FinishedReading(b.MemoryConsumptionTracker)
	}

	return finalResult, nil
}

// fillLeftOptionsFor returns the fill-left instructions for one read of thisSeries.
//
// A read that is not part of a name-retaining fill-left split group gets the zero value. The
// evaluator then adds every kept fill-left point to the main result.
//
// A matched read of a split group must keep the group's fill-left points out of its own result. Its
// output labels retain a metric name. Only the last matched read of the group has the complete
// left-side presence of the group. Only that read can decide which steps the left fill applies to.
//
// Every earlier matched read of the group therefore skips the fill-left branch. Such a read produces
// no point for those steps, and, just as important, no annotation. Prometheus never evaluates those
// steps, so it raises no annotation for them either.
//
// The last matched read of the group splits the fill-left points into their own result. It also
// passes the group's left-side presence. The evaluator then skips every step that a left series of
// the group covers. rightSide.leftSidePresence is nil when only one output series uses the group's
// right side. The group then has a single matched output series. The left side of this read is
// therefore the whole left side of the group, and the evaluator skips no step.
func (b *OneToOneVectorVectorBinaryOperation) fillLeftOptionsFor(thisSeries *oneToOneBinaryOperationOutputSeries, rightSide *oneToOneBinaryOperationRightSide, isLastUseOfRightSide bool) fillLeftOptions {
	if thisSeries.splitHolder == nil {
		return fillLeftOptions{mode: fillLeftInResult}
	}

	if !isLastUseOfRightSide {
		return fillLeftOptions{mode: fillLeftSkip}
	}

	return fillLeftOptions{mode: fillLeftSeparate, leftSidePresence: rightSide.leftSidePresence}
}

// mergeGroupFillLeftPoints merges the fill-left points of a match group into the result of the
// group's fill-left carrier.
//
// The carrier is a matched output series, so it has points of its own at the steps where its left
// series has a sample. The group's fill-left points sit at the steps where no left series of the group
// has a sample. The two sets of points are therefore disjoint, and the merged result stays in
// timestamp order.
//
// mergeGroupFillLeftPoints takes ownership of both inputs and returns any unused slice to the pool.
func (b *OneToOneVectorVectorBinaryOperation) mergeGroupFillLeftPoints(result types.InstantVectorSeriesData, fillLeft types.InstantVectorSeriesData) (types.InstantVectorSeriesData, error) {
	if len(fillLeft.Floats) == 0 && len(fillLeft.Histograms) == 0 {
		// The group has no fill-left points, which is the common case. Avoid the merge entirely.
		types.PutInstantVectorSeriesData(fillLeft, b.MemoryConsumptionTracker)
		return result, nil
	}

	// mergeGroupFillLeftPoints runs only when a matched output series is the group's fill-left
	// carrier. That happens only for a degenerate match group. Such a group holds a left series with
	// no __name__ next to a left series with a __name__. So these two small allocations are rare.
	data := []types.InstantVectorSeriesData{result, fillLeft}
	merged, conflict, err := operators.MergeSeries(data, []int{0, 1}, b.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		// The two sets of points cover disjoint steps, so a conflict is impossible.
		return types.InstantVectorSeriesData{}, fmt.Errorf("found %v at timestamp %v while merging the fill-left points of a match group for operator %v; this indicates a bug in the query engine", conflict.Description, conflict.Timestamp, b.Op)
	}

	return merged, nil
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

	// We pass an empty left operand. The evaluator's LHS fill value then produces output at every
	// right timestep, through the same per-timestep fill path used for intermittently matched groups.
	// This is a fill-left-only output series. Its labels already have no metric name, so we do not
	// split its points.
	finalResult, _, err := b.evaluator.computeResult(types.InstantVectorSeriesData{}, rightSide.mergedData, true, isLastUseOfRightSide, fillLeftOptions{})
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

		// The last matched read of a name-retaining fill-left split group stores the group's fill-left
		// points in the group's split holder. Only the group's name-dropped sibling takes them. If the
		// consumer stops before it reads the sibling, nothing else releases those points.
		//
		// Every remaining output series of the group points at the same holder, so clear the holder
		// here. That releases the points exactly once.
		if s.splitHolder != nil {
			types.PutInstantVectorSeriesData(s.splitHolder.fillLeft, b.MemoryConsumptionTracker)
			s.splitHolder.fillLeft = types.InstantVectorSeriesData{}
			s.splitHolder.computed = false
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
