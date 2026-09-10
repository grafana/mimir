// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package binops

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

var errMultipleMatchesOnManySide = errors.New("multiple matches for labels: grouping labels must ensure unique matches")

// GroupedVectorVectorBinaryOperation represents a one-to-many or many-to-one binary operation between instant vectors such as "<expr> + group_left <expr>" or "<expr> - group_right <expr>".
// One-to-one binary operations between instant vectors are not supported.
type GroupedVectorVectorBinaryOperation struct {
	Left                     types.InstantVectorOperator
	Right                    types.InstantVectorOperator
	Op                       parser.ItemType
	ReturnBool               bool
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	VectorMatching parser.VectorMatching

	expressionPosition posrange.PositionRange
	timeRange          types.QueryTimeRange
	hints              *Hints
	logger             log.Logger

	evaluator               *vectorVectorBinaryOperationEvaluator
	remainingSeries         []*groupedBinaryOperationOutputSeries
	oneSide                 types.InstantVectorOperator // Either Left or Right
	manySide                types.InstantVectorOperator
	fillValues              parser.VectorMatchFillValues
	oneSideBuffer           *operators.InstantVectorOperatorBuffer
	manySideBuffer          *operators.InstantVectorOperatorBuffer
	lastOneSideSeriesIndex  int
	lastManySideSeriesIndex int
	leftFinishedReading     bool
	rightFinishedReading    bool
	manyPresenceGroups      []*oneSideMatchGroup
	oneSideValidationGroups []*oneSideMatchGroup
	oneSideValidationSeen   []int
	oneSideValidationSource []int
	oneSideValidationRound  int

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	oneSideMetadata  []types.SeriesMetadata
	manySideMetadata []types.SeriesMetadata
}

var _ types.InstantVectorOperator = &GroupedVectorVectorBinaryOperation{}

type groupedBinaryOperationOutputSeries struct {
	manySide             *manySide
	oneSide              *oneSide
	fillCarrier          *oneSide
	manySideFillCarriers []syntheticManySideCarrier
	fillCarriersFinished bool
	referencesFinished   bool
}

func (g *groupedBinaryOperationOutputSeries) FinishedReading(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	g.finishFillCarrier(memoryConsumptionTracker)
	g.finishManySideFillCarriers(memoryConsumptionTracker)
	g.finishReferences()
	if g.manySide != nil {
		g.manySide.FinishedReading(memoryConsumptionTracker)
	}
	if g.oneSide != nil {
		g.oneSide.FinishedReading(memoryConsumptionTracker)
	}
	seen := make(map[*oneSide]struct{}, len(g.manySideFillCarriers)+1)
	if g.oneSide != nil {
		seen[g.oneSide] = struct{}{}
	}
	for _, carrier := range g.manySideFillCarriers {
		if _, exists := seen[carrier.oneSide]; exists {
			continue
		}
		seen[carrier.oneSide] = struct{}{}
		carrier.oneSide.FinishedReading(memoryConsumptionTracker)
	}
}

func (g *groupedBinaryOperationOutputSeries) finishFillCarrier(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	if g.fillCarrier == nil {
		return
	}

	if g.fillCarrier.matchGroup != nil {
		g.fillCarrier.matchGroup.fillCarrierFinished(memoryConsumptionTracker)
	}
	g.fillCarrier = nil
}

func (g *groupedBinaryOperationOutputSeries) finishManySideFillCarriers(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	if g.fillCarriersFinished {
		return
	}

	for _, carrier := range g.manySideFillCarriers {
		carrier.matchGroup.manySideFillCarrierFinished(memoryConsumptionTracker)
	}
	g.fillCarriersFinished = true
}

func (g *groupedBinaryOperationOutputSeries) finishReferences() (map[*oneSide]bool, bool) {
	if g.referencesFinished {
		return nil, false
	}

	oneSides := make(map[*oneSide]bool, len(g.manySideFillCarriers)+1)
	if g.oneSide != nil {
		oneSides[g.oneSide] = false
	}
	for _, carrier := range g.manySideFillCarriers {
		oneSides[carrier.oneSide] = false
	}
	for side := range oneSides {
		side.outputSeriesCount--
		oneSides[side] = side.outputSeriesCount == 0
	}

	lastManySideUse := false
	if g.manySide != nil {
		g.manySide.outputSeriesCount--
		lastManySideUse = g.manySide.outputSeriesCount == 0
	}
	g.referencesFinished = true
	return oneSides, lastManySideUse
}

func (g *groupedBinaryOperationOutputSeries) releaseZeroReferenceData(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	seen := make(map[*oneSide]struct{}, len(g.manySideFillCarriers)+1)
	if g.oneSide != nil {
		seen[g.oneSide] = struct{}{}
	}
	for _, carrier := range g.manySideFillCarriers {
		seen[carrier.oneSide] = struct{}{}
	}
	for side := range seen {
		if side.outputSeriesCount == 0 {
			side.FinishedReading(memoryConsumptionTracker)
		}
	}
	if g.manySide != nil && g.manySide.outputSeriesCount == 0 {
		g.manySide.FinishedReading(memoryConsumptionTracker)
	}
}

type groupedBinaryOperationOutputSeriesWithLabels struct {
	labels       labels.Labels
	outputSeries *groupedBinaryOperationOutputSeries
}

type syntheticManySideCarrier struct {
	oneSide      *oneSide
	matchGroup   *oneSideMatchGroup
	variantIndex int
}

type normalizedGroupedSides struct {
	many       types.InstantVectorOperator
	one        types.InstantVectorOperator
	fillValues parser.VectorMatchFillValues // LHS fills many, and RHS fills one.
}

func normalizeGroupedSides(left, right types.InstantVectorOperator, vectorMatching parser.VectorMatching) (normalizedGroupedSides, error) {
	switch vectorMatching.Card {
	case parser.CardManyToOne:
		return normalizedGroupedSides{many: left, one: right, fillValues: vectorMatching.FillValues}, nil
	case parser.CardOneToMany:
		return normalizedGroupedSides{
			many:       right,
			one:        left,
			fillValues: vectorMatching.FillValues,
		}, nil
	default:
		return normalizedGroupedSides{}, fmt.Errorf("unsupported cardinality %d", int(vectorMatching.Card))
	}
}

func (s normalizedGroupedSides) evaluatorFillValues(card parser.VectorMatchCardinality) (*float64, *float64, error) {
	switch card {
	case parser.CardManyToOne:
		return s.fillValues.LHS, s.fillValues.RHS, nil
	case parser.CardOneToMany:
		return s.fillValues.RHS, s.fillValues.LHS, nil
	default:
		return nil, nil, fmt.Errorf("unsupported cardinality %d", int(card))
	}
}

type manySide struct {
	// If this side has not been populated, seriesIndices will not be nil and mergedData will be empty.
	// If this side has been populated, seriesIndices will be nil.
	seriesIndices []int
	mergedData    types.InstantVectorSeriesData

	outputSeriesCount int
	matchGroup        *oneSideMatchGroup
	presenceRecorded  bool
}

// latestSeriesIndex returns the index of the last series from this side.
//
// It assumes that seriesIndices is sorted in ascending order.
func (s *manySide) latestSeriesIndex() int {
	return s.seriesIndices[len(s.seriesIndices)-1]
}

func (s *manySide) FinishedReading(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.PutInstantVectorSeriesData(s.mergedData, memoryConsumptionTracker)
	s.mergedData = types.InstantVectorSeriesData{}
}

type oneSide struct {
	// If this side has not been populated, seriesIndices will not be nil and mergedData will be empty.
	// If this side has been populated, seriesIndices will be nil.
	seriesIndices []int
	mergedData    types.InstantVectorSeriesData

	outputSeriesCount int // The number of output series that refer to this side.

	matchGroup *matchGroup // matchGroup tracks presence for fills and groups with multiple one-side variants.
}

// latestSeriesIndex returns the index of the last series from this side.
//
// It assumes that seriesIndices is sorted in ascending order.
func (s *oneSide) latestSeriesIndex() int {
	if len(s.seriesIndices) == 0 {
		return -1
	}

	return s.seriesIndices[len(s.seriesIndices)-1]
}

func (s *oneSide) FinishedReading(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.PutInstantVectorSeriesData(s.mergedData, memoryConsumptionTracker)
	s.mergedData = types.InstantVectorSeriesData{}

	if s.matchGroup != nil && s.matchGroup.fillCarrierCount == 0 {
		s.matchGroup.releasePresence(memoryConsumptionTracker)
	}
}

type matchGroup struct {
	// Time steps at which we've seen samples for any "one" side in this group.
	// Each value is the index of the source series of the sample, or -1 if no sample has been seen for this time step yet.
	presence []int

	oneSides         []*oneSide
	oneSideCount     int
	fillCarrierCount int
}

func (g *matchGroup) releasePresence(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	if g.presence != nil {
		types.IntSlicePool.Put(&g.presence, memoryConsumptionTracker)
	}
}

func (g *matchGroup) fillCarrierFinished(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	g.fillCarrierCount--
	if g.fillCarrierCount == 0 {
		g.releasePresence(memoryConsumptionTracker)
	}
}

type oneSideMatchGroup struct {
	sides                     map[string]*oneSide
	ordered                   []*oneSide
	matchGroup                *matchGroup
	relevant                  bool
	latestManySideSeriesIndex int
	manySides                 []*manySide
	manyPresence              []int
	manySideFillCarrierCount  int
}

func (g *oneSideMatchGroup) ensureMatchGroup() {
	if g.matchGroup != nil {
		return
	}

	g.matchGroup = &matchGroup{
		oneSides:     g.ordered,
		oneSideCount: len(g.ordered),
	}
	for _, side := range g.ordered {
		side.matchGroup = g.matchGroup
	}
}

func (g *oneSideMatchGroup) latestOneSideSeriesIndex() int {
	latest := -1
	for _, side := range g.ordered {
		latest = max(latest, side.latestSeriesIndex())
	}
	return latest
}

func (g *oneSideMatchGroup) requiresValidation() bool {
	seriesCount := 0
	for _, side := range g.ordered {
		seriesCount += len(side.seriesIndices)
	}
	return seriesCount > 1
}

func (g *oneSideMatchGroup) releaseManyPresence(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	if g.manyPresence != nil {
		types.IntSlicePool.Put(&g.manyPresence, memoryConsumptionTracker)
	}
}

func (g *oneSideMatchGroup) manySideFillCarrierFinished(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	if g.manySideFillCarrierCount == 0 {
		return
	}

	g.manySideFillCarrierCount--
	if g.manySideFillCarrierCount == 0 {
		g.releaseManyPresence(memoryConsumptionTracker)
	}
}

// updatePresence records the presence of a sample from the series with index seriesIdx at the timestamp with index timestampIdx.
//
// If there is already a sample present from another series at the same timestamp, updatePresence returns that series' index, or
// -1 if there was no sample present at the same timestamp from another series.
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
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	hints *Hints,
	logger log.Logger,
) (*GroupedVectorVectorBinaryOperation, error) {
	sides, err := normalizeGroupedSides(left, right, vectorMatching)
	if err != nil {
		return nil, err
	}

	fillLeft, fillRight, err := sides.evaluatorFillValues(vectorMatching.Card)
	if err != nil {
		return nil, err
	}
	e, err := newVectorVectorBinaryOperationEvaluator(op, returnBool, memoryConsumptionTracker, expressionPosition, timeRange, fillLeft, fillRight)
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
		timeRange:          timeRange,
		hints:              hints,
		logger:             logger,
		manySide:           sides.many,
		oneSide:            sides.one,
		fillValues:         sides.fillValues,
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
func (g *GroupedVectorVectorBinaryOperation) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if shouldContinue, err := g.loadSeriesMetadata(ctx, matchers); err != nil {
		return nil, err
	} else if !shouldContinue {
		if err := g.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	allMetadata, allSeries, oneSideSeriesUsed, lastOneSideSeriesUsedIndex, manySideSeriesUsed, lastManySideSeriesUsedIndex, err := g.computeOutputSeries()
	if err != nil {
		return nil, err
	}

	g.lastOneSideSeriesIndex = lastOneSideSeriesUsedIndex
	g.lastManySideSeriesIndex = lastManySideSeriesUsedIndex

	if lastOneSideSeriesUsedIndex == -1 {
		types.BoolSlicePool.Put(&oneSideSeriesUsed, g.MemoryConsumptionTracker)
		if err := g.finishOneSide(ctx); err != nil {
			return nil, err
		}
	} else {
		g.oneSideBuffer = operators.NewInstantVectorOperatorBuffer(g.oneSide, oneSideSeriesUsed, lastOneSideSeriesUsedIndex, g.MemoryConsumptionTracker)
	}
	if lastManySideSeriesUsedIndex == -1 {
		types.BoolSlicePool.Put(&manySideSeriesUsed, g.MemoryConsumptionTracker)
		if err := g.finishManySide(ctx); err != nil {
			return nil, err
		}
	} else {
		g.manySideBuffer = operators.NewInstantVectorOperatorBuffer(g.manySide, manySideSeriesUsed, lastManySideSeriesUsedIndex, g.MemoryConsumptionTracker)
	}

	if len(allMetadata) == 0 {
		types.SeriesMetadataSlicePool.Put(&allMetadata, g.MemoryConsumptionTracker)

		if err := g.FinishedReading(ctx); err != nil {
			return nil, err
		}

		return nil, nil
	}

	g.sortSeries(allMetadata, allSeries)
	g.remainingSeries = allSeries

	return allMetadata, nil
}

// loadSeriesMetadata loads child metadata and reports whether output or fill validation remains.
func (g *GroupedVectorVectorBinaryOperation) loadSeriesMetadata(ctx context.Context, matchers types.Matchers) (bool, error) {
	// We retain the series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().

	// Load the "one" side first: it is the smaller side, and once we have its metadata
	// we can use it to build hint-based matchers for the "many" side.
	//
	// Only forward outer matchers to the one side for labels it participates in the join through:
	// with "on(...)" that's MatchingLabels (so "on()" forwards none), and Include labels are never
	// forwarded (they come from the many side). Other matchers go to the many side instead;
	// forwarding them to the one side would incorrectly filter it to empty (see separateIncludeLabelMatchers).
	// Grouped fill disables all matchers because unmatched series can produce output.
	fillActive := g.VectorMatching.FillValues.LHS != nil || g.VectorMatching.FillValues.RHS != nil
	var oneSideMatchers, includeMatchers types.Matchers
	if !fillActive {
		oneSideMatchers, includeMatchers = separateIncludeLabelMatchers(matchers, g.VectorMatching.Include, g.VectorMatching.On, g.VectorMatching.MatchingLabels)
	}

	var err error
	g.oneSideMetadata, err = g.oneSide.SeriesMetadata(ctx, oneSideMatchers)
	if err != nil {
		return false, err
	}

	oneSideEmpty := len(g.oneSideMetadata) == 0
	if oneSideEmpty && g.fillValues.RHS == nil {
		return false, nil
	}

	// Use the "one" side series to narrow the data we need to fetch on the "many" side.
	// When hints have been set by the optimization pass, build matchers from the "one" side
	// metadata and merge them with any outer matchers for included labels (which belong to the
	// many side). Otherwise fall back to the same outer matchers used for the "one" side, plus those
	// that apply to the included labels (derived from the "many" side).
	var manySideMatchers types.Matchers
	if !fillActive {
		manySideMatchers = matchers
	}
	if !fillActive && g.hints != nil {
		manySideMatchers = append(BuildMatchers(ctx, g.logger, g.oneSideMetadata, g.hints), includeMatchers...)
	}

	g.manySideMetadata, err = g.manySide.SeriesMetadata(ctx, manySideMatchers)
	if err != nil {
		return false, err
	}

	manySideEmpty := len(g.manySideMetadata) == 0
	if oneSideEmpty && manySideEmpty {
		return false, nil
	}
	if manySideEmpty && g.fillValues.LHS == nil && g.fillValues.RHS == nil {
		return false, nil
	}
	if oneSideEmpty {
		if err := g.finishOneSide(ctx); err != nil {
			return false, err
		}
	}
	if manySideEmpty {
		if err := g.finishManySide(ctx); err != nil {
			return false, err
		}
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
// - the index of the last series from the "one" side that is needed to compute the output
// - a list indicating which series from the "many" side are needed to compute the output
// - the index of the last series from the "many" side that is needed to compute the output
func (g *GroupedVectorVectorBinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*groupedBinaryOperationOutputSeries, []bool, int, []bool, int, error) {
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
	oneSideMap := map[string]*oneSideMatchGroup{}
	oneSideGroups := make([]*oneSideMatchGroup, 0, len(g.oneSideMetadata))

	for idx, s := range g.oneSideMetadata {
		groupKey := groupKeyFunc(s.Labels)
		oneSideGroup, exists := oneSideMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			oneSideGroup = &oneSideMatchGroup{sides: map[string]*oneSide{}, latestManySideSeriesIndex: -1}
			oneSideMap[string(groupKey)] = oneSideGroup
			oneSideGroups = append(oneSideGroups, oneSideGroup)
		}

		additionalLabelsKey := additionalLabelsKeyFunc(s.Labels)
		side, exists := oneSideGroup.sides[string(additionalLabelsKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			side = &oneSide{}
			oneSideGroup.sides[string(additionalLabelsKey)] = side
			oneSideGroup.ordered = append(oneSideGroup.ordered, side)
		}

		side.seriesIndices = append(side.seriesIndices, idx)
	}
	for _, oneSideGroup := range oneSideGroups {
		if len(oneSideGroup.ordered) > 1 {
			oneSideGroup.ensureMatchGroup()
		}
	}

	// Now iterate through all series on the "many" side and determine all the possible output series, as
	// well as which series from the "many" side we'll actually need.
	outputSeriesMap := map[string]groupedBinaryOperationOutputSeriesWithLabels{} // All output series, keyed by their labels.
	manySideMap := map[string]*manySide{}                                        // Series from the "many" side, grouped by which output series they'll contribute to.
	manySideGroupKeyFunc := g.manySideGroupKeyFunc()
	outputSeriesLabelsFunc := g.outputSeriesLabelsFunc()
	syntheticOneSideLabelsFunc := g.syntheticOneSideLabelsFunc()
	buf := make([]byte, 0, types.LabelBytesBufferSize)

	manySideSeriesUsed, err := types.BoolSlicePool.Get(len(g.manySideMetadata), g.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	manySideSeriesUsed = manySideSeriesUsed[:len(g.manySideMetadata)]
	lastManySideSeriesUsedIndex := -1

	for idx, s := range g.manySideMetadata {
		groupKey := groupKeyFunc(s.Labels)
		oneSideGroup, exists := oneSideMap[string(groupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if !exists {
			if g.fillValues.RHS == nil {
				continue
			}

			oneSideGroup = &oneSideMatchGroup{}
		} else {
			oneSideGroup.relevant = true
			oneSideGroup.latestManySideSeriesIndex = idx
			if g.fillValues.RHS != nil {
				oneSideGroup.ensureMatchGroup()
			}
		}

		manySideSeriesUsed[idx] = true
		lastManySideSeriesUsedIndex = idx
		manySideGroupKey := manySideGroupKeyFunc(s.Labels)
		thisManySide, exists := manySideMap[string(manySideGroupKey)] // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if exists {
			// There is already at least one other "many" side series that contributes to the same set of output series, so just append this series to the same output series.
			thisManySide.seriesIndices = append(thisManySide.seriesIndices, idx)
			continue
		}

		thisManySide = &manySide{
			seriesIndices: []int{idx},
			matchGroup:    oneSideGroup,
		}
		oneSideGroup.manySides = append(oneSideGroup.manySides, thisManySide)

		manySideMap[string(manySideGroupKey)] = thisManySide

		for _, oneSide := range oneSideGroup.ordered {
			// Most of the time, the output series won't already exist (unless we have input series with different metric names),
			// so just create the series labels directly rather than trying to avoid their creation until we know for sure we'll
			// need them.
			oneSideLabels := g.oneSideMetadata[oneSide.seriesIndices[0]].Labels
			l := outputSeriesLabelsFunc(oneSideLabels, s.Labels)
			key := string(l.Bytes(buf))
			existing, exists := outputSeriesMap[key]

			if exists {
				if existing.outputSeries.manySide != thisManySide {
					return nil, nil, nil, -1, nil, -1, fmt.Errorf("real one-side output labels %s collide across many-side groups", l.String())
				}
				if existing.outputSeries.oneSide != oneSide {
					return nil, nil, nil, -1, nil, -1, fmt.Errorf("real one-side output labels %s collide across one-side variants", l.String())
				}
				continue
			}

			oneSide.outputSeriesCount++
			thisManySide.outputSeriesCount++

			// Account for the memory consumption from the labels now. This helps protect against
			// queries that return many series from this operator.
			// All series in outputSeriesMap will be returned, so this doesn't lead to over-counting
			// of memory consumption.
			if err := g.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l); err != nil {
				return nil, nil, nil, -1, nil, -1, err
			}

			outputSeriesMap[key] = groupedBinaryOperationOutputSeriesWithLabels{
				labels: l,
				outputSeries: &groupedBinaryOperationOutputSeries{
					manySide: thisManySide,
					oneSide:  oneSide,
				},
			}
		}

		if g.fillValues.RHS != nil {
			syntheticOneSideLabels := syntheticOneSideLabelsFunc(s.Labels)
			l := outputSeriesLabelsFunc(syntheticOneSideLabels, s.Labels)
			key := string(l.Bytes(buf))
			carrier := &oneSide{matchGroup: oneSideGroup.matchGroup}
			if existing, exists := outputSeriesMap[key]; exists {
				if existing.outputSeries.manySide != thisManySide {
					return nil, nil, nil, -1, nil, -1, fmt.Errorf("synthetic one-side output labels %s collide across many-side groups", l.String())
				}
				if existing.outputSeries.fillCarrier == nil {
					existing.outputSeries.fillCarrier = carrier
					if oneSideGroup.matchGroup != nil {
						oneSideGroup.matchGroup.fillCarrierCount++
					}
				}
			} else {
				thisManySide.outputSeriesCount++
				if oneSideGroup.matchGroup != nil {
					oneSideGroup.matchGroup.fillCarrierCount++
				}
				if err := g.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l); err != nil {
					return nil, nil, nil, -1, nil, -1, err
				}
				outputSeriesMap[key] = groupedBinaryOperationOutputSeriesWithLabels{
					labels: l,
					outputSeries: &groupedBinaryOperationOutputSeries{
						manySide:    thisManySide,
						fillCarrier: carrier,
					},
				}
			}
		}
	}

	if g.fillValues.LHS != nil {
		for _, oneSideGroup := range oneSideGroups {
			oneSideGroup.relevant = true
			g.manyPresenceGroups = append(g.manyPresenceGroups, oneSideGroup)
			for variantIndex, oneSide := range oneSideGroup.ordered {
				oneSideLabels := g.oneSideMetadata[oneSide.seriesIndices[0]].Labels
				syntheticManySideLabels := g.syntheticManySideLabels(oneSideLabels)
				l := outputSeriesLabelsFunc(oneSideLabels, syntheticManySideLabels)
				key := string(l.Bytes(buf))
				carrier := syntheticManySideCarrier{
					oneSide:      oneSide,
					matchGroup:   oneSideGroup,
					variantIndex: variantIndex,
				}
				oneSideGroup.manySideFillCarrierCount++

				if existing, exists := outputSeriesMap[key]; exists {
					if !outputSeriesReferencesOneSide(existing.outputSeries, oneSide) {
						oneSide.outputSeriesCount++
					}
					existing.outputSeries.manySideFillCarriers = append(existing.outputSeries.manySideFillCarriers, carrier)
					continue
				}

				oneSide.outputSeriesCount++
				if err := g.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(l); err != nil {
					return nil, nil, nil, -1, nil, -1, err
				}
				outputSeriesMap[key] = groupedBinaryOperationOutputSeriesWithLabels{
					labels: l,
					outputSeries: &groupedBinaryOperationOutputSeries{
						manySideFillCarriers: []syntheticManySideCarrier{carrier},
					},
				}
			}
		}
	}

	var oneSideValidationGroups []*oneSideMatchGroup
	if g.fillValues.LHS == nil && g.fillValues.RHS != nil {
		for _, oneSideGroup := range oneSideGroups {
			if oneSideGroup.relevant || !oneSideGroup.requiresValidation() {
				continue
			}

			oneSideGroup.relevant = true
			oneSideValidationGroups = append(oneSideValidationGroups, oneSideGroup)
		}
		sort.Slice(oneSideValidationGroups, func(i, j int) bool {
			return oneSideValidationGroups[i].latestOneSideSeriesIndex() < oneSideValidationGroups[j].latestOneSideSeriesIndex()
		})
	}

	// Next, go through all the "one" side groups again, and determine which of the "one" side series we'll actually need.
	oneSideSeriesUsed, err := types.BoolSlicePool.Get(len(g.oneSideMetadata), g.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	oneSideSeriesUsed = oneSideSeriesUsed[:len(g.oneSideMetadata)]
	lastOneSideSeriesUsedIndex := -1

	for _, oneSideGroup := range oneSideMap {
		if !oneSideGroup.relevant {
			continue
		}
		for _, oneSide := range oneSideGroup.ordered {
			for _, idx := range oneSide.seriesIndices {
				oneSideSeriesUsed[idx] = true
			}

			lastOneSideSeriesUsedIndex = max(lastOneSideSeriesUsedIndex, oneSide.latestSeriesIndex())
		}
	}

	// Finally, construct the list of series that this operator will return.
	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(outputSeriesMap), g.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, -1, nil, -1, err
	}

	outputSeries := make([]*groupedBinaryOperationOutputSeries, 0, len(outputSeriesMap))

	for _, o := range outputSeriesMap {
		// Note that we deliberately don't use types.AppendSeriesMetadata here as we've already
		// accounted for the memory consumption of every set of labels in outputSeriesMap above.
		outputMetadata = append(outputMetadata, types.SeriesMetadata{Labels: o.labels})
		outputSeries = append(outputSeries, o.outputSeries)
	}
	g.oneSideValidationGroups = oneSideValidationGroups

	return outputMetadata, outputSeries, oneSideSeriesUsed, lastOneSideSeriesUsedIndex, manySideSeriesUsed, lastManySideSeriesUsedIndex, nil
}

// additionalLabelsKeyFunc returns a function that extracts a key representing the additional labels from a "one" side series that will
// be included in the final output series labels.
func (g *GroupedVectorVectorBinaryOperation) additionalLabelsKeyFunc() func(oneSideLabels labels.Labels) []byte {
	if len(g.VectorMatching.Include) == 0 {
		return func(_ labels.Labels) []byte {
			return nil
		}
	}

	buf := make([]byte, 0, types.LabelBytesBufferSize)

	return func(oneSideLabels labels.Labels) []byte {
		buf = buf[:0]
		for _, name := range g.VectorMatching.Include {
			value := oneSideLabels.Get(name)
			if value == "" {
				continue
			}
			buf = binary.AppendUvarint(buf, uint64(len(name)))
			buf = append(buf, name...)
			buf = binary.AppendUvarint(buf, uint64(len(value)))
			buf = append(buf, value...)
		}
		return buf
	}
}

func (g *GroupedVectorVectorBinaryOperation) syntheticOneSideLabelsFunc() func(manySideLabels labels.Labels) labels.Labels {
	return func(manySideLabels labels.Labels) labels.Labels {
		return manySideLabels.MatchLabels(g.VectorMatching.On, g.VectorMatching.MatchingLabels...)
	}
}

func (g *GroupedVectorVectorBinaryOperation) syntheticManySideLabels(oneSideLabels labels.Labels) labels.Labels {
	return oneSideLabels.MatchLabels(g.VectorMatching.On, g.VectorMatching.MatchingLabels...).DropReserved(func(name string) bool {
		return name == model.MetricNameLabel
	})
}

func outputSeriesReferencesOneSide(series *groupedBinaryOperationOutputSeries, side *oneSide) bool {
	if series.oneSide == side {
		return true
	}
	for _, carrier := range series.manySideFillCarriers {
		if carrier.oneSide == side {
			return true
		}
	}
	return false
}

// manySideGroupKeyFunc returns a function that extracts a key representing the set of labels from the "many" side that will contribute
// to the same set of output series.
func (g *GroupedVectorVectorBinaryOperation) manySideGroupKeyFunc() func(manySideLabels labels.Labels) []byte {
	buf := make([]byte, 0, types.LabelBytesBufferSize)

	if !g.shouldRemoveMetricNameFromManySide() && len(g.VectorMatching.Include) == 0 {
		return func(manySideLabels labels.Labels) []byte {
			buf = manySideLabels.Bytes(buf) // FIXME: it'd be nice if we could avoid Bytes() copying the slice here
			return buf
		}
	}

	if len(g.VectorMatching.Include) == 0 {
		return func(manySideLabels labels.Labels) []byte {
			buf = manySideLabels.BytesWithoutLabels(buf, model.MetricNameLabel)
			return buf
		}
	}

	outputSeriesLabelsFunc := g.outputSeriesLabelsFunc()
	syntheticOneSideLabelsFunc := g.syntheticOneSideLabelsFunc()

	return func(manySideLabels labels.Labels) []byte {
		syntheticOneSideLabels := syntheticOneSideLabelsFunc(manySideLabels)
		outputSeriesLabels := outputSeriesLabelsFunc(syntheticOneSideLabels, manySideLabels)
		buf = outputSeriesLabels.Bytes(buf)
		return buf
	}
}

// outputSeriesLabelsFunc returns a function that determines the final output series labels for given series on both sides.
func (g *GroupedVectorVectorBinaryOperation) outputSeriesLabelsFunc() func(oneSideLabels labels.Labels, manySideLabels labels.Labels) labels.Labels {
	if len(g.VectorMatching.Include) == 0 {
		if g.shouldRemoveMetricNameFromManySide() {
			return func(_ labels.Labels, manySideLabels labels.Labels) labels.Labels {
				return manySideLabels.DropReserved(func(name string) bool {
					return name == model.MetricNameLabel
				})
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
			lb.Del(model.MetricNameLabel)

			for _, l := range g.VectorMatching.Include {
				if value := oneSideLabels.Get(l); value != "" {
					lb.Set(l, value)
				} else {
					lb.Del(l)
				}
			}

			return lb.Labels()
		}
	}

	return func(oneSideLabels labels.Labels, manySideLabels labels.Labels) labels.Labels {
		lb.Reset(manySideLabels)

		for _, l := range g.VectorMatching.Include {
			if value := oneSideLabels.Get(l); value != "" {
				lb.Set(l, value)
			} else {
				lb.Del(l)
			}
		}

		return lb.Labels()
	}
}

func (g *GroupedVectorVectorBinaryOperation) shouldRemoveMetricNameFromManySide() bool {
	// Operations that retain the metric name (comparison filters and trim operators) keep the name of
	// the "many" side; all others drop it.
	return !promqlext.RetainsMetricName(g.Op, g.ReturnBool)
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
	iMany := outputLatestManySideSeriesIndex(s.series[i])
	jMany := outputLatestManySideSeriesIndex(s.series[j])
	if iMany != jMany {
		return iMany < jMany
	}
	if outputManySideCarrierFollows(s.series[i], s.series[j]) {
		return false
	}
	if outputManySideCarrierFollows(s.series[j], s.series[i]) {
		return true
	}

	iCarrierSeries, iCarrierVariant := outputManySideCarrierOrder(s.series[i])
	jCarrierSeries, jCarrierVariant := outputManySideCarrierOrder(s.series[j])
	if iCarrierSeries != jCarrierSeries {
		return iCarrierSeries < jCarrierSeries
	}
	if iCarrierVariant != jCarrierVariant {
		return iCarrierVariant < jCarrierVariant
	}

	iOne := outputOneSideSeriesIndex(s.series[i])
	jOne := outputOneSideSeriesIndex(s.series[j])
	if iOne != jOne {
		return iOne < jOne
	}

	return labels.Compare(s.metadata[i].Labels, s.metadata[j].Labels) < 0
}

func outputLatestManySideSeriesIndex(series *groupedBinaryOperationOutputSeries) int {
	latest := -1
	if series.manySide != nil {
		latest = series.manySide.latestSeriesIndex()
	}
	for _, carrier := range series.manySideFillCarriers {
		latest = max(latest, carrier.matchGroup.latestManySideSeriesIndex)
	}
	return latest
}

func outputManySideCarrierFollows(carrier, real *groupedBinaryOperationOutputSeries) bool {
	if len(carrier.manySideFillCarriers) == 0 || real.manySide == nil || len(real.manySideFillCarriers) > 0 {
		return false
	}
	for _, candidate := range carrier.manySideFillCarriers {
		if candidate.matchGroup == real.manySide.matchGroup {
			return true
		}
	}
	return false
}

func outputManySideCarrierOrder(series *groupedBinaryOperationOutputSeries) (int, int) {
	if len(series.manySideFillCarriers) == 0 {
		return -1, -1
	}
	seriesIndex := int(^uint(0) >> 1)
	variantIndex := int(^uint(0) >> 1)
	for _, carrier := range series.manySideFillCarriers {
		candidateSeriesIndex := carrier.oneSide.latestSeriesIndex()
		if candidateSeriesIndex < seriesIndex || candidateSeriesIndex == seriesIndex && carrier.variantIndex < variantIndex {
			seriesIndex = candidateSeriesIndex
			variantIndex = carrier.variantIndex
		}
	}
	return seriesIndex, variantIndex
}

func outputOneSideSeriesIndex(series *groupedBinaryOperationOutputSeries) int {
	if series.oneSide == nil {
		return int(^uint(0) >> 1)
	}
	return series.oneSide.latestSeriesIndex()
}

func outputLatestOneSideSeriesIndexForRead(series *groupedBinaryOperationOutputSeries) int {
	latest := -1
	if series.oneSide != nil {
		latest = series.oneSide.latestSeriesIndex()
	}
	for _, carrier := range series.manySideFillCarriers {
		latest = max(latest, carrier.oneSide.latestSeriesIndex())
	}
	if series.fillCarrier != nil && series.fillCarrier.matchGroup != nil {
		for _, side := range series.fillCarrier.matchGroup.oneSides {
			latest = max(latest, side.latestSeriesIndex())
		}
	}
	return latest
}

func (s favourManySideSorter) Swap(i, j int) {
	s.metadata[i], s.metadata[j] = s.metadata[j], s.metadata[i]
	s.series[i], s.series[j] = s.series[j], s.series[i]
}

type groupedEvaluationComponent struct {
	manySide *manySide
	oneSide  *oneSide
	options  computeResultOptions
}

func (g *GroupedVectorVectorBinaryOperation) NextSeries(ctx context.Context) (result types.InstantVectorSeriesData, err error) {
	if len(g.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := g.remainingSeries[0]
	defer func() {
		thisSeries.finishFillCarrier(g.MemoryConsumptionTracker)
		thisSeries.finishManySideFillCarriers(g.MemoryConsumptionTracker)
		if err != nil {
			thisSeries.finishReferences()
			thisSeries.releaseZeroReferenceData(g.MemoryConsumptionTracker)
			g.releaseManyPresence()
		}
	}()
	validationSeriesIndex := outputLatestOneSideSeriesIndexForRead(thisSeries)
	if len(g.remainingSeries) == 1 {
		validationSeriesIndex = int(^uint(0) >> 1)
	}
	if err := g.validateOneSideGroupsThrough(ctx, validationSeriesIndex); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if thisSeries.oneSide != nil {
		if err := g.ensureOneSidePopulated(ctx, thisSeries.oneSide); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}
	for _, carrier := range thisSeries.manySideFillCarriers {
		if err := g.ensureOneSidePopulated(ctx, carrier.oneSide); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}
	if thisSeries.fillCarrier != nil && thisSeries.fillCarrier.matchGroup != nil {
		if err := g.ensureOneSideGroupPopulated(ctx, thisSeries.fillCarrier.matchGroup); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	if err := g.ensureManySidePopulated(ctx, thisSeries.manySide); err != nil {
		return types.InstantVectorSeriesData{}, err
	}
	for _, carrier := range thisSeries.manySideFillCarriers {
		if err := g.ensureManySideGroupPopulated(ctx, carrier.matchGroup); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}

	components := g.evaluationComponents(thisSeries)
	oneSideUses := make(map[*oneSide]int, len(components))
	for _, component := range components {
		if component.oneSide != nil {
			oneSideUses[component.oneSide]++
		}
	}
	lastOneSideUse, lastManySideUse := thisSeries.finishReferences()

	componentResults := make([]types.InstantVectorSeriesData, 0, len(components))
	for _, component := range components {
		if err := ctx.Err(); err != nil {
			putSeriesData(componentResults, g.MemoryConsumptionTracker)
			return types.InstantVectorSeriesData{}, fmt.Errorf("evaluate grouped output: %w", err)
		}

		takeOneSide := false
		if component.oneSide != nil {
			oneSideUses[component.oneSide]--
			takeOneSide = lastOneSideUse[component.oneSide] && oneSideUses[component.oneSide] == 0
		}
		takeManySide := component.manySide != nil && lastManySideUse

		componentResult, computeErr := g.evaluateComponent(component, takeManySide, takeOneSide)
		if takeOneSide {
			if computeErr != nil {
				component.oneSide.FinishedReading(g.MemoryConsumptionTracker)
			}
			component.oneSide.mergedData = types.InstantVectorSeriesData{}
		}
		if takeManySide {
			if computeErr != nil {
				component.manySide.FinishedReading(g.MemoryConsumptionTracker)
			}
			component.manySide.mergedData = types.InstantVectorSeriesData{}
		}
		if computeErr != nil {
			putSeriesData(componentResults, g.MemoryConsumptionTracker)
			return types.InstantVectorSeriesData{}, fmt.Errorf("evaluate grouped output component: %w", computeErr)
		}
		componentResults = append(componentResults, componentResult)
	}

	result, err = g.mergeComponentResults(ctx, componentResults)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}
	g.remainingSeries = g.remainingSeries[1:]
	return result, nil
}

func (g *GroupedVectorVectorBinaryOperation) evaluationComponents(series *groupedBinaryOperationOutputSeries) []groupedEvaluationComponent {
	components := make([]groupedEvaluationComponent, 0, 1+len(series.manySideFillCarriers))
	if series.manySide != nil {
		components = append(components, groupedEvaluationComponent{
			manySide: series.manySide,
			oneSide:  series.oneSide,
			options:  g.realComponentOptions(series),
		})
	}
	for _, carrier := range series.manySideFillCarriers {
		components = append(components, groupedEvaluationComponent{
			oneSide: carrier.oneSide,
			options: g.manySideCarrierOptions(carrier.matchGroup.manyPresence),
		})
	}
	return components
}

func (g *GroupedVectorVectorBinaryOperation) realComponentOptions(series *groupedBinaryOperationOutputSeries) computeResultOptions {
	oneMissing := missingSideOptions{mode: missingSkip}
	if series.fillCarrier != nil {
		oneMissing = missingSideOptions{}
		if series.fillCarrier.matchGroup != nil {
			oneMissing.groupPresence = series.fillCarrier.matchGroup.presence
		}
	}

	if g.VectorMatching.Card == parser.CardOneToMany {
		return computeResultOptions{
			missingLeft:  oneMissing,
			missingRight: missingSideOptions{mode: missingSkip},
		}
	}
	return computeResultOptions{
		missingLeft:  missingSideOptions{mode: missingSkip},
		missingRight: oneMissing,
	}
}

func (g *GroupedVectorVectorBinaryOperation) manySideCarrierOptions(presence []int) computeResultOptions {
	manyMissing := missingSideOptions{groupPresence: presence}
	if g.VectorMatching.Card == parser.CardOneToMany {
		return computeResultOptions{
			missingLeft:  missingSideOptions{mode: missingSkip},
			missingRight: manyMissing,
		}
	}
	return computeResultOptions{
		missingLeft:  manyMissing,
		missingRight: missingSideOptions{mode: missingSkip},
	}
}

func (g *GroupedVectorVectorBinaryOperation) evaluateComponent(component groupedEvaluationComponent, takeManySide, takeOneSide bool) (types.InstantVectorSeriesData, error) {
	manyData := types.InstantVectorSeriesData{}
	if component.manySide != nil {
		manyData = component.manySide.mergedData
	}
	oneData := types.InstantVectorSeriesData{}
	if component.oneSide != nil {
		oneData = component.oneSide.mergedData
	}

	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		result, _, err := g.evaluator.computeResult(oneData, manyData, takeOneSide, takeManySide, component.options)
		return result, err
	case parser.CardManyToOne:
		result, _, err := g.evaluator.computeResult(manyData, oneData, takeManySide, takeOneSide, component.options)
		return result, err
	default:
		return types.InstantVectorSeriesData{}, fmt.Errorf("unsupported cardinality %d", int(g.VectorMatching.Card))
	}
}

func (g *GroupedVectorVectorBinaryOperation) mergeComponentResults(ctx context.Context, results []types.InstantVectorSeriesData) (types.InstantVectorSeriesData, error) {
	if err := ctx.Err(); err != nil {
		putSeriesData(results, g.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", err)
	}
	if len(results) == 1 {
		return results[0], nil
	}

	floatCount, histogramCount := 0, 0
	for resultIndex, result := range results {
		if resultIndex&1023 == 0 {
			if err := ctx.Err(); err != nil {
				putSeriesData(results, g.MemoryConsumptionTracker)
				return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", err)
			}
		}
		floatCount += len(result.Floats)
		histogramCount += len(result.Histograms)
	}
	merged := types.InstantVectorSeriesData{}
	var err error
	if floatCount > 0 {
		merged.Floats, err = types.FPointSlicePool.Get(floatCount, g.MemoryConsumptionTracker)
		if err != nil {
			putSeriesData(results, g.MemoryConsumptionTracker)
			return types.InstantVectorSeriesData{}, fmt.Errorf("allocate merged output floats: %w", err)
		}
	}
	if histogramCount > 0 {
		merged.Histograms, err = types.HPointSlicePool.Get(histogramCount, g.MemoryConsumptionTracker)
		if err != nil {
			types.FPointSlicePool.Put(&merged.Floats, g.MemoryConsumptionTracker)
			putSeriesData(results, g.MemoryConsumptionTracker)
			return types.InstantVectorSeriesData{}, fmt.Errorf("allocate merged output histograms: %w", err)
		}
	}

	heap := make(groupedResultCursorHeap, 0, len(results))
	for resultIndex := range results {
		if resultIndex&1023 == 0 {
			if err := ctx.Err(); err != nil {
				releasePartialMergedResult(merged, g.MemoryConsumptionTracker)
				putSeriesData(results, g.MemoryConsumptionTracker)
				return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", err)
			}
		}
		cursor := groupedResultCursor{resultIndex: resultIndex}
		if cursor.advance(results) {
			heap.push(cursor)
		}
	}

	var previousTimestamp int64
	havePreviousTimestamp := false
	for pointCount := 0; len(heap) > 0; pointCount++ {
		if pointCount&1023 == 0 {
			if err := ctx.Err(); err != nil {
				releasePartialMergedResult(merged, g.MemoryConsumptionTracker)
				putSeriesData(results, g.MemoryConsumptionTracker)
				return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", err)
			}
		}

		cursor := heap.pop()
		if havePreviousTimestamp && cursor.timestamp == previousTimestamp {
			releasePartialMergedResult(merged, g.MemoryConsumptionTracker)
			putSeriesData(results, g.MemoryConsumptionTracker)
			overlapErr := fmt.Errorf("unexpected overlapping result point at timestamp %d", cursor.timestamp)
			return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", overlapErr)
		}
		previousTimestamp = cursor.timestamp
		havePreviousTimestamp = true

		result := results[cursor.resultIndex]
		if cursor.histogram {
			merged.Histograms = append(merged.Histograms, result.Histograms[cursor.pointIndex])
		} else {
			merged.Floats = append(merged.Floats, result.Floats[cursor.pointIndex])
		}
		if cursor.advance(results) {
			heap.push(cursor)
		}
	}
	if err := ctx.Err(); err != nil {
		releasePartialMergedResult(merged, g.MemoryConsumptionTracker)
		putSeriesData(results, g.MemoryConsumptionTracker)
		return types.InstantVectorSeriesData{}, fmt.Errorf("merge grouped output components: %w", err)
	}

	putTransferredSeriesData(results, g.MemoryConsumptionTracker)
	return merged, nil
}

type groupedResultCursor struct {
	resultIndex    int
	floatIndex     int
	histogramIndex int
	pointIndex     int
	timestamp      int64
	histogram      bool
}

func (c *groupedResultCursor) advance(results []types.InstantVectorSeriesData) bool {
	result := results[c.resultIndex]
	hasFloat := c.floatIndex < len(result.Floats)
	hasHistogram := c.histogramIndex < len(result.Histograms)
	if !hasFloat && !hasHistogram {
		return false
	}
	if !hasHistogram || hasFloat && result.Floats[c.floatIndex].T <= result.Histograms[c.histogramIndex].T {
		c.pointIndex = c.floatIndex
		c.timestamp = result.Floats[c.floatIndex].T
		c.histogram = false
		c.floatIndex++
		return true
	}
	c.pointIndex = c.histogramIndex
	c.timestamp = result.Histograms[c.histogramIndex].T
	c.histogram = true
	c.histogramIndex++
	return true
}

type groupedResultCursorHeap []groupedResultCursor

func (h *groupedResultCursorHeap) push(cursor groupedResultCursor) {
	*h = append(*h, cursor)
	for child := len(*h) - 1; child > 0; {
		parent := (child - 1) / 2
		if (*h)[parent].timestamp <= (*h)[child].timestamp {
			break
		}
		(*h)[parent], (*h)[child] = (*h)[child], (*h)[parent]
		child = parent
	}
}

func (h *groupedResultCursorHeap) pop() groupedResultCursor {
	result := (*h)[0]
	last := len(*h) - 1
	(*h)[0] = (*h)[last]
	*h = (*h)[:last]
	for parent := 0; ; {
		left := 2*parent + 1
		if left >= len(*h) {
			break
		}
		smallest := left
		right := left + 1
		if right < len(*h) && (*h)[right].timestamp < (*h)[left].timestamp {
			smallest = right
		}
		if (*h)[parent].timestamp <= (*h)[smallest].timestamp {
			break
		}
		(*h)[parent], (*h)[smallest] = (*h)[smallest], (*h)[parent]
		parent = smallest
	}
	return result
}

func releasePartialMergedResult(data types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	clear(data.Histograms)
	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
}

func putTransferredSeriesData(data []types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for idx := range data {
		clear(data[idx].Histograms)
		types.PutInstantVectorSeriesData(data[idx], memoryConsumptionTracker)
	}
}

func putSeriesData(data []types.InstantVectorSeriesData, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	for idx := range data {
		types.PutInstantVectorSeriesData(data[idx], memoryConsumptionTracker)
	}
}

func (g *GroupedVectorVectorBinaryOperation) ensureOneSideGroupPopulated(ctx context.Context, group *matchGroup) error {
	for _, side := range group.oneSides {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("populate grouped one-side presence: %w", err)
		}
		if err := g.ensureOneSidePopulated(ctx, side); err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) validateOneSideGroupsThrough(ctx context.Context, maxSeriesIndex int) error {
	for len(g.oneSideValidationGroups) > 0 {
		group := g.oneSideValidationGroups[0]
		if group.latestOneSideSeriesIndex() > maxSeriesIndex {
			return nil
		}

		if err := g.startOneSideValidationRound(ctx); err != nil {
			g.oneSideValidationGroups = nil
			g.releaseOneSideValidationState()
			return fmt.Errorf("prepare unmatched one-side group validation: %w", err)
		}
		g.oneSideValidationGroups = g.oneSideValidationGroups[1:]
		if err := g.validateOneSideGroup(ctx, group); err != nil {
			g.oneSideValidationGroups = nil
			g.releaseOneSideValidationState()
			return fmt.Errorf("validate unmatched one-side group for fill: %w", err)
		}
	}

	g.releaseOneSideValidationState()
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) startOneSideValidationRound(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start one-side validation: %w", err)
	}

	if g.oneSideValidationSeen == nil {
		var err error
		g.oneSideValidationSeen, err = types.IntSlicePool.Get(g.timeRange.StepCount, g.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.oneSideValidationSeen = g.oneSideValidationSeen[:g.timeRange.StepCount]

		g.oneSideValidationSource, err = types.IntSlicePool.Get(g.timeRange.StepCount, g.MemoryConsumptionTracker)
		if err != nil {
			types.IntSlicePool.Put(&g.oneSideValidationSeen, g.MemoryConsumptionTracker)
			return err
		}
		g.oneSideValidationSource = g.oneSideValidationSource[:g.timeRange.StepCount]

		for idx := range g.oneSideValidationSeen {
			if idx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("initialize one-side validation state: %w", err)
				}
			}
			g.oneSideValidationSeen[idx] = 0
		}
	}

	if g.oneSideValidationRound == int(^uint(0)>>1) {
		clear(g.oneSideValidationSeen)
		g.oneSideValidationRound = 0
	}
	g.oneSideValidationRound++
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) releaseOneSideValidationState() {
	types.IntSlicePool.Put(&g.oneSideValidationSeen, g.MemoryConsumptionTracker)
	types.IntSlicePool.Put(&g.oneSideValidationSource, g.MemoryConsumptionTracker)
	g.oneSideValidationRound = 0
}

func (g *GroupedVectorVectorBinaryOperation) validateOneSideGroup(ctx context.Context, group *oneSideMatchGroup) error {

	for _, side := range group.ordered {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("validate one-side group: %w", err)
		}

		latestSeriesIndex := side.latestSeriesIndex()
		data, err := g.oneSideBuffer.GetSeries(ctx, side.seriesIndices)
		if err != nil {
			return fmt.Errorf("read one-side group for validation: %w", err)
		}
		if latestSeriesIndex == g.lastOneSideSeriesIndex {
			g.markOneSideFinishedReading()
		}

		err = g.validateOneSideData(ctx, side.seriesIndices, data)
		putSeriesData(data, g.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
	}

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) validateOneSideData(ctx context.Context, seriesIndices []int, data []types.InstantVectorSeriesData) error {
	for dataIdx, seriesData := range data {
		seriesIndex := seriesIndices[dataIdx]
		for pointIdx, point := range seriesData.Floats {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("validate one-side float data: %w", err)
				}
			}
			if err := g.recordOneSideValidationPresence(seriesIndex, point.T); err != nil {
				return err
			}
		}
		for pointIdx, point := range seriesData.Histograms {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("validate one-side histogram data: %w", err)
				}
			}
			if err := g.recordOneSideValidationPresence(seriesIndex, point.T); err != nil {
				return err
			}
		}
	}

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) recordOneSideValidationPresence(seriesIndex int, timestamp int64) error {
	timestampIndex, err := g.presenceTimestampIndex(timestamp, len(g.oneSideValidationSeen))
	if err != nil {
		return fmt.Errorf("record one-side validation presence at timestamp %d: %w", timestamp, err)
	}
	if g.oneSideValidationSeen[timestampIndex] == g.oneSideValidationRound {
		return formatConflictError(g.oneSideValidationSource[timestampIndex], seriesIndex, "duplicate series", timestamp, g.oneSideMetadata, g.oneSideHandedness(), g.VectorMatching, g.Op, g.ReturnBool)
	}

	g.oneSideValidationSeen[timestampIndex] = g.oneSideValidationRound
	g.oneSideValidationSource[timestampIndex] = seriesIndex
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) ensureOneSidePopulated(ctx context.Context, side *oneSide) error {
	if side.seriesIndices == nil {
		// Already populated.
		return nil
	}

	// First time we've used this "one" side, populate it.
	latestSeriesIndex := side.latestSeriesIndex()
	data, err := g.oneSideBuffer.GetSeries(ctx, side.seriesIndices)
	if err != nil {
		return fmt.Errorf("read one-side series: %w", err)
	}
	if latestSeriesIndex == g.lastOneSideSeriesIndex {
		g.markOneSideFinishedReading()
	}

	if err := g.updateOneSidePresence(ctx, side, data); err != nil {
		putSeriesData(data, g.MemoryConsumptionTracker)
		if side.matchGroup != nil {
			side.matchGroup.releasePresence(g.MemoryConsumptionTracker)
		}
		return err
	}

	side.mergedData, err = g.mergeOneSide(data, side.seriesIndices)
	if err != nil {
		return fmt.Errorf("merge one-side series: %w", err)
	}

	// Clear seriesIndices to indicate that we've populated it.
	side.seriesIndices = nil

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) updateOneSidePresence(ctx context.Context, side *oneSide, data []types.InstantVectorSeriesData) error {
	matchGroup := side.matchGroup
	if matchGroup == nil {
		// If there is only one set of additional labels for this set of grouping labels, then there's nothing to do.
		return nil
	}

	// If there are multiple sets of additional labels for the same set of grouping labels, check that there is only one series at each
	// time step for each set of grouping labels.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("update one-side presence: %w", err)
	}

	if matchGroup.presence == nil {
		var err error
		matchGroup.presence, err = types.IntSlicePool.Get(g.timeRange.StepCount, g.MemoryConsumptionTracker)

		if err != nil {
			return err
		}

		matchGroup.presence = matchGroup.presence[:g.timeRange.StepCount]

		for idx := range matchGroup.presence {
			if idx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("initialize one-side presence: %w", err)
				}
			}
			matchGroup.presence[idx] = -1
		}
	}

	for dataIdx, seriesData := range data {
		seriesIdx := side.seriesIndices[dataIdx]

		for pointIdx, p := range seriesData.Floats {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("update one-side float presence: %w", err)
				}
			}
			if err := g.recordOneSidePresence(matchGroup, seriesIdx, p.T); err != nil {
				return err
			}
		}

		for pointIdx, p := range seriesData.Histograms {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("update one-side histogram presence: %w", err)
				}
			}
			if err := g.recordOneSidePresence(matchGroup, seriesIdx, p.T); err != nil {
				return err
			}
		}
	}

	matchGroup.oneSideCount--

	if matchGroup.oneSideCount == 0 && matchGroup.fillCarrierCount == 0 {
		matchGroup.releasePresence(g.MemoryConsumptionTracker)
	}

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) recordOneSidePresence(group *matchGroup, seriesIndex int, timestamp int64) error {
	timestampIndex, err := g.presenceTimestampIndex(timestamp, len(group.presence))
	if err != nil {
		return fmt.Errorf("record one-side presence at timestamp %d: %w", timestamp, err)
	}
	if otherSeriesIndex := group.updatePresence(timestampIndex, seriesIndex); otherSeriesIndex != -1 {
		return formatConflictError(otherSeriesIndex, seriesIndex, "duplicate series", timestamp, g.oneSideMetadata, g.oneSideHandedness(), g.VectorMatching, g.Op, g.ReturnBool)
	}
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) presenceTimestampIndex(timestamp int64, presenceLength int) (int64, error) {
	if timestamp < g.timeRange.StartT || timestamp > g.timeRange.EndT {
		return -1, fmt.Errorf("timestamp %d is outside query range [%d, %d]", timestamp, g.timeRange.StartT, g.timeRange.EndT)
	}
	if g.timeRange.IntervalMilliseconds <= 0 {
		return -1, fmt.Errorf("query interval %d is not positive", g.timeRange.IntervalMilliseconds)
	}
	delta := timestamp - g.timeRange.StartT
	if delta%g.timeRange.IntervalMilliseconds != 0 {
		return -1, fmt.Errorf("timestamp %d is not aligned to query interval %d", timestamp, g.timeRange.IntervalMilliseconds)
	}
	index := delta / g.timeRange.IntervalMilliseconds
	if index < 0 || index >= int64(presenceLength) {
		return -1, fmt.Errorf("timestamp %d maps to presence index %d outside length %d", timestamp, index, presenceLength)
	}
	return index, nil
}

func (g *GroupedVectorVectorBinaryOperation) mergeOneSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int) (types.InstantVectorSeriesData, error) {
	merged, conflict, err := operators.MergeSeries(data, sourceSeriesIndices, g.MemoryConsumptionTracker)

	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if conflict != nil {
		err := formatConflictError(conflict.FirstConflictingSeriesIndex, conflict.SecondConflictingSeriesIndex, conflict.Description, conflict.Timestamp, g.oneSideMetadata, g.oneSideHandedness(), g.VectorMatching, g.Op, g.ReturnBool)
		return types.InstantVectorSeriesData{}, err
	}

	return merged, nil
}

func (g *GroupedVectorVectorBinaryOperation) ensureManySideGroupPopulated(ctx context.Context, group *oneSideMatchGroup) error {
	for _, side := range group.manySides {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("populate grouped many-side presence: %w", err)
		}
		if err := g.ensureManySidePopulated(ctx, side); err != nil {
			return err
		}
	}
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) ensureManySidePopulated(ctx context.Context, side *manySide) error {
	if side == nil {
		return nil
	}
	if side.seriesIndices == nil {
		// Already populated.
		return nil
	}

	// First time we've used this "many" side, populate it.
	latestSeriesIndex := side.latestSeriesIndex()
	data, err := g.manySideBuffer.GetSeries(ctx, side.seriesIndices)
	if err != nil {
		return fmt.Errorf("read many-side series: %w", err)
	}
	if latestSeriesIndex == g.lastManySideSeriesIndex {
		g.markManySideFinishedReading()
	}

	if err := g.updateManySidePresence(ctx, side, data); err != nil {
		putSeriesData(data, g.MemoryConsumptionTracker)
		return err
	}

	side.mergedData, err = g.mergeManySide(data, side.seriesIndices)
	if err != nil {
		if errors.Is(err, errMultipleMatchesOnManySide) {
			return err
		}
		return fmt.Errorf("merge many-side series: %w", err)
	}

	// Clear seriesIndices to indicate that we've populated it.
	side.seriesIndices = nil

	return nil
}

func (g *GroupedVectorVectorBinaryOperation) updateManySidePresence(ctx context.Context, side *manySide, data []types.InstantVectorSeriesData) error {
	group := side.matchGroup
	if side.presenceRecorded || group == nil || group.manySideFillCarrierCount == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("update many-side presence: %w", err)
	}
	if group.manyPresence == nil {
		presence, err := types.IntSlicePool.Get(g.timeRange.StepCount, g.MemoryConsumptionTracker)
		if err != nil {
			return fmt.Errorf("allocate many-side presence: %w", err)
		}
		group.manyPresence = presence[:g.timeRange.StepCount]
		for idx := range group.manyPresence {
			if idx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("initialize many-side presence: %w", err)
				}
			}
			group.manyPresence[idx] = -1
		}
	}

	for dataIdx, seriesData := range data {
		seriesIndex := side.seriesIndices[dataIdx]
		for pointIdx, point := range seriesData.Floats {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("update many-side float presence: %w", err)
				}
			}
			if err := g.recordManySidePresence(group, seriesIndex, point.T); err != nil {
				return err
			}
		}
		for pointIdx, point := range seriesData.Histograms {
			if pointIdx&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return fmt.Errorf("update many-side histogram presence: %w", err)
				}
			}
			if err := g.recordManySidePresence(group, seriesIndex, point.T); err != nil {
				return err
			}
		}
	}
	side.presenceRecorded = true
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) recordManySidePresence(group *oneSideMatchGroup, seriesIndex int, timestamp int64) error {
	timestampIndex, err := g.presenceTimestampIndex(timestamp, len(group.manyPresence))
	if err != nil {
		return fmt.Errorf("record many-side presence at timestamp %d: %w", timestamp, err)
	}
	if group.manyPresence[timestampIndex] == -1 {
		group.manyPresence[timestampIndex] = seriesIndex
	}
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) releaseManyPresence() {
	for _, group := range g.manyPresenceGroups {
		group.releaseManyPresence(g.MemoryConsumptionTracker)
	}
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

// separateIncludeLabelMatchers splits matchers into those describing the one side (oneSideMatchers)
// and those to route to the many side (manySideMatchers).
//
// A matcher is kept for the one side only if its label is not in includeLabels (those come from the
// many side) and, when using "on(...)", is in matchingLabels. For "ignoring(...)"/default matching
// (on is false) this reduces to only splitting out include-label matchers.
func separateIncludeLabelMatchers(matchers types.Matchers, includeLabels []string, on bool, matchingLabels []string) (oneSideMatchers, manySideMatchers types.Matchers) {
	if len(matchers) == 0 {
		return matchers, nil
	}

	// Fast path: ignoring/default matching with no include labels keeps every matcher on the one side.
	if !on && len(includeLabels) == 0 {
		return matchers, nil
	}

	includeSet := make(map[string]struct{}, len(includeLabels))
	for _, l := range includeLabels {
		includeSet[l] = struct{}{}
	}

	var matchingSet map[string]struct{}
	if on {
		matchingSet = make(map[string]struct{}, len(matchingLabels))
		for _, l := range matchingLabels {
			matchingSet[l] = struct{}{}
		}
	}

	for _, m := range matchers {
		_, isInclude := includeSet[m.Name]
		keepForOneSide := !isInclude
		if on {
			_, isMatching := matchingSet[m.Name]
			keepForOneSide = keepForOneSide && isMatching
		}

		if keepForOneSide {
			oneSideMatchers = append(oneSideMatchers, m)
		} else {
			manySideMatchers = append(manySideMatchers, m)
		}
	}
	return oneSideMatchers, manySideMatchers
}

func (g *GroupedVectorVectorBinaryOperation) oneSideHandedness() string {
	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		return "left"
	case parser.CardManyToOne:
		return "right"
	default:
		panic(fmt.Sprintf("unsupported cardinality %d", int(g.VectorMatching.Card)))
	}
}

func (g *GroupedVectorVectorBinaryOperation) ExpressionPosition() posrange.PositionRange {
	return g.expressionPosition
}

func (g *GroupedVectorVectorBinaryOperation) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if err := g.Left.Prepare(ctx, params); err != nil {
		return err
	}

	return g.Right.Prepare(ctx, params)
}

func (g *GroupedVectorVectorBinaryOperation) AfterPrepare(ctx context.Context) error {
	if err := g.Left.AfterPrepare(ctx); err != nil {
		return err
	}

	return g.Right.AfterPrepare(ctx)
}

func (g *GroupedVectorVectorBinaryOperation) FinishedReading(ctx context.Context) error {
	var validationErr error
	if g.oneSideBuffer != nil {
		validationErr = g.validateOneSideGroupsThrough(ctx, int(^uint(0)>>1))
	}
	g.oneSideValidationGroups = nil
	g.releaseOneSideValidationState()

	types.SeriesMetadataSlicePool.Put(&g.oneSideMetadata, g.MemoryConsumptionTracker)
	types.SeriesMetadataSlicePool.Put(&g.manySideMetadata, g.MemoryConsumptionTracker)

	if g.oneSideBuffer != nil {
		g.oneSideBuffer.FinishedReading()
		g.oneSideBuffer = nil
	}

	if g.manySideBuffer != nil {
		g.manySideBuffer.FinishedReading()
		g.manySideBuffer = nil
	}

	for _, s := range g.remainingSeries {
		s.FinishedReading(g.MemoryConsumptionTracker)
	}

	g.remainingSeries = nil
	g.releaseManyPresence()

	leftErr := g.finishLeft(ctx)
	rightErr := g.finishRight(ctx)
	return errors.Join(validationErr, leftErr, rightErr)
}

func (g *GroupedVectorVectorBinaryOperation) finishOneSide(ctx context.Context) error {
	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		return g.finishLeft(ctx)
	case parser.CardManyToOne:
		return g.finishRight(ctx)
	default:
		return fmt.Errorf("unsupported cardinality %d", int(g.VectorMatching.Card))
	}
}

func (g *GroupedVectorVectorBinaryOperation) finishManySide(ctx context.Context) error {
	switch g.VectorMatching.Card {
	case parser.CardOneToMany:
		return g.finishRight(ctx)
	case parser.CardManyToOne:
		return g.finishLeft(ctx)
	default:
		return fmt.Errorf("unsupported cardinality %d", int(g.VectorMatching.Card))
	}
}

func (g *GroupedVectorVectorBinaryOperation) markOneSideFinishedReading() {
	if g.VectorMatching.Card == parser.CardOneToMany {
		g.leftFinishedReading = true
	} else {
		g.rightFinishedReading = true
	}
}

func (g *GroupedVectorVectorBinaryOperation) markManySideFinishedReading() {
	if g.VectorMatching.Card == parser.CardOneToMany {
		g.rightFinishedReading = true
	} else {
		g.leftFinishedReading = true
	}
}

func (g *GroupedVectorVectorBinaryOperation) finishLeft(ctx context.Context) error {
	if g.leftFinishedReading {
		return nil
	}
	if err := g.Left.FinishedReading(ctx); err != nil {
		return fmt.Errorf("finish left child: %w", err)
	}
	g.leftFinishedReading = true
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) finishRight(ctx context.Context) error {
	if g.rightFinishedReading {
		return nil
	}
	if err := g.Right.FinishedReading(ctx); err != nil {
		return fmt.Errorf("finish right child: %w", err)
	}
	g.rightFinishedReading = true
	return nil
}

func (g *GroupedVectorVectorBinaryOperation) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, childAnnos, err := types.FinalizeAndCombine(ctx, g.Left, g.Right)
	if err != nil {
		return nil, nil, err
	}

	g.evaluator.annotations.Merge(childAnnos)

	return stats, g.evaluator.annotations, nil
}

func (g *GroupedVectorVectorBinaryOperation) Close() {
	g.Left.Close()
	g.Right.Close()
}
