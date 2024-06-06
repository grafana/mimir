// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// BinaryOperation represents a binary operation between instant vectors such as "<expr> + <expr>" or "<expr> - <expr>".
type BinaryOperation struct {
	Left  types.InstantVectorOperator
	Right types.InstantVectorOperator
	Op    parser.ItemType
	Pool  *pooling.LimitingPool

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []types.SeriesMetadata
	rightMetadata []types.SeriesMetadata

	remainingSeries []*binaryOperationOutputSeries
	leftBuffer      *binaryOperationSeriesBuffer
	rightBuffer     *binaryOperationSeriesBuffer
	opFunc          binaryOperationFunc
}

var _ types.InstantVectorOperator = &BinaryOperation{}

type binaryOperationOutputSeries struct {
	leftSeriesIndices  []int
	rightSeriesIndices []int
}

// latestLeftSeries returns the index of the last series from the left source needed for this output series.
//
// It assumes that leftSeriesIndices is sorted in ascending order.
func (s binaryOperationOutputSeries) latestLeftSeries() int {
	return s.leftSeriesIndices[len(s.leftSeriesIndices)-1]
}

// latestRightSeries returns the index of the last series from the right source needed for this output series.
//
// It assumes that rightSeriesIndices is sorted in ascending order.
func (s binaryOperationOutputSeries) latestRightSeries() int {
	return s.rightSeriesIndices[len(s.rightSeriesIndices)-1]
}

func NewBinaryOperation(left types.InstantVectorOperator, right types.InstantVectorOperator, vectorMatching parser.VectorMatching, op parser.ItemType, pool *pooling.LimitingPool) (*BinaryOperation, error) {
	opFunc := arithmeticOperationFuncs[op]
	if opFunc == nil {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with '%s'", op))
	}

	return &BinaryOperation{
		Left:           left,
		Right:          right,
		VectorMatching: vectorMatching,
		Op:             op,
		Pool:           pool,

		opFunc: opFunc,
	}, nil
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
func (b *BinaryOperation) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if canProduceAnySeries, err := b.loadSeriesMetadata(ctx); err != nil {
		return nil, err
	} else if !canProduceAnySeries {
		return nil, nil
	}

	allMetadata, allSeries, leftSeriesUsed, rightSeriesUsed, err := b.computeOutputSeries()
	if err != nil {
		return nil, err
	}

	b.sortSeries(allMetadata, allSeries)
	b.remainingSeries = allSeries

	b.leftBuffer = newBinaryOperationSeriesBuffer(b.Left, leftSeriesUsed, b.Pool)
	b.rightBuffer = newBinaryOperationSeriesBuffer(b.Right, rightSeriesUsed, b.Pool)

	return allMetadata, nil
}

// loadSeriesMetadata loads series metadata from both sides of this operation.
// It returns false if one side returned no series and that means there is no way for this operation to return any series.
// (eg. if doing A + B and either A or B have no series, then there is no way for this operation to produce any series)
func (b *BinaryOperation) loadSeriesMetadata(ctx context.Context) (bool, error) {
	// We retain the series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().

	var err error
	b.leftMetadata, err = b.Left.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.leftMetadata) == 0 {
		// FIXME: this is incorrect for 'or'
		// No series on left-hand side, we'll never have any output series.
		return false, nil
	}

	b.rightMetadata, err = b.Right.SeriesMetadata(ctx)
	if err != nil {
		return false, err
	}

	if len(b.rightMetadata) == 0 {
		// FIXME: this is incorrect for 'or' and 'unless'
		// No series on right-hand side, we'll never have any output series.
		return false, nil
	}

	return true, nil
}

// computeOutputSeries determines the possible output series from this operator.
// It assumes leftMetadata and rightMetadata have already been populated.
//
// It returns:
// - a list of all possible series this operator could return
// - a corresponding list of the source series for each output series
// - a list indicating which series from the left side are needed to compute the output
// - a list indicating which series from the right side are needed to compute the output
func (b *BinaryOperation) computeOutputSeries() ([]types.SeriesMetadata, []*binaryOperationOutputSeries, []bool, []bool, error) {
	labelsFunc := b.labelsFunc()
	outputSeriesMap := map[string]*binaryOperationOutputSeries{}

	// Use the smaller side to populate the map of possible output series first.
	// This should ensure we don't unnecessarily populate the output series map with series that will never match in most cases.
	// (It's possible that all the series on the larger side all belong to the same group, but this is expected to be rare.)
	// FIXME: this doesn't work as-is for 'unless'.
	smallerSide := b.leftMetadata
	largerSide := b.rightMetadata
	smallerSideIsLeftSide := len(b.leftMetadata) < len(b.rightMetadata)

	if !smallerSideIsLeftSide {
		smallerSide = b.rightMetadata
		largerSide = b.leftMetadata
	}

	for idx, s := range smallerSide {
		groupLabels := labelsFunc(s.Labels).String()
		series, exists := outputSeriesMap[groupLabels]

		if !exists {
			series = &binaryOperationOutputSeries{}
			outputSeriesMap[groupLabels] = series
		}

		if smallerSideIsLeftSide {
			series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
		} else {
			series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
		}
	}

	for idx, s := range largerSide {
		groupLabels := labelsFunc(s.Labels).String()

		if series, exists := outputSeriesMap[groupLabels]; exists {
			if smallerSideIsLeftSide {
				// Currently iterating through right side.
				series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
			} else {
				series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
			}
		}

		// FIXME: if this is an 'or' operation, then we need to create the right side even if the left doesn't exist (or vice-versa)
	}

	// Remove series that cannot produce samples.
	for seriesLabels, outputSeries := range outputSeriesMap {
		if len(outputSeries.leftSeriesIndices) == 0 || len(outputSeries.rightSeriesIndices) == 0 {
			// FIXME: this is incorrect for 'or' and 'unless'
			// No matching series on at least one side for this output series, so output series will have no samples. Remove it.
			delete(outputSeriesMap, seriesLabels)
		}
	}

	allMetadata := make([]types.SeriesMetadata, 0, len(outputSeriesMap))
	allSeries := make([]*binaryOperationOutputSeries, 0, len(outputSeriesMap))

	leftSeriesUsed, err := b.Pool.GetBoolSlice(len(b.leftMetadata))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	rightSeriesUsed, err := b.Pool.GetBoolSlice(len(b.rightMetadata))
	if err != nil {
		return nil, nil, nil, nil, err
	}

	leftSeriesUsed = leftSeriesUsed[:len(b.leftMetadata)]
	rightSeriesUsed = rightSeriesUsed[:len(b.rightMetadata)]

	for _, outputSeries := range outputSeriesMap {
		firstSeriesLabels := b.leftMetadata[outputSeries.leftSeriesIndices[0]].Labels
		allMetadata = append(allMetadata, types.SeriesMetadata{Labels: labelsFunc(firstSeriesLabels)})
		allSeries = append(allSeries, outputSeries)

		for _, leftSeriesIndex := range outputSeries.leftSeriesIndices {
			leftSeriesUsed[leftSeriesIndex] = true
		}

		for _, rightSeriesIndex := range outputSeries.rightSeriesIndices {
			rightSeriesUsed[rightSeriesIndex] = true
		}
	}

	return allMetadata, allSeries, leftSeriesUsed, rightSeriesUsed, nil
}

// sortSeries sorts metadata and series in place to try to minimise the number of input series we'll need to buffer in memory.
//
// This is critical for minimising the memory consumption of this operator: if we choose a poor ordering of series,
// we'll need to buffer many input series in memory.
//
// At present, sortSeries uses a very basic heuristic to guess the best way to sort the output series, but we could make
// this more sophisticated in the future.
func (b *BinaryOperation) sortSeries(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) {
	// For one-to-one matching, we assume that each output series takes one series from each side of the operator.
	// If this is true, then the best order is the one in which we read from the highest cardinality side in order.
	// If we do this, then in the worst case, we'll have to buffer the whole of the lower cardinality side.
	// (Compare this with sorting so that we read the lowest cardinality side in order: in the worst case, we'll have
	// to buffer the whole of the higher cardinality side.)
	//
	// FIXME: this is reasonable for one-to-one matching, but likely not for one-to-many / many-to-one.
	// For one-to-many / many-to-one, it would likely be best to buffer the side used for multiple output series (the "one" side),
	// as we'll need to retain these series for multiple output series anyway.

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
	series   []*binaryOperationOutputSeries
}

type favourLeftSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourLeftSideSorter(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) favourLeftSideSorter {
	return favourLeftSideSorter{binaryOperationOutputSorter{metadata, series}}
}

type favourRightSideSorter struct {
	binaryOperationOutputSorter
}

func newFavourRightSideSorter(metadata []types.SeriesMetadata, series []*binaryOperationOutputSeries) favourRightSideSorter {
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

// labelsFunc returns a function that computes the labels of the output group this series belongs to.
func (b *BinaryOperation) labelsFunc() func(labels.Labels) labels.Labels {
	lb := labels.NewBuilder(labels.EmptyLabels())

	if b.VectorMatching.On {
		return func(l labels.Labels) labels.Labels {
			lb.Reset(l)
			lb.Keep(b.VectorMatching.MatchingLabels...)
			return lb.Labels()
		}
	}

	return func(l labels.Labels) labels.Labels {
		lb.Reset(l)
		lb.Del(b.VectorMatching.MatchingLabels...)
		lb.Del(labels.MetricName)
		return lb.Labels()
	}
}

func (b *BinaryOperation) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	allLeftSeries, err := b.leftBuffer.getSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedLeftSide, err := b.mergeOneSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.getSeries(ctx, thisSeries.rightSeriesIndices)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	mergedRightSide, err := b.mergeOneSide(allRightSeries, thisSeries.rightSeriesIndices, b.rightMetadata, "right")
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return b.computeResult(mergedLeftSide, mergedRightSide), nil
}

// mergeOneSide exists to handle the case where one side of an output series has different source series at different time steps.
//
// For example, consider the query "left_side + on (env) right_side" with the following source data:
//
//	left_side{env="test", pod="a"} 1 2 _
//	left_side{env="test", pod="b"} _ _ 3
//	right_side{env="test"} 100 200 300
//
// mergeOneSide will take in both series for left_side and return a single series with the points [1, 2, 3].
//
// mergeOneSide is optimised for the case where there is only one source series, or the source series do not overlap, as in the example above.
//
// FIXME: for many-to-one / one-to-many matching, we could avoid re-merging each time for the side used multiple times
func (b *BinaryOperation) mergeOneSide(data []types.InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []types.SeriesMetadata, side string) (types.InstantVectorSeriesData, error) {
	if len(data) == 1 {
		// Fast path: if there's only one series on this side, there's no merging required.
		return data[0], nil
	}

	if len(data) == 0 {
		return types.InstantVectorSeriesData{}, nil
	}

	slices.SortFunc(data, func(a, b types.InstantVectorSeriesData) int {
		return int(a.Floats[0].T - b.Floats[0].T)
	})

	mergedSize := len(data[0].Floats)
	haveOverlaps := false

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Floats.
	// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer b.Pool.PutFPointSlice(data[0].Floats)

	for i := 0; i < len(data)-1; i++ {
		first := data[i]
		second := data[i+1]
		mergedSize += len(second.Floats)

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Floats.
		// FIXME: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer b.Pool.PutFPointSlice(second.Floats)

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Floats is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Floats[len(first.Floats)-1].T >= second.Floats[0].T {
			haveOverlaps = true
		}
	}

	output, err := b.Pool.GetFPointSlice(mergedSize)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Floats...)
		}

		return types.InstantVectorSeriesData{Floats: output}, nil
	}

	// Slow path: there are overlaps, so we need to merge slices together and check for conflicts as we go.
	// We don't expect to have many series here, so something like a loser tree is likely unnecessary.
	remainingSeries := len(data)

	for {
		if remainingSeries == 1 {
			// Only one series left, just copy remaining points.
			for _, d := range data {
				if len(d.Floats) > 0 {
					output = append(output, d.Floats...)
					return types.InstantVectorSeriesData{Floats: output}, nil
				}
			}
		}

		nextT := int64(math.MaxInt64)
		sourceSeriesIndexInData := -1

		for seriesIndexInData, d := range data {
			if len(d.Floats) == 0 {
				continue
			}

			nextPointInSeries := d.Floats[0]
			if nextPointInSeries.T == nextT {
				// Another series has a point with the same timestamp. We have a conflict.
				firstConflictingSeriesLabels := sourceSeriesMetadata[sourceSeriesIndices[sourceSeriesIndexInData]].Labels
				secondConflictingSeriesLabels := sourceSeriesMetadata[sourceSeriesIndices[seriesIndexInData]].Labels
				groupLabels := b.labelsFunc()(firstConflictingSeriesLabels)

				return types.InstantVectorSeriesData{}, fmt.Errorf("found duplicate series for the match group %s on the %s side of the operation at timestamp %s: %s and %s", groupLabels, side, timestamp.Time(nextT).Format(time.RFC3339Nano), firstConflictingSeriesLabels, secondConflictingSeriesLabels)
			}

			if d.Floats[0].T < nextT {
				nextT = d.Floats[0].T
				sourceSeriesIndexInData = seriesIndexInData
			}
		}

		output = append(output, data[sourceSeriesIndexInData].Floats[0])
		data[sourceSeriesIndexInData].Floats = data[sourceSeriesIndexInData].Floats[1:]

		if len(data[sourceSeriesIndexInData].Floats) == 0 {
			remainingSeries--
		}
	}
}

func (b *BinaryOperation) computeResult(left types.InstantVectorSeriesData, right types.InstantVectorSeriesData) types.InstantVectorSeriesData {
	var output []promql.FPoint

	// For one-to-one matching for arithmetic operators, reuse one of the input slices to avoid allocating another slice.
	// We'll never produce more points than the smaller input side, so use that as our output slice.
	//
	// FIXME: this is not safe to do for one-to-many, many-to-one or many-to-many matching, as we may need the input series for later output series.
	if len(left.Floats) < len(right.Floats) {
		output = left.Floats[:0]
		defer b.Pool.PutFPointSlice(right.Floats)
	} else {
		output = right.Floats[:0]
		defer b.Pool.PutFPointSlice(left.Floats)
	}

	nextRightIndex := 0

	for _, leftPoint := range left.Floats {
		for nextRightIndex < len(right.Floats) && right.Floats[nextRightIndex].T < leftPoint.T {
			nextRightIndex++
		}

		if nextRightIndex == len(right.Floats) {
			// No more points on right side. We are done.
			break
		}

		if leftPoint.T == right.Floats[nextRightIndex].T {
			// We have matching points on both sides, compute the result.
			output = append(output, promql.FPoint{
				F: b.opFunc(leftPoint.F, right.Floats[nextRightIndex].F),
				T: leftPoint.T,
			})
		}
	}

	return types.InstantVectorSeriesData{
		Floats: output,
	}
}

func (b *BinaryOperation) Close() {
	b.Left.Close()
	b.Right.Close()

	if b.leftMetadata != nil {
		pooling.PutSeriesMetadataSlice(b.leftMetadata)
	}

	if b.rightMetadata != nil {
		pooling.PutSeriesMetadataSlice(b.rightMetadata)
	}

	if b.leftBuffer != nil {
		b.leftBuffer.close()
	}

	if b.rightBuffer != nil {
		b.rightBuffer.close()
	}
}

// binaryOperationSeriesBuffer buffers series data until it is needed by BinaryOperation.
//
// For example, if the source operator produces series in order A, B, C, but their corresponding output series from the
// binary operation are in order B, A, C, binaryOperationSeriesBuffer will buffer the data for series A while series B is
// produced, then return series A when needed.
type binaryOperationSeriesBuffer struct {
	source          types.InstantVectorOperator
	nextIndexToRead int

	// If seriesUsed[i] == true, then the series at index i is needed for this operation and should be buffered if not used immediately.
	// If seriesUsed[i] == false, then the series at index i is never used and can be immediately discarded.
	// FIXME: could use a bitmap here to save some memory
	seriesUsed []bool

	pool *pooling.LimitingPool

	// Stores series read but required for later series.
	buffer map[int]types.InstantVectorSeriesData

	// Reused to avoid allocating on every call to getSeries.
	output []types.InstantVectorSeriesData
}

func newBinaryOperationSeriesBuffer(source types.InstantVectorOperator, seriesUsed []bool, pool *pooling.LimitingPool) *binaryOperationSeriesBuffer {
	return &binaryOperationSeriesBuffer{
		source:     source,
		seriesUsed: seriesUsed,
		pool:       pool,
		buffer:     map[int]types.InstantVectorSeriesData{},
	}
}

// getSeries returns the data for the series in seriesIndices.
// The returned slice is only safe to use until getSeries is called again.
// seriesIndices should be sorted in ascending order to avoid unnecessary buffering.
func (b *binaryOperationSeriesBuffer) getSeries(ctx context.Context, seriesIndices []int) ([]types.InstantVectorSeriesData, error) {
	if cap(b.output) < len(seriesIndices) {
		b.output = make([]types.InstantVectorSeriesData, len(seriesIndices))
	}

	b.output = b.output[:len(seriesIndices)]

	for i, seriesIndex := range seriesIndices {
		d, err := b.getSingleSeries(ctx, seriesIndex)

		if err != nil {
			return nil, err
		}

		b.output[i] = d
	}

	return b.output, nil
}

func (b *binaryOperationSeriesBuffer) getSingleSeries(ctx context.Context, seriesIndex int) (types.InstantVectorSeriesData, error) {
	for seriesIndex > b.nextIndexToRead {
		d, err := b.source.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		if b.seriesUsed[b.nextIndexToRead] {
			// We need this series later, but not right now. Store it for later.
			b.buffer[b.nextIndexToRead] = d
		} else {
			// We don't need this series at all, return the slice to the pool now.
			b.pool.PutFPointSlice(d.Floats)
		}

		b.nextIndexToRead++
	}

	if seriesIndex == b.nextIndexToRead {
		// Don't bother buffering data if we can return it directly.
		b.nextIndexToRead++
		return b.source.NextSeries(ctx)
	}

	d := b.buffer[seriesIndex]
	delete(b.buffer, seriesIndex)

	return d, nil
}

func (b *binaryOperationSeriesBuffer) close() {
	if b.seriesUsed != nil {
		b.pool.PutBoolSlice(b.seriesUsed)
	}
}

type binaryOperationFunc func(left, right float64) float64

var arithmeticOperationFuncs = map[parser.ItemType]binaryOperationFunc{
	parser.ADD: func(left, right float64) float64 {
		return left + right
	},
	parser.SUB: func(left, right float64) float64 {
		return left - right
	},
	parser.MUL: func(left, right float64) float64 {
		return left * right
	},
	parser.DIV: func(left, right float64) float64 {
		return left / right
	},
	parser.MOD:   math.Mod,
	parser.POW:   math.Pow,
	parser.ATAN2: math.Atan2,
}
