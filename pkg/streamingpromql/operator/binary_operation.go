// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type BinaryOperation struct {
	Left  InstantVectorOperator
	Right InstantVectorOperator
	Op    parser.ItemType

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	// Note that we don't retain the output series metadata: if we need to return an error message, we can compute
	// the output series labels from these again.
	leftMetadata  []SeriesMetadata
	rightMetadata []SeriesMetadata

	remainingSeries []*binaryOperationSeriesPair
	leftBuffer      *binaryOperationSeriesBuffer
	rightBuffer     *binaryOperationSeriesBuffer
	op              binaryOperationFunc
}

var _ InstantVectorOperator = &BinaryOperation{}

type binaryOperationSeriesPair struct {
	leftSeriesIndices  []int
	rightSeriesIndices []int
}

func (b *BinaryOperation) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	b.op = arithmeticOperationFuncs[b.Op]
	if b.op == nil {
		// This should never happen, this should be caught by Query.convertToOperator
		// FIXME: move NotSupportedError to a separate package so we can use it in a constructor function for BinaryOperation and remove the check in Query.convertToOperator
		return nil, fmt.Errorf("unsupported binary operation '%s'", b.Op)
	}

	// TODO: break this into smaller functions, it's enormous

	leftMetadata, err := b.Left.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if len(leftMetadata) == 0 {
		// FIXME: this is incorrect for 'or'
		// No series on left-hand side, we'll never have any output series.
		return nil, nil
	}

	rightMetadata, err := b.Right.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// Keep series labels for later so we can use them to generate error messages.
	// We'll return them to the pool in Close().
	b.leftMetadata = leftMetadata
	b.rightMetadata = rightMetadata

	if len(rightMetadata) == 0 {
		// FIXME: this is incorrect for 'or' and 'unless'
		// No series on right-hand side, we'll never have any output series.
		return nil, nil
	}

	// TODO: Prometheus' engine uses strings for the key here, which would avoid issues with hash collisions, but seems much slower.
	hashFunc := b.hashFunc()

	// TODO: pool binaryOperationSeriesPair? Pool internal slices?
	// TODO: guess initial size of map?
	allPairs := map[uint64]*binaryOperationSeriesPair{}

	// TODO: is it better to use whichever side has fewer series for this first loop? Should result in a smaller map and therefore less work later on
	// Would need to be careful about 'or' and 'unless' cases
	for idx, s := range leftMetadata {
		hash := hashFunc(s.Labels)
		series, exists := allPairs[hash]

		if !exists {
			series = &binaryOperationSeriesPair{}
			allPairs[hash] = series
		}

		series.leftSeriesIndices = append(series.leftSeriesIndices, idx)
	}

	for idx, s := range rightMetadata {
		hash := hashFunc(s.Labels)

		if series, exists := allPairs[hash]; exists {
			series.rightSeriesIndices = append(series.rightSeriesIndices, idx)
		}

		// FIXME: if this is an 'or' operation, then we need to create the right side even if the left doesn't exist
	}

	// Remove pairs that cannot produce series.
	for hash, pair := range allPairs {
		if len(pair.leftSeriesIndices) == 0 || len(pair.rightSeriesIndices) == 0 {
			// FIXME: this is incorrect for 'or' and 'unless'
			// No matching series on at least one side for this output series, so output series will have no samples. Remove it.
			delete(allPairs, hash)
		}
	}

	allMetadata := make([]SeriesMetadata, 0, len(allPairs))
	b.remainingSeries = make([]*binaryOperationSeriesPair, 0, len(allPairs))
	labelsFunc := b.labelsFunc()

	for _, pair := range allPairs {
		firstSeriesLabels := leftMetadata[pair.leftSeriesIndices[0]].Labels
		allMetadata = append(allMetadata, SeriesMetadata{Labels: labelsFunc(firstSeriesLabels)})
		b.remainingSeries = append(b.remainingSeries, pair)
	}

	// TODO: sort output series
	// Sort output series: either to favour left side or right side
	//   - Add comment emphasising this is critical for managing peak memory consumption, could make this decision more sophisticated in the future
	//   - TODO: think this through with some examples, especially for pathological cases where a single output series has multiple series on each side for different timesteps
	//   - One-to-one matching:
	//       - assume one series on each side of output series
	//       - therefore best option to keep peak memory utilisation low is to order series so we don't hold higher cardinality side in memory
	//         eg. if LHS is 20 series, and RHS is 2 series, best option is to go through LHS series in order and potentially have to hold some RHS series in memory,
	//             as then in worst case we'll hold 2 series in memory at once
	//   - Many-to-one / one-to-many matching:
	//       - assume "one" side is higher cardinality, and series from "many" side are used multiple times
	//       - therefore best option to keep peak memory utilisation low is to order series so we don't hold higher cardinality side in memory, especially as we'll
	//         likely have to hold some "many" side series in memory anyway (doesn't make sense to have to hold both "one" and "many" side series)

	b.leftBuffer = newBinaryOperationSeriesBuffer(b.Left)
	b.rightBuffer = newBinaryOperationSeriesBuffer(b.Right)

	return allMetadata, nil
}

// hashFunc returns a function that computes the hash of the output group this series belongs to.
func (b *BinaryOperation) hashFunc() func(labels.Labels) uint64 {
	buf := make([]byte, 0, 1024)
	names := b.VectorMatching.MatchingLabels

	if b.VectorMatching.On {
		slices.Sort(names)

		return func(l labels.Labels) uint64 {
			var hash uint64
			hash, buf = l.HashForLabels(buf, names...)
			return hash
		}
	}

	names = append([]string{labels.MetricName}, names...)
	slices.Sort(names)

	return func(l labels.Labels) uint64 {
		var hash uint64
		hash, buf = l.HashWithoutLabels(buf, names...)
		return hash
	}
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

func (b *BinaryOperation) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return InstantVectorSeriesData{}, EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	allLeftSeries, err := b.leftBuffer.getSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	mergedLeftSide, err := b.mergeOneSide(allLeftSeries, thisSeries.leftSeriesIndices, b.leftMetadata, "left")
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.getSeries(ctx, thisSeries.rightSeriesIndices)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	mergedRightSide, err := b.mergeOneSide(allRightSeries, thisSeries.rightSeriesIndices, b.rightMetadata, "right")
	if err != nil {
		return InstantVectorSeriesData{}, err
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
// TODO: for many-to-one / one-to-many matching, we could avoid re-merging each time for the side used multiple times
// TODO: would this be easier to do if we were working with []float64 rather than []FPoint?
//   - would also mean that some arithmetic operations become faster, as we can use vectorisation (eg. leftPoints + rightPoints, rather than output[i] = left[i] + right[i] etc.)
//   - should we just change the InstantVectorOperator interface to use ([]float64, presence)? Would make some aggregation operations faster as well (eg. sum)
func (b *BinaryOperation) mergeOneSide(data []InstantVectorSeriesData, sourceSeriesIndices []int, sourceSeriesMetadata []SeriesMetadata, side string) (InstantVectorSeriesData, error) {
	if len(data) == 1 {
		// If there's only one series on this side, there's no merging required.
		return data[0], nil
	}

	if len(data) == 0 {
		return InstantVectorSeriesData{}, nil
	}

	slices.SortFunc(data, func(a, b InstantVectorSeriesData) int {
		return int(a.Floats[0].T - b.Floats[0].T)
	})

	mergedSize := len(data[0].Floats)
	haveOverlaps := false

	// We're going to create a new slice, so return this one to the pool.
	// We'll return the other slices in the for loop below.
	// We must defer here, rather than at the end, as the merge loop below reslices Floats.
	// TODO: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
	defer PutFPointSlice(data[0].Floats)

	for i := 0; i < len(data)-1; i++ {
		first := data[i]
		second := data[i+1]
		mergedSize += len(second.Floats)

		// We're going to create a new slice, so return this one to the pool.
		// We must defer here, rather than at the end, as the merge loop below reslices Floats.
		// TODO: this isn't correct for many-to-one / one-to-many matching - we'll need the series again (unless we store the result of the merge)
		defer PutFPointSlice(second.Floats)

		// Check if first overlaps with second.
		// InstantVectorSeriesData.Floats is required to be sorted in timestamp order, so if the last point
		// of the first series is before the first point of the second series, it cannot overlap.
		if first.Floats[len(first.Floats)-1].T >= second.Floats[0].T {
			haveOverlaps = true
		}
	}

	output := GetFPointSlice(mergedSize)

	if !haveOverlaps {
		// Fast path: no overlaps, so we can just concatenate the slices together, and there's no
		// need to check for conflicts either.
		for _, d := range data {
			output = append(output, d.Floats...)
		}

		return InstantVectorSeriesData{Floats: output}, nil
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
					return InstantVectorSeriesData{Floats: output}, nil
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

				return InstantVectorSeriesData{}, fmt.Errorf("found duplicate series for the match group %s on the %s side of the operation at timestamp %s: %s and %s", groupLabels, side, timestamp.Time(nextT).Format(time.RFC3339Nano), firstConflictingSeriesLabels, secondConflictingSeriesLabels)
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

func (b *BinaryOperation) computeResult(left InstantVectorSeriesData, right InstantVectorSeriesData) InstantVectorSeriesData {
	// TODO: return slices to pool if they're not reused
	outputLength := min(len(left.Floats), len(right.Floats)) // We can't produce more output points than input points for arithmetic operations.
	output := GetFPointSlice(outputLength)                   // FIXME: Reuse one side for the output slice?

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
				F: b.op(leftPoint.F, right.Floats[nextRightIndex].F),
				T: leftPoint.T,
			})
		}
	}

	return InstantVectorSeriesData{
		Floats: output,
	}
}

func (b *BinaryOperation) Close() {
	if b.Left != nil {
		b.Left.Close()
	}

	if b.Right != nil {
		b.Right.Close()
	}

	if b.leftMetadata != nil {
		PutSeriesMetadataSlice(b.leftMetadata)
	}

	if b.rightMetadata != nil {
		PutSeriesMetadataSlice(b.rightMetadata)
	}
}

// binaryOperationSeriesBuffer buffers series data until it is needed by BinaryOperation.
//
// For example, if the source operator produces series in order A, B, C, but their corresponding output series from the
// binary operation are in order B, A, C, binaryOperationSeriesBuffer will buffer the data for series A while series B is
// produced, then return series A when needed.
type binaryOperationSeriesBuffer struct {
	source          InstantVectorOperator
	nextIndexToRead int

	// TODO: what is the best way to store buffered data?
	buffer map[int]InstantVectorSeriesData

	// TODO: need a way to know if a series will never be used and therefore skip buffering it

	output []InstantVectorSeriesData
}

func newBinaryOperationSeriesBuffer(source InstantVectorOperator) *binaryOperationSeriesBuffer {
	return &binaryOperationSeriesBuffer{
		source: source,
		buffer: map[int]InstantVectorSeriesData{},
	}
}

// getSeries returns the data for the series in seriesIndices.
// The returned slice is only safe to use until getSeries is called again.
func (b *binaryOperationSeriesBuffer) getSeries(ctx context.Context, seriesIndices []int) ([]InstantVectorSeriesData, error) {
	if cap(b.output) < len(seriesIndices) {
		// TODO: pool?
		b.output = make([]InstantVectorSeriesData, len(seriesIndices))
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

func (b *binaryOperationSeriesBuffer) getSingleSeries(ctx context.Context, seriesIndex int) (InstantVectorSeriesData, error) {
	for seriesIndex > b.nextIndexToRead {
		d, err := b.source.Next(ctx)
		if err != nil {
			return InstantVectorSeriesData{}, err
		}

		// TODO: don't bother storing data we won't need, immediately return slice to pool
		b.buffer[b.nextIndexToRead] = d

		b.nextIndexToRead++
	}

	if seriesIndex == b.nextIndexToRead {
		// Don't bother buffering data if we can return it directly.
		b.nextIndexToRead++
		return b.source.Next(ctx)
	}

	d := b.buffer[seriesIndex]
	delete(b.buffer, seriesIndex)

	return d, nil
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
