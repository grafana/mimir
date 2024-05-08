// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"math"
	"slices"
)

type binaryOperatorFunc func(left, right float64) float64

type BinaryOperator struct {
	Left  InstantVectorOperator
	Right InstantVectorOperator
	Op    parser.ItemType

	VectorMatching parser.VectorMatching

	// We need to retain these so that NextSeries() can return an error message with the series labels when
	// multiple points match on a single side.
	leftMetadata  []SeriesMetadata
	rightMetadata []SeriesMetadata

	remainingSeries []*binaryOperatorSeriesPair
	leftBuffer      *binaryOperatorSeriesBuffer
	rightBuffer     *binaryOperatorSeriesBuffer
	operatorFunc    binaryOperatorFunc
}

var _ InstantVectorOperator = &BinaryOperator{}

type binaryOperatorSeriesPair struct {
	leftSeriesIndices  []int
	rightSeriesIndices []int
}

func (b *BinaryOperator) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	b.operatorFunc = arithmeticOperatorFuncs[b.Op]
	if b.operatorFunc == nil {
		// This should never happen, this should be caught by Query.convertToOperator
		// FIXME: move NotSupportedError to a separate package so we can use it in a constructor function for BinaryOperator and remove the check in Query.convertToOperator
		return nil, fmt.Errorf("unsupported binary operator '%s'", b.Op)
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
	b.leftMetadata = leftMetadata
	b.rightMetadata = rightMetadata

	if len(rightMetadata) == 0 {
		// FIXME: this is incorrect for 'or' and 'unless'
		// No series on right-hand side, we'll never have any output series.
		return nil, nil
	}

	// TODO: Prometheus' engine uses strings for the key here, which would avoid issues with hash collisions, but seems much slower.
	hashFunc := b.hashFunc()

	// TODO: pool binaryOperatorSeriesPair? Pool internal slices?
	// TODO: guess initial size of map?
	allPairs := map[uint64]*binaryOperatorSeriesPair{}

	// TODO: is it better to use whichever side has fewer series for this first loop? Should result in a smaller map and therefore less work later on
	// Would need to be careful about 'or' and 'unless' cases
	for idx, s := range leftMetadata {
		hash := hashFunc(s.Labels)
		series, exists := allPairs[hash]

		if !exists {
			series = &binaryOperatorSeriesPair{}
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
	b.remainingSeries = make([]*binaryOperatorSeriesPair, 0, len(allPairs))
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

	b.leftBuffer = newBinaryOperatorSeriesBuffer(b.Left)
	b.rightBuffer = newBinaryOperatorSeriesBuffer(b.Right)

	return allMetadata, nil
}

func (b *BinaryOperator) hashFunc() func(labels.Labels) uint64 {
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

func (b *BinaryOperator) labelsFunc() func(labels.Labels) labels.Labels {
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

func (b *BinaryOperator) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if len(b.remainingSeries) == 0 {
		return InstantVectorSeriesData{}, EOS
	}

	thisSeries := b.remainingSeries[0]
	b.remainingSeries = b.remainingSeries[1:]

	// TODO: need to store which series are actually used on each side in SeriesMetadata() above
	// TODO: need to return original slices from Left.Next() and Right.Next() to the pool
	// Solution: return a type like { data InstantVectorSeriesData, refCount int } from getSeries() and merge(),
	// use this to track when it is safe to return slices here or in merge() - will work for one-to-one, many-to-one and many-to-many cases
	// Populate refCount from slice that tracks the number of times each series is used in an output series

	allLeftSeries, err := b.leftBuffer.getSeries(ctx, thisSeries.leftSeriesIndices)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	allRightSeries, err := b.rightBuffer.getSeries(ctx, thisSeries.rightSeriesIndices)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	// TODO: merge left side into single slice? Or have some kind of iterator over slices?
	// TODO: merge right side into single slice? Or have some kind of iterator over slices?
	// Merging:
	// - responsible for merging multiple series on a side into one series
	// - if only one series on side, just return original slice as-is
	// - if multiple series:
	//   - sort series by first point
	//   - for each remaining series:
	//      - if points don't overlap with previous series, just copy data
	//      - if points do overlap with previous series, need to zip series together, checking for conflicts
	// - would this be easier to do if we were working with []float64 rather than []FPoint?
	//   - would also mean that some arithmetic operations become faster, as we can use vectorisation (eg. leftPoints + rightPoints, rather than output[i] = left[i] + right[i] etc.)
	//   - should we just change the InstantVectorOperator interface to use ([]float64, presence)? Would make some aggregation operations faster as well (eg. sum)

	// Compute result for each output series
	//   - Can we reuse either the left or right side slice for the output slice?
	return b.computeResult(allLeftSeries[0], allRightSeries[0]), nil

	// TODO: return series slices to the pool
}

func (b *BinaryOperator) computeResult(left InstantVectorSeriesData, right InstantVectorSeriesData) InstantVectorSeriesData {
	outputLength := min(len(left.Floats), len(right.Floats)) // We can't produce more output points than input points for arithmetic operators.
	output := GetFPointSlice(outputLength)                   // Reuse one side for the output slice?

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
				F: b.operatorFunc(leftPoint.F, right.Floats[nextRightIndex].F),
				T: leftPoint.T,
			})
		}
	}

	return InstantVectorSeriesData{
		Floats: output,
	}
}

func (b *BinaryOperator) Close() {
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

// binaryOperatorSeriesBuffer buffers series data until it is needed by BinaryOperator.
//
// For example, if the source operator produces series in order A, B, C, but their corresponding output series from the
// binary operator are in order B, A, C, binaryOperatorSeriesBuffer will buffer the data for series A while series B is
// produced, then return series A when needed.
type binaryOperatorSeriesBuffer struct {
	source          InstantVectorOperator
	nextIndexToRead int

	// TODO: what is the best way to store buffered data?
	buffer map[int]InstantVectorSeriesData

	// TODO: need a way to know if a series will never be used and therefore skip buffering it

	output []InstantVectorSeriesData
}

func newBinaryOperatorSeriesBuffer(source InstantVectorOperator) *binaryOperatorSeriesBuffer {
	return &binaryOperatorSeriesBuffer{
		source: source,
		buffer: map[int]InstantVectorSeriesData{},
	}
}

// getSeries returns the data for the series in seriesIndices.
// The returned slice is only safe to use until getSeries is called again.
func (b *binaryOperatorSeriesBuffer) getSeries(ctx context.Context, seriesIndices []int) ([]InstantVectorSeriesData, error) {
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

func (b *binaryOperatorSeriesBuffer) getSingleSeries(ctx context.Context, seriesIndex int) (InstantVectorSeriesData, error) {
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

var arithmeticOperatorFuncs = map[parser.ItemType]binaryOperatorFunc{
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
