// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// InstantVectorOperatorBuffer buffers series data until it is needed by an operator.
//
// For example, if this buffer is being used for a binary operation and the source operator produces series in order A, B, C,
// but their corresponding output series from the binary operation are in order B, A, C, InstantVectorOperatorBuffer
// will buffer the data for series A while series B is produced, then return series A when needed.
type InstantVectorOperatorBuffer struct {
	source          types.InstantVectorOperator
	nextIndexToRead int

	// If seriesUsed == nil, then all series are needed for this operation and should be buffered if not used immediately.
	// Otherwise:
	//  - If seriesUsed[i] == true, then the series at index i is needed for this operation and should be buffered if not used immediately.
	//  - If seriesUsed[i] == false, then the series at index i is never used and can be immediately discarded.
	// FIXME: could use a bitmap here to save some memory
	seriesUsed []bool

	memoryConsumptionTracker *limiting.MemoryConsumptionTracker

	// Stores series read but required for later series.
	buffer map[int]types.InstantVectorSeriesData

	// Reused to avoid allocating on every call to getSeries.
	output []types.InstantVectorSeriesData
}

func NewInstantVectorOperatorBuffer(source types.InstantVectorOperator, seriesUsed []bool, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *InstantVectorOperatorBuffer {
	return &InstantVectorOperatorBuffer{
		source:                   source,
		seriesUsed:               seriesUsed,
		memoryConsumptionTracker: memoryConsumptionTracker,
		buffer:                   map[int]types.InstantVectorSeriesData{},
	}
}

// GetSeries returns the data for the series in seriesIndices.
// The returned slice is only safe to use until GetSeries is called again.
// seriesIndices should be sorted in ascending order to avoid unnecessary buffering.
func (b *InstantVectorOperatorBuffer) GetSeries(ctx context.Context, seriesIndices []int) ([]types.InstantVectorSeriesData, error) {
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

func (b *InstantVectorOperatorBuffer) getSingleSeries(ctx context.Context, seriesIndex int) (types.InstantVectorSeriesData, error) {
	for seriesIndex > b.nextIndexToRead {
		d, err := b.source.NextSeries(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		if b.seriesUsed == nil || b.seriesUsed[b.nextIndexToRead] {
			// We need this series later, but not right now. Store it for later.
			b.buffer[b.nextIndexToRead] = d
		} else {
			// We don't need this series at all, return the slice to the pool now.
			types.PutInstantVectorSeriesData(d, b.memoryConsumptionTracker)
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

func (b *InstantVectorOperatorBuffer) Close() {
	if b.seriesUsed != nil {
		types.BoolSlicePool.Put(b.seriesUsed, b.memoryConsumptionTracker)
	}
}
