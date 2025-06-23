// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"fmt"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type SeriesDataRingBuffer struct {
	data []types.InstantVectorSeriesData

	startIndex       int // Position in data of the first series.
	firstSeriesIndex int // Series index of the first series in the buffer.
	seriesCount      int // Number of series in the buffer.
}

func (b *SeriesDataRingBuffer) Append(d types.InstantVectorSeriesData, seriesIndex int) {
	if b.seriesCount == 0 {
		// We're about to add the first series.
		b.firstSeriesIndex = seriesIndex
	} else if seriesIndex != b.firstSeriesIndex+b.seriesCount {
		panic(fmt.Sprintf("attempted to append series with index %v, but first series index in buffer is %v and have %v series", seriesIndex, b.firstSeriesIndex, b.seriesCount))
	}

	if len(b.data) == b.seriesCount {
		// Buffer is full, need to grow it.
		newSize := max(len(b.data)*2, 2) // Grow by powers of two, and ensure we have at least 2 slots.
		newData := make([]types.InstantVectorSeriesData, newSize)
		copy(newData, b.data[b.startIndex:])
		copy(newData[len(b.data)-b.startIndex:], b.data[:b.startIndex])
		b.startIndex = 0
		b.data = newData
	}

	b.data[(b.startIndex+b.seriesCount)%len(b.data)] = d
	b.seriesCount++
}

func (b *SeriesDataRingBuffer) Remove(seriesIndex int) types.InstantVectorSeriesData {
	if b.seriesCount == 0 {
		panic(fmt.Sprintf("attempted to remove series at index %v, but buffer is empty", seriesIndex))
	}

	if seriesIndex < b.firstSeriesIndex || seriesIndex >= (b.firstSeriesIndex+b.seriesCount) {
		panic(fmt.Sprintf("attempted to remove series at index %v, but have series from index %v to %v", seriesIndex, b.firstSeriesIndex, b.firstSeriesIndex+b.seriesCount-1))
	}

	idx := b.startIndex % len(b.data)
	d := b.data[idx]
	b.data[idx] = types.InstantVectorSeriesData{} // Clear the slot.
	b.startIndex++
	b.firstSeriesIndex++
	b.seriesCount--

	if b.startIndex > len(b.data) || b.seriesCount == 0 {
		b.startIndex = 0
	}

	return d
}

func (b *SeriesDataRingBuffer) RemoveFirst() types.InstantVectorSeriesData {
	if b.seriesCount == 0 {
		panic("attempted to remove first series of empty buffer")
	}

	return b.Remove(b.firstSeriesIndex)
}

func (b *SeriesDataRingBuffer) Get(seriesIndex int) types.InstantVectorSeriesData {
	if b.seriesCount == 0 {
		panic(fmt.Sprintf("attempted to get series at index %v, but buffer is empty", seriesIndex))
	}

	if seriesIndex < b.firstSeriesIndex || seriesIndex >= (b.firstSeriesIndex+b.seriesCount) {
		panic(fmt.Sprintf("attempted to get series at index %v, but have series from index %v to %v", seriesIndex, b.firstSeriesIndex, b.firstSeriesIndex+b.seriesCount-1))
	}

	offsetFromFirst := seriesIndex - b.firstSeriesIndex

	return b.data[(b.startIndex+offsetFromFirst)%len(b.data)]
}

func (b *SeriesDataRingBuffer) IsPresent(seriesIndex int) bool {
	return seriesIndex >= b.firstSeriesIndex && seriesIndex < (b.firstSeriesIndex+b.seriesCount)
}

func (b *SeriesDataRingBuffer) Size() int {
	return b.seriesCount
}
