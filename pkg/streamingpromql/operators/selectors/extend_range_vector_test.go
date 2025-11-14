// SPDX-License-Identifier: AGPL-3.0-only

package selectors

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestEmptySet(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.False(t, it.hasNext())
	it.advance()
	it.advance()
	require.False(t, it.hasNext())
}

func TestSinglePoint(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{{T: 0, F: 1}}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.peek())
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.next())
	require.False(t, it.hasNext())
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.prev())
}

func TestDoublePoint(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.peek())
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.next())
	require.Equal(t, promql.FPoint{T: 1, F: 1}, it.peek())
	require.Equal(t, promql.FPoint{T: 1, F: 1}, it.next())
	require.False(t, it.hasNext())
}

func TestAdvance(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}, {T: 3, F: 1}, {T: 4, F: 1}, {T: 5, F: 1}, {T: 6, F: 1}, {T: 7, F: 1}}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	i := 0
	for i < 8 {
		require.Equal(t, promql.FPoint{T: int64(i), F: 1}, it.peek())
		require.Equal(t, promql.FPoint{T: int64(i), F: 1}, it.next())

		if i > 0 {
			require.Equal(t, promql.FPoint{T: int64(i), F: 1}, it.prev())
			it.advance()
		}
		i++
	}
	require.False(t, it.hasNext())
}

func TestSeek(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}, {T: 4, F: 1}, {T: 5, F: 1}, {T: 6, F: 1}, {T: 8, F: 1}, {T: 9, F: 1}}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.seek(-1))
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.next())

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 0, F: 1}, it.seek(0))
	require.Equal(t, promql.FPoint{T: 1, F: 1}, it.next())

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 2, F: 1}, it.seek(2))
	require.Equal(t, promql.FPoint{T: 4, F: 1}, it.next())

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 2, F: 1}, it.seek(3))
	require.Equal(t, promql.FPoint{T: 4, F: 1}, it.next())

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 8, F: 1}, it.seek(8))
	require.Equal(t, promql.FPoint{T: 9, F: 1}, it.next())

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	require.Equal(t, promql.FPoint{T: 9, F: 1}, it.seek(9))
	require.False(t, it.hasNext())
}

func TestCopyRemainingPoints(t *testing.T) {
	floats := types.NewFPointRingBuffer(&limiter.MemoryConsumptionTracker{})
	require.NoError(t, floats.Use([]promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}, {T: 4, F: 1}, {T: 5, F: 1}, {T: 6, F: 1}, {T: 8, F: 1}, {T: 10, F: 1}}))
	it := &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff := make([]promql.FPoint, 0, it.view.Count())
	buff = it.copyRemainingPointsTo(-1, buff)
	last := it.at()

	require.Equal(t, 0, len(buff))
	require.Equal(t, promql.FPoint{T: 0, F: 1}, last)

	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(0, buff)
	last = it.at()
	require.Equal(t, 1, len(buff))
	require.Equal(t, promql.FPoint{T: 0, F: 1}, last)

	buff = buff[:0]
	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(1, buff)
	last = it.at()
	require.Equal(t, 2, len(buff))
	require.Equal(t, promql.FPoint{T: 1, F: 1}, last)

	buff = buff[:0]
	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(3, buff)
	last = it.at()
	require.Equal(t, 3, len(buff))
	require.Equal(t, promql.FPoint{T: 4, F: 1}, last)

	buff = buff[:0]
	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(4, buff)
	last = it.at()
	require.Equal(t, 4, len(buff))
	require.Equal(t, promql.FPoint{T: 4, F: 1}, last)

	buff = buff[:0]
	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(8, buff)
	last = it.at()
	require.Equal(t, 7, len(buff))
	require.Equal(t, promql.FPoint{T: 8, F: 1}, last)

	buff = buff[:0]
	it = &fPointRingBufferViewIterator{view: floats.ViewAll(nil)}
	buff = it.copyRemainingPointsTo(9, buff)
	last = it.at()
	require.Equal(t, 7, len(buff))
	require.Equal(t, promql.FPoint{T: 10, F: 1}, last)
}
