package selectors

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestHeadTailIteratorEmpty(t *testing.T) {
	head := []promql.FPoint{}
	tail := []promql.FPoint{}
	it := &headTailIterator{head: head, tail: tail}
	require.False(t, it.hasNext())
	require.Nil(t, it.next())
	require.Nil(t, it.peek())
	require.Nil(t, it.prev())
	it.inc()
	it.inc()
	require.Nil(t, it.next())
	require.Nil(t, it.peek())
	require.Nil(t, it.prev())
	require.False(t, it.hasNext())
}

func TestHeadTailIteratorSinglePointHead(t *testing.T) {
	head := []promql.FPoint{{T: 0, F: 1}}
	tail := []promql.FPoint{}
	it := &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.peek())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.next())
	require.False(t, it.hasNext())
	require.Nil(t, it.peek())
	require.Nil(t, it.next())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.prev())

	it = &headTailIterator{head: head, tail: tail}
	it.inc()
	require.False(t, it.hasNext())
	require.Nil(t, it.peek())
	require.Nil(t, it.next())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.prev())
}

func TestHeadTailIteratorSinglePointTail(t *testing.T) {
	head := []promql.FPoint{}
	tail := []promql.FPoint{{T: 0, F: 1}}
	it := &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.peek())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.next())
	require.False(t, it.hasNext())
	require.Nil(t, it.peek())
	require.Nil(t, it.next())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.prev())
}

func TestHeadTailIteratorSinglePointHeadTail(t *testing.T) {
	head := []promql.FPoint{{T: 0, F: 1}}
	tail := []promql.FPoint{{T: 1, F: 1}}
	it := &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.peek())
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.next())
	require.Equal(t, &promql.FPoint{T: 1, F: 1}, it.peek())
	require.Equal(t, &promql.FPoint{T: 1, F: 1}, it.next())
	require.Nil(t, it.peek())
	require.Nil(t, it.next())
}

func TestHeadTailIteratorUnbalanced(t *testing.T) {
	head := []promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}}
	tail := []promql.FPoint{{T: 3, F: 1}, {T: 4, F: 1}}
	it := &headTailIterator{head: head, tail: tail}
	i := 0
	for i < 5 {
		require.Equal(t, &promql.FPoint{T: int64(i), F: 1}, it.peek())
		require.Equal(t, &promql.FPoint{T: int64(i), F: 1}, it.next())

		if i > 0 {
			require.Equal(t, &promql.FPoint{T: int64(i), F: 1}, it.prev())
			it.inc()
		}
		i++
	}
	require.Nil(t, it.peek())
	require.Nil(t, it.next())
}

func TestHeadTailIteratorScanTo(t *testing.T) {
	head := []promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}, {T: 4, F: 1}}
	tail := []promql.FPoint{{T: 5, F: 1}, {T: 6, F: 1}, {T: 8, F: 1}}

	it := &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.scanTo(-1))
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.next())

	it = &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, it.scanTo(0))
	require.Equal(t, &promql.FPoint{T: 1, F: 1}, it.next())

	it = &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 2, F: 1}, it.scanTo(2))
	require.Equal(t, &promql.FPoint{T: 4, F: 1}, it.next())

	it = &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 2, F: 1}, it.scanTo(3))
	require.Equal(t, &promql.FPoint{T: 4, F: 1}, it.next())

	it = &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 8, F: 1}, it.scanTo(8))
	require.Nil(t, it.next())

	it = &headTailIterator{head: head, tail: tail}
	require.Equal(t, &promql.FPoint{T: 8, F: 1}, it.scanTo(9))
	require.Nil(t, it.next())
}

func TestAccumulateTo(t *testing.T) {
	head := []promql.FPoint{{T: 0, F: 1}, {T: 1, F: 1}, {T: 2, F: 1}, {T: 4, F: 1}}
	tail := []promql.FPoint{{T: 5, F: 1}, {T: 6, F: 1}, {T: 8, F: 1}}

	it := &headTailIterator{head: head, tail: tail}
	buff := make([]promql.FPoint, 0, len(head)+len(tail))
	buff, last := it.accumulateTo(-1, buff)

	require.Equal(t, 0, len(buff))
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, last)

	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(0, buff)
	require.Equal(t, 1, len(buff))
	require.Equal(t, &promql.FPoint{T: 0, F: 1}, last)

	buff = buff[:0]
	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(1, buff)
	require.Equal(t, 2, len(buff))
	require.Equal(t, &promql.FPoint{T: 1, F: 1}, last)

	buff = buff[:0]
	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(3, buff)
	require.Equal(t, 3, len(buff))
	require.Equal(t, &promql.FPoint{T: 4, F: 1}, last)

	buff = buff[:0]
	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(4, buff)
	require.Equal(t, 4, len(buff))
	require.Equal(t, &promql.FPoint{T: 4, F: 1}, last)

	buff = buff[:0]
	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(8, buff)
	require.Equal(t, 7, len(buff))
	require.Equal(t, &promql.FPoint{T: 8, F: 1}, last)

	buff = buff[:0]
	it = &headTailIterator{head: head, tail: tail}
	buff, last = it.accumulateTo(9, buff)
	require.Equal(t, 7, len(buff))
	require.Equal(t, &promql.FPoint{T: 8, F: 1}, last)
}
