package blockbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	t.Run("replace", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 2, 3})
		require.Equal(t, []int32{1, 2, 3}, ps.collect())

		for _, want := range []int32{1, 2} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		ps.update([]int32{5, 6, 7})
		require.Equal(t, []int32{5, 6, 7}, ps.collect())

		got := ps.next()
		require.Equal(t, int32(5), got)
	})

	t.Run("replace with self", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 2, 3})
		require.Equal(t, []int32{1, 2, 3}, ps.collect())

		for _, want := range []int32{1, 2} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		ps.update([]int32{1, 2, 3})
		require.Equal(t, []int32{1, 2, 3}, ps.collect())

		for _, want := range []int32{2, 3} {
			got := ps.next()
			require.Equal(t, want, got)
		}
	})

	t.Run("replace with empty", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 2, 3})
		require.Equal(t, []int32{1, 2, 3}, ps.collect())

		for _, want := range []int32{1, 2} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		ps.update([]int32{})
		require.Equal(t, []int32{}, ps.collect())

		got := ps.next()
		require.Equal(t, int32(-1), got)
	})

	t.Run("shrink", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 2, 3, 4})
		require.Equal(t, []int32{1, 2, 3, 4}, ps.collect())

		for _, want := range []int32{1, 2} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		ps.update([]int32{1, 3, 5})
		require.Equal(t, []int32{1, 3, 5}, ps.collect())

		for _, want := range []int32{1, 3} {
			got := ps.next()
			require.Equal(t, want, got)
		}
	})

	t.Run("expand", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 4, 7})
		require.Equal(t, []int32{1, 4, 7}, ps.collect())

		for _, want := range []int32{1, 4} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		ps.update([]int32{0, 1, 2, 3, 4, 5})
		require.Equal(t, []int32{1, 4, 0, 2, 3, 5}, ps.collect())

		for _, want := range []int32{4, 0} {
			got := ps.next()
			require.Equal(t, want, got)
		}
	})
}

func TestPartitions_reset(t *testing.T) {
	var ps partitions

	ps.update([]int32{1, 2, 3})
	for i := 0; i < 3; i++ {
		got := ps.next()
		require.Positive(t, got)
	}

	require.Equal(t, int32(-1), ps.next())

	ps.reset()

	for i := 0; i < 3; i++ {
		got := ps.next()
		require.Positive(t, got)
	}
}
