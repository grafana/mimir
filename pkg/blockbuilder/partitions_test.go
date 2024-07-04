// SPDX-License-Identifier: AGPL-3.0-only

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

		for _, want := range []int32{5, 6, 7} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		// The call to next returns -1 after we've exhausted the queue.
		require.Equal(t, int32(-1), ps.next())
	})

	t.Run("replace exhausted", func(t *testing.T) {
		var ps partitions

		ps.update([]int32{1, 2, 3})
		require.Equal(t, []int32{1, 2, 3}, ps.collect())

		for _, want := range []int32{1, 2, 3} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		// The call to next returns -1 after we've exhausted the queue.
		require.Equal(t, int32(-1), ps.next())

		ps.update([]int32{5, 6, 7})
		require.Equal(t, []int32{5, 6, 7}, ps.collect())

		for _, want := range []int32{5, 6, 7} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		// The call to next returns -1 after we've exhausted the queue.
		require.Equal(t, int32(-1), ps.next())
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

		// The call to next returns -1 after we've exhausted the queue.
		require.Equal(t, int32(-1), ps.next())
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

		require.Equal(t, int32(-1), ps.next())
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

		// The queue starts with its last cursor after the update. Because 2 (the cursor) isn't the new list
		// and 1 was processed before 2, the queue starts with 3.
		for _, want := range []int32{3, 5} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		require.Equal(t, int32(-1), ps.next())
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

		// The queue starts with its last cursor after the update. That is 1 is skipped, and the queue starts with 4.
		for _, want := range []int32{4, 0, 2, 3, 5} {
			got := ps.next()
			require.Equal(t, want, got)
		}

		require.Equal(t, int32(-1), ps.next())
	})
}

func TestPartitions_reset(t *testing.T) {
	var ps partitions

	ps.update([]int32{1, 2, 3})
	for i := 0; i < 3; i++ {
		want := int32(i + 1)
		got := ps.next()
		require.Equal(t, want, got)
	}

	require.Equal(t, int32(-1), ps.next())

	ps.reset()

	for i := 0; i < 3; i++ {
		want := int32(i + 1)
		got := ps.next()
		require.Equal(t, want, got)
	}
}
