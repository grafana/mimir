// SPDX-License-Identifier: AGPL-3.0-only

package util

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopK(t *testing.T) {
	higherIsBetter := func(a, b int) int { return cmp.Compare(b, a) }

	for _, limit := range []int{-1, 0, 1, 2, 5, 50, 100} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			items := rand.Perm(50)
			topK := NewTopK(limit, higherIsBetter)
			var dropped []int
			for _, item := range items {
				accepts := topK.Accepts(item)
				droppedItem, isDropped := topK.Push(item)
				if isDropped {
					dropped = append(dropped, droppedItem)
				}
				require.Equal(t, !accepts, isDropped && droppedItem == item, "Push must drop the new item only when Accepts returns false")
			}

			sortedItems := slices.SortedFunc(slices.Values(items), higherIsBetter)
			keptItems := len(sortedItems)
			if limit > 0 {
				keptItems = min(limit, keptItems)
			}
			require.Equal(t, sortedItems[:keptItems], topK.Sorted())
			require.ElementsMatch(t, sortedItems[keptItems:], dropped)
		})
	}

	t.Run("no items", func(t *testing.T) {
		sorted := NewTopK(3, higherIsBetter).Sorted()
		require.NotNil(t, sorted)
		require.Empty(t, sorted)
	})

	t.Run("an item equal to the worst kept item is dropped", func(t *testing.T) {
		topK := NewTopK(2, higherIsBetter)
		topK.Push(3)
		topK.Push(2)
		require.False(t, topK.Accepts(2))
		dropped, isDropped := topK.Push(2)
		require.True(t, isDropped)
		require.Equal(t, 2, dropped)
		require.Equal(t, []int{3, 2}, topK.Sorted())
	})
}
