// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestSortSeriesRefsOrdersAsSort(t *testing.T) {
	random := rand.New(rand.NewSource(1))
	for _, size := range []int{0, 1, 255, 256, 1000, 100_000} {
		for _, bits := range []uint{8, 20, 40, 64} {
			refs := make([]storage.SeriesRef, size)
			for index := range refs {
				refs[index] = storage.SeriesRef(random.Uint64() >> (64 - bits))
			}
			expected := slices.Clone(refs)
			slices.Sort(expected)
			sortSeriesRefs(refs)
			require.Equal(t, expected, refs, "%d refs of %d bits", size, bits)
		}
	}
}
