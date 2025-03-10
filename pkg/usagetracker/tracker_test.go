// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstancePartitions(t *testing.T) {
	for partitions := int32(2); partitions <= 128; partitions *= 2 {
		t.Run(fmt.Sprintf("partitions=%d", partitions), func(t *testing.T) {
			for instances := int32(1); instances <= partitions; instances++ {
				t.Run(fmt.Sprintf("instances=%d", instances), func(t *testing.T) {
					type rng struct {
						start, end int32
					}
					var ranges []rng
					l := int32(0)
					minSize := int32(math.MaxInt32)
					maxSize := int32(0)
					for i := int32(0); i < instances; i++ {
						start, end, err := instancePartitions(i, instances, partitions)
						require.NoError(t, err)
						ranges = append(ranges, rng{start, end})
						require.True(t, start <= end, "instance=%d, start=%d end=%d", i, start, end)
						require.True(t, start >= 0, "instance=%d, start=%d end=%d", i, start, end)
						require.True(t, end <= partitions, "instance=%d, start=%d end=%d", i, start, end)
						size := end - start
						minSize = min(minSize, size)
						maxSize = max(maxSize, size)
						l += size
					}
					// Sort the ranges we got by starting partition.
					slices.SortFunc(ranges, func(a, b rng) int { return int(a.start - b.start) })
					// First range should start at 0.
					require.Equal(t, int32(0), ranges[0].start, "ranges=%v", ranges)
					// Last range should end at partitions.
					require.Equal(t, partitions, ranges[len(ranges)-1].end, "ranges=%v", ranges)
					for i := 1; i < len(ranges); i++ {
						// Each range should end where the next one starts.
						require.Equal(t, ranges[i-1].end, ranges[i].start, "ranges=%v", ranges)
					}
					// We should cover the total of partitions.
					require.Equal(t, l, partitions)
				})
			}
		})
	}
}

func TestParseInstanceID(t *testing.T) {
	t.Run("with zones", func(t *testing.T) {
		actual, err := parseInstanceID("usage-tracker-zone-a-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-zone-b-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-zone-a-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("usage-tracker-zone-b-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("mimir-backend-zone-c-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("without zones", func(t *testing.T) {
		actual, err := parseInstanceID("usage-tracker-0")
		require.NoError(t, err)
		assert.EqualValues(t, 0, actual)

		actual, err = parseInstanceID("usage-tracker-1")
		require.NoError(t, err)
		assert.EqualValues(t, 1, actual)

		actual, err = parseInstanceID("mimir-backend-2")
		require.NoError(t, err)
		assert.EqualValues(t, 2, actual)
	})

	t.Run("should return error if the instance ID has a non supported format", func(t *testing.T) {
		_, err := parseInstanceID("unknown")
		require.Error(t, err)

		_, err = parseInstanceID("usage-tracker-zone-a-")
		require.Error(t, err)

		_, err = parseInstanceID("usage-tracker-zone-a")
		require.Error(t, err)
	})
}
