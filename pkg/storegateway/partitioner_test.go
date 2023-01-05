// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/partitioner_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGapBasedPartitioner_Metrics(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	p := newGapBasedPartitioner(10, reg)

	parts := p.Partition(7, func(i int) (uint64, uint64) {
		switch i {
		case 0:
			return 10, 12 // 2B, extended to next range
		case 1:
			return 15, 18 // 3B, extended to next range
		case 2:
			return 22, 27 // 5B
		case 3:
			return 38, 41 // 3B, extended to next range
		case 4:
			return 50, 52 // 2B, extended to next range
		case 5:
			return 50, 54 // 4B, with 2B overlapping with the previous range
		case 6:
			return 51, 53 // 2B, completely overlapping with the previous range
		default:
			return 0, 0
		}
	})

	expected := []Part{
		{Start: 10, End: 27, ElemRng: [2]int{0, 3}},
		{Start: 38, End: 54, ElemRng: [2]int{3, 7}},
	}
	require.Equal(t, expected, parts)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_bucket_store_partitioner_requested_bytes_total Total size of byte ranges required to fetch from the storage before they are extended by the partitioner.
		# TYPE cortex_bucket_store_partitioner_requested_bytes_total counter
		cortex_bucket_store_partitioner_requested_bytes_total 17

		# HELP cortex_bucket_store_partitioner_requested_ranges_total Total number of byte ranges required to fetch from the storage before they are passed to the partitioner.
		# TYPE cortex_bucket_store_partitioner_requested_ranges_total counter
		cortex_bucket_store_partitioner_requested_ranges_total 7

		# HELP cortex_bucket_store_partitioner_expanded_bytes_total Total size of byte ranges required to fetch from the storage after they are extended by the partitioner.
		# TYPE cortex_bucket_store_partitioner_expanded_bytes_total counter
		cortex_bucket_store_partitioner_expanded_bytes_total 33

		# HELP cortex_bucket_store_partitioner_expanded_ranges_total Total number of byte ranges returned by the partitioner after they've been combined together to reduce the number of bucket API calls.
		# TYPE cortex_bucket_store_partitioner_expanded_ranges_total counter
		cortex_bucket_store_partitioner_expanded_ranges_total 2

		# HELP cortex_bucket_store_partitioner_extended_ranges_total Total number of byte ranges that were not adjacent or overlapping but were joined because they were closer than the configured maximum gap.
		# TYPE cortex_bucket_store_partitioner_extended_ranges_total counter
		cortex_bucket_store_partitioner_extended_ranges_total 3
	`)))
}

func TestGapBasedPartitioner_Partition(t *testing.T) {
	const maxGapSize = 1024 * 512

	for _, c := range []struct {
		input    [][2]int
		expected []Part
	}{
		{
			input:    [][2]int{{1, 10}},
			expected: []Part{{Start: 1, End: 10, ElemRng: [2]int{0, 1}}},
		},
		{
			input:    [][2]int{{1, 2}, {3, 5}, {7, 10}},
			expected: []Part{{Start: 1, End: 10, ElemRng: [2]int{0, 3}}},
		},
		{
			input: [][2]int{
				{1, 2},
				{3, 5},
				{20, 30},
				{maxGapSize + 31, maxGapSize + 32},
			},
			expected: []Part{
				{Start: 1, End: 30, ElemRng: [2]int{0, 3}},
				{Start: maxGapSize + 31, End: maxGapSize + 32, ElemRng: [2]int{3, 4}},
			},
		},
		// Overlapping ranges.
		{
			input: [][2]int{
				{1, 30},
				{1, 4},
				{3, 28},
				{maxGapSize + 31, maxGapSize + 32},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: []Part{
				{Start: 1, End: 30, ElemRng: [2]int{0, 3}},
				{Start: maxGapSize + 31, End: maxGapSize + 40, ElemRng: [2]int{3, 5}},
			},
		},
		{
			input: [][2]int{
				// Mimick AllPostingsKey, where range specified whole range.
				{1, 15},
				{1, maxGapSize + 100},
				{maxGapSize + 31, maxGapSize + 40},
			},
			expected: []Part{{Start: 1, End: maxGapSize + 100, ElemRng: [2]int{0, 3}}},
		},
		// Adjacent ranges
		{
			input: [][2]int{
				{1, 10},
				{10 + maxGapSize, 20 + maxGapSize},
			},
			expected: []Part{{Start: 1, End: maxGapSize + 20, ElemRng: [2]int{0, 2}}},
		},
		// Non-adjacent ranges by one
		{
			input: [][2]int{
				{1, 10},
				{11 + maxGapSize, 20 + maxGapSize},
			},
			expected: []Part{
				{Start: 1, End: 10, ElemRng: [2]int{0, 1}},
				{Start: 11 + maxGapSize, End: 20 + maxGapSize, ElemRng: [2]int{1, 2}},
			},
		},
	} {
		p := newGapBasedPartitioner(maxGapSize, nil)
		res := p.Partition(len(c.input), func(i int) (uint64, uint64) {
			return uint64(c.input[i][0]), uint64(c.input[i][1])
		})
		assert.Equal(t, c.expected, res)
	}
}
