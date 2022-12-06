// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestSeriesChunkRef_Compare(t *testing.T) {
	input := []seriesChunkRef{
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
	}

	expected := []seriesChunkRef{
		{blockID: ulid.MustNew(2, nil), minTime: 1, maxTime: 3},
		{blockID: ulid.MustNew(1, nil), minTime: 1, maxTime: 5},
		{blockID: ulid.MustNew(0, nil), minTime: 2, maxTime: 5},
		{blockID: ulid.MustNew(4, nil), minTime: 3, maxTime: 6},
		{blockID: ulid.MustNew(3, nil), minTime: 4, maxTime: 7},
	}

	sort.Slice(input, func(i, j int) bool {
		return input[i].Compare(input[j]) > 0
	})

	assert.Equal(t, expected, input)
}

func TestSeriesChunkRefsIterator(t *testing.T) {
	c := generateSeriesChunkRef(5)
	series1 := labels.FromStrings(labels.MetricName, "metric_1")
	series2 := labels.FromStrings(labels.MetricName, "metric_2")
	series3 := labels.FromStrings(labels.MetricName, "metric_3")
	series4 := labels.FromStrings(labels.MetricName, "metric_4")

	t.Run("should iterate an empty set", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{},
		})

		require.True(t, it.Done())
		require.Zero(t, it.At())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should iterate a set with some items", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, chunks: []seriesChunkRef{c[2]}},
				{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunks: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should re-initialize the internal state on reset()", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, chunks: []seriesChunkRef{c[2]}},
				{lset: series3, chunks: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunks: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		// Reset.
		it.reset(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunks: []seriesChunkRef{c[3]}},
				{lset: series4, chunks: []seriesChunkRef{c[4]}},
			},
		})

		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunks: []seriesChunkRef{c[3]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series4, chunks: []seriesChunkRef{c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})
}

func TestFlattenedSeriesChunkRefs(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(6)

	testCases := map[string]struct {
		input    seriesChunkRefsSetIterator
		expected []seriesChunkRefs
	}{
		"should iterate on no sets": {
			input:    newSliceSeriesChunkRefsSetIterator(nil),
			expected: nil,
		},
		"should iterate an empty set": {
			input:    newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{}),
			expected: nil,
		},
		"should iterate a set with multiple items": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
			},
		},
		"should iterate multiple sets with multiple items each": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
			},
		},
		"should keep iterating on empty sets": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}},
				seriesChunkRefsSet{}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
			},
		},
	}

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			flattenedIterator := newFlattenedSeriesChunkRefsIterator(testCase.input)
			actual := readAllSeriesChunkRefs(flattenedIterator)
			require.NoError(t, flattenedIterator.Err())
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestMergedSeriesChunkRefsSet(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are merged.
	c := generateSeriesChunkRef(6)

	testCases := map[string]struct {
		batchSize    int
		set1, set2   seriesChunkRefsSetIterator
		expectedSets []seriesChunkRefsSet
		expectedErr  string
	}{
		"merges two sets without overlap": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1], c[2], c[3]}},
				}},
			},
		},
		"merges two sets with last series from each overlapping": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[0], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[0], c[0], c[2], c[3]}},
				}},
			},
		},
		"merges two sets where the first is empty": {
			batchSize: 100,
			set1:      emptySeriesChunkRefsSetIterator{},
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				}},
			},
		},
		"merges two sets with first one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with second one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with shorter one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v3"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v4"), chunks: make([]seriesChunkRef, 1)},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"should stop iterating as soon as the first underlying set returns an error": {
			batchSize: 1, // Use a batch size of 1 in this test so that we can see when the iteration stops.
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[2]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[2]}},
				}},
			},
			expectedErr: "something went wrong",
		},
		"should return merged chunks sorted by min time (assuming source sets have sorted chunks) on first chunk on first set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should return merged chunks sorted by min time (assuming source sets have sorted chunks) on first chunk on second set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3], c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 2)": {
			batchSize: 2,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3], c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}},
			},
		},
	}

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mergedSetIterator := newMergedSeriesChunkRefsSet(testCase.batchSize, testCase.set1, testCase.set2)
			sets := readAllSeriesChunkRefsSet(mergedSetIterator)

			if testCase.expectedErr != "" {
				assert.EqualError(t, mergedSetIterator.Err(), testCase.expectedErr)
			} else {
				assert.NoError(t, mergedSetIterator.Err())
			}

			require.Len(t, sets, len(testCase.expectedSets))
			for setIdx, expectedSet := range testCase.expectedSets {
				require.Len(t, sets[setIdx].series, len(expectedSet.series))
				for expectedSeriesIdx, expectedSeries := range expectedSet.series {
					assert.Equal(t, expectedSeries.lset, sets[setIdx].series[expectedSeriesIdx].lset)
					assert.Equal(t, expectedSeries.chunks, sets[setIdx].series[expectedSeriesIdx].chunks)
				}
			}
		})
	}
}

func TestSeriesSetWithoutChunks(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(6)

	testCases := map[string]struct {
		input    seriesChunkRefsSetIterator
		expected []labels.Labels
	}{
		"should iterate on no sets": {
			input:    newSliceSeriesChunkRefsSetIterator(nil),
			expected: nil,
		},
		"should iterate an empty set": {
			input:    newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{}),
			expected: nil,
		},
		"should iterate a set with multiple items": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}}),
			expected: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
			},
		},
		"should iterate multiple sets with multiple items each": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}}),
			expected: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
				labels.FromStrings("l1", "v3"),
				labels.FromStrings("l1", "v4"),
				labels.FromStrings("l1", "v5"),
			},
		},
		"should keep iterating on empty sets": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunks: []seriesChunkRef{c[5]}},
				}},
				seriesChunkRefsSet{}),
			expected: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
				labels.FromStrings("l1", "v3"),
				labels.FromStrings("l1", "v4"),
				labels.FromStrings("l1", "v5"),
			},
		},
	}

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			chainedSet := newSeriesSetWithoutChunks(testCase.input)
			actual := readAllSeriesLabels(chainedSet)
			require.NoError(t, chainedSet.Err())
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestDeduplicatingSeriesChunkRefsSetIterator(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(8)

	series1 := labels.FromStrings("l1", "v1")
	series2 := labels.FromStrings("l1", "v2")
	series3 := labels.FromStrings("l1", "v3")
	sourceSets := []seriesChunkRefsSet{
		{series: []seriesChunkRefs{
			{lset: series1, chunks: []seriesChunkRef{c[0], c[1]}},
			{lset: series1, chunks: []seriesChunkRef{c[2], c[3], c[4]}},
		}},
		{series: []seriesChunkRefs{
			{lset: series2, chunks: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
			{lset: series3, chunks: []seriesChunkRef{c[0]}},
			{lset: series3, chunks: []seriesChunkRef{c[1]}},
		}},
	}

	t.Run("batch size: 1", func(t *testing.T) {
		repeatingIterator := newSliceSeriesChunkRefsSetIterator(nil, sourceSets...)
		deduplicatingIterator := newDeduplicatingSeriesChunkRefsSetIterator(1, repeatingIterator)
		sets := readAllSeriesChunkRefsSet(deduplicatingIterator)

		require.NoError(t, deduplicatingIterator.Err())
		require.Len(t, sets, 3)

		require.Len(t, sets[0].series, 1)
		assert.Equal(t, series1, sets[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunks)

		require.Len(t, sets[1].series, 1)
		assert.Equal(t, series2, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[1].series[0].chunks)

		require.Len(t, sets[2].series, 1)
		assert.Equal(t, series3, sets[2].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[2].series[0].chunks)
	})

	t.Run("batch size: 2", func(t *testing.T) {
		repeatingIterator := newSliceSeriesChunkRefsSetIterator(nil, sourceSets...)
		duplicatingIterator := newDeduplicatingSeriesChunkRefsSetIterator(2, repeatingIterator)
		sets := readAllSeriesChunkRefsSet(duplicatingIterator)

		require.NoError(t, duplicatingIterator.Err())
		require.Len(t, sets, 2)

		// First batch.
		require.Len(t, sets[0].series, 2)

		assert.Equal(t, series1, sets[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunks)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[0].series[1].chunks)

		// Second batch.
		require.Len(t, sets[1].series, 1)

		assert.Equal(t, series3, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[1].series[0].chunks)
	})

	t.Run("batch size: 3", func(t *testing.T) {
		repeatingIterator := newSliceSeriesChunkRefsSetIterator(nil, sourceSets...)
		deduplciatingIterator := newDeduplicatingSeriesChunkRefsSetIterator(3, repeatingIterator)
		sets := readAllSeriesChunkRefsSet(deduplciatingIterator)

		require.NoError(t, deduplciatingIterator.Err())
		require.Len(t, sets, 1)
		require.Len(t, sets[0].series, 3)

		assert.Equal(t, series1, sets[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunks)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[0].series[1].chunks)

		assert.Equal(t, series3, sets[0].series[2].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[0].series[2].chunks)
	})
}

func TestDeduplicatingSeriesChunkRefsSetIterator_PropagatesErrors(t *testing.T) {
	chainedSet := newDeduplicatingSeriesChunkRefsSetIterator(100, newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
		},
	}))

	for chainedSet.Next() {
	}

	assert.ErrorContains(t, chainedSet.Err(), "something went wrong")
}

func TestLimitingSeriesChunkRefsSetIterator(t *testing.T) {
	testCases := map[string]struct {
		sets                     []seriesChunkRefsSet
		seriesLimit, chunksLimit int
		upstreamErr              error

		expectedSetsCount int
		expectedErr       string
	}{
		"doesn't exceed limits": {
			seriesLimit:       5,
			chunksLimit:       5,
			expectedSetsCount: 1,
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
			},
		},
		"exceeds chunks limit": {
			seriesLimit:       5,
			chunksLimit:       9,
			expectedSetsCount: 0,
			expectedErr:       "exceeded chunks limit",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 2)},
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 3)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 4)},
				}},
			},
		},
		"exceeds chunks limit on second set": {
			seriesLimit:       5,
			chunksLimit:       3,
			expectedSetsCount: 1,
			expectedErr:       "exceeded chunks limit",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
			},
		},
		"exceeds series limit": {
			seriesLimit:       3,
			chunksLimit:       5,
			expectedSetsCount: 0,
			expectedErr:       "exceeded series limit",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
			},
		},
		"exceeds series limit on second set": {
			seriesLimit:       3,
			chunksLimit:       5,
			expectedSetsCount: 1,
			expectedErr:       "exceeded series limit",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
			},
		},
		"propagates error": {
			seriesLimit:       5,
			chunksLimit:       5,
			upstreamErr:       errors.New("something went wrong"),
			expectedSetsCount: 2,
			expectedErr:       "something went wrong",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunks: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunks: make([]seriesChunkRef, 1)},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			iterator := newLimitingSeriesChunkRefsSetIterator(
				newSliceSeriesChunkRefsSetIterator(testCase.upstreamErr, testCase.sets...),
				&limiter{limit: testCase.chunksLimit},
				&limiter{limit: testCase.seriesLimit},
			)

			sets := readAllSeriesChunkRefsSet(iterator)
			assert.Equal(t, testCase.expectedSetsCount, len(sets))
			if testCase.expectedErr == "" {
				assert.NoError(t, iterator.Err())
			} else {
				assert.ErrorContains(t, iterator.Err(), testCase.expectedErr)
			}
		})
	}
}

func TestLoadingSeriesChunkRefsSetIterator(t *testing.T) {
	newTestBlock := prepareTestBlock(test.NewTB(t), func(t testing.TB, appender storage.Appender) {
		for i := 0; i < 100; i++ {
			_, err := appender.Append(0, labels.FromStrings("l1", fmt.Sprintf("v%d", i)), int64(i*10), 0)
			assert.NoError(t, err)
		}
		assert.NoError(t, appender.Commit())
	})

	testCases := map[string]struct {
		shard        *sharding.ShardSelector
		matchers     []*labels.Matcher
		seriesHasher mockSeriesHasher
		skipChunks   bool
		minT, maxT   int64
		batchSize    int

		expectedSets []seriesChunkRefsSet
	}{
		"loads one batch": {
			minT:      0,
			maxT:      10000,
			batchSize: 100,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{{minTime: 10, maxTime: 10, ref: 26}}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{{minTime: 20, maxTime: 20, ref: 234}}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"loads multiple batches": {
			minT:      0,
			maxT:      10000,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{{minTime: 10, maxTime: 10, ref: 26}}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{{minTime: 20, maxTime: 20, ref: 234}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"skips chunks": {
			skipChunks: true,
			minT:       0,
			maxT:       40,
			batchSize:  100,
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
		"doesn't return series if they are outside of minT/maxT": {
			minT:         20,
			maxT:         30,
			batchSize:    100,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v1")},
			expectedSets: []seriesChunkRefsSet{},
		},
		"omits empty batches because they fall outside of minT/maxT": {
			minT:      30,
			maxT:      40,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []seriesChunkRef{{minTime: 40, maxTime: 40, ref: 650}}},
				}},
			},
		},
		"returns no batches when no series are owned by shard": {
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			minT:         0,
			maxT:         40,
			batchSize:    2,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{},
		},
		"returns only series that are owned by shard": {
			seriesHasher: mockSeriesHasher{
				hashes: map[string]uint64{`{l1="v3"}`: 1},
			},
			shard:     &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			minT:      0,
			maxT:      40,
			batchSize: 2,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunks: []seriesChunkRef{{minTime: 30, maxTime: 30, ref: 442}}},
				}},
			},
		},
		"ignores mixT/maxT when skipping chunks": {
			minT:       0,
			maxT:       10,
			skipChunks: true,
			batchSize:  4,
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			// Setup
			block := newTestBlock()
			indexr := block.indexReader()
			postings, err := indexr.ExpandedPostings(context.Background(), testCase.matchers, newSafeQueryStats())
			require.NoError(t, err)
			postingsIterator := newPostingsSetsIterator(
				postings,
				testCase.batchSize,
			)
			loadingIterator := newLoadingSeriesChunkRefsSetIterator(
				context.Background(),
				postingsIterator,
				indexr,
				newSafeQueryStats(),
				block.meta,
				testCase.shard,
				testCase.seriesHasher,
				testCase.skipChunks,
				testCase.minT,
				testCase.maxT,
			)

			// Tests
			sets := readAllSeriesChunkRefsSet(loadingIterator)
			assert.NoError(t, loadingIterator.Err())
			if !assert.Len(t, sets, len(testCase.expectedSets)) {
				return
			}

			for i, actualSet := range sets {
				expectedSet := testCase.expectedSets[i]
				if !assert.Equalf(t, expectedSet.len(), actualSet.len(), "%d", i) {
					continue
				}
				for j, actualSeries := range actualSet.series {
					expectedSeries := expectedSet.series[j]
					assert.Truef(t, labels.Equal(actualSeries.lset, expectedSeries.lset), "%d, %d: expected labels %s got %s", i, j, expectedSeries.lset, actualSeries.lset)
					if !assert.Lenf(t, actualSeries.chunks, len(expectedSeries.chunks), "%d, %d", i, j) {
						continue
					}
					for k, actualChunk := range actualSeries.chunks {
						expectedChunk := expectedSeries.chunks[k]
						assert.Equalf(t, expectedChunk.maxTime, actualChunk.maxTime, "%d, %d, %d", i, j, k)
						assert.Equalf(t, expectedChunk.minTime, actualChunk.minTime, "%d, %d, %d", i, j, k)
						assert.Equalf(t, int(expectedChunk.ref), int(actualChunk.ref), "%d, %d, %d", i, j, k)
						assert.Equalf(t, block.meta.ULID, actualChunk.blockID, "%d, %d, %d", i, j, k)
					}
				}
			}
		})
	}
}

func TestOpenBlockSeriesChunkRefsSetsIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	testCases := map[string]struct {
		matcher        *labels.Matcher
		batchSize      int
		chunksLimit    int
		seriesLimit    int
		expectedErr    string
		expectedSeries []seriesChunkRefsSet
	}{
		"chunks limits reached": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   100,
			chunksLimit: 1,
			seriesLimit: 100,
			expectedErr: "test limit exceeded",
		},
		"series limits reached": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   100,
			chunksLimit: 100,
			seriesLimit: 1,
			expectedErr: "test limit exceeded",
		},
		"selects all series in a single batch": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   100,
			chunksLimit: 100,
			seriesLimit: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
					{lset: labels.FromStrings("a", "1", "b", "2")},
					{lset: labels.FromStrings("a", "2", "b", "1")},
					{lset: labels.FromStrings("a", "2", "b", "2")},
				}},
			},
		},
		"selects all series in multiple batches": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   1,
			chunksLimit: 100,
			seriesLimit: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "1")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "2")},
				}},
			},
		},
		"selects some series in single batch": {
			matcher:     labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize:   100,
			chunksLimit: 100,
			seriesLimit: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
					{lset: labels.FromStrings("a", "1", "b", "2")},
				}},
			},
		},
		"selects some series in multiple batches": {
			matcher:     labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize:   1,
			chunksLimit: 100,
			seriesLimit: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2")},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			suite := prepareStoreWithTestBlocks(t, objstore.NewInMemBucket(), defaultPrepareStoreConfig(t))
			var firstBlock *bucketBlock
			// Find the block with the smallest timestamp in its ULID.
			// The test setup creates two blocks - each takes 4 different timeseries; each has
			// a timestamp of time.Now() when its being created.
			// We want the first created block because we want to assert on the series inside it.
			// The block created first contains a known set of 4 series.
			// TODO dimitarvdimitrov change this setup to use prepareTestBlock() and assert on the chunks too
			for _, b := range suite.store.blocks {
				if firstBlock == nil {
					firstBlock = b
					continue
				}
				if b.meta.ULID.Time() < firstBlock.meta.ULID.Time() {
					firstBlock = b
				}
			}

			indexReader := firstBlock.indexReader()
			defer indexReader.Close()

			iterator, err := openBlockSeriesChunkRefsSetsIterator(
				ctx,
				testCase.batchSize,
				indexReader,
				firstBlock.meta,
				[]*labels.Matcher{testCase.matcher},
				nil,
				hashcache.NewSeriesHashCache(1024*1024).GetBlockCache(firstBlock.meta.ULID.String()),
				&limiter{limit: testCase.chunksLimit},
				&limiter{limit: testCase.seriesLimit},
				false,
				firstBlock.meta.MinTime,
				firstBlock.meta.MaxTime,
				newSafeQueryStats(),
				log.NewNopLogger(),
			)
			require.NoError(t, err)

			actualSeriesSets := readAllSeriesChunkRefsSet(iterator)

			require.Lenf(t, actualSeriesSets, len(testCase.expectedSeries), "expected %d sets, but got %d", len(testCase.expectedSeries), len(actualSeriesSets))
			for i, actualSeriesSet := range actualSeriesSets {
				expectedSeriesSet := testCase.expectedSeries[i]
				require.Equal(t, expectedSeriesSet.len(), actualSeriesSet.len())
				for j, actualSeries := range actualSeriesSet.series {
					expectedSeries := testCase.expectedSeries[i].series[j]

					actualLset := actualSeries.lset
					expectedLset := expectedSeries.lset
					assert.Truef(t, labels.Equal(actualLset, expectedLset), "%d, %d: expected labels %s got labels %s", i, j, expectedLset, actualLset)

					// We can't test anything else from the chunk ref because it is generated on the go in each test case
					assert.Len(t, actualSeries.chunks, 1)
					assert.Equal(t, firstBlock.meta.ULID, actualSeries.chunks[0].blockID)
				}
			}
			if testCase.expectedErr != "" {
				assert.ErrorContains(t, iterator.Err(), "test limit exceeded")
			} else {
				assert.NoError(t, iterator.Err())
			}
		})
	}
}

func TestPostingsSetsIterator(t *testing.T) {
	testCases := map[string]struct {
		postings        []storage.SeriesRef
		batchSize       int
		expectedBatches [][]storage.SeriesRef
	}{
		"single series": {
			postings:        []storage.SeriesRef{1},
			batchSize:       3,
			expectedBatches: [][]storage.SeriesRef{{1}},
		},
		"single batch": {
			postings:        []storage.SeriesRef{1, 2, 3},
			batchSize:       3,
			expectedBatches: [][]storage.SeriesRef{{1, 2, 3}},
		},
		"two batches, evenly split": {
			postings:        []storage.SeriesRef{1, 2, 3, 4},
			batchSize:       2,
			expectedBatches: [][]storage.SeriesRef{{1, 2}, {3, 4}},
		},
		"two batches, last not full": {
			postings:        []storage.SeriesRef{1, 2, 3, 4, 5},
			batchSize:       3,
			expectedBatches: [][]storage.SeriesRef{{1, 2, 3}, {4, 5}},
		},
		"empty postings": {
			postings:        []storage.SeriesRef{},
			batchSize:       2,
			expectedBatches: [][]storage.SeriesRef{},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			iterator := newPostingsSetsIterator(testCase.postings, testCase.batchSize)

			var actualBatches [][]storage.SeriesRef
			for iterator.Next() {
				actualBatches = append(actualBatches, iterator.At())
			}

			assert.ElementsMatch(t, testCase.expectedBatches, actualBatches)
		})
	}
}

type mockSeriesHasher struct {
	hashes map[string]uint64
}

func (a mockSeriesHasher) Hash(seriesID storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64 {
	return a.hashes[lset.String()]
}

// sliceSeriesChunkRefsSetIterator implements seriesChunkRefsSetIterator and
// returns the provided err when the sets are exhausted.
//

type sliceSeriesChunkRefsSetIterator struct {
	current int
	sets    []seriesChunkRefsSet
	err     error
}

func newSliceSeriesChunkRefsSetIterator(err error, sets ...seriesChunkRefsSet) seriesChunkRefsSetIterator {
	return &sliceSeriesChunkRefsSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
	}
}

func (s *sliceSeriesChunkRefsSetIterator) Next() bool {
	s.current++
	return s.current < len(s.sets)
}

func (s *sliceSeriesChunkRefsSetIterator) At() seriesChunkRefsSet {
	return s.sets[s.current]
}

func (s *sliceSeriesChunkRefsSetIterator) Err() error {
	if s.current >= len(s.sets) {
		return s.err
	}
	return nil
}

type limiter struct {
	limit   int
	current atomic.Uint64
}

func (l *limiter) Reserve(num uint64) error {
	if l.current.Add(num) > uint64(l.limit) {
		return errors.New("test limit exceeded")
	}
	return nil
}

func generateSeriesChunkRef(num int) []seriesChunkRef {
	out := make([]seriesChunkRef, 0, num)

	for i := 0; i < num; i++ {
		out = append(out, seriesChunkRef{
			blockID: ulid.MustNew(uint64(i), nil),
			ref:     chunks.ChunkRef(i),
			minTime: int64(i),
			maxTime: int64(i),
		})
	}

	return out
}

func readAllSeriesChunkRefsSet(it seriesChunkRefsSetIterator) []seriesChunkRefsSet {
	var out []seriesChunkRefsSet
	for it.Next() {
		out = append(out, it.At())
	}
	return out
}

func readAllSeriesChunkRefs(it seriesChunkRefsIterator) []seriesChunkRefs {
	var out []seriesChunkRefs
	for it.Next() {
		out = append(out, it.At())
	}
	return out
}
