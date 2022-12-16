// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func init() {
	// Track the balance of gets/puts to seriesChunkRefsSetPool in all tests.
	seriesChunkRefsSetPool = &pool.TrackedPool{Parent: seriesChunkRefsSetPool}
}

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
	c := generateSeriesChunkRef(ulid.MustNew(1, nil), 5)
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
	c := generateSeriesChunkRef(ulid.MustNew(1, nil), 6)

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
	c := generateSeriesChunkRef(ulid.MustNew(1, nil), 6)

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

func TestMergedSeriesChunkRefsSet_Concurrency(t *testing.T) {
	const (
		concurrency        = 10
		runs               = 100
		minIterators       = 2
		maxIterators       = 20
		minSetsPerIterator = 2
		maxSetsPerIterator = 10
		minSeriesPerSet    = 10
		maxSeriesPerSet    = 50
	)

	runTest := func() {
		// Randomize the test setup.
		var (
			numIterators       = minIterators + rand.Intn(maxIterators-minIterators)
			numSetsPerIterator = minSetsPerIterator + rand.Intn(maxSetsPerIterator-minSetsPerIterator)
			numSeriesPerSet    = minSeriesPerSet + rand.Intn(maxSeriesPerSet-minSeriesPerSet)
		)

		// Create the iterators.
		iterators := make([]seriesChunkRefsSetIterator, 0, numIterators)
		for iteratorIdx := 0; iteratorIdx < numIterators; iteratorIdx++ {
			// Create the sets for this iterator.
			sets := make([]seriesChunkRefsSet, 0, numSetsPerIterator)
			for setIdx := 0; setIdx < numSetsPerIterator; setIdx++ {
				minSeriesID := (iteratorIdx * numSetsPerIterator * numSeriesPerSet) + (setIdx * numSeriesPerSet)
				maxSeriesID := minSeriesID + numSeriesPerSet - 1
				sets = append(sets, createSeriesChunkRefsSet(minSeriesID, maxSeriesID, true))
			}

			iterators = append(iterators, newSliceSeriesChunkRefsSetIterator(nil, sets...))
		}

		// Run the actual test.
		it := mergedSeriesChunkRefsSetIterators(50, iterators...)

		actualSeries := 0
		for it.Next() {
			set := it.At()
			actualSeries += len(set.series)
			set.release()
		}

		require.NoError(t, it.Err())
		require.Equal(t, numIterators*numSetsPerIterator*numSeriesPerSet, actualSeries)
	}

	// Reset the memory pool tracker.
	seriesChunkRefsSetPool.(*pool.TrackedPool).Reset()

	g, _ := errgroup.WithContext(context.Background())
	for c := 0; c < concurrency; c++ {
		g.Go(func() error {
			for r := 0; r < runs/concurrency; r++ {
				runTest()
			}
			return nil
		})
	}

	require.NoError(t, g.Wait())

	// Ensure the seriesChunkRefsSet memory pool has been used and all slices pulled from
	// pool have put back.
	assert.Greater(t, seriesChunkRefsSetPool.(*pool.TrackedPool).Gets.Load(), int64(0))
	assert.Equal(t, int64(0), seriesChunkRefsSetPool.(*pool.TrackedPool).Balance.Load())
}

func BenchmarkMergedSeriesChunkRefsSetIterators(b *testing.B) {
	const (
		numSetsPerIterator = 10
		numSeriesPerSet    = 10
		mergedBatchSize    = 5000
	)

	// Creates the series sets, guaranteeing series sorted by labels.
	createUnreleasableSets := func(iteratorIdx int) []seriesChunkRefsSet {
		sets := make([]seriesChunkRefsSet, 0, numSetsPerIterator)
		for setIdx := 0; setIdx < numSetsPerIterator; setIdx++ {
			minSeriesID := (iteratorIdx * numSetsPerIterator * numSeriesPerSet) + (setIdx * numSeriesPerSet)
			maxSeriesID := minSeriesID + numSeriesPerSet - 1

			// This set cannot be released because reused between multiple benchmark runs.
			set := createSeriesChunkRefsSet(minSeriesID, maxSeriesID, false)
			sets = append(sets, set)
		}

		return sets
	}

	for _, withDuplicatedSeries := range []bool{true, false} {
		for numIterators := 1; numIterators <= 64; numIterators *= 2 {
			// Create empty iterators that we can reuse in each benchmark run.
			iterators := make([]seriesChunkRefsSetIterator, 0, numIterators)
			for i := 0; i < numIterators; i++ {
				iterators = append(iterators, newSliceSeriesChunkRefsSetIterator(nil))
			}

			// Create the sets for each underlying iterator. These sets cannot be released because
			// will be used in multiple benchmark runs.
			perIteratorSets := make([][]seriesChunkRefsSet, 0, numIterators)
			for iteratorIdx := 0; iteratorIdx < numIterators; iteratorIdx++ {
				if withDuplicatedSeries {
					perIteratorSets = append(perIteratorSets, createUnreleasableSets(0))
				} else {
					perIteratorSets = append(perIteratorSets, createUnreleasableSets(iteratorIdx))
				}
			}

			b.Run(fmt.Sprintf("with duplicated series = %t number of iterators = %d", withDuplicatedSeries, numIterators), func(b *testing.B) {
				for n := 0; n < b.N; n++ {
					// Reset iterators.
					for i := 0; i < numIterators; i++ {
						iterators[i].(*sliceSeriesChunkRefsSetIterator).reset(perIteratorSets[i])
					}

					// Merge the iterators and run through them.
					it := mergedSeriesChunkRefsSetIterators(mergedBatchSize, iterators...)

					actualSeries := 0
					for it.Next() {
						set := it.At()
						actualSeries += len(set.series)

						set.release()
					}

					if err := it.Err(); err != nil {
						b.Fatal(it.Err())
					}

					// Ensure each benchmark run go through the same data set.
					var expectedSeries int
					if withDuplicatedSeries {
						expectedSeries = numSetsPerIterator * numSeriesPerSet
					} else {
						expectedSeries = numIterators * numSetsPerIterator * numSeriesPerSet
					}

					if actualSeries != expectedSeries {
						b.Fatalf("benchmark iterated through an unexpected number of series (expected: %d got: %d)", expectedSeries, actualSeries)
					}
				}
			})
		}
	}
}

func TestSeriesSetWithoutChunks(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(ulid.MustNew(1, nil), 6)

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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			chainedSet := newSeriesSetWithoutChunks(ctx, testCase.input)
			actual := readAllSeriesLabels(chainedSet)
			require.NoError(t, chainedSet.Err())
			assert.Equal(t, testCase.expected, actual)
		})
	}
}

func TestDeduplicatingSeriesChunkRefsSetIterator(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(ulid.MustNew(1, nil), 8)

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
		seriesHasher seriesHasher
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
			shard: &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			seriesHasher: mockSeriesHasher{
				hashes: map[string]uint64{
					`{l1="v1"}`: 0,
					`{l1="v2"}`: 0,
					`{l1="v3"}`: 0,
					`{l1="v4"}`: 0,
				},
			},
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
			hasher := testCase.seriesHasher
			if hasher == nil {
				hasher = cachedSeriesHasher{hashcache.NewSeriesHashCache(100).GetBlockCache("")}
			}
			loadingIterator := newLoadingSeriesChunkRefsSetIterator(
				context.Background(),
				postingsIterator,
				indexr,
				noopCache{},
				newSafeQueryStats(),
				block.meta,
				testCase.matchers,
				testCase.shard,
				hasher,
				testCase.skipChunks,
				testCase.minT,
				testCase.maxT,
				"t1",
				log.NewNopLogger(),
			)

			// Tests
			sets := readAllSeriesChunkRefsSet(loadingIterator)
			assert.NoError(t, loadingIterator.Err())
			if !assert.Len(t, sets, len(testCase.expectedSets), testName) {
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

	newTestBlock := prepareTestBlock(test.NewTB(t), func(tb testing.TB, appender storage.Appender) {
		earlySeries := []labels.Labels{
			labels.FromStrings("a", "1", "b", "1"),
			labels.FromStrings("a", "1", "b", "2"),
		}

		// Series with samples that start later, so we can expect their chunks' minT/maxT to be different
		lateSeries := []labels.Labels{
			labels.FromStrings("a", "2", "b", "1"),
			labels.FromStrings("a", "2", "b", "2"),
		}

		const numSamples = 200
		for i := int64(0); i < numSamples; i++ { // write 200 samples, so we get two chunks
			for _, s := range earlySeries {
				_, err := appender.Append(0, s, i, 0)
				assert.NoError(t, err)
			}
			for _, s := range lateSeries {
				_, err := appender.Append(0, s, numSamples+i, 0)
				assert.NoError(t, err)
			}
		}
		assert.NoError(t, appender.Commit())
	})

	testCases := map[string]struct {
		matcher     *labels.Matcher
		batchSize   int
		chunksLimit int
		seriesLimit int
		skipChunks  bool

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
					{lset: labels.FromStrings("a", "1", "b", "1"), chunks: []seriesChunkRef{{ref: 8, minTime: 0, maxTime: 124}, {ref: 57, minTime: 125, maxTime: 199}}},
					{lset: labels.FromStrings("a", "1", "b", "2"), chunks: []seriesChunkRef{{ref: 95, minTime: 0, maxTime: 124}, {ref: 144, minTime: 125, maxTime: 199}}},
					{lset: labels.FromStrings("a", "2", "b", "1"), chunks: []seriesChunkRef{{ref: 182, minTime: 200, maxTime: 332}, {ref: 234, minTime: 333, maxTime: 399}}},
					{lset: labels.FromStrings("a", "2", "b", "2"), chunks: []seriesChunkRef{{ref: 270, minTime: 200, maxTime: 332}, {ref: 322, minTime: 333, maxTime: 399}}},
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
					{lset: labels.FromStrings("a", "1", "b", "1"), chunks: []seriesChunkRef{{ref: 8, minTime: 0, maxTime: 124}, {ref: 57, minTime: 125, maxTime: 199}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2"), chunks: []seriesChunkRef{{ref: 95, minTime: 0, maxTime: 124}, {ref: 144, minTime: 125, maxTime: 199}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "1"), chunks: []seriesChunkRef{{ref: 182, minTime: 200, maxTime: 332}, {ref: 234, minTime: 333, maxTime: 399}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "2"), chunks: []seriesChunkRef{{ref: 270, minTime: 200, maxTime: 332}, {ref: 322, minTime: 333, maxTime: 399}}},
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
					{lset: labels.FromStrings("a", "1", "b", "1"), chunks: []seriesChunkRef{{ref: 8, minTime: 0, maxTime: 124}, {ref: 57, minTime: 125, maxTime: 199}}},
					{lset: labels.FromStrings("a", "1", "b", "2"), chunks: []seriesChunkRef{{ref: 95, minTime: 0, maxTime: 124}, {ref: 144, minTime: 125, maxTime: 199}}},
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
					{lset: labels.FromStrings("a", "1", "b", "1"), chunks: []seriesChunkRef{{ref: 8, minTime: 0, maxTime: 124}, {ref: 57, minTime: 125, maxTime: 199}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2"), chunks: []seriesChunkRef{{ref: 95, minTime: 0, maxTime: 124}, {ref: 144, minTime: 125, maxTime: 199}}},
				}},
			},
		},
		"selects all series in a single batch with skipChunks": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   100,
			skipChunks:  true,
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
		"selects all series in multiple batches with skipChunks": {
			matcher:     labels.MustNewMatcher(labels.MatchRegexp, "a", ".+"),
			batchSize:   1,
			skipChunks:  true,
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
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var block = newTestBlock()
			indexReader := block.indexReader()
			defer indexReader.Close()

			hashCache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache(block.meta.ULID.String())

			iterator, err := openBlockSeriesChunkRefsSetsIterator(
				ctx,
				testCase.batchSize,
				"",
				indexReader,
				newInMemoryIndexCache(t),
				block.meta,
				[]*labels.Matcher{testCase.matcher},
				nil,
				cachedSeriesHasher{hashCache},
				&limiter{limit: testCase.chunksLimit},
				&limiter{limit: testCase.seriesLimit},
				testCase.skipChunks,
				block.meta.MinTime,
				block.meta.MaxTime,
				newSafeQueryStats(),
				NewBucketStoreMetrics(prometheus.NewRegistry()),
				nil,
			)
			require.NoError(t, err)

			actualSeriesSets := readAllSeriesChunkRefsSet(iterator)

			require.Lenf(t, actualSeriesSets, len(testCase.expectedSeries), "expected %d sets, but got %d", len(testCase.expectedSeries), len(actualSeriesSets))
			for i, actualSeriesSet := range actualSeriesSets {
				expectedSeriesSet := testCase.expectedSeries[i]
				require.Equal(t, expectedSeriesSet.len(), actualSeriesSet.len(), i)
				for j, actualSeries := range actualSeriesSet.series {
					expectedSeries := testCase.expectedSeries[i].series[j]

					actualLset := actualSeries.lset
					expectedLset := expectedSeries.lset
					assert.Truef(t, labels.Equal(actualLset, expectedLset), "%d, %d: expected labels %s got labels %s", i, j, expectedLset, actualLset)

					require.Lenf(t, actualSeries.chunks, len(expectedSeries.chunks), "%d, %d", i, j)
					for k, actualChunk := range actualSeries.chunks {
						expectedChunk := expectedSeries.chunks[k]
						assert.Equalf(t, block.meta.ULID, actualChunk.blockID, "%d, %d, %d", i, j, k)
						assert.Equalf(t, int(expectedChunk.ref), int(actualChunk.ref), "%d, %d, %d", i, j, k)
						assert.Equalf(t, expectedChunk.minTime, actualChunk.minTime, "%d, %d, %d", i, j, k)
						assert.Equalf(t, expectedChunk.maxTime, actualChunk.maxTime, "%d, %d, %d", i, j, k)
					}
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

// TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching currently tests logic in loadingSeriesChunkRefsSetIterator.
// If openBlockSeriesChunkRefsSetsIterator becomes more complex, consider making this a test for loadingSeriesChunkRefsSetIterator only.
func TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching(t *testing.T) {
	newTestBlock := prepareTestBlock(test.NewTB(t), func(tb testing.TB, appender storage.Appender) {
		existingSeries := []labels.Labels{
			labels.FromStrings("a", "1", "b", "1"), // series ref 32
			labels.FromStrings("a", "1", "b", "2"), // series ref 48
			labels.FromStrings("a", "2", "b", "1"), // series ref 64
			labels.FromStrings("a", "2", "b", "2"), // series ref 80
			labels.FromStrings("a", "3", "b", "1"), // series ref 96
			labels.FromStrings("a", "3", "b", "2"), // series ref 112
		}
		for ts := int64(0); ts < 10; ts++ {
			for _, s := range existingSeries {
				_, err := appender.Append(0, s, ts, 0)
				assert.NoError(t, err)
			}
		}
		assert.NoError(t, appender.Commit())
	})

	mockedSeriesHashes := map[string]uint64{
		`{a="1", b="1"}`: 1,
		`{a="1", b="2"}`: 0,
		`{a="2", b="1"}`: 1,
		`{a="2", b="2"}`: 0,
		`{a="3", b="1"}`: 1,
		`{a="3", b="2"}`: 0,
	}

	testCases := map[string]struct {
		batchSizes []int
		shard      *sharding.ShardSelector
		matchers   []*labels.Matcher

		cachedSeriesHashesWithColdCache map[storage.SeriesRef]uint64
		cachedSeriesHashesWithWarmCache map[storage.SeriesRef]uint64

		expectedLabelSets                        []labels.Labels
		expectedSeriesReadFromBlockWithColdCache int
		expectedSeriesReadFromBlockWithWarmCache int
	}{
		"without sharding, without series hash caches": {
			matchers:                                 []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			batchSizes:                               []int{1, 2, 3, 4, 5, 6, 7},
			expectedSeriesReadFromBlockWithColdCache: 6,
			expectedSeriesReadFromBlockWithWarmCache: 0,
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "2", "b", "2"),
				labels.FromStrings("a", "3", "b", "1"),
				labels.FromStrings("a", "3", "b", "2"),
			},
		},
		"without sharding; series hash isn't looked at when request isn't sharded": {
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			batchSizes: []int{1, 2, 3, 4, 5, 6, 7},
			cachedSeriesHashesWithWarmCache: map[storage.SeriesRef]uint64{ // have some bogus series hash cache
				32:  1,
				48:  2,
				64:  3,
				80:  4,
				96:  5,
				112: 6,
			},
			expectedSeriesReadFromBlockWithColdCache: 6,
			expectedSeriesReadFromBlockWithWarmCache: 0,
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "1", "b", "2"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "2", "b", "2"),
				labels.FromStrings("a", "3", "b", "1"),
				labels.FromStrings("a", "3", "b", "2"),
			},
		},
		"with sharding, cached series hashes": {
			batchSizes: []int{1, 2, 3, 4},
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			shard:      &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cachedSeriesHashesWithColdCache: map[storage.SeriesRef]uint64{
				32:  1,
				48:  0,
				64:  1,
				80:  0,
				96:  1,
				112: 0,
			},
			cachedSeriesHashesWithWarmCache: map[storage.SeriesRef]uint64{
				32:  1,
				48:  0,
				64:  1,
				80:  0,
				96:  1,
				112: 0,
			},
			expectedSeriesReadFromBlockWithColdCache: 3, // we omit reading the block for series when know are outside of our shard
			expectedSeriesReadFromBlockWithWarmCache: 0,
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "3", "b", "1"),
			},
		},
		"with sharding without series hash cache": {
			batchSizes:                               []int{1, 2, 3, 4},
			matchers:                                 []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			shard:                                    &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cachedSeriesHashesWithColdCache:          map[storage.SeriesRef]uint64{},
			expectedSeriesReadFromBlockWithColdCache: 6,
			expectedSeriesReadFromBlockWithWarmCache: 0,
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "3", "b", "1"),
			},
		},
		// This case simulates having different series hash caches on different store-gateway replicas.
		// Having a batch size of 1 means that some batches are entirely of series what don't fall in our shard.
		// The series in such batches may end up being read from the block and then discarded.
		"with sharding, with different series hash caches before and after caching, whole batches with unowned series": {
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			batchSizes: []int{1},
			shard:      &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cachedSeriesHashesWithColdCache: map[storage.SeriesRef]uint64{
				32: 1,
				48: 0,
				64: 1,
			},
			cachedSeriesHashesWithWarmCache: map[storage.SeriesRef]uint64{
				80:  0,
				96:  1,
				112: 0,
			},
			expectedSeriesReadFromBlockWithColdCache: 3 + 2, // 2 reads for the series on which we miss the hash cache, and 3 for series in our shard
			expectedSeriesReadFromBlockWithWarmCache: 1,     // 1 read for the series on which we miss the hash cache and are not in our shard (so we also didn't cache them last time)
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "3", "b", "1"),
			},
		},
		// This case simulates having different series hash caches on different store-gateway replicas.
		// With batch size 2 at least one of the series in each batch is in our shard.
		// This causes us to cache the result of the batch and not have to refresh the series even though we miss on the series hash cache.
		"with sharding, with different series hash caches before and after caching, batches with some non-owned series": {
			matchers:   []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "a", ".+")},
			batchSizes: []int{2},
			shard:      &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			cachedSeriesHashesWithColdCache: map[storage.SeriesRef]uint64{
				32: 1,
				48: 0,
				64: 1,
			},
			cachedSeriesHashesWithWarmCache: map[storage.SeriesRef]uint64{
				80:  0,
				96:  1,
				112: 0,
			},
			expectedSeriesReadFromBlockWithColdCache: 3 + 2, // 2 reads for the series on which we miss the hash cache, and 3 for series in our shard
			expectedSeriesReadFromBlockWithWarmCache: 0,     // at least one series in each batch was in our shard, so we should've cached everything
			expectedLabelSets: []labels.Labels{
				labels.FromStrings("a", "1", "b", "1"),
				labels.FromStrings("a", "2", "b", "1"),
				labels.FromStrings("a", "3", "b", "1"),
			},
		},
	}

	for testName, testCase := range testCases {
		testCase := testCase
		t.Run(testName, func(t *testing.T) {
			for _, batchSize := range testCase.batchSizes {
				batchSize := batchSize
				t.Run(fmt.Sprintf("batch size %d", batchSize), func(t *testing.T) {
					b := newTestBlock()
					b.indexCache = newInMemoryIndexCache(t)

					// Run 1 with a cold cache
					seriesHasher := mockSeriesHasher{
						hashes: mockedSeriesHashes,
						cached: testCase.cachedSeriesHashesWithColdCache,
					}

					statsColdCache := newSafeQueryStats()
					indexReader := b.indexReader()
					ss, err := openBlockSeriesChunkRefsSetsIterator(
						context.Background(),
						batchSize,
						"",
						indexReader,
						b.indexCache,
						b.meta,
						testCase.matchers,
						testCase.shard,
						seriesHasher,
						&limiter{limit: 1000},
						&limiter{limit: 1000},
						true,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsColdCache,
						NewBucketStoreMetrics(nil),
						log.NewNopLogger(),
					)

					require.NoError(t, err)
					lset := extractLabelsFromSeriesChunkRefsSets(readAllSeriesChunkRefsSet(ss))
					require.NoError(t, ss.Err())
					assert.Equal(t, testCase.expectedLabelSets, lset)
					assert.Equal(t, testCase.expectedSeriesReadFromBlockWithColdCache, statsColdCache.export().seriesFetched)

					// Run 2 with a warm cache
					seriesHasher = mockSeriesHasher{
						hashes: mockedSeriesHashes,
						cached: testCase.cachedSeriesHashesWithWarmCache,
					}

					statsWarnCache := newSafeQueryStats()
					ss, err = openBlockSeriesChunkRefsSetsIterator(
						context.Background(),
						batchSize,
						"",
						indexReader,
						b.indexCache,
						b.meta,
						testCase.matchers,
						testCase.shard,
						seriesHasher,
						&limiter{limit: 1000},
						&limiter{limit: 1000},
						true,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsWarnCache,
						NewBucketStoreMetrics(nil),
						log.NewNopLogger(),
					)
					require.NoError(t, err)
					lset = extractLabelsFromSeriesChunkRefsSets(readAllSeriesChunkRefsSet(ss))
					require.NoError(t, ss.Err())
					assert.Equal(t, testCase.expectedLabelSets, lset)
					assert.Equal(t, testCase.expectedSeriesReadFromBlockWithWarmCache, statsWarnCache.export().seriesFetched)
				})
			}
		})
	}
}

type forbiddenFetchMultiSeriesForRefsIndexCache struct {
	indexcache.IndexCache

	t *testing.T
}

func (c forbiddenFetchMultiSeriesForRefsIndexCache) FetchMultiSeriesForRefs(ctx context.Context, userID string, blockID ulid.ULID, ids []storage.SeriesRef) (hits map[storage.SeriesRef][]byte, misses []storage.SeriesRef) {
	assert.Fail(c.t, "index cache FetchMultiSeriesForRefs should not be called")
	return nil, nil
}

func extractLabelsFromSeriesChunkRefsSets(sets []seriesChunkRefsSet) (result []labels.Labels) {
	for _, set := range sets {
		for _, series := range set.series {
			result = append(result, series.lset)
		}
	}
	return
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
	cached map[storage.SeriesRef]uint64
	hashes map[string]uint64
}

func (a mockSeriesHasher) CachedHash(seriesID storage.SeriesRef, stats *queryStats) (uint64, bool) {
	hash, isCached := a.cached[seriesID]
	return hash, isCached
}

func (a mockSeriesHasher) Hash(seriesID storage.SeriesRef, lset labels.Labels, stats *queryStats) uint64 {
	return a.hashes[lset.String()]
}

// sliceSeriesChunkRefsSetIterator implements seriesChunkRefsSetIterator and
// returns the provided err when the sets are exhausted.
type sliceSeriesChunkRefsSetIterator struct {
	current int
	sets    []seriesChunkRefsSet
	err     error
}

func newSliceSeriesChunkRefsSetIterator(err error, sets ...seriesChunkRefsSet) *sliceSeriesChunkRefsSetIterator {
	s := &sliceSeriesChunkRefsSetIterator{err: err}
	s.reset(sets)
	return s
}

func (s *sliceSeriesChunkRefsSetIterator) reset(sets []seriesChunkRefsSet) {
	s.current = -1
	s.sets = sets
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

func generateSeriesChunkRef(blockID ulid.ULID, num int) []seriesChunkRef {
	out := make([]seriesChunkRef, 0, num)

	for i := 0; i < num; i++ {
		out = append(out, seriesChunkRef{
			blockID: blockID,
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

// createSeriesChunkRefsSet creates a seriesChunkRefsSet with series whose name is generated
// based on the provided minSeriesID and maxSeriesID (both inclusive). Each series has the ID
// incremented by +1.
func createSeriesChunkRefsSet(minSeriesID, maxSeriesID int, releasable bool) seriesChunkRefsSet {
	set := newSeriesChunkRefsSet(maxSeriesID-minSeriesID+1, releasable)

	for seriesID := minSeriesID; seriesID <= maxSeriesID; seriesID++ {
		set.series = append(set.series, seriesChunkRefs{
			lset: labels.FromStrings(labels.MetricName, fmt.Sprintf("metric_%06d", seriesID)),
		})
	}

	return set
}

func TestCreateSeriesChunkRefsSet(t *testing.T) {
	set := createSeriesChunkRefsSet(5, 7, true)
	require.Len(t, set.series, 3)
	assert.Equal(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000005")}, set.series[0])
	assert.Equal(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000006")}, set.series[1])
	assert.Equal(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000007")}, set.series[2])
}
