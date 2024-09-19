// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/storage/tsdb"
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
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 5)
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
				{lset: series1, refs: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, refs: []seriesChunkRef{c[2]}},
				{lset: series3, refs: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, refs: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, refs: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series3, refs: []seriesChunkRef{c[3], c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should re-initialize the internal state on reset()", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, refs: []seriesChunkRef{c[0], c[1]}},
				{lset: series2, refs: []seriesChunkRef{c[2]}},
				{lset: series3, refs: []seriesChunkRef{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, refs: []seriesChunkRef{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, refs: []seriesChunkRef{c[2]}}, it.At())
		require.False(t, it.Done())

		// Reset.
		it.reset(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, refs: []seriesChunkRef{c[3]}},
				{lset: series4, refs: []seriesChunkRef{c[4]}},
			},
		})

		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, refs: []seriesChunkRef{c[3]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series4, refs: []seriesChunkRef{c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})
}

func TestFlattenedSeriesChunkRefs(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 6)

	testCases := map[string]struct {
		input    iterator[seriesChunkRefsSet]
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
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
			},
		},
		"should iterate multiple sets with multiple items each": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
			},
		},
		"should keep iterating on empty sets": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}},
				seriesChunkRefsSet{}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
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
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 6)

	testCases := map[string]struct {
		batchSize    int
		set1, set2   iterator[seriesChunkRefsSet]
		expectedSets []seriesChunkRefsSet
		expectedErr  string
	}{
		"merges two sets without overlap": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1], c[2], c[3]}},
				}},
			},
		},
		"merges two sets with last series from each overlapping": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[0], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[0], c[0], c[2], c[3]}},
				}},
			},
		},
		"merges two sets where the first is empty": {
			batchSize: 100,
			set1:      emptySeriesChunkRefsSetIterator{},
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
				}},
			},
		},
		"merges two sets with first one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with second one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with shorter one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: make([]seriesChunkRef, 1)},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v3"), refs: make([]seriesChunkRef, 1)},
					{lset: labels.FromStrings("l1", "v4"), refs: make([]seriesChunkRef, 1)},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"should stop iterating as soon as the first underlying set returns an error": {
			batchSize: 1, // Use a batch size of 1 in this test so that we can see when the iteration stops.
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[2]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[1]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[2]}},
				}},
			},
			expectedErr: "something went wrong",
		},
		"should return merged chunks ranges sorted by min time (assuming source sets have sorted ranges) on first chunk on first set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should return merged chunks ranges sorted by min time (assuming source sets have sorted ranges) on first chunk on second set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3], c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 2)": {
			batchSize: 2,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3], c[3]}},
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
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
					assert.Equal(t, expectedSeries.refs, sets[setIdx].series[expectedSeriesIdx].refs)
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
		iterators := make([]iterator[seriesChunkRefsSet], 0, numIterators)
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
	for numIterators := 1; numIterators <= 64; numIterators *= 2 {
		b.Run(fmt.Sprintf("number of iterators = %d", numIterators), func(b *testing.B) {
			for _, withDuplicatedSeries := range []bool{true, false} {
				b.Run(fmt.Sprintf("with duplicates = %v", withDuplicatedSeries), func(b *testing.B) {
					for _, withIO := range []bool{false, true} {
						b.Run(fmt.Sprintf("with IO = %v", withIO), func(b *testing.B) {
							benchmarkMergedSeriesChunkRefsSetIterators(b, numIterators, withDuplicatedSeries, withIO)
						})
					}
				})
			}
		})
	}
}

func benchmarkMergedSeriesChunkRefsSetIterators(b *testing.B, numIterators int, withDuplicatedSeries, withIO bool) {
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

	// Create empty iterators that we can reuse in each benchmark run.
	iterators := make([]iterator[seriesChunkRefsSet], 0, numIterators)
	for i := 0; i < numIterators; i++ {
		iterators = append(iterators, newSliceSeriesChunkRefsSetIterator(nil))
	}

	batch := make([]iterator[seriesChunkRefsSet], len(iterators))
	for i := 0; i < numIterators; i++ {
		if withIO {
			// The delay represents an IO operation, that happens inside real set iterations.
			batch[i] = newDelayedIterator(10*time.Microsecond, iterators[i])
		} else {
			batch[i] = iterators[i]
		}
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

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Reset batch's underlying iterators.
		for i := 0; i < numIterators; i++ {
			iterators[i].(*sliceSeriesChunkRefsSetIterator).reset(perIteratorSets[i])
		}

		// Merge the iterators and run through them.
		it := mergedSeriesChunkRefsSetIterators(mergedBatchSize, batch...)

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
}

func TestSeriesSetWithoutChunks(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 6)

	testCases := map[string]struct {
		input              iterator[seriesChunkRefsSet]
		expectedSeries     []labels.Labels
		expectedBatchCount int
	}{
		"should iterate on no sets": {
			input:              newSliceSeriesChunkRefsSetIterator(nil),
			expectedSeries:     nil,
			expectedBatchCount: 0,
		},
		"should iterate an empty set": {
			input:              newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{}),
			expectedSeries:     nil,
			expectedBatchCount: 1,
		},
		"should iterate a set with multiple items": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}}),
			expectedSeries: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
			},
			expectedBatchCount: 1,
		},
		"should iterate multiple sets with multiple items each": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}}),
			expectedSeries: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
				labels.FromStrings("l1", "v3"),
				labels.FromStrings("l1", "v4"),
				labels.FromStrings("l1", "v5"),
			},
			expectedBatchCount: 3,
		},
		"should keep iterating on empty sets": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), refs: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), refs: []seriesChunkRef{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), refs: []seriesChunkRef{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), refs: []seriesChunkRef{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), refs: []seriesChunkRef{c[5]}},
				}},
				seriesChunkRefsSet{}),
			expectedSeries: []labels.Labels{
				labels.FromStrings("l1", "v1"),
				labels.FromStrings("l1", "v2"),
				labels.FromStrings("l1", "v3"),
				labels.FromStrings("l1", "v4"),
				labels.FromStrings("l1", "v5"),
			},
			expectedBatchCount: 7,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			stats := newSafeQueryStats()
			chainedSet := newSeriesSetWithoutChunks(ctx, testCase.input, stats)
			actual := readAllSeriesLabels(chainedSet)
			require.NoError(t, chainedSet.Err())
			assert.Equal(t, testCase.expectedSeries, actual)
			assert.Equal(t, testCase.expectedBatchCount, stats.export().streamingSeriesBatchCount)
		})
	}
}

func TestDeduplicatingSeriesChunkRefsSetIterator(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 8)

	series1 := labels.FromStrings("l1", "v1")
	series2 := labels.FromStrings("l1", "v2")
	series3 := labels.FromStrings("l1", "v3")
	sourceSets := []seriesChunkRefsSet{
		{series: []seriesChunkRefs{
			{lset: series1, refs: []seriesChunkRef{c[0], c[1]}},
			{lset: series1, refs: []seriesChunkRef{c[2], c[3], c[4]}},
		}},
		{series: []seriesChunkRefs{
			{lset: series2, refs: []seriesChunkRef{c[0], c[1], c[2], c[3]}},
			{lset: series3, refs: []seriesChunkRef{c[0]}},
			{lset: series3, refs: []seriesChunkRef{c[1]}},
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
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].refs)

		require.Len(t, sets[1].series, 1)
		assert.Equal(t, series2, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[1].series[0].refs)

		require.Len(t, sets[2].series, 1)
		assert.Equal(t, series3, sets[2].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[2].series[0].refs)
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
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].refs)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[0].series[1].refs)

		// Second batch.
		require.Len(t, sets[1].series, 1)

		assert.Equal(t, series3, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[1].series[0].refs)
	})

	t.Run("batch size: 3", func(t *testing.T) {
		repeatingIterator := newSliceSeriesChunkRefsSetIterator(nil, sourceSets...)
		deduplciatingIterator := newDeduplicatingSeriesChunkRefsSetIterator(3, repeatingIterator)
		sets := readAllSeriesChunkRefsSet(deduplciatingIterator)

		require.NoError(t, deduplciatingIterator.Err())
		require.Len(t, sets, 1)
		require.Len(t, sets[0].series, 3)

		assert.Equal(t, series1, sets[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].refs)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, sets[0].series[1].refs)

		assert.Equal(t, series3, sets[0].series[2].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, sets[0].series[2].refs)
	})
}

func TestDeduplicatingSeriesChunkRefsSetIterator_PropagatesErrors(t *testing.T) {
	chainedSet := newDeduplicatingSeriesChunkRefsSetIterator(100, newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("l1", "v1"), refs: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v1"), refs: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v2"), refs: make([]seriesChunkRef, 1)},
			{lset: labels.FromStrings("l1", "v2"), refs: make([]seriesChunkRef, 1)},
		},
	}))

	// nolint:revive // We want to read through all series.
	for chainedSet.Next() {
	}

	assert.ErrorContains(t, chainedSet.Err(), "something went wrong")
}

func TestLimitingSeriesChunkRefsSetIterator(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 2)},
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 3)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 4)},
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), refs: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), refs: generateSeriesChunksRanges(blockID, 1)},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			iterator := newLimitingSeriesChunkRefsSetIterator(
				newSliceSeriesChunkRefsSetIterator(testCase.upstreamErr, testCase.sets...),
				&staticLimiter{limit: testCase.chunksLimit, msg: "exceeded chunks limit"},
				&staticLimiter{limit: testCase.seriesLimit, msg: "exceeded series limit"},
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
	b := labels.NewScratchBuilder(1)
	oneLabel := func(name, value string) labels.Labels {
		b.Reset()
		b.Add(name, value)
		return b.Labels()
	}
	defaultTestBlockFactory := prepareTestBlock(test.NewTB(t), func(t testing.TB, appenderFactory func() storage.Appender) {
		appender := appenderFactory()
		for i := 0; i < 100; i++ {
			_, err := appender.Append(0, oneLabel("l1", fmt.Sprintf("v%d", i)), int64(i*10), 0)
			assert.NoError(t, err)
		}
		assert.NoError(t, appender.Commit())
	})

	const largerTestBlockSeriesCount = 100_000
	largerTestBlockFactory := prepareTestBlock(test.NewTB(t), func(t testing.TB, appenderFactory func() storage.Appender) {
		for i := 0; i < largerTestBlockSeriesCount; i++ {
			appender := appenderFactory()
			lbls := oneLabel("l1", fmt.Sprintf("v%d", i))
			var ref storage.SeriesRef
			const numSamples = 240 // Write enough samples to have two chunks per series
			for j := 0; j < numSamples; j++ {
				var err error
				ref, err = appender.Append(ref, lbls, int64(i*10+j), float64(j))
				assert.NoError(t, err)
			}
			assert.NoError(t, appender.Commit())
		}
	})

	type testCase struct {
		blockFactory func() *bucketBlock // if nil, defaultTestBlockFactory is used
		shard        *sharding.ShardSelector
		matchers     []*labels.Matcher
		seriesHasher seriesHasher
		strategy     seriesIteratorStrategy
		minT, maxT   int64
		batchSize    int

		expectedSets []seriesChunkRefsSet
	}

	sharedSeriesHasher := cachedSeriesHasher{hashcache.NewSeriesHashCache(1000).GetBlockCache("")}
	sharedSeriesHasher2 := cachedSeriesHasher{hashcache.NewSeriesHashCache(1000).GetBlockCache("")}

	testCases := map[string]testCase{
		"loads one batch": {
			minT:      0,
			maxT:      10000,
			batchSize: 100,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
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
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
		"skips chunks": {
			strategy:  noChunkRefs,
			minT:      0,
			maxT:      40,
			batchSize: 100,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
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
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
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
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},
		"ignores mixT/maxT when skipping chunks": {
			minT:      0,
			maxT:      10,
			strategy:  noChunkRefs,
			batchSize: 4,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
					{lset: labels.FromStrings("l1", "v4")},
				}},
			},
		},
		"works with many series in a single batch": {
			blockFactory: largerTestBlockFactory,
			minT:         0,
			maxT:         math.MaxInt64,
			batchSize:    largerTestBlockSeriesCount,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", ".*")},
			expectedSets: func() []seriesChunkRefsSet {
				set := newSeriesChunkRefsSet(largerTestBlockSeriesCount, true)
				for i := 0; i < largerTestBlockSeriesCount; i++ {
					set.series = append(set.series, seriesChunkRefs{lset: oneLabel("l1", fmt.Sprintf("v%d", i))})
				}
				// The order of series in the block is by their labels, so we need to sort what we generated.
				sort.Slice(set.series, func(i, j int) bool {
					return labels.Compare(set.series[i].lset, set.series[j].lset) < 0
				})
				return []seriesChunkRefsSet{set}
			}(),
		},
		"works with many series in many batches": {
			blockFactory: largerTestBlockFactory,
			minT:         0,
			maxT:         math.MaxInt64,
			batchSize:    5000,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", ".*")},
			expectedSets: func() []seriesChunkRefsSet {
				series := make([]seriesChunkRefs, 0, largerTestBlockSeriesCount)
				for i := 0; i < largerTestBlockSeriesCount; i++ {
					series = append(series, seriesChunkRefs{lset: oneLabel("l1", fmt.Sprintf("v%d", i))})
				}
				// The order of series in the block is by their labels, so we need to sort what we generated.
				sort.Slice(series, func(i, j int) bool {
					return labels.Compare(series[i].lset, series[j].lset) < 0
				})

				const numBatches = largerTestBlockSeriesCount / 5000
				sets := make([]seriesChunkRefsSet, numBatches)
				for setIdx := 0; setIdx < numBatches; setIdx++ {
					sets[setIdx].series = series[setIdx*largerTestBlockSeriesCount/numBatches : (setIdx+1)*largerTestBlockSeriesCount/numBatches]
				}
				return sets
			}(),
		},
		"skip chunks with streaming on block 1": {
			minT:      0,
			maxT:      25,
			batchSize: 100,
			strategy:  noChunkRefs | overlapMintMaxt,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1")},
					{lset: labels.FromStrings("l1", "v2")},
				}},
			},
		},
		"skip chunks with streaming on block 2": {
			minT:      15,
			maxT:      35,
			batchSize: 100,
			strategy:  noChunkRefs | overlapMintMaxt,
			matchers:  []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-4]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},

		// If the first test case stored incorrect hashes in the cache, the second test case would fail.
		"doesn't pollute the series hash cache with incorrect hashes (pt. 1)": {
			minT:         15,
			maxT:         45,
			seriesHasher: sharedSeriesHasher,
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			batchSize:    100,
			strategy:     noChunkRefs | overlapMintMaxt,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-5]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},
		"doesn't pollute the series hash cache with incorrect hashes (pt. 2)": {
			minT:         15,
			maxT:         45,
			seriesHasher: sharedSeriesHasher,
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			batchSize:    100,
			strategy:     noChunkRefs | overlapMintMaxt,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-5]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},
		"doesn't pollute the series hash cache with incorrect hashes (without streaming; pt. 1)": {
			minT:         15,
			maxT:         45,
			seriesHasher: sharedSeriesHasher2,
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			batchSize:    100,
			strategy:     overlapMintMaxt,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-5]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},
		"doesn't pollute the series hash cache with incorrect hashes (without streaming; pt. 2)": {
			minT:         15,
			maxT:         45,
			seriesHasher: sharedSeriesHasher2,
			shard:        &sharding.ShardSelector{ShardIndex: 1, ShardCount: 2},
			batchSize:    100,
			strategy:     overlapMintMaxt,
			matchers:     []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "l1", "v[1-5]")},
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2")},
					{lset: labels.FromStrings("l1", "v3")},
				}},
			},
		},
	}

	sortedTestCases := make([]struct {
		name string
		tc   testCase
	}, 0, len(testCases))
	for name, tc := range testCases {
		sortedTestCases = append(sortedTestCases, struct {
			name string
			tc   testCase
		}{name, tc})
	}
	sort.Slice(sortedTestCases, func(i, j int) bool {
		return sortedTestCases[i].name < sortedTestCases[j].name
	})

	for _, testCase := range sortedTestCases {
		testName, tc := testCase.name, testCase.tc
		t.Run(testName, func(t *testing.T) {
			// Setup
			blockFactory := defaultTestBlockFactory
			if tc.blockFactory != nil {
				blockFactory = tc.blockFactory
			}
			block := blockFactory()
			indexr := block.indexReader(selectAllStrategy{})
			postings, _, err := indexr.ExpandedPostings(context.Background(), tc.matchers, newSafeQueryStats())
			require.NoError(t, err)
			postingsIterator := newPostingsSetsIterator(
				postings,
				tc.batchSize,
			)
			hasher := tc.seriesHasher
			if hasher == nil {
				hasher = cachedSeriesHasher{hashcache.NewSeriesHashCache(100).GetBlockCache("")}
			}
			if tc.strategy == 0 {
				tc.strategy = defaultStrategy // the `0` strategy is not usable, so test cases probably meant to not set it
			}
			loadingIterator := newLoadingSeriesChunkRefsSetIterator(
				context.Background(),
				postingsIterator,
				indexr,
				noopCache{},
				newSafeQueryStats(),
				block.meta,
				tc.shard,
				hasher,
				tc.strategy,
				tc.minT,
				tc.maxT,
				"t1",
				log.NewNopLogger(),
			)

			// Tests
			sets := readAllSeriesChunkRefsSet(loadingIterator)
			assert.NoError(t, loadingIterator.Err())
			assertSeriesChunkRefsSetsEqual(t, block.meta.ULID, block.bkt.(localBucket).dir, tc.minT, tc.maxT, tc.strategy, tc.expectedSets, sets)
		})
	}
}

func assertSeriesChunkRefsSetsEqual(t testing.TB, blockID ulid.ULID, blockDir string, minT, maxT int64, strategy seriesIteratorStrategy, expected, actual []seriesChunkRefsSet) {
	t.Helper()
	if !assert.Len(t, actual, len(expected)) {
		return
	}
	if strategy.isOnEntireBlock() {
		// Adjust minT and maxT because we will use them to filter out chunks returned by prometheus
		minT, maxT = math.MinInt64, math.MaxInt64
	}

	promBlock := openPromBlocks(t, blockDir)[0]

	for i, actualSet := range actual {
		expectedSet := expected[i]
		if !assert.Equalf(t, expectedSet.len(), actualSet.len(), "%d", i) {
			continue
		}
		for j, actualSeries := range actualSet.series {
			expectedSeries := expectedSet.series[j]
			assert.True(t, labels.Equal(actualSeries.lset, expectedSeries.lset), "[%d, %d]: expected labels %s got %s", i, j, expectedSeries.lset, actualSeries.lset)
			promChunks := storage.NewListChunkSeriesIterator(filterPromChunksByTime(queryPromSeriesChunkMetas(t, actualSeries.lset, promBlock), minT, maxT)...)
			prevChunkRef, prevChunkLen := chunks.ChunkRef(0), uint64(0)

			for k, actualChunk := range actualSeries.refs {
				if !promChunks.Next() {
					require.Truef(t, false, "out of prometheus chunks; left %d chunks: %v", len(actualSeries.refs)-k, actualSeries.refs[k:])
				}
				promChunk := promChunks.At()
				assertEqualf(t, blockID, actualChunk.blockID, "blockID [%d, %d, %d]", i, j, k)
				assertEqualf(t, promChunk.Ref, actualChunk.ref(), "ref [%d, %d, %d]", i, j, k)
				assertEqualf(t, promChunk.MinTime, actualChunk.minTime, "minT [%d, %d, %d]", i, j, k)
				assertEqualf(t, promChunk.MaxTime, actualChunk.maxTime, "maxT [%d, %d, %d]", i, j, k)
				assert.True(t, uint64(prevChunkRef)+prevChunkLen <= uint64(promChunk.Ref),
					"estimated length shouldn't extend into the next chunk [%d, %d, %d]", i, j, k)
				assert.True(t, actualChunk.length <= uint32(tsdb.EstimatedMaxChunkSize),
					"chunks can be larger than 16KB, but the estimated length should be capped to 16KB to limit the impact of bugs in estimations [%d, %d, %d]", i, j, k)

				prevChunkRef, prevChunkLen = promChunk.Ref, uint64(actualChunk.length)
			}
			if !strategy.isNoChunkRefs() {
				// There shouldn't be extra chunks returned by prometheus
				assert.False(t, promChunks.Next())
			}
		}
	}
}

func assertEqualf[T comparable](t testing.TB, a, b T, msg string, args ...any) {
	if a != b {
		t.Helper()
		assert.Equalf(t, a, b, msg, args...)
	}
}

func TestOpenBlockSeriesChunkRefsSetsIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	newTestBlock := prepareTestBlock(test.NewTB(t), func(_ testing.TB, appenderFactory func() storage.Appender) {
		const (
			samplesFor1Chunk   = 100                  // not a complete chunk
			samplesFor2Chunks  = samplesFor1Chunk * 2 // not a complete chunk
			samplesFor13Chunks = 1560                 // 120 samples per chunk
		)
		earlySeries := []labels.Labels{
			labels.FromStrings("a", "1", "b", "1"),
			labels.FromStrings("a", "1", "b", "2"),
		}

		// Series with samples that start later, so we can expect their chunks' minT/maxT to be different
		lateSeries := []labels.Labels{
			labels.FromStrings("a", "2", "b", "1"),
			labels.FromStrings("a", "2", "b", "2"),
		}
		appender := appenderFactory()
		for i := int64(0); i < samplesFor2Chunks; i++ { // write 200 samples, so we get two chunks
			for _, s := range earlySeries {
				_, err := appender.Append(0, s, i, 0)
				assert.NoError(t, err)
			}
			for _, s := range lateSeries {
				_, err := appender.Append(0, s, samplesFor2Chunks+i, 0)
				assert.NoError(t, err)
			}
		}

		seriesWith50Chunks := []labels.Labels{
			labels.FromStrings("a", "3", "b", "1"),
			labels.FromStrings("a", "3", "b", "2"),
		}

		for i := int64(0); i < samplesFor13Chunks; i++ {
			for _, s := range seriesWith50Chunks {
				_, err := appender.Append(0, s, i, 0)
				assert.NoError(t, err)
			}
		}

		seriesWithSparseChunks := []labels.Labels{
			labels.FromStrings("a", "4", "b", "1"),
			labels.FromStrings("a", "4", "b", "2"),
		}

		for i := int64(0); i < samplesFor1Chunk; i++ {
			// Write the first chunk with earlier timestamp
			for _, s := range seriesWithSparseChunks {
				_, err := appender.Append(0, s, i, 0)
				assert.NoError(t, err)
			}
		}
		for i := int64(0); i < samplesFor1Chunk; i++ {
			// Write the next chunk with later timestamp
			for _, s := range seriesWithSparseChunks {
				_, err := appender.Append(0, s, 1000+i, 0)
				assert.NoError(t, err)
			}
		}

		assert.NoError(t, appender.Commit())
	})

	testCases := map[string]struct {
		matcher    *labels.Matcher
		batchSize  int
		minT, maxT int64 // optional, will use block minT/maxT if 0
		skipChunks bool

		expectedErr    string
		expectedSeries []seriesChunkRefsSet
	}{
		"selects all series in a single batch": {
			matcher:   labels.MustNewMatcher(labels.MatchRegexp, "a", "[12]"),
			batchSize: 100,
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
			matcher:   labels.MustNewMatcher(labels.MatchRegexp, "a", "[12]"),
			batchSize: 1,
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
			matcher:   labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
					{lset: labels.FromStrings("a", "1", "b", "2")},
				}},
			},
		},
		"selects some series in multiple batches": {
			matcher:   labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize: 1,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1")},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2")},
				}},
			},
		},
		"selects all series in a single batch with skipChunks": {
			matcher:    labels.MustNewMatcher(labels.MatchRegexp, "a", "[12]"),
			batchSize:  100,
			skipChunks: true,
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
			matcher:    labels.MustNewMatcher(labels.MatchRegexp, "a", "[12]"),
			batchSize:  1,
			skipChunks: true,
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
		"doesn't return a series if its chunks are around minT/maxT but not within it": {
			matcher: labels.MustNewMatcher(labels.MatchRegexp, "a", "4"),
			minT:    500, maxT: 600, // The chunks for this timeseries are between 0 and 99 and 1000 and 1099
			batchSize:      100,
			expectedSeries: []seriesChunkRefsSet{},
		},
		"correctly selects series from larger blocks": {
			matcher: labels.MustNewMatcher(labels.MatchRegexp, "a", "3"),
			minT:    0, maxT: 600,
			batchSize: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "3", "b", "1")},
					{lset: labels.FromStrings("a", "3", "b", "2")},
				}},
			},
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			var block = newTestBlock()
			// All test cases have a single matcher, so the strategy wouldn't really make a difference.
			// Pending matchers are tested in other tests.
			indexReader := block.indexReader(selectAllStrategy{})
			defer indexReader.Close()

			hashCache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache(block.meta.ULID.String())

			minT, maxT := block.meta.MinTime, block.meta.MaxTime
			if testCase.minT != 0 {
				minT = testCase.minT
			}
			if testCase.maxT != 0 {
				maxT = testCase.maxT
			}

			strategy := defaultStrategy
			if testCase.skipChunks {
				strategy = noChunkRefs
			}
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
				strategy,
				minT,
				maxT,
				newSafeQueryStats(),
				log.NewNopLogger(),
				nil,
			)
			require.NoError(t, err)

			actualSeriesSets := readAllSeriesChunkRefsSet(iterator)
			assertSeriesChunkRefsSetsEqual(t, block.meta.ULID, block.bkt.(localBucket).dir, minT, maxT, strategy, testCase.expectedSeries, actualSeriesSets)
			if testCase.expectedErr != "" {
				assert.ErrorContains(t, iterator.Err(), "test limit exceeded")
			} else {
				assert.NoError(t, iterator.Err())
			}
		})
	}
}

func TestOpenBlockSeriesChunkRefsSetsIterator_pendingMatchers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	newTestBlock := prepareTestBlock(test.NewTB(t), appendTestSeries(10_000))

	testCases := map[string]struct {
		matchers        []*labels.Matcher
		pendingMatchers []*labels.Matcher
		batchSize       int
	}{
		"applies pending matchers": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "n", "1_1.*"),
				labels.MustNewMatcher(labels.MatchRegexp, "i", "100.*"),
			},
			pendingMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "n", "1_1.*"),
			},
			batchSize: 100,
		},
		"applies pending matchers when they match all series": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "n", ""),
				labels.MustNewMatcher(labels.MatchEqual, "s", "foo"),
				labels.MustNewMatcher(labels.MatchRegexp, "i", "100.*"),
			},
			pendingMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "n", ""),
			},
			batchSize: 100,
		},
		"applies pending matchers when they match all series (with some completely empty batches)": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "n", ""),
				labels.MustNewMatcher(labels.MatchEqual, "s", "foo"),
				labels.MustNewMatcher(labels.MatchRegexp, "i", "100.*"),
			},
			pendingMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, "n", ""),
			},
			batchSize: 1,
		},
		"applies pending matchers when they match no series": {
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "n", ""),
				labels.MustNewMatcher(labels.MatchEqual, "s", "foo"),
				labels.MustNewMatcher(labels.MatchRegexp, "i", "100.*"),
			},
			pendingMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "n", ""),
			},
			batchSize: 100,
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			matchersAsStrings := func(ms []*labels.Matcher) (matcherStr []string) {
				for _, m := range ms {
					matcherStr = append(matcherStr, m.String())
				}
				return
			}
			require.Subset(t, matchersAsStrings(testCase.matchers), matchersAsStrings(testCase.pendingMatchers), "pending matchers should be a subset of all matchers")

			var block = newTestBlock()
			block.pendingReaders.Add(2) // this is hacky, but can be replaced only block.indexReade() accepts a strategy
			querySeries := func(indexReader *bucketIndexReader) []seriesChunkRefsSet {
				hashCache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache(block.meta.ULID.String())
				iterator, err := openBlockSeriesChunkRefsSetsIterator(
					ctx,
					testCase.batchSize,
					"",
					indexReader,
					newInMemoryIndexCache(t),
					block.meta,
					testCase.matchers,
					nil,
					cachedSeriesHasher{hashCache},
					noChunkRefs, // skip chunks since we are testing labels filtering
					block.meta.MinTime,
					block.meta.MaxTime,
					newSafeQueryStats(),
					log.NewNopLogger(),
					nil,
				)
				require.NoError(t, err)
				allSets := readAllSeriesChunkRefsSet(iterator)
				assert.NoError(t, iterator.Err())
				return allSets
			}

			indexReaderOmittingMatchers := newBucketIndexReader(block, omitMatchersStrategy{testCase.pendingMatchers})
			defer indexReaderOmittingMatchers.Close()

			indexReader := newBucketIndexReader(block, selectAllStrategy{})
			defer indexReader.Close()

			requireEqual(t, querySeries(indexReader), querySeries(indexReaderOmittingMatchers))
		})
	}

}

// Wrapper to instruct go-cmp package to compare a list of structs with unexported fields.
func requireEqual(t *testing.T, expected, actual interface{}, msgAndArgs ...interface{}) {
	testutil.RequireEqualWithOptions(t, expected, actual,
		[]cmp.Option{cmp.AllowUnexported(seriesChunkRefsSet{}), cmp.AllowUnexported(seriesChunkRefs{})},
		msgAndArgs...)
}

func BenchmarkOpenBlockSeriesChunkRefsSetsIterator(b *testing.B) {
	const series = 5e6

	newTestBlock := prepareTestBlock(test.NewTB(b), appendTestSeries(series))

	testSetups := map[string]struct {
		indexCache indexcache.IndexCache
	}{
		"without index cache": {indexCache: noopCache{}},
		"with index cache":    {indexCache: newInMemoryIndexCache(b)},
	}

	for name, setup := range testSetups {
		b.Run(name, func(b *testing.B) {
			for _, testCase := range seriesSelectionTestCases(test.NewTB(b), series) {
				b.Run(testCase.name, func(b *testing.B) {
					ctx, cancel := context.WithCancel(context.Background())
					b.Cleanup(cancel)

					var block = newTestBlock()
					indexReader := block.indexReader(selectAllStrategy{})
					b.Cleanup(func() { require.NoError(b, indexReader.Close()) })

					hashCache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache(block.meta.ULID.String())

					b.ResetTimer()
					b.ReportAllocs()

					for i := 0; i < b.N; i++ {
						iterator, err := openBlockSeriesChunkRefsSetsIterator(
							ctx,
							5000,
							"",
							indexReader,
							setup.indexCache,
							block.meta,
							testCase.matchers,
							nil,
							cachedSeriesHasher{hashCache},
							defaultStrategy, // we don't skip chunks, so we can measure impact in loading chunk refs too
							block.meta.MinTime,
							block.meta.MaxTime,
							newSafeQueryStats(),
							log.NewNopLogger(),
							nil,
						)
						require.NoError(b, err)

						actualSeriesSets := readAllSeriesChunkRefs(newFlattenedSeriesChunkRefsIterator(iterator))
						assert.Len(b, actualSeriesSets, testCase.expectedSeriesLen)
						assert.NoError(b, iterator.Err())
					}
				})
			}
		})
	}
}

func TestMetasToChunkRefs(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	testCases := map[string]struct {
		partitions     []chunks.Meta
		minT, maxT     int64
		expectedChunks []seriesChunkRef
	}{
		"returns no refs if no individual chunk overlaps with the range": {
			minT: 16,
			maxT: 17,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 23), MinTime: 1, MaxTime: 15},
				{Ref: chunkRef(1, 45), MinTime: 20, MaxTime: 27},
				{Ref: chunkRef(1, 58), MinTime: 78, MaxTime: 90},
				{Ref: chunkRef(1, 79), MinTime: 91, MaxTime: 105},
			},
			expectedChunks: nil,
		},
		"returns all chunks": {
			minT: 5,
			maxT: 500,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20},
				{Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88},
				{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100},
				{Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120},
				{Ref: chunkRef(1, 58), MinTime: 130, MaxTime: 150},
			},
			expectedChunks: []seriesChunkRef{
				{blockID: blockID, segmentFile: 1, segFileOffset: 2, length: 8, minTime: 9, maxTime: 20},
				{blockID: blockID, segmentFile: 1, segFileOffset: 10, length: 13, minTime: 50, maxTime: 88},
				{blockID: blockID, segmentFile: 1, segFileOffset: 23, length: 22, minTime: 90, maxTime: 100},
				{blockID: blockID, segmentFile: 1, segFileOffset: 45, length: 13, minTime: 101, maxTime: 120},
				{blockID: blockID, segmentFile: 1, segFileOffset: 58, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150},
			},
		},
		"doesn't estimate length of chunks when they are from different segment files": {
			minT: 5,
			maxT: 500,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20},
				{Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88},
				{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100},
				{Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120},
				{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150},
			},
			expectedChunks: []seriesChunkRef{
				{blockID: blockID, segmentFile: 1, segFileOffset: 2, length: 8, minTime: 9, maxTime: 20},
				{blockID: blockID, segmentFile: 1, segFileOffset: 10, length: 13, minTime: 50, maxTime: 88},
				{blockID: blockID, segmentFile: 1, segFileOffset: 23, length: 22, minTime: 90, maxTime: 100},
				{blockID: blockID, segmentFile: 1, segFileOffset: 45, length: tsdb.EstimatedMaxChunkSize, minTime: 101, maxTime: 120},
				{blockID: blockID, segmentFile: 2, segFileOffset: 4, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150},
			},
		},
		"clamps the length of a chunk if the diff to the next chunk is larger than 16KB": {
			minT: 5,
			maxT: 500,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20},
				{Ref: chunkRef(1, 100_000), MinTime: 50, MaxTime: 88},
			},
			expectedChunks: []seriesChunkRef{
				{blockID: blockID, segmentFile: 1, segFileOffset: 2, length: tsdb.EstimatedMaxChunkSize, minTime: 9, maxTime: 20},
				{blockID: blockID, segmentFile: 1, segFileOffset: 100_000, length: tsdb.EstimatedMaxChunkSize, minTime: 50, maxTime: 88},
			},
		},
		"returns some chunks they overlap at the edge with minT/maxT": {
			minT: 120,
			maxT: 130,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20},
				{Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88},
				{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100},
				{Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120},
				{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150},
			},
			expectedChunks: []seriesChunkRef{
				{blockID: blockID, segmentFile: 1, segFileOffset: 45, length: tsdb.EstimatedMaxChunkSize, minTime: 101, maxTime: 120},
				{blockID: blockID, segmentFile: 2, segFileOffset: 4, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150},
			},
		},
		"still estimates length using chunks that aren't returned": {
			minT: 95,
			maxT: 105,
			partitions: []chunks.Meta{
				{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20},
				{Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88},
				{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100},
				{Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120},
				{Ref: chunkRef(1, 57), MinTime: 121, MaxTime: 125},
				{Ref: chunkRef(1, 78), MinTime: 126, MaxTime: 129},
				{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150},
			},
			expectedChunks: []seriesChunkRef{
				{blockID: blockID, segmentFile: 1, segFileOffset: 23, length: 22, minTime: 90, maxTime: 100},
				{blockID: blockID, segmentFile: 1, segFileOffset: 45, length: 12, minTime: 101, maxTime: 120},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualRanges := metasToChunkRefs(testCase.partitions, blockID, testCase.minT, testCase.maxT)
			assert.Equal(t, testCase.expectedChunks, actualRanges)
			assert.Equal(t, len(testCase.expectedChunks), cap(actualRanges)) // Assert that we've done the slice preallocation correctly. This won't always catch all incorrect or missing preallocations, but might catch some.
		})
	}
}

// TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching currently tests logic in loadingSeriesChunkRefsSetIterator.
// If openBlockSeriesChunkRefsSetsIterator becomes more complex, consider making this a test for loadingSeriesChunkRefsSetIterator only.
func TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching(t *testing.T) {
	newTestBlock := prepareTestBlock(test.NewTB(t), func(tb testing.TB, appenderFactory func() storage.Appender) {
		existingSeries := []labels.Labels{
			labels.FromStrings("a", "1", "b", "1"), // series ref 32
			labels.FromStrings("a", "1", "b", "2"), // series ref 48
			labels.FromStrings("a", "2", "b", "1"), // series ref 64
			labels.FromStrings("a", "2", "b", "2"), // series ref 80
			labels.FromStrings("a", "3", "b", "1"), // series ref 96
			labels.FromStrings("a", "3", "b", "2"), // series ref 112
		}
		appender := appenderFactory()
		for ts := int64(0); ts < 10; ts++ {
			for _, s := range existingSeries {
				_, err := appender.Append(0, s, ts, 0)
				assert.NoError(tb, err)
			}
		}
		assert.NoError(tb, appender.Commit())
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
		t.Run(testName, func(t *testing.T) {
			for _, batchSize := range testCase.batchSizes {
				t.Run(fmt.Sprintf("batch size %d", batchSize), func(t *testing.T) {
					b := newTestBlock()
					b.indexCache = newInMemoryIndexCache(t)

					// Run 1 with a cold cache
					seriesHasher := mockSeriesHasher{
						hashes: mockedSeriesHashes,
						cached: testCase.cachedSeriesHashesWithColdCache,
					}

					statsColdCache := newSafeQueryStats()
					// All test cases have a single matcher, so the strategy wouldn't really make a difference.
					// Pending matchers are tested in other tests.
					indexReader := b.indexReader(selectAllStrategy{})
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
						noChunkRefs,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsColdCache,
						log.NewNopLogger(),
						nil,
					)

					require.NoError(t, err)
					lset := extractLabelsFromSeriesChunkRefsSets(readAllSeriesChunkRefsSet(ss))
					require.NoError(t, ss.Err())
					testutil.RequireEqual(t, testCase.expectedLabelSets, lset)
					assert.Equal(t, testCase.expectedSeriesReadFromBlockWithColdCache, statsColdCache.export().seriesFetched)

					// Run 2 with a warm cache
					seriesHasher = mockSeriesHasher{
						hashes: mockedSeriesHashes,
						cached: testCase.cachedSeriesHashesWithWarmCache,
					}

					statsWarmCache := newSafeQueryStats()
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
						noChunkRefs,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsWarmCache,
						log.NewNopLogger(),
						nil,
					)
					require.NoError(t, err)
					lset = extractLabelsFromSeriesChunkRefsSets(readAllSeriesChunkRefsSet(ss))
					require.NoError(t, ss.Err())
					assert.Equal(t, testCase.expectedLabelSets, lset)
					assert.Equal(t, testCase.expectedSeriesReadFromBlockWithWarmCache, statsWarmCache.export().seriesFetched)
				})
			}
		})
	}
}

type forbiddenFetchMultiPostingsIndexCache struct {
	indexcache.IndexCache

	t *testing.T
}

func (c forbiddenFetchMultiPostingsIndexCache) FetchMultiPostings(context.Context, string, ulid.ULID, []labels.Label) indexcache.BytesResult {
	assert.Fail(c.t, "index cache FetchMultiPostings should not be called")
	return nil
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
			expectedBatches: nil,
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

			require.Equal(t, testCase.expectedBatches, actualBatches)
		})
	}
}

type mockSeriesHasher struct {
	cached map[storage.SeriesRef]uint64
	hashes map[string]uint64
}

func (a mockSeriesHasher) CachedHash(seriesID storage.SeriesRef, _ *queryStats) (uint64, bool) {
	hash, isCached := a.cached[seriesID]
	return hash, isCached
}

func (a mockSeriesHasher) Hash(_ storage.SeriesRef, lset labels.Labels, _ *queryStats) uint64 {
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

type staticLimiter struct {
	limit   int
	msg     string
	current atomic.Uint64
}

func (l *staticLimiter) Reserve(num uint64) error {
	if l.current.Add(num) > uint64(l.limit) {
		return errors.New(l.msg)
	}
	return nil
}

func generateSeriesChunksRanges(blockID ulid.ULID, numRefs int) []seriesChunkRef {
	refs := make([]seriesChunkRef, 0, numRefs)

	for i := 0; i < numRefs; i++ {
		refs = append(refs, seriesChunkRef{segFileOffset: 10 * uint32(i),
			minTime:     int64(i),
			maxTime:     int64(i),
			length:      10,
			blockID:     blockID,
			segmentFile: 1,
		})
	}

	return refs
}

func readAllSeriesChunkRefsSet(it iterator[seriesChunkRefsSet]) []seriesChunkRefsSet {
	var out []seriesChunkRefsSet
	for it.Next() {
		out = append(out, it.At())
	}
	return out
}

func readAllSeriesChunkRefs(it iterator[seriesChunkRefs]) []seriesChunkRefs {
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
	b := labels.NewScratchBuilder(1)

	for seriesID := minSeriesID; seriesID <= maxSeriesID; seriesID++ {
		b.Reset()
		b.Add(labels.MetricName, fmt.Sprintf("metric_%06d", seriesID))
		set.series = append(set.series, seriesChunkRefs{lset: b.Labels()})
	}

	return set
}

func TestCreateSeriesChunkRefsSet(t *testing.T) {
	set := createSeriesChunkRefsSet(5, 7, true)
	require.Len(t, set.series, 3)
	requireEqual(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000005")}, set.series[0])
	requireEqual(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000006")}, set.series[1])
	requireEqual(t, seriesChunkRefs{lset: labels.FromStrings(labels.MetricName, "metric_000007")}, set.series[2])
}

func BenchmarkFetchCachedSeriesForPostings(b *testing.B) {
	somePostingsKey := indexcache.CanonicalPostingsKey([]storage.SeriesRef{1, 2, 3})

	testCases := map[string]struct {
		cachedEntryLabels          []labels.Labels
		cachedEntryEncodedPostings []storage.SeriesRef
		shard                      *sharding.ShardSelector

		requestedEncodedPostings []storage.SeriesRef
		requestedPostingsKey     indexcache.PostingsKey

		expectedHit bool
	}{
		"6000 series with 6 labels each with sharding": {
			cachedEntryEncodedPostings: generatePostings(6000),
			cachedEntryLabels:          generateSeries([]int{1, 2, 3, 10, 10, 10}),
			shard:                      &sharding.ShardSelector{ShardIndex: 10, ShardCount: 100},

			requestedEncodedPostings: generatePostings(6000),
			requestedPostingsKey:     somePostingsKey,
			expectedHit:              true,
		},
		"6000 series with 6 labels each": {
			cachedEntryEncodedPostings: generatePostings(6000),
			cachedEntryLabels:          generateSeries([]int{1, 2, 3, 10, 10, 10}),

			requestedPostingsKey:     somePostingsKey,
			requestedEncodedPostings: generatePostings(6000),
			expectedHit:              true,
		},
		"6000 series with 6 labels with more repetitions": {
			cachedEntryEncodedPostings: generatePostings(6000),
			cachedEntryLabels:          generateSeries([]int{1, 1, 1, 1, 1, 6000}),

			requestedPostingsKey:     somePostingsKey,
			requestedEncodedPostings: generatePostings(6000),
			expectedHit:              true,
		},
		"1000 series with 1 matcher": {
			cachedEntryEncodedPostings: generatePostings(1000),
			cachedEntryLabels:          generateSeries([]int{10, 10, 10}),

			requestedPostingsKey:     somePostingsKey,
			requestedEncodedPostings: generatePostings(1000),
			expectedHit:              true,
		},
		"1000 series with 1 matcher, mismatching encoded postings": {
			cachedEntryEncodedPostings: generatePostings(999),

			requestedEncodedPostings: generatePostings(1000),
			expectedHit:              false,
		},
	}

	for testName, testCase := range testCases {
		b.Run(testName, func(b *testing.B) {
			ctx := context.Background()
			logger := log.NewNopLogger()

			blockID := ulid.MustNew(1671103209, nil)

			cacheEntryContents, err := encodeCachedSeriesForPostings(seriesChunkRefsSetFromLabelSets(testCase.cachedEntryLabels), mustDiffVarintSnappyEncode(b, testCase.cachedEntryEncodedPostings))
			require.NoError(b, err)

			var mockCache indexcache.IndexCache = mockIndexCache{fetchSeriesForPostingsResponse: mockIndexCacheEntry{
				contents: cacheEntryContents,
				cached:   true,
			}}

			cachedSeriesID := cachedSeriesForPostingsID{
				postingsKey:     testCase.requestedPostingsKey,
				encodedPostings: mustDiffVarintSnappyEncode(b, testCase.requestedEncodedPostings),
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				set, ok := fetchCachedSeriesForPostings(ctx, "tenant-1", mockCache, blockID, testCase.shard, cachedSeriesID, logger)
				assert.Equal(b, testCase.expectedHit, ok)
				if testCase.expectedHit {
					assert.NotZero(b, set)
				} else {
					assert.Zero(b, set)
				}
			}
		})
	}
}

func seriesChunkRefsSetFromLabelSets(labelSets []labels.Labels) (result seriesChunkRefsSet) {
	for _, lset := range labelSets {
		result.series = append(result.series, seriesChunkRefs{lset: lset})
	}
	return
}

func BenchmarkStoreCachedSeriesForPostings(b *testing.B) {
	testCases := map[string]struct {
		seriesToCache seriesChunkRefsSet

		matchersToCache []*labels.Matcher
		shardToCache    *sharding.ShardSelector
	}{
		"with sharding": {
			seriesToCache: seriesChunkRefsSetFromLabelSets(generateSeries([]int{1})),
			shardToCache:  &sharding.ShardSelector{ShardIndex: 1, ShardCount: 10},
		},
		"without sharding": {
			seriesToCache: seriesChunkRefsSetFromLabelSets([]labels.Labels{labels.FromStrings("a", "1")}),
		},
		"6000 series with 6 labels each": {
			seriesToCache: seriesChunkRefsSetFromLabelSets(generateSeries([]int{1, 2, 3, 10, 10, 10})),
		},
		"6000 series with 6 labels with more repetitions": {
			seriesToCache: seriesChunkRefsSetFromLabelSets(generateSeries([]int{1, 1, 1, 1, 1, 6000})),
		},
		"1000 series with 1 matcher": {
			seriesToCache: seriesChunkRefsSetFromLabelSets(generateSeries([]int{10, 10, 10})),
		},
	}

	for testName, testCase := range testCases {
		b.Run(testName, func(b *testing.B) {
			ctx := context.Background()
			// We use a logger that fails the benchmark when used.
			// We assume that on a failed cache attempt we log an error.
			var logger log.Logger = testFailingLogger{b}
			blockID := ulid.MustNew(1671103209, nil)
			cachedSeriesID := cachedSeriesForPostingsID{
				// We can use the same postings key for all cases because it's a fixed-length hash
				postingsKey:     indexcache.CanonicalPostingsKey([]storage.SeriesRef{1, 2, 3}),
				encodedPostings: mustDiffVarintSnappyEncode(b, generatePostings(testCase.seriesToCache.len())),
			}

			var mockCache indexcache.IndexCache = noopCache{}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				storeCachedSeriesForPostings(ctx, mockCache, "tenant-1", blockID, testCase.shardToCache, cachedSeriesID, testCase.seriesToCache, logger)
			}
		})
	}
}

func TestCachedSeriesHasher_Hash_ShouldGuaranteeSeriesShardingConsistencyOverTheTime(t *testing.T) {
	// You should NEVER CHANGE the expected hashes here, otherwise it means you're introducing
	// a backward incompatible change.
	expectedHashesBySeries := map[uint64]labels.Labels{
		13889224011166452370: labels.FromStrings("series_id", "0"),
		307103556485211698:   labels.FromStrings("series_id", "1"),
		17573969331475051845: labels.FromStrings("series_id", "2"),
		8613144804601828350:  labels.FromStrings("series_id", "3"),
		16472193458282740080: labels.FromStrings("series_id", "4"),
		7729501881553818438:  labels.FromStrings("series_id", "5"),
		14697344322548709486: labels.FromStrings("series_id", "6"),
		969809569297678032:   labels.FromStrings("series_id", "7"),
		4148606829831788279:  labels.FromStrings("series_id", "8"),
		7860098726487602753:  labels.FromStrings("series_id", "9"),
	}

	cache := hashcache.NewSeriesHashCache(1024 * 1024).GetBlockCache("test")
	hasher := cachedSeriesHasher{cache}

	seriesRef := storage.SeriesRef(0)

	for expectedHash, seriesLabels := range expectedHashesBySeries {
		actualHash := hasher.Hash(seriesRef, seriesLabels, &queryStats{})
		assert.Equal(t, expectedHash, actualHash, "series: %s", seriesLabels.String())

		seriesRef++
	}
}

func generatePostings(numPostings int) []storage.SeriesRef {
	postings := make([]storage.SeriesRef, numPostings)
	for i := range postings {
		postings[i] = storage.SeriesRef(i * 100)
	}
	return postings
}

func mustDiffVarintSnappyEncode(t testing.TB, postings []storage.SeriesRef) []byte {
	b, err := diffVarintSnappyEncode(index.NewListPostings(postings), len(postings))
	require.NoError(t, err)
	return b
}

type testFailingLogger struct {
	tb testing.TB
}

func (t testFailingLogger) Log(keyvals ...interface{}) error {
	assert.Fail(t.tb, "didn't expect logger to be called", "was called with %v", keyvals)
	return nil
}

type mockIndexCache struct {
	indexcache.IndexCache

	fetchSeriesForPostingsResponse mockIndexCacheEntry
}

type mockIndexCacheEntry struct {
	contents []byte
	cached   bool
}

func (c mockIndexCache) FetchSeriesForPostings(context.Context, string, ulid.ULID, *sharding.ShardSelector, indexcache.PostingsKey) ([]byte, bool) {
	return c.fetchSeriesForPostingsResponse.contents, c.fetchSeriesForPostingsResponse.cached
}

func TestSeriesIteratorStrategy(t *testing.T) {
	require.False(t, defaultStrategy.isNoChunkRefs())
	require.True(t, defaultStrategy.withNoChunkRefs().isNoChunkRefs())
	require.False(t, defaultStrategy.withNoChunkRefs().withChunkRefs().isNoChunkRefs())
	require.False(t, defaultStrategy.withChunkRefs().isNoChunkRefs())
}
