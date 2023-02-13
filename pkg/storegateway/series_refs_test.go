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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
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

func TestSeriesChunkRefsRange_Compare(t *testing.T) {
	testCases := map[string]struct {
		input    []seriesChunkRefsRange
		expected []seriesChunkRefsRange
	}{
		"sorts ranges with single chunk": {
			input: []seriesChunkRefsRange{
				{blockID: ulid.MustNew(0, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 5}}},
				{blockID: ulid.MustNew(1, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 5}}},
				{blockID: ulid.MustNew(2, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 3}}},
				{blockID: ulid.MustNew(3, nil), refs: []seriesChunkRef{{minTime: 4, maxTime: 7}}},
				{blockID: ulid.MustNew(4, nil), refs: []seriesChunkRef{{minTime: 3, maxTime: 6}}},
			},
			expected: []seriesChunkRefsRange{
				{blockID: ulid.MustNew(2, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 3}}},
				{blockID: ulid.MustNew(1, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 5}}},
				{blockID: ulid.MustNew(0, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 5}}},
				{blockID: ulid.MustNew(4, nil), refs: []seriesChunkRef{{minTime: 3, maxTime: 6}}},
				{blockID: ulid.MustNew(3, nil), refs: []seriesChunkRef{{minTime: 4, maxTime: 7}}},
			},
		},
		"sorts ranges with multiple chunks": {
			input: []seriesChunkRefsRange{
				// max time of the whole range may not be the max time of the last chunk
				{blockID: ulid.MustNew(0, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 7}, {minTime: 3, maxTime: 5}}},
				{blockID: ulid.MustNew(2, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 10}}},
				{blockID: ulid.MustNew(1, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 5}}},
			},
			expected: []seriesChunkRefsRange{
				{blockID: ulid.MustNew(2, nil), refs: []seriesChunkRef{{minTime: 1, maxTime: 10}}},
				{blockID: ulid.MustNew(1, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 5}}},
				{blockID: ulid.MustNew(0, nil), refs: []seriesChunkRef{{minTime: 2, maxTime: 7}, {minTime: 3, maxTime: 5}}},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			input, expected := testCase.input, testCase.expected
			sort.Slice(input, func(i, j int) bool {
				return input[i].Compare(input[j]) < 0
			})

			assert.Equal(t, expected, input)
		})
	}
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
				{lset: series1, chunksRanges: []seriesChunkRefsRange{c[0], c[1]}},
				{lset: series2, chunksRanges: []seriesChunkRefsRange{c[2]}},
				{lset: series3, chunksRanges: []seriesChunkRefsRange{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunksRanges: []seriesChunkRefsRange{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunksRanges: []seriesChunkRefsRange{c[2]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series3, chunksRanges: []seriesChunkRefsRange{c[3], c[4]}}, it.At())
		require.False(t, it.Done())

		require.False(t, it.Next())
		require.True(t, it.Done())
		require.Zero(t, it.At())
	})

	t.Run("should re-initialize the internal state on reset()", func(t *testing.T) {
		it := newSeriesChunkRefsIterator(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunksRanges: []seriesChunkRefsRange{c[0], c[1]}},
				{lset: series2, chunksRanges: []seriesChunkRefsRange{c[2]}},
				{lset: series3, chunksRanges: []seriesChunkRefsRange{c[3], c[4]}},
			},
		})

		require.False(t, it.Done())
		require.Zero(t, it.At())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunksRanges: []seriesChunkRefsRange{c[0], c[1]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series2, chunksRanges: []seriesChunkRefsRange{c[2]}}, it.At())
		require.False(t, it.Done())

		// Reset.
		it.reset(seriesChunkRefsSet{
			series: []seriesChunkRefs{
				{lset: series1, chunksRanges: []seriesChunkRefsRange{c[3]}},
				{lset: series4, chunksRanges: []seriesChunkRefsRange{c[4]}},
			},
		})

		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series1, chunksRanges: []seriesChunkRefsRange{c[3]}}, it.At())
		require.False(t, it.Done())

		require.True(t, it.Next())
		require.Equal(t, seriesChunkRefs{lset: series4, chunksRanges: []seriesChunkRefsRange{c[4]}}, it.At())
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
			},
		},
		"should iterate multiple sets with multiple items each": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
				}}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
			},
		},
		"should keep iterating on empty sets": {
			input: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
				}},
				seriesChunkRefsSet{}),
			expected: []seriesChunkRefs{
				{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
				{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
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
		set1, set2   seriesChunkRefsSetIterator
		expectedSets []seriesChunkRefsSet
		expectedErr  string
	}{
		"merges two sets without overlap": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1], c[2], c[3]}},
				}},
			},
		},
		"merges two sets with last series from each overlapping": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[0]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[0], c[2], c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[0], c[0], c[2], c[3]}},
				}},
			},
		},
		"merges two sets where the first is empty": {
			batchSize: 100,
			set1:      emptySeriesChunkRefsSetIterator{},
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				}},
			},
		},
		"merges two sets with first one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with second one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"merges two sets with shorter one erroring at the end": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: make([]seriesChunkRefsRange, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: make([]seriesChunkRefsRange, 1)},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: make([]seriesChunkRefsRange, 1)},
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: make([]seriesChunkRefsRange, 1)},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: make([]seriesChunkRefsRange, 1)},
				},
			}),
			expectedSets: nil, // We expect no returned sets because an error occurred while creating the first one.
			expectedErr:  "something went wrong",
		},
		"should stop iterating as soon as the first underlying set returns an error": {
			batchSize: 1, // Use a batch size of 1 in this test so that we can see when the iteration stops.
			set1: newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[1]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
			},
			expectedErr: "something went wrong",
		},
		"should return merged chunks ranges sorted by min time (assuming source sets have sorted ranges) on first chunk on first set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should return merged chunks ranges sorted by min time (assuming source sets have sorted ranges) on first chunk on second set": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0], c[3]}},
				},
			}),
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1], c[2]}},
				},
			}),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}},
				}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3], c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			},
		},
		"should keep iterating on empty underlying sets (batch size = 2)": {
			batchSize: 2,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
				seriesChunkRefsSet{},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3], c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
				}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			},
		},
		"should keep iterating on second set after first set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
				}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 1)": {
			batchSize: 1,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			},
		},
		"should keep iterating on first set after second set is exhausted (batch size = 100)": {
			batchSize: 100,
			set1: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}}}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}}}},
			),
			set2: newSliceSeriesChunkRefsSetIterator(nil,
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}}}},
				seriesChunkRefsSet{series: []seriesChunkRefs{{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}}}},
			),
			expectedSets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
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
					assert.Equal(t, expectedSeries.chunksRanges, sets[setIdx].series[expectedSeriesIdx].chunksRanges)
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
	c := generateSeriesChunksRanges(ulid.MustNew(1, nil), 6)

	testCases := map[string]struct {
		input              seriesChunkRefsSetIterator
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				}},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{c[2]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{c[3]}},
				}},
				seriesChunkRefsSet{},
				seriesChunkRefsSet{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{c[4]}},
					{lset: labels.FromStrings("l1", "v5"), chunksRanges: []seriesChunkRefsRange{c[5]}},
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
			{lset: series1, chunksRanges: []seriesChunkRefsRange{c[0], c[1]}},
			{lset: series1, chunksRanges: []seriesChunkRefsRange{c[2], c[3], c[4]}},
		}},
		{series: []seriesChunkRefs{
			{lset: series2, chunksRanges: []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}},
			{lset: series3, chunksRanges: []seriesChunkRefsRange{c[0]}},
			{lset: series3, chunksRanges: []seriesChunkRefsRange{c[1]}},
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
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunksRanges)

		require.Len(t, sets[1].series, 1)
		assert.Equal(t, series2, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}, sets[1].series[0].chunksRanges)

		require.Len(t, sets[2].series, 1)
		assert.Equal(t, series3, sets[2].series[0].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1]}, sets[2].series[0].chunksRanges)
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
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunksRanges)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}, sets[0].series[1].chunksRanges)

		// Second batch.
		require.Len(t, sets[1].series, 1)

		assert.Equal(t, series3, sets[1].series[0].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1]}, sets[1].series[0].chunksRanges)
	})

	t.Run("batch size: 3", func(t *testing.T) {
		repeatingIterator := newSliceSeriesChunkRefsSetIterator(nil, sourceSets...)
		deduplciatingIterator := newDeduplicatingSeriesChunkRefsSetIterator(3, repeatingIterator)
		sets := readAllSeriesChunkRefsSet(deduplciatingIterator)

		require.NoError(t, deduplciatingIterator.Err())
		require.Len(t, sets, 1)
		require.Len(t, sets[0].series, 3)

		assert.Equal(t, series1, sets[0].series[0].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3], c[4]}, sets[0].series[0].chunksRanges)

		assert.Equal(t, series2, sets[0].series[1].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1], c[2], c[3]}, sets[0].series[1].chunksRanges)

		assert.Equal(t, series3, sets[0].series[2].lset)
		assert.Equal(t, []seriesChunkRefsRange{c[0], c[1]}, sets[0].series[2].chunksRanges)
	})
}

func TestDeduplicatingSeriesChunkRefsSetIterator_PropagatesErrors(t *testing.T) {
	chainedSet := newDeduplicatingSeriesChunkRefsSetIterator(100, newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
		series: []seriesChunkRefs{
			{lset: labels.FromStrings("l1", "v1"), chunksRanges: make([]seriesChunkRefsRange, 1)},
			{lset: labels.FromStrings("l1", "v1"), chunksRanges: make([]seriesChunkRefsRange, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunksRanges: make([]seriesChunkRefsRange, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunksRanges: make([]seriesChunkRefsRange, 1)},
		},
	}))

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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 2)},
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 3)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 4)},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
				}},
			},
		},
		"exceeds chunks limit with multiple chunks per range": {
			seriesLimit:       2,
			chunksLimit:       5,
			expectedSetsCount: 0,
			expectedErr:       "exceeded chunks limit",
			sets: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRangesN(blockID, 2, 3)},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l2", "v1"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
					{lset: labels.FromStrings("l2", "v2"), chunksRanges: generateSeriesChunksRanges(blockID, 1)},
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
	newTestBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), func(t testing.TB, appender storage.Appender) {
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 10, maxTime: 10, segFileOffset: 26, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 20, maxTime: 20, segFileOffset: 234, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 30, maxTime: 30, segFileOffset: 442, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						// For the rest of the chunks we could take the diff with the next series' chunk, but for the last we use the default estimate.
						{minTime: 40, maxTime: 40, segFileOffset: 650, length: tsdb.EstimatedMaxChunkSize},
					}}}},
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
					{lset: labels.FromStrings("l1", "v1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 10, maxTime: 10, segFileOffset: 26, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 20, maxTime: 20, segFileOffset: 234, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 30, maxTime: 30, segFileOffset: 442, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 40, maxTime: 40, segFileOffset: 650, length: tsdb.EstimatedMaxChunkSize},
					}}}},
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
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 30, maxTime: 30, segFileOffset: 442, length: 208},
					}}}},
					{lset: labels.FromStrings("l1", "v4"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{minTime: 40, maxTime: 40, segFileOffset: 650, length: tsdb.EstimatedMaxChunkSize},
					}}}},
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
					{lset: labels.FromStrings("l1", "v3"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						// We select only one series, but there are other series which aren't returned because of the shard.
						// We still use those series' chunk refs to estimate lengths of other series. So in this case
						// the last chunk didn't use the default estimation.
						{minTime: 30, maxTime: 30, segFileOffset: 442, length: 208},
					}}}},
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
			assertSeriesChunkRefsSetsEqual(t, block.meta.ULID, testCase.expectedSets, sets)
		})
	}
}

func assertSeriesChunkRefsSetsEqual(t testing.TB, blockID ulid.ULID, expected, actual []seriesChunkRefsSet) {
	t.Helper()
	if !assert.Len(t, actual, len(expected)) {
		return
	}
	for i, actualSet := range actual {
		expectedSet := expected[i]
		if !assert.Equalf(t, expectedSet.len(), actualSet.len(), "%d", i) {
			continue
		}
		for j, actualSeries := range actualSet.series {
			expectedSeries := expectedSet.series[j]
			assert.Truef(t, labels.Equal(actualSeries.lset, expectedSeries.lset), "[%d, %d]: expected labels %s got %s", i, j, expectedSeries.lset, actualSeries.lset)
			if !assert.Lenf(t, actualSeries.chunksRanges, len(expectedSeries.chunksRanges), "chunk ranges len [%d, %d]", i, j) {
				continue
			}
			for k, actualChunksRange := range actualSeries.chunksRanges {
				expectedRange := expectedSeries.chunksRanges[k]
				assert.Equalf(t, expectedRange.firstRef(), actualChunksRange.firstRef(), "first ref [%d, %d, %d]", i, j, k)
				assert.Equalf(t, blockID, actualChunksRange.blockID, "blockID [%d, %d, %d]", i, j, k)
				require.Lenf(t, actualChunksRange.refs, len(expectedRange.refs), "chunks len [%d, %d, %d]", i, j, k)
				for l, actualChunk := range actualChunksRange.refs {
					expectedChunk := expectedRange.refs[l]
					assert.Equalf(t, int(expectedChunk.segFileOffset), int(actualChunk.segFileOffset), "ref [%d, %d, %d, %d]", i, j, k, l)
					assert.Equalf(t, expectedChunk.minTime, actualChunk.minTime, "minT [%d, %d, %d, %d]", i, j, k, l)
					assert.Equalf(t, expectedChunk.maxTime, actualChunk.maxTime, "maxT [%d, %d, %d, %d]", i, j, k, l)
					assert.Equalf(t, int(expectedChunk.length), int(actualChunk.length), "length [%d, %d, %d, %d]", i, j, k, l)
				}
			}
		}
	}
}

func TestOpenBlockSeriesChunkRefsSetsIterator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	newTestBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), func(tb testing.TB, appender storage.Appender) {
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
					{lset: labels.FromStrings("a", "1", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 8, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 57, minTime: 125, maxTime: 199, length: 38},
					}}}},
					{lset: labels.FromStrings("a", "1", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 95, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 144, minTime: 125, maxTime: 199, length: 38},
					}}}},
					{lset: labels.FromStrings("a", "2", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 182, minTime: 200, maxTime: 332, length: 52},
						{segFileOffset: 234, minTime: 333, maxTime: 399, length: 36},
					}}}},
					{lset: labels.FromStrings("a", "2", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 270, minTime: 200, maxTime: 332, length: 52},
						{segFileOffset: 322, minTime: 333, maxTime: 399, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
			},
		},
		"selects all series in multiple batches": {
			matcher:   labels.MustNewMatcher(labels.MatchRegexp, "a", "[12]"),
			batchSize: 1,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 8, minTime: 0, maxTime: 124, length: 49},
						// Since we don't know the ref of the next chunk we use the default estimation for the last chunk
						{segFileOffset: 57, minTime: 125, maxTime: 199, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 95, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 144, minTime: 125, maxTime: 199, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 182, minTime: 200, maxTime: 332, length: 52},
						{segFileOffset: 234, minTime: 333, maxTime: 399, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "2", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 270, minTime: 200, maxTime: 332, length: 52},
						{segFileOffset: 322, minTime: 333, maxTime: 399, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
			},
		},
		"selects some series in single batch": {
			matcher:   labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize: 100,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 8, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 57, minTime: 125, maxTime: 199, length: 38},
					}}}},
					{lset: labels.FromStrings("a", "1", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 95, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 144, minTime: 125, maxTime: 199, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
			},
		},
		"selects some series in multiple batches": {
			matcher:   labels.MustNewMatcher(labels.MatchEqual, "a", "1"),
			batchSize: 1,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "1"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 8, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 57, minTime: 125, maxTime: 199, length: tsdb.EstimatedMaxChunkSize},
					}}}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "1", "b", "2"), chunksRanges: []seriesChunkRefsRange{{refs: []seriesChunkRef{
						{segFileOffset: 95, minTime: 0, maxTime: 124, length: 49},
						{segFileOffset: 144, minTime: 125, maxTime: 199, length: tsdb.EstimatedMaxChunkSize},
					}}}},
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
		"partitions multiple chunks into 2 ranges": {
			matcher:   labels.MustNewMatcher(labels.MatchRegexp, "a", "3"),
			batchSize: 1,
			expectedSeries: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "3", "b", "1"), chunksRanges: []seriesChunkRefsRange{
						{refs: []seriesChunkRef{
							{segFileOffset: 358, length: 49, minTime: 0, maxTime: 124},
							{segFileOffset: 407, length: 50, minTime: 125, maxTime: 249},
							{segFileOffset: 457, length: 50, minTime: 250, maxTime: 374},
							{segFileOffset: 507, length: 50, minTime: 375, maxTime: 499},
							{segFileOffset: 557, length: 50, minTime: 500, maxTime: 624},
							{segFileOffset: 607, length: 50, minTime: 625, maxTime: 749},
							{segFileOffset: 657, length: 50, minTime: 750, maxTime: 874},
							{segFileOffset: 707, length: 50, minTime: 875, maxTime: 999},
							{segFileOffset: 757, length: 50, minTime: 1000, maxTime: 1124},
							{segFileOffset: 807, length: 50, minTime: 1125, maxTime: 1249},
						}},
						{refs: []seriesChunkRef{
							{segFileOffset: 857, length: 50, minTime: 1250, maxTime: 1374},
							{segFileOffset: 907, length: 50, minTime: 1375, maxTime: 1499},
							{segFileOffset: 957, length: tsdb.EstimatedMaxChunkSize, minTime: 1500, maxTime: 1559},
						}},
					}},
				}},
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("a", "3", "b", "2"), chunksRanges: []seriesChunkRefsRange{
						{refs: []seriesChunkRef{
							{segFileOffset: 991, length: 49, minTime: 0, maxTime: 124},
							{segFileOffset: 1040, length: 50, minTime: 125, maxTime: 249},
							{segFileOffset: 1090, length: 50, minTime: 250, maxTime: 374},
							{segFileOffset: 1140, length: 50, minTime: 375, maxTime: 499},
							{segFileOffset: 1190, length: 50, minTime: 500, maxTime: 624},
							{segFileOffset: 1240, length: 50, minTime: 625, maxTime: 749},
							{segFileOffset: 1290, length: 50, minTime: 750, maxTime: 874},
							{segFileOffset: 1340, length: 50, minTime: 875, maxTime: 999},
							{segFileOffset: 1390, length: 50, minTime: 1000, maxTime: 1124},
							{segFileOffset: 1440, length: 50, minTime: 1125, maxTime: 1249},
						}},
						{refs: []seriesChunkRef{
							{segFileOffset: 1490, length: 50, minTime: 1250, maxTime: 1374},
							{segFileOffset: 1540, length: 50, minTime: 1375, maxTime: 1499},
							{segFileOffset: 1590, length: tsdb.EstimatedMaxChunkSize, minTime: 1500, maxTime: 1559},
						}},
					}},
				}},
			},
		},
		"doesn't return a series if its chunks are around minT/maxT but not within it": {
			matcher: labels.MustNewMatcher(labels.MatchRegexp, "a", "4"),
			minT:    500, maxT: 600, // The chunks for this timeseries are between 0 and 99 and 1000 and 1099
			batchSize:      100,
			expectedSeries: []seriesChunkRefsSet{},
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

			minT, maxT := block.meta.MinTime, block.meta.MaxTime
			if testCase.minT != 0 {
				minT = testCase.minT
			}
			if testCase.maxT != 0 {
				maxT = testCase.maxT
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
				testCase.skipChunks,
				minT,
				maxT,
				newSafeQueryStats(),
				nil,
			)
			require.NoError(t, err)

			actualSeriesSets := readAllSeriesChunkRefsSet(iterator)
			assertSeriesChunkRefsSetsEqual(t, block.meta.ULID, testCase.expectedSeries, actualSeriesSets)
			if testCase.expectedErr != "" {
				assert.ErrorContains(t, iterator.Err(), "test limit exceeded")
			} else {
				assert.NoError(t, iterator.Err())
			}
		})
	}
}

func TestMetasToRanges(t *testing.T) {
	blockID := ulid.MustNew(1, nil)
	testCases := map[string]struct {
		partitions     [][]chunks.Meta
		minT, maxT     int64
		expectedRanges []seriesChunkRefsRange
	}{
		"returns no ranges if partitions cover minT/maxT, but no individual chunk overlaps with the range": {
			minT: 16,
			maxT: 17,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 23), MinTime: 1, MaxTime: 15}, {Ref: chunkRef(1, 45), MinTime: 20, MaxTime: 27}},
				{{Ref: chunkRef(1, 58), MinTime: 78, MaxTime: 90}, {Ref: chunkRef(1, 79), MinTime: 91, MaxTime: 105}},
			},
			expectedRanges: nil,
		},
		"returns no ranges if no partitions cover minT/maxT": {
			minT: 5,
			maxT: 50,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 23), MinTime: 78, MaxTime: 90}, {Ref: chunkRef(1, 45), MinTime: 90, MaxTime: 100}},
				{{Ref: chunkRef(1, 58), MinTime: 101, MaxTime: 120}},
			},
			expectedRanges: nil,
		},
		"returns all partitions": {
			minT: 5,
			maxT: 500,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
				{{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100}, {Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120}},
				{{Ref: chunkRef(1, 58), MinTime: 130, MaxTime: 150}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 2, length: 8, minTime: 9, maxTime: 20}, {segFileOffset: 10, length: 13, minTime: 50, maxTime: 88}}},
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 23, length: 22, minTime: 90, maxTime: 100}, {segFileOffset: 45, length: 13, minTime: 101, maxTime: 120}}},
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 58, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150}}},
			},
		},
		"a single input partition": {
			minT: 5,
			maxT: 500,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 2, length: 8, minTime: 9, maxTime: 20}, {segFileOffset: 10, length: tsdb.EstimatedMaxChunkSize, minTime: 50, maxTime: 88}}},
			},
		},
		"doesn't estimate length of chunks when they are from different segment files": {
			minT: 5,
			maxT: 500,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
				{{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100}, {Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120}},
				{{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 2, length: 8, minTime: 9, maxTime: 20}, {segFileOffset: 10, length: 13, minTime: 50, maxTime: 88}}},
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 23, length: 22, minTime: 90, maxTime: 100}, {segFileOffset: 45, length: tsdb.EstimatedMaxChunkSize, minTime: 101, maxTime: 120}}},
				{blockID: blockID, segmentFile: 2, refs: []seriesChunkRef{{segFileOffset: 4, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150}}},
			},
		},
		"clamps the length of a chunk if the diff to the next chunk is larger than 16KB": {
			minT: 5,
			maxT: 500,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 100_000), MinTime: 50, MaxTime: 88}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 2, length: tsdb.EstimatedMaxChunkSize, minTime: 9, maxTime: 20}, {segFileOffset: 100_000, length: tsdb.EstimatedMaxChunkSize, minTime: 50, maxTime: 88}}},
			},
		},
		"returns some ranges when some partitions overlap with minT/maxT": {
			minT: 100,
			maxT: 140,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
				{{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100}, {Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120}},
				{{Ref: chunkRef(1, 58), MinTime: 130, MaxTime: 150}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 23, length: 22, minTime: 90, maxTime: 100}, {segFileOffset: 45, length: 13, minTime: 101, maxTime: 120}}},
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 58, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150}}},
			},
		},
		"returns some ranges when some partitions overlap at the edge with with minT/maxT": {
			minT: 120,
			maxT: 130,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
				{{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100}, {Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120}},
				{{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 23, length: 22, minTime: 90, maxTime: 100}, {segFileOffset: 45, length: tsdb.EstimatedMaxChunkSize, minTime: 101, maxTime: 120}}},
				{blockID: blockID, segmentFile: 2, refs: []seriesChunkRef{{segFileOffset: 4, length: tsdb.EstimatedMaxChunkSize, minTime: 130, maxTime: 150}}},
			},
		},
		"still estimates length using chunks that aren't returned in any range": {
			minT: 95,
			maxT: 105,
			partitions: [][]chunks.Meta{
				{{Ref: chunkRef(1, 2), MinTime: 9, MaxTime: 20}, {Ref: chunkRef(1, 10), MinTime: 50, MaxTime: 88}},
				{{Ref: chunkRef(1, 23), MinTime: 90, MaxTime: 100}, {Ref: chunkRef(1, 45), MinTime: 101, MaxTime: 120}},
				{{Ref: chunkRef(1, 57), MinTime: 121, MaxTime: 125}, {Ref: chunkRef(1, 78), MinTime: 126, MaxTime: 129}},
				{{Ref: chunkRef(2, 4), MinTime: 130, MaxTime: 150}},
			},
			expectedRanges: []seriesChunkRefsRange{
				{blockID: blockID, segmentFile: 1, refs: []seriesChunkRef{{segFileOffset: 23, length: 22, minTime: 90, maxTime: 100}, {segFileOffset: 45, length: 12, minTime: 101, maxTime: 120}}},
			},
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualRanges := metasToRanges(testCase.partitions, blockID, testCase.minT, testCase.maxT)
			assert.Equal(t, testCase.expectedRanges, actualRanges)
			assert.Equal(t, len(testCase.expectedRanges), cap(actualRanges)) // Assert that we've done the slice preallocation correctly. This won't always catch all incorrect or missing preallocations, but might catch some.
		})
	}
}

func TestPartitionChunks(t *testing.T) {
	testCases := map[string]struct {
		targetNumRanges, minChunksPerRange int
		input                              []chunks.Meta
		expectedPartitions                 [][]chunks.Meta
	}{
		"empty input chunks": {
			targetNumRanges: 10, minChunksPerRange: 3,
		},
		"even distribution into partitions": {
			targetNumRanges: 4, minChunksPerRange: 1,
			input: []chunks.Meta{
				{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
				{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
				{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
				{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
				{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
				{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
				{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
				{Ref: chunkRef(1, 12), MinTime: 23, MaxTime: 24},
			},
			expectedPartitions: [][]chunks.Meta{
				{
					{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
					{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
					{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				},
				{
					{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
					{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
					{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				},
				{
					{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
					{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
					{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
				},
				{
					{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
					{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
					{Ref: chunkRef(1, 12), MinTime: 23, MaxTime: 24},
				},
			},
		},
		"when number of chunks per range would be less than minChunksPerRange, then targetNumRanges isn't used": {
			targetNumRanges: 3, minChunksPerRange: 4,
			input: []chunks.Meta{
				{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
				{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
				{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
				{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
				{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
				{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
				{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
				{Ref: chunkRef(1, 12), MinTime: 23, MaxTime: 24},
			},
			expectedPartitions: [][]chunks.Meta{
				{
					{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
					{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
					{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
					{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				},
				{
					{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
					{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
					{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
					{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
				},
				{
					{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
					{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
					{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
					{Ref: chunkRef(1, 12), MinTime: 23, MaxTime: 24},
				},
			},
		},
		"remainder of division with targetNumRanges is put in the last range": {
			targetNumRanges: 3, minChunksPerRange: 1,
			input: []chunks.Meta{
				{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
				{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
				{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
				{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
				{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
				{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
				{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
			},
			expectedPartitions: [][]chunks.Meta{
				{
					{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
					{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
					{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				},
				{
					{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
					{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
					{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				},
				{
					{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
					{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
					{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
					{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
					{Ref: chunkRef(1, 11), MinTime: 21, MaxTime: 22},
				},
			},
		},
		"targetNumRanges can be exceeded when there are chunks from multiple segment files": {
			targetNumRanges: 3, minChunksPerRange: 1,
			input: []chunks.Meta{
				{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
				{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
				{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
				{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				{Ref: chunkRef(2, 1), MinTime: 15, MaxTime: 16},
				{Ref: chunkRef(2, 2), MinTime: 17, MaxTime: 18},
				{Ref: chunkRef(2, 3), MinTime: 19, MaxTime: 20},
				{Ref: chunkRef(2, 4), MinTime: 21, MaxTime: 22},
			},
			expectedPartitions: [][]chunks.Meta{
				{
					{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
					{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
					{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				},
				{
					{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
					{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
					{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				},
				{
					// This is an unfortunate case where one chunk is on its own because the next chunk is in a different segment file.
					{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				},
				{
					{Ref: chunkRef(2, 1), MinTime: 15, MaxTime: 16},
					{Ref: chunkRef(2, 2), MinTime: 17, MaxTime: 18},
					{Ref: chunkRef(2, 3), MinTime: 19, MaxTime: 20},
				},
				{
					{Ref: chunkRef(2, 4), MinTime: 21, MaxTime: 22},
				},
			},
		},
		"chunks from separate segment files get their own range even when below minChunksPerRange": {
			targetNumRanges: 3, minChunksPerRange: 1,
			input: []chunks.Meta{
				{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
				{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
				{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
				{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
				{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
				{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
				{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
				{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
				{Ref: chunkRef(2, 1), MinTime: 21, MaxTime: 22},
			},
			expectedPartitions: [][]chunks.Meta{
				{
					{Ref: chunkRef(1, 1), MinTime: 1, MaxTime: 2},
					{Ref: chunkRef(1, 2), MinTime: 3, MaxTime: 4},
					{Ref: chunkRef(1, 3), MinTime: 5, MaxTime: 6},
				},
				{
					{Ref: chunkRef(1, 4), MinTime: 7, MaxTime: 8},
					{Ref: chunkRef(1, 5), MinTime: 9, MaxTime: 10},
					{Ref: chunkRef(1, 6), MinTime: 11, MaxTime: 12},
				},
				{
					{Ref: chunkRef(1, 7), MinTime: 13, MaxTime: 14},
					{Ref: chunkRef(1, 8), MinTime: 15, MaxTime: 16},
					{Ref: chunkRef(1, 9), MinTime: 17, MaxTime: 18},
					{Ref: chunkRef(1, 10), MinTime: 19, MaxTime: 20},
				},
				{
					{Ref: chunkRef(2, 1), MinTime: 21, MaxTime: 22},
				},
			},
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			actualPartitions := partitionChunks(testCase.input, testCase.targetNumRanges, testCase.minChunksPerRange)
			assert.Equal(t, testCase.expectedPartitions, actualPartitions)
		})
	}
}

// TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching currently tests logic in loadingSeriesChunkRefsSetIterator.
// If openBlockSeriesChunkRefsSetsIterator becomes more complex, consider making this a test for loadingSeriesChunkRefsSetIterator only.
func TestOpenBlockSeriesChunkRefsSetsIterator_SeriesCaching(t *testing.T) {
	newTestBlock := prepareTestBlockWithBinaryReader(test.NewTB(t), func(tb testing.TB, appender storage.Appender) {
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
						true,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsColdCache,
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
						true,
						b.meta.MinTime,
						b.meta.MaxTime,
						statsWarnCache,
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

type forbiddenFetchMultiPostingsIndexCache struct {
	indexcache.IndexCache

	t *testing.T
}

func (c forbiddenFetchMultiPostingsIndexCache) FetchMultiPostings(ctx context.Context, userID string, blockID ulid.ULID, keys []labels.Label) (hits map[labels.Label][]byte, misses []labels.Label) {
	assert.Fail(c.t, "index cache FetchMultiPostings should not be called")
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

func generateSeriesChunksRanges(blockID ulid.ULID, numRanges int) []seriesChunkRefsRange {
	return generateSeriesChunksRangesN(blockID, 1, numRanges)
}

func generateSeriesChunksRangesN(blockID ulid.ULID, numChunksPerRange, numRanges int) []seriesChunkRefsRange {
	chunksRanges := make([]seriesChunkRefsRange, 0, numRanges)

	for i := 0; i < numRanges; i++ {
		refs := make([]seriesChunkRef, numChunksPerRange)
		for rIdx := range refs {
			refs[rIdx] = seriesChunkRef{segFileOffset: 10 * uint32(i),
				minTime: int64(i),
				maxTime: int64(i),
				length:  10,
			}
		}
		chunksRanges = append(chunksRanges, seriesChunkRefsRange{
			blockID:     blockID,
			segmentFile: 1,
			refs:        refs,
		})
	}

	return chunksRanges
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
		testCase := testCase
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
		testCase := testCase
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

func (c mockIndexCache) FetchSeriesForPostings(ctx context.Context, userID string, blockID ulid.ULID, shard *sharding.ShardSelector, postingsKey indexcache.PostingsKey) ([]byte, bool) {
	return c.fetchSeriesForPostingsResponse.contents, c.fetchSeriesForPostingsResponse.cached
}
