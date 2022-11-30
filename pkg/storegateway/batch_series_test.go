// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestDeduplicatingBatchSet(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are returned.
	c := generateSeriesChunkRef(8)

	series1 := labels.FromStrings("l1", "v1")
	series2 := labels.FromStrings("l1", "v2")
	series3 := labels.FromStrings("l1", "v3")
	sourceBatches := []seriesChunkRefsSet{
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
		repeatingBatchSet := newSliceSeriesChunkRefsSetIterator(nil, sourceBatches...)
		uniqueSet := newDeduplicatingBatchSet(1, repeatingBatchSet)
		batches := readAllSeriesChunkRefsSet(uniqueSet)

		require.NoError(t, uniqueSet.Err())
		require.Len(t, batches, 3)

		require.Len(t, batches[0].series, 1)
		assert.Equal(t, series1, batches[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, batches[0].series[0].chunks)

		require.Len(t, batches[1].series, 1)
		assert.Equal(t, series2, batches[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, batches[1].series[0].chunks)

		require.Len(t, batches[2].series, 1)
		assert.Equal(t, series3, batches[2].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, batches[2].series[0].chunks)
	})

	t.Run("batch size: 2", func(t *testing.T) {
		repeatingBatchSet := newSliceSeriesChunkRefsSetIterator(nil, sourceBatches...)
		uniqueSet := newDeduplicatingBatchSet(2, repeatingBatchSet)
		batches := readAllSeriesChunkRefsSet(uniqueSet)

		require.NoError(t, uniqueSet.Err())
		require.Len(t, batches, 2)

		// First batch.
		require.Len(t, batches[0].series, 2)

		assert.Equal(t, series1, batches[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, batches[0].series[0].chunks)

		assert.Equal(t, series2, batches[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, batches[0].series[1].chunks)

		// Second batch.
		require.Len(t, batches[1].series, 1)

		assert.Equal(t, series3, batches[1].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, batches[1].series[0].chunks)
	})

	t.Run("batch size: 3", func(t *testing.T) {
		repeatingBatchSet := newSliceSeriesChunkRefsSetIterator(nil, sourceBatches...)
		uniqueSet := newDeduplicatingBatchSet(3, repeatingBatchSet)
		batches := readAllSeriesChunkRefsSet(uniqueSet)

		require.NoError(t, uniqueSet.Err())
		require.Len(t, batches, 1)
		require.Len(t, batches[0].series, 3)

		assert.Equal(t, series1, batches[0].series[0].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3], c[4]}, batches[0].series[0].chunks)

		assert.Equal(t, series2, batches[0].series[1].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1], c[2], c[3]}, batches[0].series[1].chunks)

		assert.Equal(t, series3, batches[0].series[2].lset)
		assert.Equal(t, []seriesChunkRef{c[0], c[1]}, batches[0].series[2].chunks)
	})
}

func TestDeduplicatingBatchSet_PropagatesErrors(t *testing.T) {
	chainedSet := newDeduplicatingBatchSet(100, newSliceSeriesChunkRefsSetIterator(errors.New("something went wrong"), seriesChunkRefsSet{
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

func TestMergedBatchSet(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are merged.
	c := generateSeriesChunkRef(6)

	testCases := map[string]struct {
		batchSize       int
		set1, set2      seriesChunkRefsSetIterator
		expectedBatches []seriesChunkRefsSet
		expectedErr     string
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[0], c[0], c[2], c[3]}},
				}},
			},
		},
		"merges two sets where the first is empty": {
			batchSize: 100,
			set1:      emptyBatchSet{},
			set2: newSliceSeriesChunkRefsSetIterator(nil, seriesChunkRefsSet{
				series: []seriesChunkRefs{
					{lset: labels.FromStrings("l1", "v1"), chunks: []seriesChunkRef{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []seriesChunkRef{c[1]}},
				},
			}),
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
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
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
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
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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
			expectedBatches: []seriesChunkRefsSet{
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

			mergedSet := newMergedBatchSet(testCase.batchSize, testCase.set1, testCase.set2)
			batches := readAllSeriesChunkRefsSet(mergedSet)

			if testCase.expectedErr != "" {
				assert.EqualError(t, mergedSet.Err(), testCase.expectedErr)
			} else {
				assert.NoError(t, mergedSet.Err())
			}

			require.Len(t, batches, len(testCase.expectedBatches))
			for batchIdx, expectedBatch := range testCase.expectedBatches {
				require.Len(t, batches[batchIdx].series, len(expectedBatch.series))
				for expectedSeriesIdx, expectedSeries := range expectedBatch.series {
					assert.Equal(t, expectedSeries.lset, batches[batchIdx].series[expectedSeriesIdx].lset)
					assert.Equal(t, expectedSeries.chunks, batches[batchIdx].series[expectedSeriesIdx].chunks)
				}
			}
		})
	}
}

func TestChainedBatchSetIterator(t *testing.T) {
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

			chainedSet := newChainedSeriesSet(testCase.input)
			actual := readAllSeriesChunkRefs(chainedSet)
			require.NoError(t, chainedSet.Err())
			assert.Equal(t, testCase.expected, actual)
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

func TestPreloadingBatchSet(t *testing.T) {
	test.VerifyNoLeak(t)

	const delay = 10 * time.Millisecond

	// Create some batches, each batch containing 1 series.
	batches := make([]seriesChunksSet, 0, 10)
	for i := 0; i < 10; i++ {
		batches = append(batches, seriesChunksSet{
			series: []seriesEntry{{
				lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
				refs: []chunks.ChunkRef{chunks.ChunkRef(i)},
			}},
		})
	}

	t.Run("should iterate all batches if no error occurs", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(batches)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIterator(batches...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingBatchSet(context.Background(), preloadSize, source)

				// Ensure expected batches are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, batches[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure all batches have been returned.
				require.NoError(t, preloading.Err())
				require.Equal(t, len(batches), expectedIdx)
			})
		}
	})

	t.Run("should stop iterating once an error is found", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(batches)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(batches), batches...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingBatchSet(context.Background(), preloadSize, source)

				// Ensure expected batches are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, batches[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure an error is returned at the end.
				require.Error(t, preloading.Err())
				require.Equal(t, len(batches), expectedIdx)
			})
		}
	})

	t.Run("should not leak preloading goroutine if caller doesn't iterated until the end of batches but context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(batches), batches...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingBatchSet(ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, batches[0], preloading.At())

		// Cancel the context.
		cancelCtx()

		// Give a short time to the preloader goroutine to react to the context cancellation.
		// This is required to avoid a flaky test.
		time.Sleep(100 * time.Millisecond)

		// At this point we expect Next() to return false.
		require.False(t, preloading.Next())
		require.NoError(t, preloading.Err())
	})

	t.Run("should not leak preloading goroutine if caller doesn't call Next() until false but context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(batches), batches...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingBatchSet(ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, batches[0], preloading.At())

		// Cancel the context. Do NOT call Next() after canceling the context.
		cancelCtx()
	})
}

func TestPreloadingBatchSet_Concurrency(t *testing.T) {
	const (
		numRuns     = 100
		numBatches  = 100
		preloadSize = 10
	)

	// Create some batches.
	batches := make([]seriesChunksSet, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		batches = append(batches, seriesChunksSet{
			series: []seriesEntry{{
				lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
			}},
		})
	}

	// Run many times to increase the likelihood to find a race (if any).
	for i := 0; i < numRuns; i++ {
		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(batches), batches...)
		preloading := newPreloadingBatchSet(context.Background(), preloadSize, source)

		for preloading.Next() {
			require.NoError(t, preloading.Err())
			require.NotZero(t, preloading.At())
		}
		require.Error(t, preloading.Err())
	}

}

func TestBlockSeriesChunkRefsSetsIterator_ErrorPropagation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	suite := prepareStoreWithTestBlocks(t, t.TempDir(), objstore.NewInMemBucket(), false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0))
	var firstBlock *bucketBlock
	for _, b := range suite.store.blocks {
		firstBlock = b
		break
	}
	suite.cache.SwapWith(noopCache{})

	indexReader := firstBlock.indexReader()
	defer indexReader.Close()

	matcher, err := labels.NewMatcher(labels.MatchRegexp, "a", ".+")
	require.NoError(t, err)

	iterator, err := openBlockSeriesChunkRefsSetsIterator(
		ctx,
		100,
		indexReader,
		firstBlock.meta.ULID,
		[]*labels.Matcher{matcher},
		nil,
		suite.store.seriesHashCache.GetBlockCache(firstBlock.meta.ULID.String()),
		&limiter{limit: 1},
		&limiter{limit: 100000},
		false,
		firstBlock.meta.MinTime,
		firstBlock.meta.MaxTime,
		nil,
		newSafeQueryStats(),
		suite.logger,
	)
	require.NoError(t, err)

	_ = readAllSeriesChunkRefsSet(iterator)
	assert.ErrorContains(t, iterator.Err(), "test limit exceeded")
}

func TestBatchedSeriesSet(t *testing.T) {
	c := generateAggrChunk(6)

	series1 := labels.FromStrings(labels.MetricName, "metric_1")
	series2 := labels.FromStrings(labels.MetricName, "metric_2")
	series3 := labels.FromStrings(labels.MetricName, "metric_3")
	series4 := labels.FromStrings(labels.MetricName, "metric_4")
	series5 := labels.FromStrings(labels.MetricName, "metric_4")

	// Utility function to create sets, so that each test starts from a clean setup (e.g. releaser is not released).
	createSets := func() (sets []seriesChunksSet, releasers []*releaserMock) {
		for i := 0; i < 3; i++ {
			releasers = append(releasers, newReleaserMock())
		}

		sets = append(sets,
			seriesChunksSet{
				chunksReleaser: releasers[0],
				series: []seriesEntry{
					{lset: series1, chks: []storepb.AggrChunk{c[1]}},
					{lset: series2, chks: []storepb.AggrChunk{c[2]}},
				}},
			seriesChunksSet{
				chunksReleaser: releasers[1],
				series: []seriesEntry{
					{lset: series3, chks: []storepb.AggrChunk{c[3]}},
					{lset: series4, chks: []storepb.AggrChunk{c[4]}},
				}},
			seriesChunksSet{
				chunksReleaser: releasers[2],
				series: []seriesEntry{
					{lset: series5, chks: []storepb.AggrChunk{c[5]}},
				}},
		)

		return
	}

	t.Run("should iterate over a single set and release it once done", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0])
		it := newBatchedSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should iterate over a multiple sets and release each set once we begin to iterate the next one", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0], sets[1])
		it := newBatchedSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		// Set 1.
		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		// Set 2.
		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series3, lbls)
		require.Equal(t, []storepb.AggrChunk{c[3]}, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series4, lbls)
		require.Equal(t, []storepb.AggrChunk{c[4]}, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
		require.False(t, releasers[1].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should release the current set on error", func(t *testing.T) {
		expectedErr := errors.New("mocked error")

		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIteratorWithError(expectedErr, 1, sets[0], sets[1], sets[2])
		it := newBatchedSeriesSet(source)

		lbls, chks := it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.NoError(t, it.Err())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series1, lbls)
		require.Equal(t, []storepb.AggrChunk{c[1]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.True(t, it.Next())
		lbls, chks = it.At()
		require.Equal(t, series2, lbls)
		require.Equal(t, []storepb.AggrChunk{c[2]}, chks)
		require.NoError(t, it.Err())
		require.False(t, releasers[0].isReleased())

		require.False(t, it.Next())
		lbls, chks = it.At()
		require.Zero(t, lbls)
		require.Zero(t, chks)
		require.Equal(t, expectedErr, it.Err())

		// The current set is released.
		require.True(t, releasers[0].isReleased())

		// Can't release the next ones because can't move forward with the iteration (due to the error).
		require.False(t, releasers[1].isReleased())
		require.False(t, releasers[2].isReleased())
	})
}

func TestLoadingBatchSet(t *testing.T) {
	type testBlock struct {
		ulid   ulid.ULID
		series []seriesEntry
	}

	block1 := testBlock{
		ulid:   ulid.MustNew(1, nil),
		series: generateSeriesEntriesWithChunks(t, 10),
	}

	block2 := testBlock{
		ulid:   ulid.MustNew(2, nil),
		series: generateSeriesEntriesWithChunks(t, 10),
	}

	toSeriesChunkRefs := func(block testBlock, seriesIndex int) seriesChunkRefs {
		series := block.series[seriesIndex]

		chunkRefs := make([]seriesChunkRef, len(series.chks))
		for i, c := range series.chks {
			chunkRefs[i] = seriesChunkRef{
				blockID: block.ulid,
				ref:     series.refs[i],
				minTime: c.MinTime,
				maxTime: c.MaxTime,
			}
		}

		return seriesChunkRefs{
			lset:   series.lset,
			chunks: chunkRefs,
		}
	}

	testCases := map[string]struct {
		existingBlocks      []testBlock
		setsToLoad          []seriesChunkRefsSet
		expectedSets        []seriesChunksSet
		addLoadErr, loadErr error
		expectedErr         string
	}{
		"loads single set from single block": {
			existingBlocks: []testBlock{block1},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
			},
			expectedSets: []seriesChunksSet{
				{series: []seriesEntry{block1.series[0], block1.series[1]}},
			},
		},
		"loads multiple sets from single block": {
			existingBlocks: []testBlock{block1},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 2), toSeriesChunkRefs(block1, 3)}},
			},
			expectedSets: []seriesChunksSet{
				{series: []seriesEntry{block1.series[0], block1.series[1]}},
				{series: []seriesEntry{block1.series[2], block1.series[3]}},
			},
		},
		"loads single set from multiple blocks": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
			},
			expectedSets: []seriesChunksSet{
				{series: []seriesEntry{block1.series[0], block1.series[1]}},
			},
		},
		"loads multiple sets from multiple blocks": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
				{series: []seriesChunkRefs{toSeriesChunkRefs(block2, 0), toSeriesChunkRefs(block2, 1)}},
			},
			expectedSets: []seriesChunksSet{
				{series: []seriesEntry{block1.series[0], block1.series[1]}},
				{series: []seriesEntry{block2.series[0], block2.series[1]}},
			},
		},
		"loads sets from multiple blocks mixed": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block2, 0)}},
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 1), toSeriesChunkRefs(block2, 1)}},
			},
			expectedSets: []seriesChunksSet{
				{series: []seriesEntry{block1.series[0], block2.series[0]}},
				{series: []seriesEntry{block1.series[1], block2.series[1]}},
			},
		},
		"loads series with chunks from different blocks": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: func() []seriesChunkRefs {
					series := toSeriesChunkRefs(block1, 0)
					series.chunks = append(series.chunks, toSeriesChunkRefs(block2, 0).chunks...)
					return []seriesChunkRefs{series}
				}()},
			},
			expectedSets: []seriesChunksSet{
				{series: func() []seriesEntry {
					entry := block1.series[0]
					entry.chks = append(entry.chks, block2.series[0].chks...)
					return []seriesEntry{entry}
				}()},
			},
		},
		"handles error in addLoad": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
				{series: []seriesChunkRefs{toSeriesChunkRefs(block2, 0), toSeriesChunkRefs(block2, 1)}},
			},
			expectedSets: []seriesChunksSet{},
			addLoadErr:   errors.New("test err"),
			expectedErr:  "test err",
		},
		"handles error in load": {
			existingBlocks: []testBlock{block1, block2},
			setsToLoad: []seriesChunkRefsSet{
				{series: []seriesChunkRefs{toSeriesChunkRefs(block1, 0), toSeriesChunkRefs(block1, 1)}},
				{series: []seriesChunkRefs{toSeriesChunkRefs(block2, 0), toSeriesChunkRefs(block2, 1)}},
			},
			expectedSets: []seriesChunksSet{},
			loadErr:      errors.New("test err"),
			expectedErr:  "test err",
		},
	}

	for testName, testCase := range testCases {
		testName, testCase := testName, testCase
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			// Setup
			bytesPool := &mockedPool{parent: pool.NoopBytes{}}
			chunkBytes := &pool.BatchBytes{Delegate: bytesPool}
			readersMap := make(map[ulid.ULID]chunkReader, len(testCase.existingBlocks))
			for _, block := range testCase.existingBlocks {
				readersMap[block.ulid] = newFakeChunkReaderWithSeries(block.series, testCase.addLoadErr, testCase.loadErr, chunkBytes)
			}
			readers := newChunkReaders(readersMap, chunkBytes, bytesPool)

			// Run test
			set := newLoadingBatchSet(*readers, newSliceSeriesChunkRefsSetIterator(nil, testCase.setsToLoad...), newSafeQueryStats())
			loadedSets := readAllSeriesChunksSets(set)

			// Assertions
			if testCase.expectedErr != "" {
				assert.ErrorContains(t, set.Err(), testCase.expectedErr)
			} else {
				assert.NoError(t, set.Err())
			}
			// NoopBytes should allocate slices just the right size, so the packing optimization in BatchBytes should not be used
			// This allows to assert on the exact number of bytes allocated.
			var expectedReservedBytes int
			for _, set := range testCase.expectedSets {
				for _, s := range set.series {
					for _, c := range s.chks {
						expectedReservedBytes += len(c.Raw.Data)
					}
				}
			}
			assert.Equal(t, expectedReservedBytes, int(bytesPool.balance.Load()))

			// Check that chunks bytes are what we expect
			require.Len(t, loadedSets, len(testCase.expectedSets))
			for i, loadedSet := range loadedSets {
				require.Len(t, loadedSet.series, len(testCase.expectedSets[i].series))
				for j, loadedSeries := range loadedSet.series {
					assert.ElementsMatch(t, testCase.expectedSets[i].series[j].chks, loadedSeries.chks)
					assert.Truef(t, labels.Equal(testCase.expectedSets[i].series[j].lset, loadedSeries.lset),
						"%d, %d: labels don't match, expected %s, got %s", i, j, testCase.expectedSets[i].series[j].lset, loadedSeries.lset,
					)
				}
			}

			// Release the sets and expect that they also return their chunk bytes to the pool
			for _, s := range loadedSets {
				s.release()
			}
			assert.Zero(t, int(bytesPool.balance.Load()))
		})
	}
}

type chunkReaderMock struct {
	chunks              map[chunks.ChunkRef]storepb.AggrChunk
	addLoadErr, loadErr error

	chunkBytes *pool.BatchBytes
	toLoad     map[chunks.ChunkRef]loadIdx
}

func newFakeChunkReaderWithSeries(series []seriesEntry, addLoadErr, loadErr error, chunkBytes *pool.BatchBytes) *chunkReaderMock {
	chks := map[chunks.ChunkRef]storepb.AggrChunk{}
	for _, s := range series {
		for i := range s.chks {
			chks[s.refs[i]] = s.chks[i]
		}
	}
	return &chunkReaderMock{
		chunks:     chks,
		addLoadErr: addLoadErr,
		loadErr:    loadErr,
		chunkBytes: chunkBytes,
		toLoad:     make(map[chunks.ChunkRef]loadIdx),
	}
}

func (f *chunkReaderMock) Close() error {
	return nil
}

func (f *chunkReaderMock) addLoad(id chunks.ChunkRef, seriesEntry, chunk int) error {
	if f.addLoadErr != nil {
		return f.addLoadErr
	}
	f.toLoad[id] = loadIdx{seriesEntry: seriesEntry, chunk: chunk}
	return nil
}

func (f *chunkReaderMock) load(result []seriesEntry, _ []storepb.Aggr, _ *safeQueryStats) error {
	if f.loadErr != nil {
		return f.loadErr
	}
	for chunkRef, indices := range f.toLoad {
		// Take bytes from the pool, so we can assert on number of allocations and that frees are happening
		chunkData := f.chunks[chunkRef].Raw.Data
		copiedChunkData, err := f.chunkBytes.Get(len(chunkData))
		if err != nil {
			return fmt.Errorf("couldn't copy test data: %w", err)
		}
		copy(copiedChunkData, chunkData)
		result[indices.seriesEntry].chks[indices.chunk].Raw = &storepb.Chunk{Data: copiedChunkData}
	}
	return nil
}

func (f *chunkReaderMock) reset(chunkBytes *pool.BatchBytes) {
	f.chunkBytes = chunkBytes
	f.toLoad = make(map[chunks.ChunkRef]loadIdx)
}

// nolint this is used in a skipped test
type limiter struct {
	limit   uint64
	current atomic.Uint64
}

// nolint this is used in a skipped test
func (l *limiter) Reserve(num uint64) error {
	if l.current.Add(num) > l.limit {
		return errors.New("test limit exceeded")
	}
	return nil
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

func readAllSeriesChunksSets(it seriesChunksSetIterator) []seriesChunksSet {
	var out []seriesChunksSet
	for it.Next() {
		out = append(out, it.At())
	}
	return out
}

func readAllSeriesLabels(it storepb.SeriesSet) []labels.Labels {
	var out []labels.Labels
	for it.Next() {
		lbls, _ := it.At()
		out = append(out, lbls)
	}
	return out
}

// generateSeriesEntriesWithChunks generates seriesEntries with chunks. Each chunk is a random byte slice.
func generateSeriesEntriesWithChunks(t *testing.T, numSeries int) []seriesEntry {
	const numChunksPerSeries = 2

	out := make([]seriesEntry, 0, numSeries)
	labels := generateSeries([]int{numSeries})

	for i := 0; i < numSeries; i++ {
		entry := seriesEntry{
			lset: labels[i],
			refs: make([]chunks.ChunkRef, 0, numChunksPerSeries),
			chks: make([]storepb.AggrChunk, 0, numChunksPerSeries),
		}

		for j := 0; j < numChunksPerSeries; j++ {
			chunkBytes := make([]byte, 10)
			readBytes, err := rand.Read(chunkBytes)
			require.NoError(t, err, "couldn't generate test data")
			require.Equal(t, 10, readBytes, "couldn't generate test data")

			entry.refs = append(entry.refs, chunks.ChunkRef(i*numChunksPerSeries+j))
			entry.chks = append(entry.chks, storepb.AggrChunk{
				MinTime: int64(10 * j),
				MaxTime: int64(10 * (j + 1)),
				Raw:     &storepb.Chunk{Data: chunkBytes},
			})
		}
		out = append(out, entry)
	}
	return out
}

type releaserMock struct {
	released *atomic.Bool
}

func newReleaserMock() *releaserMock {
	return &releaserMock{
		released: atomic.NewBool(false),
	}
}

func (r *releaserMock) Release() {
	r.released.Store(true)
}

func (r *releaserMock) isReleased() bool {
	return r.released.Load()
}
