package storegateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/util/test"
)

// sliceUnloadedBatchSet implement unloadedBatchSet and
// returns the provided err when the batches are exhausted
type sliceUnloadedBatchSet struct {
	current int
	batches []unloadedBatch

	err error
}

func newSliceUnloadedBatchSet(err error, batches ...unloadedBatch) *sliceUnloadedBatchSet {
	for i := range batches {
		if batches[i].Stats == nil {
			batches[i].Stats = newSafeQueryStats()
		}
	}
	return &sliceUnloadedBatchSet{
		current: -1,
		batches: batches,
		err:     err,
	}
}

func (s *sliceUnloadedBatchSet) Next() bool {
	s.current++
	return s.current < len(s.batches)
}

func (s *sliceUnloadedBatchSet) At() unloadedBatch {
	return s.batches[s.current]
}

func (s *sliceUnloadedBatchSet) Err() error {
	if s.current >= len(s.batches) {
		return s.err
	}
	return nil
}

// sliceLoadedBatchSet implement loadedBatchSet and
// returns the provided err when the batches are exhausted
type sliceLoadedBatchSet struct {
	current int
	batches []loadedBatch

	err error
}

func newSliceLoadedBatchSet(err error, batches ...loadedBatch) *sliceLoadedBatchSet {
	return &sliceLoadedBatchSet{
		current: -1,
		batches: batches,
		err:     err,
	}
}

func (s *sliceLoadedBatchSet) Next() bool {
	s.current++
	return s.current < len(s.batches)
}

func (s *sliceLoadedBatchSet) At() loadedBatch {
	return s.batches[s.current]
}

func (s *sliceLoadedBatchSet) Err() error {
	if s.current >= len(s.batches) {
		return s.err
	}
	return nil
}

func TestDeduplicatingBatchSet(t *testing.T) {
	repeatingBatchSet := newSliceUnloadedBatchSet(nil,
		unloadedBatch{
			Entries: []unloadedBatchEntry{
				{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 2)},
				{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 3)},
			}},
		unloadedBatch{
			Entries: []unloadedBatchEntry{
				{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 4)},
				{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 4)},
			}},
	)
	uniqueSet := newDeduplicatingBatchSet(100, repeatingBatchSet)

	batches := readAllBatches(uniqueSet)

	assert.NoError(t, uniqueSet.Err())
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0].Entries, 2)
	assert.Len(t, batches[0].Entries[0].chunks, 5)
	assert.Len(t, batches[0].Entries[1].chunks, 8)
}

func TestDeduplicatingBatchSet_PropagatesErrors(t *testing.T) {
	chainedSet := newDeduplicatingBatchSet(100, newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
		Entries: []unloadedBatchEntry{
			{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
			{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
			{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
		},
	}))

	for chainedSet.Next() {
	}

	assert.ErrorContains(t, chainedSet.Err(), "something went wrong")
}

func TestMergedBatchSet(t *testing.T) {
	// Generate some chunk fixtures so that we can ensure the right chunks are merged.
	var c []unloadedChunk
	for i := 0; i < 4; i++ {
		c = append(c, unloadedChunk{
			BlockID: ulid.MustNew(uint64(i), nil),
			Ref:     chunks.ChunkRef(i),
		})
	}

	testCases := map[string]struct {
		batchSize       int
		set1, set2      unloadedBatchSet
		expectedBatches []unloadedBatch
		expectedErr     string
	}{
		"merges two batches without overlap": {
			batchSize: 100,
			set1: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1], c[2], c[3]}},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1], c[2], c[3]}},
				}},
			},
		},
		"merges two batches with last series from each overlapping": {
			batchSize: 100,
			set1: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[0]}},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[0], c[2], c[3]}},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[1]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[0], c[2], c[3], c[0]}},
				}},
			},
		},
		"merges two batches where the first is empty": {
			batchSize: 100,
			set1:      emptyBatchSet{},
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
				}},
			},
		},
		"merges two batches with first one erroring at the end": {
			batchSize: 100,
			set1: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
				},
			}),
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
		},
		"merges two batches with second one erroring at the end": {
			batchSize: 100,
			set1: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
				},
			}),
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
		},
		"merges two batches with shorter one erroring at the end": {
			batchSize: 100,
			set1: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v3"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v4"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			expectedBatches: nil, // We expect no returned batches because an error occurred while creating the first one.
			expectedErr:     "something went wrong",
		},
		"should stop iterating as soon as the first underlying set returns an error": {
			batchSize: 1, // Use a batch size of 1 in this test so that we can see when the iteration stops.
			set1: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
					{lset: labels.FromStrings("l1", "v3"), chunks: []unloadedChunk{c[2]}},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
					{lset: labels.FromStrings("l1", "v4"), chunks: []unloadedChunk{c[3]}},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: []unloadedChunk{c[0]}},
				}},
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: []unloadedChunk{c[1]}},
				}},
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v3"), chunks: []unloadedChunk{c[2]}},
				}},
			},
			expectedErr: "something went wrong",
		},
	}

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mergedSet := newMergedBatchSet(testCase.batchSize, testCase.set1, testCase.set2)
			batches := readAllBatches(mergedSet)

			if testCase.expectedErr != "" {
				assert.EqualError(t, mergedSet.Err(), testCase.expectedErr)
			} else {
				assert.NoError(t, mergedSet.Err())
			}

			assert.Len(t, batches, len(testCase.expectedBatches))
			for batchIdx, expectedBatch := range testCase.expectedBatches {
				assert.Len(t, batches[batchIdx].Entries, len(expectedBatch.Entries))
				for expectedSeriesIdx, expectedSeries := range expectedBatch.Entries {
					assert.Equal(t, expectedSeries.lset, batches[batchIdx].Entries[expectedSeriesIdx].lset)
					assert.Equal(t, expectedSeries.chunks, batches[batchIdx].Entries[expectedSeriesIdx].chunks)
				}
			}
		})
	}
}

func TestPreloadingBatchSet(t *testing.T) {
	test.VerifyNoLeak(t)

	// Create some batches, each batch containing 1 series.
	batches := make([]loadedBatch, 0, 10)
	for i := 0; i < 10; i++ {
		batches = append(batches, loadedBatch{
			Entries: []seriesEntry{{
				lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
				refs: []chunks.ChunkRef{chunks.ChunkRef(i)},
			}},
		})
	}

	t.Run("should iterate all batches if no error occurs", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(batches)+1; preloadSize++ {
			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				source := newSliceLoadedBatchSet(nil, batches...)
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
			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				source := newSliceLoadedBatchSet(errors.New("mocked error"), batches...)
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
		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceLoadedBatchSet(errors.New("mocked error"), batches...)
		preloading := newPreloadingBatchSet(ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, batches[0], preloading.At())

		// Cancel the context. At this point we expect Next() to return false.
		cancelCtx()
		require.False(t, preloading.Next())
		require.NoError(t, preloading.Err())
	})

	t.Run("should not leak preloading goroutine if caller doesn't call Next() until false but context is canceled", func(t *testing.T) {
		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceLoadedBatchSet(errors.New("mocked error"), batches...)
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
	batches := make([]loadedBatch, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		batches = append(batches, loadedBatch{
			Entries: []seriesEntry{{
				lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
			}},
		})
	}

	// Run many times to increase the likelihood to find a race (if any).
	for i := 0; i < numRuns; i++ {
		source := newSliceLoadedBatchSet(errors.New("mocked error"), batches...)
		preloading := newPreloadingBatchSet(context.Background(), preloadSize, source)

		for preloading.Next() {
			require.NoError(t, preloading.Err())
			require.NotZero(t, preloading.At())
		}
		require.Error(t, preloading.Err())
	}

}

func TestBucketBatchSet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	suite := prepareStoreWithTestBlocks(t, t.TempDir(), objstore.NewInMemBucket(), false, NewChunksLimiterFactory(0), NewSeriesLimiterFactory(0))
	var firstBlock *bucketBlock
	for _, b := range suite.store.blocks {
		firstBlock = b
		break
	}

	indexReader := firstBlock.indexReader()
	defer indexReader.Close()

	batchSet, err := unloadedBucketBatches(
		ctx,
		100,
		indexReader,
		firstBlock.meta.ULID,
		[]*labels.Matcher{{Type: labels.MatchRegexp, Name: "a", Value: ".+"}},
		nil,
		suite.store.seriesHashCache.GetBlockCache(firstBlock.meta.ULID.String()),
		&limiter{limit: 1},
		&limiter{limit: 100000},
		false,
		firstBlock.meta.MinTime,
		firstBlock.meta.MaxTime,
		nil,
		suite.logger,
	)
	require.NoError(t, err)

	_ = readAllBatches(batchSet)
	assert.ErrorContains(t, batchSet.Err(), "test limit exceeded")
}

type limiter struct {
	limit   uint64
	current atomic.Uint64
}

func (l *limiter) Reserve(num uint64) error {
	if l.current.Add(num) > l.limit {
		return errors.New("test limit exceeded")
	}
	return nil
}

func readAllBatches(set unloadedBatchSet) []unloadedBatch {
	var batches []unloadedBatch
	for set.Next() {
		batches = append(batches, set.At())
	}
	return batches
}
