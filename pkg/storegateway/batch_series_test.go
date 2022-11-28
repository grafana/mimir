package storegateway

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
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
	testCases := map[string]struct {
		set1, set2      unloadedBatchSet
		expectedBatches []unloadedBatch
		expectedErr     string
	}{
		"merges two batches without overlap": {
			set1: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 3)},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 3)},
				}},
			},
		},
		"merges two batches with last series from each overlapping": {
			set1: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 2)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 3)},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 2)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 4)}, // the chunks of two series with the same label set are merged
				}},
			},
		},
		"merges two batches where the first is empty": {
			set1: emptyBatchSet{},
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				}},
			},
		},
		"merges two batches with first one erroring at the end": {
			set1: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			set2: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 2)},
				}},
			},
			expectedErr: "something went wrong",
		},
		"merges two batches with second one erroring at the end": {
			set1: newSliceUnloadedBatchSet(nil, unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			set2: newSliceUnloadedBatchSet(errors.New("something went wrong"), unloadedBatch{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v3"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v4"), chunks: make([]unloadedChunk, 1)},
				},
			}),
			expectedBatches: []unloadedBatch{
				{Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 2)},
					{lset: labels.FromStrings("l1", "v3"), chunks: make([]unloadedChunk, 1)},
					{lset: labels.FromStrings("l1", "v4"), chunks: make([]unloadedChunk, 1)},
				}},
			},
			expectedErr: "something went wrong",
		},
	}

	for name, testCase := range testCases {
		name, testCase := name, testCase
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mergedSet := newMergedBatchSet(100, testCase.set1, testCase.set2)
			batches := readAllBatches(mergedSet)

			if testCase.expectedErr != "" {
				assert.EqualError(t, mergedSet.Err(), testCase.expectedErr)
			} else {
				assert.NoError(t, mergedSet.Err())
			}

			assert.Len(t, batches, len(testCase.expectedBatches))
			for batchIdx, expectedBatch := range testCase.expectedBatches {
				assert.Len(t, batches[batchIdx].Entries, len(expectedBatch.Entries))
				for entryIdx, entry := range expectedBatch.Entries {
					assert.Len(t, batches[batchIdx].Entries[entryIdx].chunks, len(entry.chunks))
					assert.Zero(t, labels.Compare(batches[batchIdx].Entries[entryIdx].lset, entry.lset), "series labels don't match")
				}
			}
		})
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
