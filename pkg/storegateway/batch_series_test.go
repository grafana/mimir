package storegateway

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
)

type sliceUnloadedBatchSet struct {
	current int
	batches []unloadedBatch
}

func (s *sliceUnloadedBatchSet) Next() bool {
	s.current++
	return s.current < len(s.batches)
}

func (s *sliceUnloadedBatchSet) At() unloadedBatch {
	return s.batches[s.current]
}

func (s *sliceUnloadedBatchSet) Err() error {
	return nil
}

func TestDeduplicatingBatchSet(t *testing.T) {
	repeatingBatchSet := &sliceUnloadedBatchSet{
		current: -1,
		batches: []unloadedBatch{
			{Entries: []unloadedBatchEntry{
				{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 2)},
				{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 3)},
			}},
			{Entries: []unloadedBatchEntry{
				{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 4)},
				{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 4)},
			}},
		},
	}
	uniqueSet := newDeduplicatingBatchSet(100, repeatingBatchSet)

	batches := readAllBatches(uniqueSet)

	assert.NoError(t, uniqueSet.Err())
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0].Entries, 2)
	assert.Len(t, batches[0].Entries[0].chunks, 5)
	assert.Len(t, batches[0].Entries[1].chunks, 8)
}

func TestMergedBatchSet(t *testing.T) {
	batchSet1 := &sliceUnloadedBatchSet{
		current: -1,
		batches: []unloadedBatch{
			{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 1)},
				},
				Stats: newSafeQueryStats(),
			},
		},
	}

	batchSet2 := &sliceUnloadedBatchSet{
		current: -1,
		batches: []unloadedBatch{
			{
				Entries: []unloadedBatchEntry{
					{lset: labels.FromStrings("l1", "v1"), chunks: make([]unloadedChunk, 2)},
					{lset: labels.FromStrings("l1", "v2"), chunks: make([]unloadedChunk, 3)},
				},
				Stats: newSafeQueryStats(),
			},
		},
	}

	mergedSet := newMergedBatchSet(100, batchSet1, batchSet2)
	batches := readAllBatches(mergedSet)

	assert.NoError(t, mergedSet.Err())
	assert.Len(t, batches, 1)
	assert.Len(t, batches[0].Entries, 2)
	assert.Len(t, batches[0].Entries[0].chunks, 2)
	assert.Zero(t, labels.Compare(batches[0].Entries[0].lset, labels.FromStrings("l1", "v1")), "first series labels don't match")
	assert.Len(t, batches[0].Entries[1].chunks, 4)
	assert.Zero(t, labels.Compare(batches[0].Entries[1].lset, labels.FromStrings("l1", "v2")), "second series labels don't match")
}

func readAllBatches(set unloadedBatchSet) []unloadedBatch {
	var batches []unloadedBatch
	for set.Next() {
		batches = append(batches, set.At())
	}
	return batches
}
