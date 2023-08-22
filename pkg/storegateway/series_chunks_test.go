// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/pkg/util/test"
)

func init() {
	// Track the balance of gets/puts in pools in all tests.
	seriesChunksSlicePool = &pool.TrackedPool{Parent: seriesChunksSlicePool}
	chunksSlicePool = &pool.TrackedPool{Parent: chunksSlicePool}
	chunkBytesSlicePool = &pool.TrackedPool{Parent: chunkBytesSlicePool}
}

func TestSeriesChunksSet(t *testing.T) {
	t.Run("newSeriesChunksSet() should guarantee requested capacity", func(t *testing.T) {
		const (
			numRuns     = 1000
			minCapacity = 10
			maxCapacity = 1000
		)

		for r := 0; r < numRuns; r++ {
			capacity := minCapacity + rand.Intn(maxCapacity-minCapacity)
			set := newSeriesChunksSet(capacity, true)
			require.GreaterOrEqual(t, cap(set.series), capacity)

			set.release()
		}
	})

	t.Run("release() should reset the series and chunk entries before putting them back to the pool", func(t *testing.T) {
		const (
			numRuns            = 1000
			numSeries          = 10
			numChunksPerSeries = 10
		)

		// Reset the memory pool tracker.
		seriesChunksSlicePool.(*pool.TrackedPool).Reset()

		for r := 0; r < numRuns; r++ {
			set := newSeriesChunksSet(numSeries, true)

			// Ensure the series slice is made of all zero values. Then write something inside before releasing it again.
			// The slice is expected to be picked from the pool, at least in some runs (there's an assertion on it at
			// the end of the test).
			set.series = set.series[:numSeries]
			for i := 0; i < numSeries; i++ {
				require.Zero(t, set.series[i])

				set.series[i].lset = labels.FromStrings(labels.MetricName, "metric")
				set.series[i].chks = set.newSeriesAggrChunkSlice(numChunksPerSeries)
			}

			// Do the same with the chunks.
			for i := 0; i < numSeries; i++ {
				set.series[i].chks = set.newSeriesAggrChunkSlice(numChunksPerSeries)
				for j := 0; j < numChunksPerSeries; j++ {
					require.Equal(t, storepb.AggrChunk{}, set.series[i].chks[j])

					set.series[i].chks[j].MinTime = 10
					set.series[i].chks[j].MaxTime = 10
					set.series[i].chks[j].Raw = &storepb.Chunk{Data: []byte{1, 2, 3}}
				}
			}

			set.release()
		}

		// Ensure at least 1 series slice has been pulled from the pool.
		assert.Greater(t, seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
		assert.Zero(t, seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load())
	})

	t.Run("newSeriesAggrChunkSlice() should allocate slices from the pool and release() should put it back if the set is releasable", func(t *testing.T) {
		chunksSlicePool.(*pool.TrackedPool).Reset()

		set := newSeriesChunksSet(1, true)

		slice := set.newSeriesAggrChunkSlice(seriesChunksSlabSize - 1)
		assert.Equal(t, seriesChunksSlabSize-1, len(slice))
		assert.Equal(t, seriesChunksSlabSize-1, cap(slice))
		assert.Equal(t, 1, int(chunksSlicePool.(*pool.TrackedPool).Gets.Load()))

		slice = set.newSeriesAggrChunkSlice(seriesChunksSlabSize)
		assert.Equal(t, seriesChunksSlabSize, len(slice))
		assert.Equal(t, seriesChunksSlabSize, cap(slice))
		assert.Equal(t, 2, int(chunksSlicePool.(*pool.TrackedPool).Gets.Load()))

		set.release()
		assert.Equal(t, 0, int(chunksSlicePool.(*pool.TrackedPool).Balance.Load()))
	})

	t.Run("newSeriesAggrChunkSlice() should directly allocate a new slice and release() should not put back to the pool if the set is not releasable", func(t *testing.T) {
		chunksSlicePool.(*pool.TrackedPool).Reset()

		set := newSeriesChunksSet(1, false)

		slice := set.newSeriesAggrChunkSlice(seriesChunksSlabSize)
		assert.Equal(t, seriesChunksSlabSize, len(slice))
		assert.Equal(t, seriesChunksSlabSize, cap(slice))
		assert.Equal(t, 0, int(chunksSlicePool.(*pool.TrackedPool).Gets.Load()))
		assert.Equal(t, 0, int(chunksSlicePool.(*pool.TrackedPool).Balance.Load()))

		set.release()

		assert.Equal(t, 0, int(chunksSlicePool.(*pool.TrackedPool).Gets.Load()))
		assert.Equal(t, 0, int(chunksSlicePool.(*pool.TrackedPool).Balance.Load()))
	})
}

func TestSeriesChunksSeriesSet(t *testing.T) {
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

		set1 := newSeriesChunksSet(2, true)
		set1.chunksReleaser = releasers[0]
		set1.series = append(set1.series,
			seriesChunks{lset: series1, chks: []storepb.AggrChunk{c[1]}},
			seriesChunks{lset: series2, chks: []storepb.AggrChunk{c[2]}},
		)

		set2 := newSeriesChunksSet(2, true)
		set2.chunksReleaser = releasers[1]
		set2.series = append(set2.series,
			seriesChunks{lset: series3, chks: []storepb.AggrChunk{c[3]}},
			seriesChunks{lset: series4, chks: []storepb.AggrChunk{c[4]}},
		)

		set3 := newSeriesChunksSet(1, true)
		set3.chunksReleaser = releasers[2]
		set3.series = append(set3.series,
			seriesChunks{lset: series5, chks: []storepb.AggrChunk{c[5]}},
		)

		sets = append(sets, set1, set2, set3)
		return
	}

	t.Run("should iterate over a single set and release it once done", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.True(t, lbls.IsEmpty())
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
		require.True(t, lbls.IsEmpty())
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should iterate over a multiple sets and release each set once we begin to iterate the next one", func(t *testing.T) {
		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIterator(sets[0], sets[1])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.True(t, lbls.IsEmpty())
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
		require.True(t, lbls.IsEmpty())
		require.Zero(t, chks)
		require.NoError(t, it.Err())
		require.True(t, releasers[0].isReleased())
	})

	t.Run("should release the current set on error", func(t *testing.T) {
		expectedErr := errors.New("mocked error")

		sets, releasers := createSets()
		source := newSliceSeriesChunksSetIteratorWithError(expectedErr, 1, sets[0], sets[1], sets[2])
		it := newSeriesChunksSeriesSet(source)

		lbls, chks := it.At()
		require.True(t, lbls.IsEmpty())
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
		require.True(t, lbls.IsEmpty())
		require.Zero(t, chks)
		require.Equal(t, expectedErr, it.Err())

		// The current set is released.
		require.True(t, releasers[0].isReleased())

		// Can't release the next ones because can't move forward with the iteration (due to the error).
		require.False(t, releasers[1].isReleased())
		require.False(t, releasers[2].isReleased())
	})
}

func TestPreloadingSetIterator(t *testing.T) {
	test.VerifyNoLeak(t)

	const delay = 10 * time.Millisecond

	// Create some sets, each set containing 1 series.
	sets := make([]seriesChunksSet, 0, 10)
	for i := 0; i < 10; i++ {
		// The set is not releseable because will be reused by multiple tests.
		set := newSeriesChunksSet(1, false)
		set.series = append(set.series, seriesChunks{
			lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
		})

		sets = append(sets, set)
	}

	t.Run("should iterate all sets if no error occurs", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(sets)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIterator(sets...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

				// Ensure expected sets are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, sets[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure all sets have been returned.
				require.NoError(t, preloading.Err())
				require.Equal(t, len(sets), expectedIdx)
			})
		}
	})

	t.Run("should stop iterating once an error is found", func(t *testing.T) {
		for preloadSize := 1; preloadSize <= len(sets)+1; preloadSize++ {
			preloadSize := preloadSize

			t.Run(fmt.Sprintf("preload size: %d", preloadSize), func(t *testing.T) {
				t.Parallel()

				source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
				source = newDelayedSeriesChunksSetIterator(delay, source)

				preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

				// Ensure expected sets are returned in order.
				expectedIdx := 0
				for preloading.Next() {
					require.NoError(t, preloading.Err())
					require.Equal(t, sets[expectedIdx], preloading.At())
					expectedIdx++
				}

				// Ensure an error is returned at the end.
				require.Error(t, preloading.Err())
				require.Equal(t, len(sets), expectedIdx)
			})
		}
	})

	t.Run("should not leak preloading goroutine if caller doesn't iterated until the end of sets but context is canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancelCtx := context.WithCancel(context.Background())

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingSetIterator[seriesChunksSet](ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, sets[0], preloading.At())

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

		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		source = newDelayedSeriesChunksSetIterator(delay, source)

		preloading := newPreloadingSetIterator[seriesChunksSet](ctx, 1, source)

		// Just call Next() once.
		require.True(t, preloading.Next())
		require.NoError(t, preloading.Err())
		require.Equal(t, sets[0], preloading.At())

		// Cancel the context. Do NOT call Next() after canceling the context.
		cancelCtx()
	})
}

func TestPreloadingSetIterator_Concurrency(t *testing.T) {
	const (
		numRuns     = 100
		numBatches  = 100
		preloadSize = 10
	)

	// Create some sets.
	sets := make([]seriesChunksSet, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		// This set is unreleseable because reused by multiple test runs.
		set := newSeriesChunksSet(1, false)
		set.series = append(set.series, seriesChunks{
			lset: labels.FromStrings("__name__", fmt.Sprintf("metric_%d", i)),
		})

		sets = append(sets, set)
	}

	// Run many times to increase the likelihood to find a race (if any).
	for i := 0; i < numRuns; i++ {
		source := newSliceSeriesChunksSetIteratorWithError(errors.New("mocked error"), len(sets), sets...)
		preloading := newPreloadingSetIterator[seriesChunksSet](context.Background(), preloadSize, source)

		for preloading.Next() {
			require.NoError(t, preloading.Err())
			require.NotZero(t, preloading.At())
		}
		require.Error(t, preloading.Err())
	}

}

type testBlock struct {
	ulid   ulid.ULID
	series []testBlockSeries
}

type testBlockSeries struct {
	lset labels.Labels
	refs []chunks.ChunkRef
	chks []storepb.AggrChunk
}

func (b testBlock) toSeriesChunksOverlapping(seriesIdx int, minT, maxT int64) seriesChunks {
	s := b.series[seriesIdx]
	var filtered []storepb.AggrChunk
	for _, c := range s.chks {
		if c.MinTime > maxT || c.MaxTime < minT {
			continue
		}
		filtered = append(filtered, c)
	}

	return seriesChunks{
		lset: s.lset,
		chks: filtered,
	}
}

func (b testBlock) seriesChunks(seriesIdx int) seriesChunks {
	return seriesChunks{
		lset: b.series[seriesIdx].lset,
		chks: b.series[seriesIdx].chks,
	}
}

func (b testBlock) toSeriesChunkRefsWithNRanges(seriesIndex, numRanges int) seriesChunkRefs {
	return b.toSeriesChunkRefsWithNRangesOverlapping(seriesIndex, numRanges, 0, 100000)
}

func (b testBlock) toSeriesChunkRefsWithNRangesOverlapping(seriesIndex, numRanges int, minT, maxT int64) seriesChunkRefs {
	series := b.series[seriesIndex]

	ranges := make([]seriesChunkRefsRange, numRanges)
	chunksPerRange := len(series.refs) / numRanges
	for i := range ranges {
		for j := 0; j < chunksPerRange; j++ {
			ranges[i].refs = append(ranges[i].refs, seriesChunkRef{
				segFileOffset: chunkOffset(series.refs[i*chunksPerRange+j]),
				minTime:       series.chks[i*chunksPerRange+j].MinTime,
				maxTime:       series.chks[i*chunksPerRange+j].MaxTime,
				blockID:       b.ulid,
				segmentFile:   uint32(chunkSegmentFile(series.refs[i*chunksPerRange+j])),
			})
		}
	}

	// Account for integer division (e.g. 10 chunks in 3 ranges)
	for i := chunksPerRange * len(ranges); i < len(series.refs); i++ {
		ranges[len(ranges)-1].refs = append(ranges[len(ranges)-1].refs, seriesChunkRef{
			segFileOffset: chunkOffset(series.refs[i]),
			minTime:       series.chks[i].MinTime,
			maxTime:       series.chks[i].MaxTime,
			blockID:       b.ulid,
			segmentFile:   uint32(chunkSegmentFile(series.refs[i])),
		})
	}

	for rIdx := 0; rIdx < len(ranges); {
		someChunkOverlaps := false
		for _, c := range ranges[rIdx].refs {
			if c.minTime <= maxT && c.maxTime >= minT {
				someChunkOverlaps = true
				break
			}
		}
		if !someChunkOverlaps {
			ranges = append(ranges[:rIdx], ranges[rIdx+1:]...)
		} else {
			rIdx++
		}
	}

	return seriesChunkRefs{
		lset:         series.lset,
		chunksRanges: ranges,
	}
}

func (b testBlock) toSeriesChunkRefs(seriesIndex int) seriesChunkRefs {
	return b.toSeriesChunkRefsWithNRanges(seriesIndex, 1)
}

func TestLoadingSeriesChunksSetIterator(t *testing.T) {
	block1 := testBlock{
		ulid:   ulid.MustNew(1, nil),
		series: generateSeriesEntries(t, 10),
	}

	block2 := testBlock{
		ulid:   ulid.MustNew(2, nil),
		series: generateSeriesEntries(t, 20)[10:], // Make block2 contain different 10 series from those in block1
	}

	type loadRequest struct {
		existingBlocks      []testBlock
		setsToLoad          []seriesChunkRefsSet
		expectedSets        []seriesChunksSet
		minT, maxT          int64 // optional; if empty, select a wide time range
		addLoadErr, loadErr error
		expectedErr         string
	}

	testCases := map[string][]loadRequest{
		"loads single set from single block": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block1.seriesChunks(1)}},
				},
			},
		},
		"loads single set from single block with multiple ranges": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithNRanges(0, 10), block1.toSeriesChunkRefsWithNRanges(1, 10)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block1.seriesChunks(1)}},
				},
			},
		},
		"loads single set from single block with multiple ranges with mint/maxt": {
			{
				existingBlocks: []testBlock{block1},
				minT:           0,
				maxT:           50,
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithNRanges(0, 10), block1.toSeriesChunkRefsWithNRanges(1, 10)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.toSeriesChunksOverlapping(0, 0, 50), block1.toSeriesChunksOverlapping(1, 0, 50)}},
				},
			},
		},
		"loads multiple sets from single block": {
			{
				existingBlocks: []testBlock{block1},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(2), block1.toSeriesChunkRefs(3)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block1.seriesChunks(1)}},
					{series: []seriesChunks{block1.seriesChunks(2), block1.seriesChunks(3)}},
				},
			},
		},
		"loads single set from multiple blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block2.seriesChunks(1)}},
				},
			},
		},
		"loads multiple sets from multiple blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block1.seriesChunks(1)}},
					{series: []seriesChunks{block2.seriesChunks(0), block2.seriesChunks(1)}},
				},
			},
		},
		"loads multiple sets from multiple blocks with multiple ranges": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefsWithNRanges(0, 4), block1.toSeriesChunkRefsWithNRanges(1, 4)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefsWithNRanges(0, 4), block2.toSeriesChunkRefsWithNRanges(1, 4)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block1.seriesChunks(1)}},
					{series: []seriesChunks{block2.seriesChunks(0), block2.seriesChunks(1)}},
				},
			},
		},
		"loads sets from multiple blocks mixed": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(0)}},
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(1), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{
					{series: []seriesChunks{block1.seriesChunks(0), block2.seriesChunks(0)}},
					{series: []seriesChunks{block1.seriesChunks(1), block2.seriesChunks(1)}},
				},
			},
		},
		"loads series with chunks from different blocks": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefs(0)
						series.chunksRanges = append(series.chunksRanges, block2.toSeriesChunkRefs(0).chunksRanges...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesChunks {
						series := block1.seriesChunks(0)
						series.chks = append(series.chks, block2.seriesChunks(0).chks...)
						return []seriesChunks{series}
					}()},
				},
			},
		},
		"loads series with chunks from different blocks and multiple ranges": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefsWithNRanges(0, 3)
						series.chunksRanges = append(series.chunksRanges, block2.toSeriesChunkRefsWithNRanges(0, 4).chunksRanges...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesChunks {
						series := block1.seriesChunks(0)
						series.chks = append(series.chks, block2.seriesChunks(0).chks...)
						return []seriesChunks{series}
					}()},
				},
			},
		},
		"loads series with chunks from different blocks and multiple chunks within minT/maxT": {
			{
				existingBlocks: []testBlock{block1, block2},
				minT:           0,
				maxT:           10,
				setsToLoad: []seriesChunkRefsSet{
					{series: func() []seriesChunkRefs {
						series := block1.toSeriesChunkRefsWithNRanges(0, 3)
						series.chunksRanges = append(series.chunksRanges, block2.toSeriesChunkRefsWithNRanges(0, 4).chunksRanges...)
						return []seriesChunkRefs{series}
					}()},
				},
				expectedSets: []seriesChunksSet{
					{series: func() []seriesChunks {
						series := block1.toSeriesChunksOverlapping(0, 0, 10)
						series.chks = append(series.chks, block2.toSeriesChunksOverlapping(0, 0, 10).chks...)
						return []seriesChunks{series}
					}()},
				},
			},
		},
		"handles error in addLoad": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{},
				addLoadErr:   errors.New("test err"),
				expectedErr:  "test err",
			},
		},
		"handles error in load": {
			{
				existingBlocks: []testBlock{block1, block2},
				setsToLoad: []seriesChunkRefsSet{
					{series: []seriesChunkRefs{block1.toSeriesChunkRefs(0), block1.toSeriesChunkRefs(1)}},
					{series: []seriesChunkRefs{block2.toSeriesChunkRefs(0), block2.toSeriesChunkRefs(1)}},
				},
				expectedSets: []seriesChunksSet{},
				loadErr:      errors.New("test err"),
				expectedErr:  "test err",
			},
		},
	}

	for testName, loadRequests := range testCases {
		t.Run(testName, func(t *testing.T) {
			for scenarioIdx, testCase := range loadRequests {
				t.Run("step "+strconv.Itoa(scenarioIdx), func(t *testing.T) {

					// Reset the memory pool tracker.
					seriesChunksSlicePool.(*pool.TrackedPool).Reset()
					chunksSlicePool.(*pool.TrackedPool).Reset()
					chunkBytesSlicePool.(*pool.TrackedPool).Reset()

					// Setup
					readersMap := make(map[ulid.ULID]chunkReader, len(testCase.existingBlocks))
					for _, block := range testCase.existingBlocks {
						readersMap[block.ulid] = newChunkReaderMockWithSeries(block.series, testCase.addLoadErr, testCase.loadErr)
					}
					readers := newChunkReaders(readersMap)
					minT, maxT := testCase.minT, testCase.maxT
					if minT == 0 && maxT == 0 {
						minT, maxT = 0, 100000 // select everything by default
					}

					// Run test
					set := newLoadingSeriesChunksSetIterator(context.Background(), log.NewNopLogger(), "tenant", *readers, newSliceSeriesChunkRefsSetIterator(nil, testCase.setsToLoad...), 100, newSafeQueryStats(), minT, maxT)
					loadedSets := readAllSeriesChunksSets(set)

					// Assertions
					if testCase.expectedErr != "" {
						assert.ErrorContains(t, set.Err(), testCase.expectedErr)
					} else {
						assert.NoError(t, set.Err())
					}
					// Check that chunk bytes are what we expect
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

					assert.Zero(t, chunkBytesSlicePool.(*pool.TrackedPool).Balance.Load())
					assert.Zero(t, seriesChunksSlicePool.(*pool.TrackedPool).Balance.Load())
					assert.Greater(t, seriesChunksSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
					assert.Zero(t, chunksSlicePool.(*pool.TrackedPool).Balance.Load())
					assert.Greater(t, chunksSlicePool.(*pool.TrackedPool).Gets.Load(), int64(0))
				})
			}
		})
	}
}

func BenchmarkLoadingSeriesChunksSetIterator(b *testing.B) {
	for batchSize := 10; batchSize <= 1000; batchSize *= 10 {
		b.Run(fmt.Sprintf("batch size: %d", batchSize), func(b *testing.B) {
			b.ReportAllocs()
			var (
				numSets         = 100
				numSeriesPerSet = batchSize
				// For simplicity all series have the same number of chunks. This shouldn't affect the benchmark.
				numChunksPerSeries = 50
			)

			blockID := ulid.MustNew(1, nil)

			testBlk := testBlock{
				ulid:   blockID,
				series: generateSeriesEntries(b, numSeriesPerSet*numSets),
			}

			// Creates the series sets, guaranteeing series sorted by labels.
			sets := make([]seriesChunkRefsSet, 0, numSets)
			setSeriesOffset := 0
			for setIdx := 0; setIdx < numSets; setIdx++ {
				// This set cannot be released because reused between multiple benchmark runs.
				refs := newSeriesChunkRefsSet(numSeriesPerSet, false)
				refs.series = refs.series[:cap(refs.series)]
				for refIdx := range refs.series {
					refs.series[refIdx] = testBlk.toSeriesChunkRefs(setSeriesOffset)
					setSeriesOffset++
				}
				sets = append(sets, refs)
			}

			// Mock the chunk reader.
			readersMap := map[ulid.ULID]chunkReader{
				blockID: newChunkReaderMockWithSeries(testBlk.series, nil, nil),
			}

			chunkReaders := newChunkReaders(readersMap)
			stats := newSafeQueryStats()

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				batchSize := numSeriesPerSet
				it := newLoadingSeriesChunksSetIterator(context.Background(), log.NewNopLogger(), "tenant", *chunkReaders, newSliceSeriesChunkRefsSetIterator(nil, sets...), batchSize, stats, 0, 10000)

				actualSeries := 0
				actualChunks := 0

				for it.Next() {
					set := it.At()

					for _, series := range set.series {
						actualSeries++
						actualChunks += len(series.chks)
					}

					set.release()
				}

				if err := it.Err(); err != nil {
					b.Fatal(it.Err())
				}

				// Ensure each benchmark run go through the same data set.
				if expectedSeries := numSets * numSeriesPerSet; actualSeries != expectedSeries {
					b.Fatalf("benchmark iterated through an unexpected number of series (expected: %d got: %d)", expectedSeries, actualSeries)
				}
				if expectedChunks := numSets * numSeriesPerSet * numChunksPerSeries; actualChunks != expectedChunks {
					b.Fatalf("benchmark iterated through an unexpected number of chunks (expected: %d got: %d)", expectedChunks, actualChunks)
				}
			}
		})
	}
}

type chunkReaderMock struct {
	chunks              map[chunks.ChunkRef]storepb.AggrChunk
	addLoadErr, loadErr error

	toLoad map[chunks.ChunkRef]loadIdx
}

func newChunkReaderMockWithSeries(series []testBlockSeries, addLoadErr, loadErr error) *chunkReaderMock {
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
		toLoad:     make(map[chunks.ChunkRef]loadIdx),
	}
}

func (f *chunkReaderMock) Close() error {
	return nil
}

func (f *chunkReaderMock) addLoad(id chunks.ChunkRef, seriesEntry, chunkEntry int, _ uint32) error {
	if f.addLoadErr != nil {
		return f.addLoadErr
	}
	f.toLoad[id] = loadIdx{seriesEntry: seriesEntry, chunkEntry: chunkEntry}
	return nil
}

func (f *chunkReaderMock) load(result []seriesChunks, chunksPool *pool.SafeSlabPool[byte], _ *safeQueryStats) error {
	if f.loadErr != nil {
		return f.loadErr
	}
	for chunkRef, indices := range f.toLoad {
		// Take bytes from the pool, so we can assert on number of allocations and that frees are happening
		chunkData := f.chunks[chunkRef].Raw.Data
		copiedChunkData := chunksPool.Get(len(chunkData))
		copy(copiedChunkData, chunkData)
		result[indices.seriesEntry].chks[indices.chunkEntry].Raw = &storepb.Chunk{Data: copiedChunkData}
	}
	return nil
}

func (f *chunkReaderMock) reset() {
	f.toLoad = make(map[chunks.ChunkRef]loadIdx)
}

// generateSeriesEntriesWithChunks generates seriesEntries with chunks. Each chunk is a random byte slice.
func generateSeriesEntriesWithChunks(t testing.TB, numSeries, numChunksPerSeries int) []testBlockSeries {
	return generateSeriesEntriesWithChunksSize(t, numSeries, numChunksPerSeries, 256)
}

func generateSeriesEntriesWithChunksSize(t testing.TB, numSeries, numChunksPerSeries, chunkDataLenBytes int) []testBlockSeries {

	out := make([]testBlockSeries, 0, numSeries)
	lbls := generateSeries([]int{numSeries})

	for i := 0; i < numSeries; i++ {
		series := testBlockSeries{
			lset: lbls[i],
			refs: make([]chunks.ChunkRef, 0, numChunksPerSeries),
			chks: make([]storepb.AggrChunk, 0, numChunksPerSeries),
		}

		for j := 0; j < numChunksPerSeries; j++ {
			chunkBytes := make([]byte, chunkDataLenBytes)
			readBytes, err := crand.Read(chunkBytes)
			require.NoError(t, err, "couldn't generate test data")
			require.Equal(t, chunkDataLenBytes, readBytes, "couldn't generate test data")

			series.refs = append(series.refs, chunks.ChunkRef(i*numChunksPerSeries+j))
			series.chks = append(series.chks, storepb.AggrChunk{
				MinTime: int64(10 * j),
				MaxTime: int64(10 * (j + 1)),
				Raw:     &storepb.Chunk{Data: chunkBytes},
			})
		}
		out = append(out, series)
	}
	return out
}

// generateSeriesEntries generates seriesEntries with 50 chunks. Each chunk is a random byte slice.
func generateSeriesEntries(t testing.TB, numSeries int) []testBlockSeries {
	return generateSeriesEntriesWithChunks(t, numSeries, 50)
}

// sliceSeriesChunksSetIterator implements seriesChunksSetIterator and
// returns the provided err when the sets are exhausted
type sliceSeriesChunksSetIterator struct {
	current int
	sets    []seriesChunksSet

	err   error
	errAt int
}

func newSliceSeriesChunksSetIterator(sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
	}
}

func newSliceSeriesChunksSetIteratorWithError(err error, errAt int, sets ...seriesChunksSet) seriesChunksSetIterator {
	return &sliceSeriesChunksSetIterator{
		current: -1,
		sets:    sets,
		err:     err,
		errAt:   errAt,
	}
}

func (s *sliceSeriesChunksSetIterator) Next() bool {
	s.current++

	// If the next item should fail, we return false. The Err() function will return the error.
	if s.err != nil && s.current >= s.errAt {
		return false
	}

	return s.current < len(s.sets)
}

func (s *sliceSeriesChunksSetIterator) At() seriesChunksSet {
	return s.sets[s.current]
}

func (s *sliceSeriesChunksSetIterator) Err() error {
	if s.err != nil && s.current >= s.errAt {
		return s.err
	}
	return nil
}

// delayedSeriesChunksSetIterator implements seriesChunksSetIterator and
// introduces an artificial delay before returning from Next() and At().
type delayedSeriesChunksSetIterator struct {
	wrapped seriesChunksSetIterator
	delay   time.Duration
}

func newDelayedSeriesChunksSetIterator(delay time.Duration, wrapped seriesChunksSetIterator) seriesChunksSetIterator {
	return &delayedSeriesChunksSetIterator{
		wrapped: wrapped,
		delay:   delay,
	}
}

func (s *delayedSeriesChunksSetIterator) Next() bool {
	time.Sleep(s.delay)
	return s.wrapped.Next()
}

func (s *delayedSeriesChunksSetIterator) At() seriesChunksSet {
	time.Sleep(s.delay)
	return s.wrapped.At()
}

func (s *delayedSeriesChunksSetIterator) Err() error {
	return s.wrapped.Err()
}

func generateAggrChunk(num int) []storepb.AggrChunk {
	out := make([]storepb.AggrChunk, 0, num)

	for i := 0; i < num; i++ {
		out = append(out, storepb.AggrChunk{
			MinTime: int64(i),
			MaxTime: int64(i),
		})
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
