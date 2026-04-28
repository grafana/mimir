// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/stats/stats_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package stats

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStats_WallTime(t *testing.T) {
	t.Run("add and load wall time", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddWallTime(time.Second)
		stats.AddWallTime(time.Second)

		assert.Equal(t, 2*time.Second, stats.LoadWallTime())
	})

	t.Run("add and load wall time nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddWallTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadWallTime())
	})
}

func TestStats_AddFetchedSeries(t *testing.T) {
	t.Run("add and load series", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedSeries(100)
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(150), stats.LoadFetchedSeries())
	})

	t.Run("add and load series nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddFetchedSeries(50)

		assert.Equal(t, uint64(0), stats.LoadFetchedSeries())
	})
}

func TestStats_AddFetchedChunkBytes(t *testing.T) {
	t.Run("add and load bytes", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedChunkBytes(4096)
		stats.AddFetchedChunkBytes(4096)

		assert.Equal(t, uint64(8192), stats.LoadFetchedChunkBytes())
	})

	t.Run("add and load bytes nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddFetchedChunkBytes(1024)

		assert.Equal(t, uint64(0), stats.LoadFetchedChunkBytes())
	})
}

func TestStats_AddFetchedChunks(t *testing.T) {
	t.Run("add and load chunks", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddFetchedChunks(20)
		stats.AddFetchedChunks(22)

		assert.Equal(t, uint64(42), stats.LoadFetchedChunks())
	})

	t.Run("add and load chunks nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddFetchedChunks(3)

		assert.Equal(t, uint64(0), stats.LoadFetchedChunks())
	})
}

func TestStats_AddShardedQueries(t *testing.T) {
	t.Run("add and load sharded queries", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddShardedQueries(20)
		stats.AddShardedQueries(22)

		assert.Equal(t, uint32(42), stats.LoadShardedQueries())
	})

	t.Run("add and load sharded queries nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddShardedQueries(3)

		assert.Equal(t, uint32(0), stats.LoadShardedQueries())
	})
}

func TestStats_AddSplitQueries(t *testing.T) {
	t.Run("add and load split queries", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddSplitQueries(10)
		stats.AddSplitQueries(11)

		assert.Equal(t, uint32(21), stats.LoadSplitQueries())
	})

	t.Run("add and load split queries nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddSplitQueries(1)

		assert.Equal(t, uint32(0), stats.LoadSplitQueries())
	})
}

func TestStats_QueueTime(t *testing.T) {
	t.Run("add and load queue time", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddQueueTime(time.Second)
		stats.AddQueueTime(time.Second)

		assert.Equal(t, 2*time.Second, stats.LoadQueueTime())
	})

	t.Run("add and load queue time nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddQueueTime(time.Second)

		assert.Equal(t, time.Duration(0), stats.LoadQueueTime())
	})
}

func TestStats_SamplesProcessed(t *testing.T) {
	t.Run("add and load samples processed", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddSamplesProcessed(10)
		stats.AddSamplesProcessed(20)

		assert.Equal(t, uint64(30), stats.LoadSamplesProcessed())
	})

	t.Run("add and load samples processed nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddSamplesProcessed(10)

		assert.Equal(t, uint64(0), stats.LoadSamplesProcessed())
	})
}

func TestStats_AddRemoteExecutionRequests(t *testing.T) {
	t.Run("add and load requests", func(t *testing.T) {
		stats, _ := ContextWithEmptyStats(context.Background())
		stats.AddRemoteExecutionRequests(4096)
		stats.AddRemoteExecutionRequests(4096)

		assert.Equal(t, uint32(8192), stats.LoadRemoteExecutionRequestCount())
	})

	t.Run("add and load requests nil receiver", func(t *testing.T) {
		var stats *SafeStats
		stats.AddRemoteExecutionRequests(1024)

		assert.Equal(t, uint32(0), stats.LoadRemoteExecutionRequestCount())
	})
}

func TestStats_Merge(t *testing.T) {
	t.Run("merge two stats objects", func(t *testing.T) {
		stats1 := &SafeStats{}
		stats1.AddWallTime(time.Millisecond)
		stats1.AddFetchedSeries(50)
		stats1.AddFetchedChunkBytes(42)
		stats1.AddFetchedChunks(10)
		stats1.AddShardedQueries(20)
		stats1.AddSplitQueries(10)
		stats1.AddQueueTime(5 * time.Second)
		stats1.AddSamplesProcessed(10)
		stats1.AddSamplesProcessedPerStep([]StepStat{
			{Timestamp: 1, Value: 10},
			{Timestamp: 2, Value: 20},
		})
		stats1.AddRemoteExecutionRequests(12)

		stats2 := &SafeStats{}
		stats2.AddWallTime(time.Second)
		stats2.AddFetchedSeries(60)
		stats2.AddFetchedChunkBytes(100)
		stats2.AddFetchedChunks(11)
		stats2.AddShardedQueries(21)
		stats2.AddSplitQueries(11)
		stats2.AddQueueTime(10 * time.Second)
		stats2.AddSamplesProcessed(20)
		stats2.AddSamplesProcessedPerStep([]StepStat{
			{Timestamp: 1, Value: 10},
			{Timestamp: 2, Value: 20},
		})
		stats2.AddRemoteExecutionRequests(14)

		stats1.Merge(stats2)

		assert.Equal(t, 1001*time.Millisecond, stats1.LoadWallTime())
		assert.Equal(t, uint64(110), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(142), stats1.LoadFetchedChunkBytes())
		assert.Equal(t, uint64(21), stats1.LoadFetchedChunks())
		assert.Equal(t, uint32(41), stats1.LoadShardedQueries())
		assert.Equal(t, uint32(21), stats1.LoadSplitQueries())
		assert.Equal(t, 15*time.Second, stats1.LoadQueueTime())
		assert.Equal(t, uint64(30), stats1.LoadSamplesProcessed())
		assert.Equal(t, []StepStat{
			{Timestamp: 1, Value: 20},
			{Timestamp: 2, Value: 40},
		}, stats1.LoadSamplesProcessedPerStep())
		assert.Equal(t, uint32(26), stats1.LoadRemoteExecutionRequestCount())
	})

	t.Run("merge two nil stats objects", func(t *testing.T) {
		var stats1 *SafeStats
		var stats2 *SafeStats

		stats1.Merge(stats2)

		assert.Equal(t, time.Duration(0), stats1.LoadWallTime())
		assert.Equal(t, uint64(0), stats1.LoadFetchedSeries())
		assert.Equal(t, uint64(0), stats1.LoadFetchedChunkBytes())
		assert.Equal(t, uint64(0), stats1.LoadFetchedChunks())
		assert.Equal(t, uint32(0), stats1.LoadShardedQueries())
		assert.Equal(t, uint32(0), stats1.LoadSplitQueries())
		assert.Equal(t, time.Duration(0), stats1.LoadQueueTime())
		assert.Equal(t, uint64(0), stats1.LoadSamplesProcessed())
		assert.Equal(t, uint32(0), stats1.LoadRemoteExecutionRequestCount())
	})
}

func TestStats_Copy(t *testing.T) {
	s1 := &SafeStats{
		Stats: Stats{
			WallTime:             1,
			FetchedSeriesCount:   2,
			FetchedChunkBytes:    3,
			FetchedChunksCount:   4,
			ShardedQueries:       5,
			SplitQueries:         6,
			FetchedIndexBytes:    7,
			EstimatedSeriesCount: 8,
			QueueTime:            9,
			SamplesProcessed:     10,
			SamplesProcessedPerStep: []StepStat{
				{Timestamp: 1, Value: 5},
				{Timestamp: 2, Value: 5},
			},
			RemoteExecutionRequestCount: 12,
		},
	}
	s2 := s1.Copy()
	assert.NotSame(t, s1, s2)
	assert.EqualValues(t, s1, s2)

	assert.Nil(t, (*SafeStats)(nil).Copy())
}

func TestStats_ConcurrentMerge(t *testing.T) {
	// Create parent stats object (initially empty)
	parentStats := &SafeStats{}

	// Verify parent is initially empty
	assert.Equal(t, time.Duration(0), parentStats.LoadWallTime())
	assert.Equal(t, uint64(0), parentStats.LoadFetchedSeries())
	assert.Equal(t, uint64(0), parentStats.LoadFetchedChunkBytes())

	// Create 10 child stats objects with known values
	const numChildren = 10
	childStats := make([]*SafeStats, numChildren)

	for i := 0; i < numChildren; i++ {
		child := &SafeStats{}

		child.AddWallTime(100 * time.Millisecond)
		child.AddFetchedSeries(uint64(50))
		child.AddFetchedChunkBytes(uint64(1000))
		child.AddFetchedChunks(uint64(25))
		child.AddShardedQueries(uint32(5))
		child.AddSplitQueries(uint32(10))
		child.AddQueueTime(200 * time.Millisecond)
		child.AddSamplesProcessed(uint64(100))
		child.AddSamplesProcessedPerStep([]StepStat{
			{Timestamp: 1, Value: 1},
			{Timestamp: 2, Value: 2},
			{Timestamp: 3, Value: 3},
			{Timestamp: 4, Value: 4},
			{Timestamp: 5, Value: 5},
			{Timestamp: 6, Value: 6},
			{Timestamp: 7, Value: 7},
			{Timestamp: 8, Value: 8},
			{Timestamp: 9, Value: 9},
			{Timestamp: 10, Value: 10},
		})
		child.AddRemoteExecutionRequests(12)

		childStats[i] = child
	}

	// Channel to coordinate goroutine start
	start := make(chan struct{})
	var wg sync.WaitGroup

	// Launch goroutines to concurrently merge each child with parent
	for i := 0; i < numChildren; i++ {
		wg.Add(1)
		go func(childIndex int) {
			defer wg.Done()

			// Wait for all goroutines to be ready
			<-start

			// Merge this child's stats with parent
			parentStats.Merge(childStats[childIndex])
		}(i)
	}

	// Start all goroutines simultaneously
	close(start)

	// Wait for all goroutines to complete
	wg.Wait()

	// Calculate expected totals (same stats * numChildren)
	expectedWallTime := time.Duration(numChildren) * 100 * time.Millisecond
	expectedSeries := uint64(numChildren * 50)
	expectedChunkBytes := uint64(numChildren * 1000)
	expectedChunks := uint64(numChildren * 25)
	expectedShardedQueries := uint32(numChildren * 5)
	expectedSplitQueries := uint32(numChildren * 10)
	expectedQueueTime := time.Duration(numChildren) * 200 * time.Millisecond
	expectedSamplesProcessed := uint64(numChildren * 100)
	expectedRemoteExecutionRequestCount := uint32(numChildren * 12)

	// Verify all values match expected totals
	assert.Equal(t, expectedWallTime, parentStats.LoadWallTime(), "WallTime should be sum of all children")
	assert.Equal(t, expectedSeries, parentStats.LoadFetchedSeries(), "FetchedSeries should be sum of all children")
	assert.Equal(t, expectedChunkBytes, parentStats.LoadFetchedChunkBytes(), "FetchedChunkBytes should be sum of all children")
	assert.Equal(t, expectedChunks, parentStats.LoadFetchedChunks(), "FetchedChunks should be sum of all children")
	assert.Equal(t, expectedShardedQueries, parentStats.LoadShardedQueries(), "ShardedQueries should be sum of all children")
	assert.Equal(t, expectedSplitQueries, parentStats.LoadSplitQueries(), "SplitQueries should be sum of all children")
	assert.Equal(t, expectedQueueTime, parentStats.LoadQueueTime(), "QueueTime should be sum of all children")
	assert.Equal(t, expectedSamplesProcessed, parentStats.LoadSamplesProcessed(), "SamplesProcessed should be sum of all children")
	assert.Equal(t, expectedRemoteExecutionRequestCount, parentStats.LoadRemoteExecutionRequestCount(), "RemoteExecutionRequestCount should be sum of all children")

	// Verify that SamplesProcessedPerStep was merged correctly
	expectedPerStepStats := []StepStat{
		{Timestamp: 1, Value: int64(numChildren * 1)},
		{Timestamp: 2, Value: int64(numChildren * 2)},
		{Timestamp: 3, Value: int64(numChildren * 3)},
		{Timestamp: 4, Value: int64(numChildren * 4)},
		{Timestamp: 5, Value: int64(numChildren * 5)},
		{Timestamp: 6, Value: int64(numChildren * 6)},
		{Timestamp: 7, Value: int64(numChildren * 7)},
		{Timestamp: 8, Value: int64(numChildren * 8)},
		{Timestamp: 9, Value: int64(numChildren * 9)},
		{Timestamp: 10, Value: int64(numChildren * 10)},
	}
	assert.Equal(t, expectedPerStepStats, parentStats.LoadSamplesProcessedPerStep(), "SamplesProcessedPerStep should be sum of all children")
}
