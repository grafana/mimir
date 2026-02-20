// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestIngester_SendStreamingQuerySeries_ConcurrentMemoryUsage validates memory bounds when
// multiple concurrent queries reach the end of their series-streaming phase simultaneously.
//
// The test uses a blocking stream server that blocks on the final Send() call (the one with
// IsEndOfSeriesStream: true). At that point, each query has:
// - Iterated through all series (labels read)
// - Stored all IteratorFactory() results
// - Sent all label batches except the final one
//
// If labels are properly released (the optimization we're testing), memory should be bounded
// by approximately: batchSize × labelsPerSeries × numGoroutines
// If labels are retained, memory would grow unbounded with: totalSeries × labelsPerSeries × numGoroutines
func TestIngester_SendStreamingQuerySeries_ConcurrentMemoryUsage(t *testing.T) {
	const (
		numSeries     = 1000 // Series to push to TSDB
		numLabels     = 20   // Labels per series
		labelValLen   = 1024 // 1KB per label value (~20KB per series)
		numGoroutines = 100  // Concurrent queries
	)

	// Setup ingester and push series with large labels
	cfg := defaultIngesterTestConfig(t)
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), "test-user")
	now := time.Now().UnixMilli()

	// Push test series with large labels to make memory differences measurable
	for s := 0; s < numSeries; s++ {
		kvs := make([]string, 0, (numLabels+1)*2)
		kvs = append(kvs, model.MetricNameLabel, fmt.Sprintf("series_%04d", s))
		for j := 0; j < numLabels; j++ {
			kvs = append(kvs, fmt.Sprintf("label_%02d", j), strings.Repeat("x", labelValLen))
		}
		lset := labels.FromStrings(kvs...)
		_, err := i.Push(ctx, writeRequestSingleSeries(lset, []mimirpb.Sample{{TimestampMs: now, Value: float64(s)}}))
		require.NoError(t, err)
	}

	db := i.getTSDB("test-user")
	require.NotNil(t, db)

	// Force head compaction to move all series into a block.
	// This is critical: when querying from the head, TSDB reuses in-memory label strings.
	// When querying from a block, each query reads labels from disk and allocates new strings.
	// This makes the memory difference measurable.
	i.compactBlocks(context.Background(), true, time.Now().Add(1*time.Minute).UnixMilli(), nil)
	require.Equal(t, uint64(0), db.Head().NumSeries(), "all series should be compacted out of head")

	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*")}

	// Phase 1: Dry run to count how many Send() calls occur
	countingStream := &countingQueryStreamServer{ctx: ctx}
	innerQ, err := db.ChunkQuerier(now-1000, now+1000)
	require.NoError(t, err)

	_, seriesCount, err := i.sendStreamingQuerySeries(ctx, innerQ, now-1000, now+1000, matchers, nil, countingStream)
	require.NoError(t, err)
	require.Equal(t, numSeries, seriesCount)
	innerQ.Close()

	expectedCalls := countingStream.sendCount.Load()
	require.Greater(t, expectedCalls, int64(1), "expected multiple Send calls")

	// Phase 2: Run concurrent queries with blocking streams
	// Create coordinator for synchronizing all goroutines
	coordinator := &blockingCoordinator{
		expectedCalls:   expectedCalls,
		totalGoroutines: int64(numGoroutines),
		blockCh:         make(chan struct{}),
		allBlockedCh:    make(chan struct{}),
	}

	// Collect baseline memory before starting concurrent queries
	runtime.GC()
	var baselineStats runtime.MemStats
	runtime.ReadMemStats(&baselineStats)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	errCh := make(chan error, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			defer wg.Done()

			// Each goroutine needs its own querier and stream
			q, err := db.ChunkQuerier(now-1000, now+1000)
			if err != nil {
				errCh <- err
				return
			}
			defer q.Close()

			stream := &blockingQueryStreamServer{
				ctx:         ctx,
				coordinator: coordinator,
			}

			_, _, err = i.sendStreamingQuerySeries(ctx, q, now-1000, now+1000, matchers, nil, stream)
			if err != nil {
				errCh <- err
			}
		}()
	}

	// Wait until all goroutines are blocked on their last Send()
	select {
	case <-coordinator.allBlockedCh:
		// All goroutines are now blocked at the end of iteration
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for all goroutines to block")
	}

	// Measure memory at peak - all series have been iterated, labels should be released
	runtime.GC()
	var peakStats runtime.MemStats
	runtime.ReadMemStats(&peakStats)

	// Release all blocked goroutines
	close(coordinator.blockCh)

	// Wait for all goroutines to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for goroutines to complete")
	}

	// Check for errors
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	// Calculate memory growth
	memoryGrowth := int64(peakStats.HeapAlloc) - int64(baselineStats.HeapAlloc)

	// Calculate expected max memory based on the optimization:
	// If labels are released after each batch is sent, memory should be bounded by
	// the current batch size (not total series) times the number of concurrent queries.
	//
	// Each series has ~20KB of labels (numLabels × labelValLen).
	// With batch size of 128 (queryStreamBatchSize), the active batch holds at most
	// 128 × 20KB = 2.5MB per query. With 100 queries: ~250MB max.
	//
	// If labels were NOT released (the bug we're guarding against), memory would be:
	// 1000 series × 20KB × 100 queries = 2GB

	labelBytesPerSeries := numLabels * labelValLen
	batchLabelBytes := labelBytesPerSeries * queryStreamBatchSize
	// Allow 4x the theoretical minimum to account for Go's memory management overhead,
	// iterator factory storage, and other allocations during query processing.
	maxAllowedMemory := int64(4 * batchLabelBytes * numGoroutines)

	// Memory that would be used if labels were retained (the bad case)
	retainedMemory := int64(numSeries * labelBytesPerSeries * numGoroutines)

	t.Logf("Memory stats: baseline=%dMB, peak=%dMB, growth=%dMB, maxAllowed=%dMB, wouldRetain=%dMB",
		baselineStats.HeapAlloc/(1024*1024),
		peakStats.HeapAlloc/(1024*1024),
		memoryGrowth/(1024*1024),
		maxAllowedMemory/(1024*1024),
		retainedMemory/(1024*1024))

	// The memory growth should be significantly less than what it would be if labels were retained
	require.Less(t, memoryGrowth, maxAllowedMemory,
		"memory growth (%dMB) exceeded maximum allowed (%dMB); labels may not be released properly",
		memoryGrowth/(1024*1024), maxAllowedMemory/(1024*1024))
}

// countingQueryStreamServer counts the number of Send() calls.
type countingQueryStreamServer struct {
	grpc.ServerStream
	ctx       context.Context
	sendCount atomic.Int64
}

func (s *countingQueryStreamServer) Send(*client.QueryStreamResponse) error {
	s.sendCount.Add(1)
	return nil
}

func (s *countingQueryStreamServer) Context() context.Context {
	return s.ctx
}

// blockingCoordinator coordinates blocking across multiple goroutines.
type blockingCoordinator struct {
	expectedCalls   int64         // Number of Send() calls expected per goroutine
	totalGoroutines int64         // Total number of goroutines
	blockedCount    atomic.Int64  // Number of goroutines currently blocked
	blockCh         chan struct{} // Closed to release all blocked goroutines
	allBlockedCh    chan struct{} // Closed when all goroutines are blocked
	allBlockedOnce  sync.Once     // Ensures allBlockedCh is closed only once
}

// blockingQueryStreamServer blocks on the last Send() call to measure peak memory.
type blockingQueryStreamServer struct {
	grpc.ServerStream
	ctx         context.Context
	coordinator *blockingCoordinator
	currentCall atomic.Int64
}

func (s *blockingQueryStreamServer) Send(*client.QueryStreamResponse) error {
	callNum := s.currentCall.Add(1)

	// Block on the last call (the one with IsEndOfSeriesStream: true)
	if callNum == s.coordinator.expectedCalls {
		blocked := s.coordinator.blockedCount.Add(1)
		if blocked == s.coordinator.totalGoroutines {
			// All goroutines are now blocked - signal this
			s.coordinator.allBlockedOnce.Do(func() {
				close(s.coordinator.allBlockedCh)
			})
		}
		// Wait for release
		<-s.coordinator.blockCh
	}

	return nil
}

func (s *blockingQueryStreamServer) Context() context.Context {
	return s.ctx
}
