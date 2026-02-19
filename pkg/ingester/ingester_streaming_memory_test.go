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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestStreamingQuerySeriesMemoryFootprint validates that the ingester only retains
// the minimum data needed for chunk iteration between the series-streaming and
// chunk-streaming phases of a streaming query.
//
// A streaming query has two phases:
//  1. sendStreamingQuerySeries: iterates the series set, sends series labels to the
//     querier, and saves a per-series reference for use in phase 2.
//  2. sendStreamingQueryChunks: reads chunk data from the saved references and
//     streams chunks back to the querier.
//
// Between the two phases the querier processes the series labels, which can take a
// non-trivial amount of time when many queries are inflight simultaneously.  During
// this window the ingester must not hold more memory than necessary.
//
// The fix under test: phase 1 calls series.IteratorFactory() and stores only the
// returned ChunkIterable (which retains only the chunk-iterator function) instead of
// the full ChunkSeries (which also retains the labels and the series object itself).
// This lets the labels — and the ChunkSeries object — be garbage-collected as soon
// as phase 1 has read them, rather than keeping them alive until phase 2 finishes.
//
// The test toggles between the old behaviour (store full ChunkSeries) and the new
// behaviour (store IteratorFactory() result) and uses runtime.SetFinalizer to
// observe whether ChunkSeriesEntry objects become unreachable after phase 1.
func TestStreamingQuerySeriesMemoryFootprint(t *testing.T) {
	const (
		numSeries   = 200
		numLabels   = 20
		labelValLen = 200 // bytes per label value; makes the difference measurable
	)

	// buildSeries creates numSeries test ChunkSeries with large label sets.
	// Each entry's ChunkIteratorFn closes over a standalone chunk slice (not over
	// the ChunkSeriesEntry itself), which mirrors how real TSDB iterator factories
	// work: the factory captures chunk data but not the parent series object.
	// A finalizer on each entry lets us detect when it becomes unreachable.
	buildSeries := func() ([]*storage.ChunkSeriesEntry, *atomic.Int64) {
		finalizedCount := &atomic.Int64{}
		entries := make([]*storage.ChunkSeriesEntry, numSeries)
		for i := range entries {
			kvs := make([]string, 0, (numLabels+1)*2)
			kvs = append(kvs, labels.MetricName, fmt.Sprintf("series_%04d", i))
			for j := 0; j < numLabels; j++ {
				kvs = append(kvs, fmt.Sprintf("label_%02d", j), strings.Repeat("x", labelValLen))
			}
			lset := labels.FromStrings(kvs...)

			// The closure captures chks (a local var) but not the entry itself,
			// so the entry is eligible for GC once no other reference holds it.
			chks := []chunks.Meta{}
			entry := &storage.ChunkSeriesEntry{
				Lset: lset,
				ChunkIteratorFn: func(it chunks.Iterator) chunks.Iterator {
					return storage.NewListChunkSeriesIterator(chks...)
				},
				ChunkCountFn: func() (int, error) { return len(chks), nil },
			}
			entries[i] = entry
			runtime.SetFinalizer(entry, func(_ *storage.ChunkSeriesEntry) {
				finalizedCount.Add(1)
			})
		}
		return entries, finalizedCount
	}

	// gcAndWaitForFinalizers runs two GC cycles with a short sleep between them.
	// The first cycle marks unreachable objects as finalizable and queues their
	// finalizers for execution by the finalizer goroutine; the sleep gives that
	// goroutine time to run; the second cycle collects the now-finalized objects.
	gcAndWaitForFinalizers := func() {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		runtime.GC()
	}

	// New approach: store only the IteratorFactory() result (ChunkIterable).
	// After phase 1 the full ChunkSeries — including its label set — is no longer
	// referenced and can be garbage-collected before phase 2 begins.
	t.Run("storing IteratorFactory allows series and labels to be freed", func(t *testing.T) {
		entries, finalizedCount := buildSeries()

		// Phase 1 (new approach): retain only the iterator factory.
		iterables := make([]storage.ChunkIterable, len(entries))
		for i, e := range entries {
			iterables[i] = e.IteratorFactory()
		}

		// Drop all direct references to the entries, as sendStreamingQuerySeries
		// does when each series goes out of scope at the end of its loop body.
		entries = nil

		gcAndWaitForFinalizers()

		require.EqualValues(t, numSeries, finalizedCount.Load(),
			"all ChunkSeriesEntry objects (and their labels) should be freed "+
				"once only the IteratorFactory() result is retained")

		// Phase 2: chunks must still be readable from the retained iterables.
		var it chunks.Iterator
		for _, s := range iterables {
			it = s.Iterator(it)
			require.NoError(t, it.Err())
		}
	})

	// Old approach: store the full ChunkSeries.
	// The ChunkSeries — and the labels it holds — cannot be freed until the last
	// reference in the retained slice is dropped at the end of phase 2.
	t.Run("storing full ChunkSeries retains series and labels", func(t *testing.T) {
		entries, finalizedCount := buildSeries()

		// Phase 1 (old approach): retain the full ChunkSeries.
		retained := make([]storage.ChunkSeries, len(entries))
		for i, e := range entries {
			retained[i] = e
		}

		// Drop direct references, same as above.
		entries = nil

		gcAndWaitForFinalizers()

		require.EqualValues(t, 0, finalizedCount.Load(),
			"ChunkSeriesEntry objects should remain alive when the full series is retained")

		// Phase 2: chunks are still accessible.
		var it chunks.Iterator
		for _, s := range retained {
			it = s.Iterator(it)
			require.NoError(t, it.Err())
		}

		// Keep retained alive until here so the compiler cannot collect it early,
		// which would make the finalizer count non-deterministic.
		runtime.KeepAlive(retained)
	})
}

// TestStreamingQuerySeriesMemoryFootprint_RealIngester is a higher-level companion to
// TestStreamingQuerySeriesMemoryFootprint. It exercises the actual ingester code path
// — real TSDB storage, real series set iteration, the real sendStreamingQuerySeries and
// sendStreamingQueryChunks implementations — rather than synthetic in-memory stubs.
//
// The test injects a trackingChunkQuerier between the ingester and TSDB. The tracker
// wraps every series returned by Select in a thin trackingChunkSeries shell and
// registers a finalizer on the shell. Because trackingChunkSeries.IteratorFactory()
// delegates to the inner series and does not capture the shell itself, the shell
// becomes unreachable as soon as sendStreamingQuerySeries stores the IteratorFactory()
// result and the loop-local series variable goes out of scope.
//
// After phase 1 completes, a GC run must collect all shells, proving that the ingester
// no longer holds any reference to the series objects (and their labels). Phase 2 must
// still succeed, confirming that chunk data remains accessible via the stored iterables.
func TestStreamingQuerySeriesMemoryFootprint_RealIngester(t *testing.T) {
	const (
		numSeries   = 50
		numLabels   = 20
		labelValLen = 200 // bytes per label value
	)

	cfg := defaultIngesterTestConfig(t)
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	ctx := user.InjectOrgID(context.Background(), "test-user")
	now := time.Now().UnixMilli()

	// Push test series with large labels so the memory difference is meaningful.
	for s := 0; s < numSeries; s++ {
		kvs := make([]string, 0, (numLabels+1)*2)
		kvs = append(kvs, labels.MetricName, fmt.Sprintf("series_%04d", s))
		for j := 0; j < numLabels; j++ {
			kvs = append(kvs, fmt.Sprintf("label_%02d", j), strings.Repeat("x", labelValLen))
		}
		lset := labels.FromStrings(kvs...)
		_, err := i.Push(ctx, writeRequestSingleSeries(lset, []mimirpb.Sample{{TimestampMs: now, Value: float64(s)}}))
		require.NoError(t, err)
	}

	db := i.getTSDB("test-user")
	require.NotNil(t, db)

	innerQ, err := db.ChunkQuerier(now-1000, now+1000)
	require.NoError(t, err)
	defer innerQ.Close()

	// Wrap the real ChunkQuerier so we can observe series lifecycle.
	finalizedCount := &atomic.Int64{}
	trackingQ := &trackingChunkQuerier{
		inner:       innerQ,
		onFinalized: func() { finalizedCount.Add(1) },
	}

	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")}
	stream := &mockQueryStreamServer{ctx: ctx}

	// Phase 1: send series labels to the (mock) querier and populate allSeries.
	allSeries, seriesCount, err := i.sendStreamingQuerySeries(ctx, trackingQ, now-1000, now+1000, matchers, nil, stream)
	require.NoError(t, err)
	require.Equal(t, numSeries, seriesCount)

	// The first GC marks unreachable tracking shells as finalizable; the sleep
	// lets the finalizer goroutine run; the second GC collects them.
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	runtime.GC()

	require.EqualValues(t, numSeries, finalizedCount.Load(),
		"all trackingChunkSeries shells (holding references to series labels) should "+
			"be freed after the series-streaming phase when only IteratorFactory() results are retained")

	// Phase 2: chunk data must still be accessible via the stored iterator factories.
	numSamples, numChunks, _, err := i.sendStreamingQueryChunks(allSeries, stream, 64)
	require.NoError(t, err)
	require.Equal(t, numSeries, numChunks, "expected one chunk per series")
	require.Equal(t, numSeries, numSamples, "expected one sample per series")
}

// trackingChunkQuerier wraps a storage.ChunkQuerier and registers a finalizer on every
// series returned by Select, so tests can detect when the series shell is freed.
type trackingChunkQuerier struct {
	inner       storage.ChunkQuerier
	onFinalized func()
}

func (q *trackingChunkQuerier) Select(ctx context.Context, sorted bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	return &trackingChunkSeriesSet{
		inner:       q.inner.Select(ctx, sorted, hints, matchers...),
		onFinalized: q.onFinalized,
	}
}

func (q *trackingChunkQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *trackingChunkQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.inner.LabelNames(ctx, hints, matchers...)
}

func (q *trackingChunkQuerier) Close() error { return q.inner.Close() }

// trackingChunkSeriesSet wraps a storage.ChunkSeriesSet. At() wraps each series in a
// trackingChunkSeries shell and sets a finalizer on it.
type trackingChunkSeriesSet struct {
	inner       storage.ChunkSeriesSet
	onFinalized func()
}

func (s *trackingChunkSeriesSet) Next() bool                        { return s.inner.Next() }
func (s *trackingChunkSeriesSet) Err() error                        { return s.inner.Err() }
func (s *trackingChunkSeriesSet) Warnings() annotations.Annotations { return s.inner.Warnings() }

func (s *trackingChunkSeriesSet) At() storage.ChunkSeries {
	shell := &trackingChunkSeries{inner: s.inner.At()}
	runtime.SetFinalizer(shell, func(_ *trackingChunkSeries) { s.onFinalized() })
	return shell
}

// trackingChunkSeries is a thin shell around a real storage.ChunkSeries.
// IteratorFactory() delegates to the inner series and does NOT capture the shell
// itself, so the shell is eligible for GC as soon as the caller drops its reference.
type trackingChunkSeries struct {
	inner storage.ChunkSeries
}

func (t *trackingChunkSeries) Labels() labels.Labels { return t.inner.Labels() }
func (t *trackingChunkSeries) Iterator(it chunks.Iterator) chunks.Iterator {
	return t.inner.Iterator(it)
}
func (t *trackingChunkSeries) ChunkCount() (int, error) { return t.inner.ChunkCount() }
func (t *trackingChunkSeries) IteratorFactory() storage.ChunkIterable {
	return t.inner.IteratorFactory()
}

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
		numSeries     = 1000  // Series to push to TSDB
		numLabels     = 20    // Labels per series
		labelValLen   = 1024  // 1KB per label value (~20KB per series)
		numGoroutines = 100   // Concurrent queries
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
		kvs = append(kvs, labels.MetricName, fmt.Sprintf("series_%04d", s))
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

	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, ".*")}

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
	expectedCalls   int64          // Number of Send() calls expected per goroutine
	totalGoroutines int64          // Total number of goroutines
	blockedCount    atomic.Int64   // Number of goroutines currently blocked
	blockCh         chan struct{}  // Closed to release all blocked goroutines
	allBlockedCh    chan struct{}  // Closed when all goroutines are blocked
	allBlockedOnce  sync.Once      // Ensures allBlockedCh is closed only once
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
