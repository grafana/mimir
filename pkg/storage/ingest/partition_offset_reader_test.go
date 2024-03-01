// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestPartitionOffsetReader(t *testing.T) {
	const (
		partitionID = int32(0)
	)

	var (
		ctx = context.Background()
	)

	t.Run("should notify waiting goroutines when stopped", func(t *testing.T) {
		metadata := NewMetadataStore(newMetadataDatabaseMemory(), log.NewNopLogger())

		// Run with a very high polling interval, so that it will never run in this test.
		reader := newPartitionOffsetReader(metadata, partitionID, time.Hour, nil, log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))

		// Run few goroutines waiting for the last produced offset.
		wg := sync.WaitGroup{}

		for i := 0; i < 2; i++ {
			runAsync(&wg, func() {
				_, err := reader.FetchLastProducedOffset(ctx)
				assert.Equal(t, errPartitionOffsetReaderStopped, err)
			})
		}

		// Stop the reader.
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))

		// At the point we expect the waiting goroutines to be unblocked.
		wg.Wait()

		// The next call to FetchLastProducedOffset() should return immediately.
		_, err := reader.FetchLastProducedOffset(ctx)
		assert.Equal(t, errPartitionOffsetReaderStopped, err)
	})
}

func TestPartitionOffsetReader_getLastProducedOffset(t *testing.T) {
	const (
		partitionID  = int32(0)
		pollInterval = time.Second
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should return the last produced offset, or -1 if the partition is empty", func(t *testing.T) {
		t.Parallel()

		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
		require.NoError(t, err)

		var (
			reg      = prometheus.NewPedanticRegistry()
			metadata = NewMetadataStore(newMetadataDatabaseMemory(), log.NewNopLogger())
			storage  = NewSegmentStorage(bucket, metadata)
			reader   = newPartitionOffsetReader(metadata, partitionID, pollInterval, reg, logger)
		)

		offset, err := reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), offset)

		// Write the 1st message.
		produceSegment(ctx, t, storage, partitionID, "message 1")

		offset, err = reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(0), offset)

		// Write the 2nd message.
		produceSegment(ctx, t, storage, partitionID, "message 2")

		offset, err = reader.getLastProducedOffset(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(1), offset)

		assert.NoError(t, promtest.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_reader_last_produced_offset_failures_total Total number of failed requests to get the last produced offset.
			# TYPE cortex_ingest_storage_reader_last_produced_offset_failures_total counter
			cortex_ingest_storage_reader_last_produced_offset_failures_total{partition="0"} 0

			# HELP cortex_ingest_storage_reader_last_produced_offset_requests_total Total number of requests issued to get the last produced offset.
			# TYPE cortex_ingest_storage_reader_last_produced_offset_requests_total counter
			cortex_ingest_storage_reader_last_produced_offset_requests_total{partition="0"} 3
		`), "cortex_ingest_storage_reader_last_produced_offset_requests_total",
			"cortex_ingest_storage_reader_last_produced_offset_failures_total"))
	})

	t.Run("should honor context deadline and not fail other in-flight requests issued while the canceled one was still running", func(t *testing.T) {
		t.Parallel()

		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
		require.NoError(t, err)

		var (
			reg      = prometheus.NewPedanticRegistry()
			db       = newMetadataDatabaseMemory()
			metadata = NewMetadataStore(db, log.NewNopLogger())
			storage  = NewSegmentStorage(bucket, metadata)
			reader   = newPartitionOffsetReader(metadata, partitionID, pollInterval, reg, logger)

			firstRequest         = atomic.NewBool(true)
			firstRequestReceived = make(chan struct{})
			firstRequestTimeout  = time.Second
		)

		// Write some messages.
		produceSegment(ctx, t, storage, partitionID, "message 1")
		produceSegment(ctx, t, storage, partitionID, "message 2")
		expectedOffset := int64(1)

		// Slow down the 1st MaxPartitionOffset() request.
		db.registerBeforeMaxPartitionOffsetHook(func(ctx context.Context, partitionID int32) (*int64, error, bool) {
			if firstRequest.CompareAndSwap(true, false) {
				close(firstRequestReceived)
				time.Sleep(2 * firstRequestTimeout)
			}

			// Let the in-memory database handle the request.
			return nil, nil, false
		})

		wg := sync.WaitGroup{}

		// Run the 1st getLastProducedOffset() with a timeout which is expected to expire
		// before the request will succeed.
		runAsync(&wg, func() {
			ctxWithTimeout, cancel := context.WithTimeout(ctx, firstRequestTimeout)
			defer cancel()

			_, err := reader.getLastProducedOffset(ctxWithTimeout)
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})

		// Run a 2nd getLastProducedOffset() once the 1st request is received. This request
		// is expected to succeed.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			offset, err := reader.getLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, expectedOffset, offset)
		})

		wg.Wait()
	})
}

func TestPartitionOffsetReader_FetchLastProducedOffset(t *testing.T) {
	const (
		partitionID  = int32(0)
		pollInterval = time.Second
	)

	var (
		ctx    = context.Background()
		logger = log.NewNopLogger()
	)

	t.Run("should wait the result of the next request issued", func(t *testing.T) {
		var (
			db       = newMetadataDatabaseMemory()
			metadata = NewMetadataStore(db, log.NewNopLogger())
			reader   = newPartitionOffsetReader(metadata, partitionID, pollInterval, nil, logger)

			lastOffset           = atomic.NewInt64(0)
			firstRequestReceived = make(chan struct{})
		)

		db.registerBeforeMaxPartitionOffsetHook(func(ctx context.Context, partitionID int32) (*int64, error, bool) {
			if lastOffset.Load() == 0 {
				close(firstRequestReceived)
			}

			// Mock the response so that we can increase the offset each time.
			offset := lastOffset.Inc()
			return &offset, nil, true
		})

		wg := sync.WaitGroup{}

		// The 1st FetchLastProducedOffset() is called before the service start so it's expected
		// to wait the result of the 1st request.
		runAsync(&wg, func() {
			actual, err := reader.FetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(1), actual)
		})

		// The 2nd FetchLastProducedOffset() is called while the 1st request is running, so it's expected
		// to wait the result of the 2nd request.
		runAsyncAfter(&wg, firstRequestReceived, func() {
			actual, err := reader.FetchLastProducedOffset(ctx)
			require.NoError(t, err)
			assert.Equal(t, int64(2), actual)
		})

		// Now we can start the service.
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		wg.Wait()
	})

	t.Run("should immediately return if the context gets canceled", func(t *testing.T) {
		var (
			metadata = NewMetadataStore(newMetadataDatabaseMemory(), log.NewNopLogger())
		)

		// Create the reader but do NOT start it, so that the "last produced offset" will be never fetched.
		reader := newPartitionOffsetReader(metadata, partitionID, pollInterval, nil, logger)

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		_, err := reader.FetchLastProducedOffset(canceledCtx)
		assert.ErrorIs(t, err, context.Canceled)
	})
}
