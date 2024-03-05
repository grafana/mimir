package ingest

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

func TestWriteAgent(t *testing.T) {
	logger := log.NewLogfmtLogger(os.Stdout)
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	metadataDB := newMetadataDatabaseMemory()
	metadata := NewMetadataStore(metadataDB, logger)

	mgr, err := services.NewManager(metadata)
	require.NoError(t, err)

	segStorage := NewSegmentStorage(objstore.WrapWithMetrics(bkt, nil, "test"), metadata, nil)

	wa := newWriteAgent(250*time.Millisecond, 1024*1024, time.Second, segStorage, logger, nil, mgr)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), wa))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), wa))
	})

	const writeRequests = 100
	const partitions = 5
	const concurrentWriters = 15

	writeRequestsCh := make(chan *ingestpb.WriteRequest)

	wg := sync.WaitGroup{}
	for i := 0; i < concurrentWriters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for wr := range writeRequestsCh {
				_, err := wa.Write(context.Background(), wr)
				require.NoError(t, err)
			}
		}()
	}

	expectedPieces := map[string]bool{}
	for i := 0; i < writeRequests; i++ {
		partition := int32(i) % partitions

		// create pieceID that's stored into tenant. We will check if all pieces were stored.
		pieceID := fmt.Sprintf("%d-%d", partition, i) // We will test if all write requests made it into segments.
		expectedPieces[pieceID] = true

		writeRequestsCh <- &ingestpb.WriteRequest{
			Piece: &ingestpb.Piece{
				TenantId: pieceID,
			},
			PartitionId: partition,
		}
	}

	close(writeRequestsCh)
	wg.Wait()

	// // Stop WriteAgent to flush remaining in-memory segments.
	// We actually don't need to do that... since Write calls wait until writes are flushed.
	// require.NoError(t, services.StopAndAwaitTerminated(context.Background(), wa))

	foundPieces := map[string]bool{}

	segmentsCount := 0
	piecesCount := 0

	for p := int32(0); p < partitions; p++ {
		segments, err := metadataDB.ListSegments(context.Background(), p, -1)
		require.NoError(t, err)

		segmentsCount += len(segments)
		partitionPrefix := fmt.Sprintf("%d-", p)

		for _, s := range segments {
			seg, err := segStorage.FetchSegment(context.Background(), s)
			require.NoError(t, err)

			piecesCount += len(seg.Data.Pieces)

			for _, p := range seg.Data.Pieces {
				foundPieces[p.TenantId] = true
				require.True(t, strings.HasPrefix(p.TenantId, partitionPrefix))
			}
		}
	}

	require.Equal(t, expectedPieces, foundPieces)
	require.Greater(t, float64(piecesCount)/float64(segmentsCount), float64(1)) // with our concurrency, we expect more than 1 write requests per segment on average.
}

func TestWriteAgent_ConcurrentFlushes(t *testing.T) {
	const (
		numWriteRequests = 100
		numPartitions    = 5
		numWriters       = 15
		numFlushers      = 5
	)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	var (
		logger        = log.NewLogfmtLogger(os.Stdout)
		metadataDB    = newMetadataDatabaseMemory()
		metadataStore = NewMetadataStore(metadataDB, logger)
		segmentStore  = NewSegmentStorage(objstore.WrapWithMetrics(bkt, nil, "test"), metadataStore, nil)
	)

	dependencies, err := services.NewManager(metadataStore)
	require.NoError(t, err)

	// Configured the agent with a very high flush interval, so we're in control of triggering it.
	agent := newWriteAgent(time.Hour, 100*1024*1024, time.Second, segmentStore, logger, nil, dependencies)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), agent))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), agent))
	})

	// Start writers.
	writeRequestsCh := make(chan *ingestpb.WriteRequest)
	writers := sync.WaitGroup{}
	for i := 0; i < numWriters; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()

			for wr := range writeRequestsCh {
				_, err := agent.Write(context.Background(), wr)
				require.NoError(t, err)
			}
		}()
	}

	// Start flushers.
	flushers := sync.WaitGroup{}
	flushersDone := make(chan struct{})
	for i := 0; i < numFlushers; i++ {
		flushers.Add(1)
		go func() {
			defer flushers.Done()

			for {
				select {
				case <-flushersDone:
					return

				default:
					agent.syncFlushPendingSegments(context.Background())
				}
			}
		}()
	}

	// Write all requests while flushers continuously flush segments.
	expectedPieces := map[string]bool{}
	for i := 0; i < numWriteRequests; i++ {
		partition := int32(i) % numPartitions

		// create pieceID that's stored into tenant. We will check if all pieces were stored.
		pieceID := fmt.Sprintf("%d-%d", partition, i) // We will test if all write requests made it into segments.
		expectedPieces[pieceID] = true

		writeRequestsCh <- &ingestpb.WriteRequest{
			Piece: &ingestpb.Piece{
				TenantId: pieceID,
			},
			PartitionId: partition,
		}
	}

	// Let all writers to process all write requests and then stop flushers.
	close(writeRequestsCh)
	writers.Wait()
	close(flushersDone)

	// Check that all expected pieces have been flushed.
	var (
		foundPieces = map[string]bool{}
		piecesCount = 0
	)

	for p := int32(0); p < numPartitions; p++ {
		segments, err := metadataDB.ListSegments(context.Background(), p, -1)
		require.NoError(t, err)

		partitionPrefix := fmt.Sprintf("%d-", p)

		for _, s := range segments {
			seg, err := segmentStore.FetchSegment(context.Background(), s)
			require.NoError(t, err)
			require.Greater(t, len(seg.Data.Pieces), 0)

			piecesCount += len(seg.Data.Pieces)

			for _, p := range seg.Data.Pieces {
				foundPieces[p.TenantId] = true
				require.True(t, strings.HasPrefix(p.TenantId, partitionPrefix))
			}
		}
	}

	require.Equal(t, expectedPieces, foundPieces)
}

func TestWriteAgent_FlushOnceSegmentSizeIsGreaterThanMax(t *testing.T) {
	const (
		numWriteRequests = 1000
		numPartitions    = 1 // Single partition in this test, to guarantee all writers append to the same segment and so the max size is reached continuously.
		numWriters       = 15
		maxSegmentSize   = int64(20)
	)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	var (
		logger        = log.NewLogfmtLogger(os.Stdout)
		metadataDB    = newMetadataDatabaseMemory()
		metadataStore = NewMetadataStore(metadataDB, logger)
		segmentStore  = NewSegmentStorage(objstore.WrapWithMetrics(bkt, nil, "test"), metadataStore, nil)
	)

	dependencies, err := services.NewManager(metadataStore)
	require.NoError(t, err)

	// Configured the agent with a very high flush interval, because we want the max size to trigger.
	agent := newWriteAgent(time.Hour, maxSegmentSize, time.Second, segmentStore, logger, nil, dependencies)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), agent))

	// Start writers.
	writeRequestsCh := make(chan *ingestpb.WriteRequest)
	writers := sync.WaitGroup{}
	for i := 0; i < numWriters; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()

			for wr := range writeRequestsCh {
				_, err := agent.Write(context.Background(), wr)
				require.NoError(t, err)
			}
		}()
	}

	// Write all requests while flushers continuously flush segments.
	expectedPieces := map[string]bool{}
	for i := 0; i < numWriteRequests; i++ {
		partition := int32(i) % numPartitions

		// create pieceID that's stored into tenant. We will check if all pieces were stored.
		pieceID := fmt.Sprintf("%d-%d", partition, i) // We will test if all write requests made it into segments.
		expectedPieces[pieceID] = true

		writeRequestsCh <- &ingestpb.WriteRequest{
			Piece: &ingestpb.Piece{
				TenantId: pieceID,
			},
			PartitionId: partition,
		}
	}

	// Notify writers that there are no more write requests to write.
	close(writeRequestsCh)

	// Wait a bit. Quick and dirty wait to let all Write() requests to be issued.
	time.Sleep(time.Second)

	// At this point we expect that all write requests have been pushed.
	// We have to stop the agent service to trigger the final flush and unblock pending writes.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), agent))

	// Wait until all Write() returned.
	writers.Wait()

	// Check that all expected pieces have been flushed.
	var (
		foundPieces = map[string]bool{}
		piecesCount = 0
	)

	for p := int32(0); p < numPartitions; p++ {
		segments, err := metadataDB.ListSegments(context.Background(), p, -1)
		require.NoError(t, err)

		partitionPrefix := fmt.Sprintf("%d-", p)

		for _, s := range segments {
			seg, err := segmentStore.FetchSegment(context.Background(), s)
			require.NoError(t, err)
			require.Greater(t, len(seg.Data.Pieces), 0)

			piecesCount += len(seg.Data.Pieces)

			for _, p := range seg.Data.Pieces {
				foundPieces[p.TenantId] = true
				require.True(t, strings.HasPrefix(p.TenantId, partitionPrefix))
			}

			// The segment size should not be significantly bigger than the configured max.
			actualSize := int64(0)
			for _, p := range seg.Data.Pieces {
				actualSize += int64(p.Size())
			}
			require.Less(t, actualSize, int64(math.Round(float64(maxSegmentSize)*2)))
		}
	}

	require.Equal(t, expectedPieces, foundPieces)
}
