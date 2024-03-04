package ingest

import (
	"context"
	"fmt"
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

	wa := newWriteAgent(250*time.Millisecond, segStorage, logger, nil, mgr)
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
