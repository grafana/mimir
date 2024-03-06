// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

func TestSegmentReader_WaitNextSegment(t *testing.T) {
	var (
		partitionID = int32(1)
		tenantID    = "user-1"
		segment1    = mockSegmentData(tenantID, mockPreallocTimeseries("series_1"))
		segment2    = mockSegmentData(tenantID, mockPreallocTimeseries("series_2"))
		segment3    = mockSegmentData(tenantID, mockPreallocTimeseries("series_3"))
	)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	instrumentedBucket := objstore.WrapWithMetrics(bucket, nil, "test")

	t.Run("should return the next segment", func(t *testing.T) {
		// Ensure the test will not block indefinitely in case of bugs.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		t.Cleanup(cancel)

		// Start the metadata store service.
		metadata := NewMetadataStore(newMetadataDatabaseMemory(), log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, metadata))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, metadata))
		})

		// Start the reader.
		reader := NewSegmentReader(instrumentedBucket, metadata, partitionID, -1, 1, time.Second, time.Second, nil, log.NewNopLogger())
		require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
		t.Cleanup(func() {
			require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
		})

		// Commit some segments.
		storage := NewSegmentStorage(instrumentedBucket, metadata, nil)
		ref1, err := storage.CommitSegment(ctx, partitionID, segment1, time.Second, time.Now())
		require.NoError(t, err)
		ref2, err := storage.CommitSegment(ctx, partitionID, segment2, time.Second, time.Now())
		require.NoError(t, err)

		// Read the committed segments.
		actual, err := reader.WaitNextSegment(ctx)
		require.NoError(t, err)

		actual.Data.ClearUnmarshalData()
		require.Equal(t, ref1, actual.Ref)
		require.Equal(t, segment1, actual.Data)

		actual, err = reader.WaitNextSegment(ctx)
		require.NoError(t, err)

		actual.Data.ClearUnmarshalData()
		require.Equal(t, ref2, actual.Ref)
		require.Equal(t, segment2, actual.Data)

		// Now wait for the next segment, which hasn't been committed yet.
		var (
			asyncSegment *Segment
			asyncErr     error
			wg           = sync.WaitGroup{}
		)

		wg.Add(1)
		go func() {
			defer wg.Done()
			asyncSegment, asyncErr = reader.WaitNextSegment(ctx)
		}()

		// Commit the next segment. We expect WaitNextSegment() will return it.
		ref3, err := storage.CommitSegment(ctx, partitionID, segment3, time.Second, time.Now())
		require.NoError(t, err)

		wg.Wait()

		require.NoError(t, asyncErr)
		asyncSegment.Data.ClearUnmarshalData()
		require.Equal(t, ref3, asyncSegment.Ref)
		require.Equal(t, segment3, asyncSegment.Data)
	})
}

func mockSegmentData(tenantID string, series mimirpb.PreallocTimeseries) *ingestpb.Segment {
	wr := &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series}}
	d, err := wr.Marshal()
	if err != nil {
		panic(err.Error())
	}

	return &ingestpb.Segment{Pieces: []*ingestpb.Piece{
		{
			Data:     d,
			TenantId: tenantID,
		},
	}}
}
