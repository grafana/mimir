// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket"
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
		reader := NewSegmentReader(instrumentedBucket, metadata, partitionID, -1, 1, time.Second, time.Second, nil, log.NewLogfmtLogger(os.Stdout))
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

func TestSegmentReader_RefetchSegments(t *testing.T) {
	var (
		partitionID = int32(1)
		tenantID    = "user-1"
	)

	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	instrumentedBucket := objstore.WrapWithMetrics(bkt, nil, "test")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)

	// Start the metadata store service.
	metadata := NewMetadataStore(newMetadataDatabaseMemory(), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(ctx, metadata))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, metadata))
	})

	storage := NewSegmentStorage(instrumentedBucket, metadata, nil)
	storage.backoffConfig = backoff.Config{
		MinBackoff: 1 * time.Millisecond,
		MaxBackoff: 1 * time.Millisecond,
		MaxRetries: 3,
	}

	var (
		injectErrors   = map[string]*atomic.Int32{} // Number of errors to inject for given path.
		pathOperations = map[string]*atomic.Int32{} // Number of Get operations observed for given path.
		refs           []SegmentRef                 // Generated refs
		segments       []*ingestpb.Segment          // Generated segments.
	)

	// Commit some segments.
	const segmentsCount = 25
	for i := 0; i < segmentsCount; i++ {
		segment := mockSegmentData(tenantID, mockPreallocTimeseries(fmt.Sprintf("series_%d", i)))
		segments = append(segments, segment)

		ref, err := storage.CommitSegment(ctx, partitionID, segment, time.Second, time.Now())
		require.NoError(t, err)
		refs = append(refs, ref)

		path := getSegmentObjectPath(ref.PartitionID, ref.ObjectID)

		// Further segments will get fewer errors.
		errorsForSegment := segmentsCount - i
		injectErrors[path] = atomic.NewInt32(int32(errorsForSegment))
		pathOperations[path] = atomic.NewInt32(0)
	}

	errorInjector := func(op bucket.Operation, path string) error {
		if op != bucket.OpGet {
			panic(fmt.Sprintf("unexpected op: %d", op))
		}
		pathOperations[path].Inc()
		if injectErrors[path].Dec() >= 0 {
			return fmt.Errorf("bucket error")
		}
		return nil
	}

	errBucket := objstore.WrapWithMetrics(&bucket.ErrorInjectedBucketClient{
		Bucket:   instrumentedBucket,
		Injector: errorInjector,
	}, nil, "err-bucket")

	// Start the reader.
	reader := NewSegmentReader(errBucket, metadata, partitionID, -1, 1, time.Second, time.Second, nil, log.NewLogfmtLogger(os.Stdout))
	reader.backoffConfig = backoff.Config{
		MinBackoff: 1 * time.Millisecond,
		MaxBackoff: 1 * time.Millisecond,
		MaxRetries: 0,
	}

	require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
	})

	// Eventually we should get those segments in correct order.
	for i := 0; i < segmentsCount; i++ {
		actual, err := reader.WaitNextSegment(ctx)
		require.NoError(t, err)
		require.Equal(t, refs[i], actual.Ref)
		require.Equal(t, segments[i], actual.Data)

		// Make sure that bucket injector really produced all errors, and segment was only fetched one more extra time.
		errorsForSegment := segmentsCount - i

		path := getSegmentObjectPath(refs[i].PartitionID, refs[i].ObjectID)

		require.Equal(t, int32(errorsForSegment+1), pathOperations[path].Load())
		require.Equal(t, int32(-1), injectErrors[path].Load())
	}
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
