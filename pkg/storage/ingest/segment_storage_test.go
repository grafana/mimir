// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

func TestSegmentStorage_CommitSegment(t *testing.T) {
	var (
		ctx      = context.Background()
		tenantID = "user-1"
		series1  = mockPreallocTimeseries("series_1")
	)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	t.Run("should upload segment data to object storage and commit reference to metadata store", func(t *testing.T) {
		var committedRefs []SegmentRef

		metadata := &metadataStoreMock{}
		metadata.onCommitSegment = func(_ context.Context, partitionID int32, objectID ulid.ULID) (SegmentRef, error) {
			ref := SegmentRef{
				PartitionID: partitionID,
				OffsetID:    2,
				ObjectID:    objectID,
			}

			committedRefs = append(committedRefs, ref)
			return ref, nil
		}

		expectedData := &ingestpb.Segment{Pieces: []*ingestpb.Piece{
			{
				WriteRequests: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}},
				TenantId:      tenantID,
			},
		}}

		storage := NewSegmentStorage(bucket, metadata)
		ref, err := storage.CommitSegment(ctx, 1, expectedData)
		require.NoError(t, err)

		// Ensure the returned SegmentRef is the one committed to the metadata store.
		require.Len(t, committedRefs, 1)
		assert.Equal(t, committedRefs[0], ref)

		// Ensure the segment data has been uploaded to the storage.
		exists, err := bucket.Exists(ctx, getSegmentObjectPath(ref.PartitionID, ref.ObjectID))
		require.NoError(t, err)
		require.True(t, exists)

		reader, err := bucket.Get(ctx, getSegmentObjectPath(ref.PartitionID, ref.ObjectID))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reader.Close()) })

		actualData, err := readSegmentObject(reader)
		require.NoError(t, err)

		actualData.ClearUnmarshalData()
		require.Equal(t, expectedData, actualData)
	})
}

func TestSegmentStorage_FetchSegment(t *testing.T) {
	var (
		ctx         = context.Background()
		tenantID    = "user-1"
		partitionID = int32(1)
		objectID    = ulid.MustNew(uint64(time.Now().UnixMilli()), nil)
		series1     = mockPreallocTimeseries("series_1")
	)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	t.Run("should read a segment from the storage", func(t *testing.T) {
		// Upload a segment to the storage.
		expectedData := &ingestpb.Segment{Pieces: []*ingestpb.Piece{
			{
				WriteRequests: &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{series1}},
				TenantId:      tenantID,
			},
		}}

		rawData, err := expectedData.Marshal()
		require.NoError(t, err)
		require.NoError(t, bucket.Upload(ctx, getSegmentObjectPath(partitionID, objectID), bytes.NewReader(rawData)))

		// Then read it back via SegmentStorage.
		storage := NewSegmentStorage(bucket, &metadataStoreMock{})
		actual, err := storage.FetchSegment(ctx, SegmentRef{PartitionID: partitionID, OffsetID: 0, ObjectID: objectID})
		require.NoError(t, err)

		actual.Data.ClearUnmarshalData()
		require.Equal(t, expectedData, actual.Data)
	})
}

type metadataStoreMock struct {
	onCommitSegment func(ctx context.Context, partitionID int32, objectID ulid.ULID) (SegmentRef, error)
}

func (m *metadataStoreMock) CommitSegment(ctx context.Context, partitionID int32, objectID ulid.ULID) (SegmentRef, error) {
	if m.onCommitSegment != nil {
		return m.onCommitSegment(ctx, partitionID, objectID)
	}

	return SegmentRef{}, errors.New("not mocked")
}

func (m *metadataStoreMock) WatchSegments(ctx context.Context, partitionID int32, lastOffsetID int64) []SegmentRef {
	return nil
}

func (m *metadataStoreMock) GetLastProducedOffsetID(ctx context.Context, partitionID int32) (int64, error) {
	return 0, errors.New("not implemented")
}

func (m *metadataStoreMock) CommitLastConsumedOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error {
	return errors.New("not implemented")
}

func (m *metadataStoreMock) GetLastConsumedOffsetID(ctx context.Context, partitionID int32, consumerID string) (int64, error) {
	return 0, errors.New("not implemented")
}
