// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestSegmentStorage_CommitSegment(t *testing.T) {
	var (
		ctx      = context.Background()
		tenantID = "user-1"
		series1  = mockPreallocTimeseries("series_1")
	)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	db := &metadataDatabaseMock{}

	metadata := NewMetadataStore(db, log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(ctx, metadata))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, metadata))
	})

	t.Run("should upload segment data to object storage and commit reference to metadata store", func(t *testing.T) {
		var committedRefs []SegmentRef

		db.onMaxPartitionOffset = func(ctx context.Context, partitionID int32) (*int64, error) {
			value := int64(2)
			return &value, nil
		}

		db.onInsertSegment = func(ctx context.Context, ref SegmentRef) error {
			committedRefs = append(committedRefs, ref)
			return nil
		}

		storage := NewSegmentStorage(bucket, metadata)

		expectedData := mockSegmentData(tenantID, series1)
		ref, err := storage.CommitSegment(ctx, 1, expectedData)
		require.NoError(t, err)

		// Ensure the returned SegmentRef is the one committed to the metadata store.
		require.Len(t, committedRefs, 1)
		assert.Equal(t, committedRefs[0], ref)
		assert.Equal(t, int64(3), committedRefs[0].OffsetID)

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
		expectedData := mockSegmentData(tenantID, series1)

		rawData, err := expectedData.Marshal()
		require.NoError(t, err)
		require.NoError(t, bucket.Upload(ctx, getSegmentObjectPath(partitionID, objectID), bytes.NewReader(rawData)))

		// Then read it back via SegmentStorage.
		storage := NewSegmentStorage(bucket, NewMetadataStore(&metadataDatabaseMock{}, log.NewNopLogger()))
		actual, err := storage.FetchSegment(ctx, SegmentRef{PartitionID: partitionID, OffsetID: 0, ObjectID: objectID})
		require.NoError(t, err)

		actual.Data.ClearUnmarshalData()
		require.Equal(t, expectedData, actual.Data)
	})
}

type metadataDatabaseMock struct {
	onInsertSegment        func(ctx context.Context, ref SegmentRef) error
	onListSegments         func(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error)
	onMaxPartitionOffset   func(ctx context.Context, partitionID int32) (*int64, error)
	onUpsertConsumerOffset func(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error
	onGetConsumerOffset    func(ctx context.Context, partitionID int32, consumerID string) (*int64, error)
}

func (m *metadataDatabaseMock) Open(ctx context.Context) error {
	return nil
}

func (m *metadataDatabaseMock) Close() {}

func (m *metadataDatabaseMock) InsertSegment(ctx context.Context, ref SegmentRef) error {
	if m.onInsertSegment != nil {
		return m.onInsertSegment(ctx, ref)
	}

	return errors.New("not mocked")
}

func (m *metadataDatabaseMock) ListSegments(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error) {
	if m.onListSegments != nil {
		return m.onListSegments(ctx, partitionID, lastOffsetID)
	}

	return nil, errors.New("not mocked")
}

func (m *metadataDatabaseMock) MaxPartitionOffset(ctx context.Context, partitionID int32) (*int64, error) {
	if m.onMaxPartitionOffset != nil {
		return m.onMaxPartitionOffset(ctx, partitionID)
	}

	return nil, errors.New("not mocked")
}

func (m *metadataDatabaseMock) UpsertConsumerOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error {
	if m.onUpsertConsumerOffset != nil {
		return m.onUpsertConsumerOffset(ctx, partitionID, consumerID, offsetID)
	}

	return errors.New("not mocked")
}

func (m *metadataDatabaseMock) GetConsumerOffset(ctx context.Context, partitionID int32, consumerID string) (*int64, error) {
	if m.onGetConsumerOffset != nil {
		return m.onGetConsumerOffset(ctx, partitionID, consumerID)
	}

	return nil, errors.New("not mocked")
}

// metadataDatabaseMemory is an in-memory MetadataStoreDatabase implementation.
type metadataDatabaseMemory struct {
	mtx      sync.Mutex
	segments []SegmentRef
	offsets  map[offsetKey]int64
}

func newMetadataDatabaseMemory() *metadataDatabaseMemory {
	return &metadataDatabaseMemory{
		offsets: make(map[offsetKey]int64),
	}
}

type offsetKey struct {
	partitionID int32
	consumerID  string
}

func (m *metadataDatabaseMemory) Open(ctx context.Context) error {
	return nil
}

func (m *metadataDatabaseMemory) Close() {}

func (m *metadataDatabaseMemory) InsertSegment(_ context.Context, ref SegmentRef) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.segments = append(m.segments, ref)
	return nil
}

func (m *metadataDatabaseMemory) ListSegments(_ context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// Filter segments.
	var res []SegmentRef
	for _, segment := range m.segments {
		if segment.PartitionID == partitionID && segment.OffsetID > lastOffsetID {
			res = append(res, segment)
		}
	}

	// Sort results by offset ID.
	slices.SortFunc(res, func(a, b SegmentRef) int {
		return int(a.OffsetID) - int(b.OffsetID)
	})

	return res, nil
}

func (m *metadataDatabaseMemory) MaxPartitionOffset(_ context.Context, partitionID int32) (*int64, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var res *int64
	for _, segment := range m.segments {
		if segment.PartitionID != partitionID {
			continue
		}

		if res == nil || segment.OffsetID > *res {
			res = &segment.OffsetID
		}
	}

	return res, nil
}

func (m *metadataDatabaseMemory) UpsertConsumerOffset(_ context.Context, partitionID int32, consumerID string, offsetID int64) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.offsets[offsetKey{partitionID, consumerID}] = offsetID
	return nil
}

func (m *metadataDatabaseMemory) GetConsumerOffset(_ context.Context, partitionID int32, consumerID string) (*int64, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	var offsetVar int64 = -1
	if offset, ok := m.offsets[offsetKey{partitionID, consumerID}]; ok {
		offsetVar = offset
	}

	return &offsetVar, nil
}
