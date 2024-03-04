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
	"github.com/thanos-io/objstore"

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

	instrumentedBucket := objstore.WrapWithMetrics(bucket, nil, "test")

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

		db.onInsertSegment = func(ctx context.Context, ref SegmentRef, _ time.Time) error {
			committedRefs = append(committedRefs, ref)
			return nil
		}

		storage := NewSegmentStorage(instrumentedBucket, metadata, nil)

		expectedData := mockSegmentData(tenantID, series1)
		ref, err := storage.CommitSegment(ctx, 1, expectedData, time.Now())
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

	instrumentedBucket := objstore.WrapWithMetrics(bucket, nil, "test")

	t.Run("should read a segment from the storage", func(t *testing.T) {
		// Upload a segment to the storage.
		expectedData := mockSegmentData(tenantID, series1)

		rawData, err := expectedData.Marshal()
		require.NoError(t, err)
		require.NoError(t, bucket.Upload(ctx, getSegmentObjectPath(partitionID, objectID), bytes.NewReader(rawData)))

		// Then read it back via SegmentStorage.
		storage := NewSegmentStorage(instrumentedBucket, NewMetadataStore(&metadataDatabaseMock{}, log.NewNopLogger()), nil)
		actual, err := storage.FetchSegment(ctx, SegmentRef{PartitionID: partitionID, OffsetID: 0, ObjectID: objectID})
		require.NoError(t, err)

		actual.Data.ClearUnmarshalData()
		require.Equal(t, expectedData, actual.Data)
	})
}

func TestSegmentStorage_DeleteSegment(t *testing.T) {
	var (
		ctx      = context.Background()
		segment1 = mockSegmentData("user-1", mockPreallocTimeseries("series_1"))
		segment2 = mockSegmentData("user-2", mockPreallocTimeseries("series_2"))
		segment3 = mockSegmentData("user-3", mockPreallocTimeseries("series_3"))
	)

	bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
	require.NoError(t, err)

	instrumentedBucket := objstore.WrapWithMetrics(bucket, nil, "test")

	t.Run("should delete the requested segment", func(t *testing.T) {
		var (
			deletedRefsMx sync.Mutex
			deletedRefs   []SegmentRef
		)

		metadataDB := newMetadataDatabaseMemory()
		metadataDB.registerBeforeDeleteSegmentHook(func(ctx context.Context, ref SegmentRef) (error, bool) {
			deletedRefsMx.Lock()
			deletedRefs = append(deletedRefs, ref)
			deletedRefsMx.Unlock()

			return nil, false
		})

		storage := NewSegmentStorage(instrumentedBucket, NewMetadataStore(metadataDB, log.NewNopLogger()), nil)

		// Commit some segments.
		ref1, err := storage.CommitSegment(ctx, 1, segment1, time.Now())
		require.NoError(t, err)
		ref2, err := storage.CommitSegment(ctx, 1, segment2, time.Now())
		require.NoError(t, err)
		ref3, err := storage.CommitSegment(ctx, 1, segment3, time.Now())
		require.NoError(t, err)

		// Delete a segment.
		require.NoError(t, storage.DeleteSegment(ctx, ref2))

		// Ensure the right segment has been deleted from the metadata store.
		func() {
			deletedRefsMx.Lock()
			defer deletedRefsMx.Unlock()

			assert.Equal(t, []SegmentRef{ref2}, deletedRefs)
		}()

		// Ensure the right segment has been deleted from the object storage.
		exists, err := bucket.Exists(ctx, getSegmentObjectPath(ref1.PartitionID, ref1.ObjectID))
		require.NoError(t, err)
		assert.True(t, exists)

		exists, err = bucket.Exists(ctx, getSegmentObjectPath(ref2.PartitionID, ref2.ObjectID))
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = bucket.Exists(ctx, getSegmentObjectPath(ref3.PartitionID, ref3.ObjectID))
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("should NOT return error if the segment has already been deleted", func(t *testing.T) {
		metadataDB := newMetadataDatabaseMemory()
		storage := NewSegmentStorage(instrumentedBucket, NewMetadataStore(metadataDB, log.NewNopLogger()), nil)

		// Commit a segment.
		ref1, err := storage.CommitSegment(ctx, 1, segment1, time.Now())
		require.NoError(t, err)

		// Delete the same segment multiple times.
		require.NoError(t, storage.DeleteSegment(ctx, ref1))
		require.NoError(t, storage.DeleteSegment(ctx, ref1))

		// Ensure the segment has been deleted from the object storage.
		exists, err := bucket.Exists(ctx, getSegmentObjectPath(ref1.PartitionID, ref1.ObjectID))
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

type metadataDatabaseMock struct {
	onInsertSegment             func(ctx context.Context, ref SegmentRef, now time.Time) error
	onDeleteSegment             func(ctx context.Context, ref SegmentRef) error
	onListSegments              func(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error)
	onListSegmentsCreatedBefore func(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error)
	onMaxPartitionOffset        func(ctx context.Context, partitionID int32) (*int64, error)
	onUpsertConsumerOffset      func(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error
	onGetConsumerOffset         func(ctx context.Context, partitionID int32, consumerID string) (*int64, error)
}

func (m *metadataDatabaseMock) Open(ctx context.Context) error {
	return nil
}

func (m *metadataDatabaseMock) Close() {}

func (m *metadataDatabaseMock) InsertSegment(ctx context.Context, ref SegmentRef, now time.Time) error {
	if m.onInsertSegment != nil {
		return m.onInsertSegment(ctx, ref, now)
	}

	return errors.New("not mocked")
}

func (m *metadataDatabaseMock) DeleteSegment(ctx context.Context, ref SegmentRef) error {
	if m.onDeleteSegment != nil {
		return m.onDeleteSegment(ctx, ref)
	}

	return errors.New("not mocked")
}

func (m *metadataDatabaseMock) ListSegments(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error) {
	if m.onListSegments != nil {
		return m.onListSegments(ctx, partitionID, lastOffsetID)
	}

	return nil, errors.New("not mocked")
}

func (m *metadataDatabaseMock) ListSegmentsCreatedBefore(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error) {
	if m.onListSegmentsCreatedBefore != nil {
		return m.onListSegmentsCreatedBefore(ctx, threshold, limit)
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

type offsetKey struct {
	partitionID int32
	consumerID  string
}

type segmentsRow struct {
	ref       SegmentRef
	createdAt time.Time
}

// metadataDatabaseMemory is an in-memory MetadataStoreDatabase implementation.
type metadataDatabaseMemory struct {
	mtx      sync.Mutex
	segments []segmentsRow
	offsets  map[offsetKey]int64

	// Hooks to add custom logic before database APIs are called.
	beforeHooksMx                   sync.Mutex
	beforeInsertSegment             func(ctx context.Context, ref SegmentRef, now time.Time) (error, bool)
	beforeDeleteSegment             func(ctx context.Context, ref SegmentRef) (error, bool)
	beforeListSegments              func(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error, bool)
	beforeListSegmentsCreatedBefore func(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error, bool)
	beforeMaxPartitionOffset        func(ctx context.Context, partitionID int32) (*int64, error, bool)
	beforeUpsertConsumerOffset      func(ctx context.Context, partitionID int32, consumerID string, offsetID int64) (error, bool)
	beforeGetConsumerOffset         func(ctx context.Context, partitionID int32, consumerID string) (*int64, error, bool)
}

func newMetadataDatabaseMemory() *metadataDatabaseMemory {
	return &metadataDatabaseMemory{
		offsets: make(map[offsetKey]int64),
	}
}

func (m *metadataDatabaseMemory) Open(ctx context.Context) error {
	return nil
}

func (m *metadataDatabaseMemory) Close() {}

func (m *metadataDatabaseMemory) InsertSegment(ctx context.Context, ref SegmentRef, now time.Time) error {
	if hook := m.getBeforeInsertSegmentHook(); hook != nil {
		if err, handled := hook(ctx, ref, now); handled {
			return err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.segments = append(m.segments, segmentsRow{
		ref:       ref,
		createdAt: now,
	})
	return nil
}

func (m *metadataDatabaseMemory) DeleteSegment(ctx context.Context, ref SegmentRef) error {
	if hook := m.getBeforeDeleteSegmentHook(); hook != nil {
		if err, handled := hook(ctx, ref); handled {
			return err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	for idx, row := range m.segments {
		if row.ref.PartitionID == ref.PartitionID && row.ref.OffsetID == ref.OffsetID && row.ref.ObjectID.Compare(ref.ObjectID) == 0 {
			m.segments = append(m.segments[:idx], m.segments[idx+1:]...)
			break
		}
	}

	return nil
}

func (m *metadataDatabaseMemory) ListSegments(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error) {
	if hook := m.getBeforeListSegmentsHook(); hook != nil {
		if refs, err, handled := hook(ctx, partitionID, lastOffsetID); handled {
			return refs, err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	// Filter segments.
	var res []SegmentRef
	for _, row := range m.segments {
		if row.ref.PartitionID == partitionID && row.ref.OffsetID > lastOffsetID {
			res = append(res, row.ref)
		}
	}

	// Sort results by offset ID.
	slices.SortFunc(res, func(a, b SegmentRef) int {
		return int(a.OffsetID) - int(b.OffsetID)
	})

	return res, nil
}

func (m *metadataDatabaseMemory) ListSegmentsCreatedBefore(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error) {
	if hook := m.getBeforeListSegmentsCreatedBeforeHook(); hook != nil {
		if refs, err, handled := hook(ctx, threshold, limit); handled {
			return refs, err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	var res []SegmentRef
	for _, row := range m.segments {
		// Enforce limit.
		if len(res) > limit {
			break
		}

		if row.createdAt.Before(threshold) {
			res = append(res, row.ref)
		}
	}

	return res, nil
}

func (m *metadataDatabaseMemory) MaxPartitionOffset(ctx context.Context, partitionID int32) (*int64, error) {
	if hook := m.getBeforeMaxPartitionOffsetHook(); hook != nil {
		if offset, err, handled := hook(ctx, partitionID); handled {
			return offset, err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	var res *int64
	for _, row := range m.segments {
		if row.ref.PartitionID != partitionID {
			continue
		}

		if res == nil || row.ref.OffsetID > *res {
			value := row.ref.OffsetID
			res = &value
		}
	}

	return res, nil
}

func (m *metadataDatabaseMemory) UpsertConsumerOffset(ctx context.Context, partitionID int32, consumerID string, offsetID int64) error {
	if hook := m.getBeforeUpsertConsumerOffsetHook(); hook != nil {
		if err, handled := hook(ctx, partitionID, consumerID, offsetID); handled {
			return err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.offsets[offsetKey{partitionID, consumerID}] = offsetID
	return nil
}

func (m *metadataDatabaseMemory) GetConsumerOffset(ctx context.Context, partitionID int32, consumerID string) (*int64, error) {
	if hook := m.getBeforeGetConsumerOffsetHook(); hook != nil {
		if offset, err, handled := hook(ctx, partitionID, consumerID); handled {
			return offset, err
		}
	}

	// Ensure context hasn't been canceled in the meanwhile.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	var offsetVar int64 = -1
	if offset, ok := m.offsets[offsetKey{partitionID, consumerID}]; ok {
		offsetVar = offset
	}

	return &offsetVar, nil
}

func (m *metadataDatabaseMemory) registerBeforeInsertSegmentHook(hook func(ctx context.Context, ref SegmentRef, now time.Time) (error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeInsertSegment = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeDeleteSegmentHook(hook func(ctx context.Context, ref SegmentRef) (error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeDeleteSegment = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeListSegmentsHook(hook func(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeListSegments = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeListSegmentsCreatedBeforeHook(hook func(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeListSegmentsCreatedBefore = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeMaxPartitionOffsetHook(hook func(ctx context.Context, partitionID int32) (*int64, error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeMaxPartitionOffset = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeUpsertConsumerOffsetHook(hook func(ctx context.Context, partitionID int32, consumerID string, offsetID int64) (error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeUpsertConsumerOffset = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) registerBeforeGetConsumerOffsetHook(hook func(ctx context.Context, partitionID int32, consumerID string) (*int64, error, bool)) {
	m.beforeHooksMx.Lock()
	m.beforeGetConsumerOffset = hook
	m.beforeHooksMx.Unlock()
}

func (m *metadataDatabaseMemory) getBeforeInsertSegmentHook() func(ctx context.Context, ref SegmentRef, now time.Time) (error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeInsertSegment
}

func (m *metadataDatabaseMemory) getBeforeDeleteSegmentHook() func(ctx context.Context, ref SegmentRef) (error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeDeleteSegment
}

func (m *metadataDatabaseMemory) getBeforeListSegmentsHook() func(ctx context.Context, partitionID int32, lastOffsetID int64) ([]SegmentRef, error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeListSegments
}

func (m *metadataDatabaseMemory) getBeforeListSegmentsCreatedBeforeHook() func(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeListSegmentsCreatedBefore
}

func (m *metadataDatabaseMemory) getBeforeMaxPartitionOffsetHook() func(ctx context.Context, partitionID int32) (*int64, error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeMaxPartitionOffset
}

func (m *metadataDatabaseMemory) getBeforeUpsertConsumerOffsetHook() func(ctx context.Context, partitionID int32, consumerID string, offsetID int64) (error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeUpsertConsumerOffset
}

func (m *metadataDatabaseMemory) getBeforeGetConsumerOffsetHook() func(ctx context.Context, partitionID int32, consumerID string) (*int64, error, bool) {
	m.beforeHooksMx.Lock()
	defer m.beforeHooksMx.Unlock()

	return m.beforeGetConsumerOffset
}
