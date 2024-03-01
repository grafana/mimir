// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
)

func TestGarbageCollector_cleanupSegments(t *testing.T) {
	var (
		ctx      = context.Background()
		segment1 = mockSegmentData("user-1", mockPreallocTimeseries("series_1"))
		segment2 = mockSegmentData("user-2", mockPreallocTimeseries("series_2"))
		segment3 = mockSegmentData("user-3", mockPreallocTimeseries("series_3"))
	)

	cfg := GarbageCollectorConfig{}
	flagext.DefaultValues(&cfg)

	t.Run("should return error if fails to list segments to delete from metadata store", func(t *testing.T) {
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
		require.NoError(t, err)

		var (
			reg           = prometheus.NewPedanticRegistry()
			metadataDB    = newMetadataDatabaseMemory()
			metadataStore = NewMetadataStore(metadataDB, log.NewNopLogger())
			segmentStore  = NewSegmentStorage(bucket, metadataStore)
			gc            = NewGarbageCollector(cfg, metadataStore, segmentStore, log.NewNopLogger(), reg)
			expectedErr   = errors.New("mocked error")
		)

		metadataDB.registerBeforeListSegmentsCreatedBeforeHook(func(ctx context.Context, threshold time.Time, limit int) ([]SegmentRef, error, bool) {
			return nil, expectedErr, true
		})

		require.ErrorIs(t, gc.cleanupSegments(ctx), expectedErr)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_segments_cleaned_total Total number of segments deleted.
			# TYPE cortex_ingest_storage_segments_cleaned_total counter
			cortex_ingest_storage_segments_cleaned_total 0
			
			# HELP cortex_ingest_storage_segments_cleanup_failures_total Total number of segments failed to be deleted.
			# TYPE cortex_ingest_storage_segments_cleanup_failures_total counter
			cortex_ingest_storage_segments_cleanup_failures_total 0
		`),
			"cortex_ingest_storage_segments_cleaned_total",
			"cortex_ingest_storage_segments_cleanup_failures_total"))
	})

	t.Run("should delete segments created longer than retention period ago", func(t *testing.T) {
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
		require.NoError(t, err)

		var (
			reg           = prometheus.NewPedanticRegistry()
			metadataDB    = newMetadataDatabaseMemory()
			metadataStore = NewMetadataStore(metadataDB, log.NewNopLogger())
			segmentStore  = NewSegmentStorage(bucket, metadataStore)
			gc            = NewGarbageCollector(cfg, metadataStore, segmentStore, log.NewNopLogger(), reg)
			expectedErr   = errors.New("mocked error")

			deletedRefsMx sync.Mutex
			deletedRefs   []SegmentRef
		)

		metadataDB.registerBeforeDeleteSegmentHook(func(ctx context.Context, ref SegmentRef) (error, bool) {
			deletedRefsMx.Lock()
			deletedRefs = append(deletedRefs, ref)
			deletedRefsMx.Unlock()

			return nil, false
		})

		// Commit some segments.
		ref1, err := segmentStore.CommitSegment(ctx, 1, segment1, time.Now())
		require.NoError(t, err)
		ref2, err := segmentStore.CommitSegment(ctx, 1, segment2, time.Now().Add(-2*cfg.RetentionPeriod))
		require.NoError(t, err)
		ref3, err := segmentStore.CommitSegment(ctx, 1, segment3, time.Now())
		require.NoError(t, err)

		// Run garbage collection.
		require.NoError(t, gc.cleanupSegments(ctx), expectedErr)

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

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_segments_cleaned_total Total number of segments deleted.
			# TYPE cortex_ingest_storage_segments_cleaned_total counter
			cortex_ingest_storage_segments_cleaned_total 1
			
			# HELP cortex_ingest_storage_segments_cleanup_failures_total Total number of segments failed to be deleted.
			# TYPE cortex_ingest_storage_segments_cleanup_failures_total counter
			cortex_ingest_storage_segments_cleanup_failures_total 0
		`),
			"cortex_ingest_storage_segments_cleaned_total",
			"cortex_ingest_storage_segments_cleanup_failures_total"))
	})

	t.Run("should support deleting multiple segments", func(t *testing.T) {
		bucket, err := filesystem.NewBucketClient(filesystem.Config{Directory: t.TempDir()})
		require.NoError(t, err)

		var (
			reg           = prometheus.NewPedanticRegistry()
			metadataDB    = newMetadataDatabaseMemory()
			metadataStore = NewMetadataStore(metadataDB, log.NewNopLogger())
			segmentStore  = NewSegmentStorage(bucket, metadataStore)
			gc            = NewGarbageCollector(cfg, metadataStore, segmentStore, log.NewNopLogger(), reg)
			expectedErr   = errors.New("mocked error")

			deletedRefsMx sync.Mutex
			deletedRefs   []SegmentRef
		)

		metadataDB.registerBeforeDeleteSegmentHook(func(ctx context.Context, ref SegmentRef) (error, bool) {
			deletedRefsMx.Lock()
			deletedRefs = append(deletedRefs, ref)
			deletedRefsMx.Unlock()

			return nil, false
		})

		// Commit some segments.
		ref1, err := segmentStore.CommitSegment(ctx, 1, segment1, time.Now().Add(-2*cfg.RetentionPeriod))
		require.NoError(t, err)
		ref2, err := segmentStore.CommitSegment(ctx, 1, segment2, time.Now().Add(-2*cfg.RetentionPeriod))
		require.NoError(t, err)
		ref3, err := segmentStore.CommitSegment(ctx, 1, segment3, time.Now().Add(-2*cfg.RetentionPeriod))
		require.NoError(t, err)

		// Run garbage collection.
		require.NoError(t, gc.cleanupSegments(ctx), expectedErr)

		// Ensure the segments have been deleted from the metadata store.
		func() {
			deletedRefsMx.Lock()
			defer deletedRefsMx.Unlock()

			assert.ElementsMatch(t, []SegmentRef{ref1, ref2, ref3}, deletedRefs)
		}()

		// Ensure the segments have been deleted from the object storage.
		exists, err := bucket.Exists(ctx, getSegmentObjectPath(ref1.PartitionID, ref1.ObjectID))
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = bucket.Exists(ctx, getSegmentObjectPath(ref2.PartitionID, ref2.ObjectID))
		require.NoError(t, err)
		assert.False(t, exists)

		exists, err = bucket.Exists(ctx, getSegmentObjectPath(ref3.PartitionID, ref3.ObjectID))
		require.NoError(t, err)
		assert.False(t, exists)

		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP cortex_ingest_storage_segments_cleaned_total Total number of segments deleted.
			# TYPE cortex_ingest_storage_segments_cleaned_total counter
			cortex_ingest_storage_segments_cleaned_total 3
			
			# HELP cortex_ingest_storage_segments_cleanup_failures_total Total number of segments failed to be deleted.
			# TYPE cortex_ingest_storage_segments_cleanup_failures_total counter
			cortex_ingest_storage_segments_cleanup_failures_total 0
		`),
			"cortex_ingest_storage_segments_cleaned_total",
			"cortex_ingest_storage_segments_cleanup_failures_total"))
	})
}
