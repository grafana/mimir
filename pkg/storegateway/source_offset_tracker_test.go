// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSourceOffsetTracker_Update(t *testing.T) {
	t.Run("nil offsets are ignored", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(nil)

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Empty(t, tracker.offsets)
	})

	t.Run("empty offsets are ignored", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(map[int32]int64{})

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Empty(t, tracker.offsets)
	})

	t.Run("offsets are stored", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(map[int32]int64{0: 100, 1: 200})

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(100), tracker.offsets[0])
		assert.Equal(t, int64(200), tracker.offsets[1])
	})

	t.Run("higher offsets replace lower ones", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(map[int32]int64{0: 100})
		tracker.update(map[int32]int64{0: 200})

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(200), tracker.offsets[0])
	})

	t.Run("lower offsets do not replace higher ones", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(map[int32]int64{0: 200})
		tracker.update(map[int32]int64{0: 100})

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(200), tracker.offsets[0])
	})

	t.Run("different partitions are tracked independently", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		tracker.update(map[int32]int64{0: 100})
		tracker.update(map[int32]int64{1: 50})
		tracker.update(map[int32]int64{0: 150, 1: 200})

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(150), tracker.offsets[0])
		assert.Equal(t, int64(200), tracker.offsets[1])
	})
}

func TestSourceOffsetTracker_Metric(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	tracker := newSourceOffsetTracker(reg)
	tracker.update(map[int32]int64{0: 100, 5: 500})

	expected := `
		# HELP cortex_bucket_store_highest_seen_source_offset The highest Kafka source offset seen across all blocks for each partition.
		# TYPE cortex_bucket_store_highest_seen_source_offset gauge
		cortex_bucket_store_highest_seen_source_offset{partition="0"} 100
		cortex_bucket_store_highest_seen_source_offset{partition="5"} 500
	`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "cortex_bucket_store_highest_seen_source_offset"))

	tracker.update(map[int32]int64{0: 300})

	expected = `
		# HELP cortex_bucket_store_highest_seen_source_offset The highest Kafka source offset seen across all blocks for each partition.
		# TYPE cortex_bucket_store_highest_seen_source_offset gauge
		cortex_bucket_store_highest_seen_source_offset{partition="0"} 300
		cortex_bucket_store_highest_seen_source_offset{partition="5"} 500
	`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "cortex_bucket_store_highest_seen_source_offset"))
}

func TestSourceOffsetTracker_ConcurrentUpdates(t *testing.T) {
	t.Run("concurrent updates to the same partition converge to the max", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())

		const numGoroutines = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			offset := int64(i)
			go func() {
				defer wg.Done()
				tracker.update(map[int32]int64{0: offset})
			}()
		}
		wg.Wait()

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(numGoroutines-1), tracker.offsets[0])
	})

	t.Run("concurrent updates to different partitions", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())

		const numPartitions = 50
		const numUpdatesPerPartition = 100
		var wg sync.WaitGroup
		wg.Add(numPartitions * numUpdatesPerPartition)

		for p := int32(0); p < numPartitions; p++ {
			for i := 0; i < numUpdatesPerPartition; i++ {
				partition := p
				offset := int64(i)
				go func() {
					defer wg.Done()
					tracker.update(map[int32]int64{partition: offset})
				}()
			}
		}
		wg.Wait()

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		for p := int32(0); p < numPartitions; p++ {
			assert.Equal(t, int64(numUpdatesPerPartition-1), tracker.offsets[p], "partition %d", p)
		}
	})

	t.Run("concurrent updates with multi-partition maps", func(t *testing.T) {
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())

		const numGoroutines = 100
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			offset := int64(i)
			go func() {
				defer wg.Done()
				tracker.update(map[int32]int64{0: offset, 1: offset * 2})
			}()
		}
		wg.Wait()

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Equal(t, int64(numGoroutines-1), tracker.offsets[0])
		assert.Equal(t, int64((numGoroutines-1)*2), tracker.offsets[1])
	})
}

func TestBucketStore_FetchAndTrackSourceOffsets(t *testing.T) {
	blockID := ulid.MustNew(1, nil)

	uploadMetaJSON := func(t *testing.T, bkt objstore.Bucket, id ulid.ULID, offsets map[int32]int64) {
		t.Helper()
		meta := block.Meta{
			Thanos: block.ThanosMeta{
				Version:       block.ThanosVersion1,
				Source:        block.BlockBuilderSource,
				SourceOffsets: offsets,
			},
		}
		var buf bytes.Buffer
		require.NoError(t, meta.Write(&buf))
		require.NoError(t, bkt.Upload(context.Background(), id.String()+"/meta.json", &buf))
	}

	t.Run("reads source offsets from block meta.json", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		uploadMetaJSON(t, bkt, blockID, map[int32]int64{0: 100, 3: 500})

		reg := prometheus.NewPedanticRegistry()
		tracker := newSourceOffsetTracker(reg)
		store := &BucketStore{
			bkt:           objstore.WithNoopInstr(bkt),
			logger:        log.NewNopLogger(),
			sourceOffsets: tracker,
		}

		store.fetchAndTrackSourceOffsets(context.Background(), blockID)

		tracker.mu.Lock()
		assert.Equal(t, int64(100), tracker.offsets[0])
		assert.Equal(t, int64(500), tracker.offsets[3])
		tracker.mu.Unlock()

		expected := `
			# HELP cortex_bucket_store_highest_seen_source_offset The highest Kafka source offset seen across all blocks for each partition.
			# TYPE cortex_bucket_store_highest_seen_source_offset gauge
			cortex_bucket_store_highest_seen_source_offset{partition="0"} 100
			cortex_bucket_store_highest_seen_source_offset{partition="3"} 500
		`
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "cortex_bucket_store_highest_seen_source_offset"))
	})

	t.Run("no-op when tracker is nil", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		store := &BucketStore{
			bkt:    objstore.WithNoopInstr(bkt),
			logger: log.NewNopLogger(),
		}
		// Should not panic.
		store.fetchAndTrackSourceOffsets(context.Background(), blockID)
	})

	t.Run("handles missing meta.json gracefully", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		store := &BucketStore{
			bkt:           objstore.WithNoopInstr(bkt),
			logger:        log.NewNopLogger(),
			sourceOffsets: tracker,
		}
		// Should not panic; tracker should remain empty.
		store.fetchAndTrackSourceOffsets(context.Background(), blockID)

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Empty(t, tracker.offsets)
	})

	t.Run("handles meta.json without source offsets", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		uploadMetaJSON(t, bkt, blockID, nil)

		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		store := &BucketStore{
			bkt:           objstore.WithNoopInstr(bkt),
			logger:        log.NewNopLogger(),
			sourceOffsets: tracker,
		}

		store.fetchAndTrackSourceOffsets(context.Background(), blockID)

		tracker.mu.Lock()
		defer tracker.mu.Unlock()
		assert.Empty(t, tracker.offsets)
	})

	t.Run("high watermark across multiple blocks", func(t *testing.T) {
		bkt := objstore.NewInMemBucket()
		blockID1 := ulid.MustNew(1, nil)
		blockID2 := ulid.MustNew(2, nil)
		uploadMetaJSON(t, bkt, blockID1, map[int32]int64{0: 100, 1: 200})
		uploadMetaJSON(t, bkt, blockID2, map[int32]int64{0: 50, 1: 300})

		tracker := newSourceOffsetTracker(prometheus.NewPedanticRegistry())
		store := &BucketStore{
			bkt:           objstore.WithNoopInstr(bkt),
			logger:        log.NewNopLogger(),
			sourceOffsets: tracker,
		}

		store.fetchAndTrackSourceOffsets(context.Background(), blockID1)
		store.fetchAndTrackSourceOffsets(context.Background(), blockID2)

		tracker.mu.Lock()
		assert.Equal(t, int64(100), tracker.offsets[0])
		assert.Equal(t, int64(300), tracker.offsets[1])
		tracker.mu.Unlock()
	})
}
