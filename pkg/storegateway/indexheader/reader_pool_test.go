// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled             bool
		lazyReaderIdleTimeout         time.Duration
		eagerLoadReaderEnabled        bool
		persistLazyLoadedHeaderFn     func(blockId ulid.ULID) lazyLoadedHeadersSnapshot
		expectedLoadCountMetricBefore int
		expectedLoadCountMetricAfter  int
	}{
		"lazy reader is disabled": {
			lazyReaderEnabled:            false,
			expectedLoadCountMetricAfter: 0, // no lazy loading
		},
		"lazy reader is enabled but close on idle timeout is disabled": {
			lazyReaderEnabled:            true,
			lazyReaderIdleTimeout:        0,
			expectedLoadCountMetricAfter: 1,
		},
		"lazy reader and close on idle timeout are both enabled": {
			lazyReaderEnabled:            true,
			lazyReaderIdleTimeout:        time.Minute,
			expectedLoadCountMetricAfter: 1,
		},
		"lazy reader preShutdownLoadedBlocks is present": {
			lazyReaderEnabled:             true,
			lazyReaderIdleTimeout:         time.Minute,
			eagerLoadReaderEnabled:        true,
			expectedLoadCountMetricBefore: 1, // the index header will be eagerly loaded before the operation
			expectedLoadCountMetricAfter:  1,
			persistLazyLoadedHeaderFn: func(blockId ulid.ULID) lazyLoadedHeadersSnapshot {
				return lazyLoadedHeadersSnapshot{
					IndexHeaderLastUsedTime: map[ulid.ULID]int64{blockId: time.Now().UnixMilli()},
					UserID:                  "anonymous",
				}
			},
		},
		"no valid preShutdownLoadedBlocks is present": {
			lazyReaderEnabled:             true,
			lazyReaderIdleTimeout:         time.Minute,
			eagerLoadReaderEnabled:        true,
			expectedLoadCountMetricBefore: 0, // although eager loading is enabled, this test will not do eager loading because the block ID is not in the lazy loaded file.
			expectedLoadCountMetricAfter:  1,
			persistLazyLoadedHeaderFn: func(_ ulid.ULID) lazyLoadedHeadersSnapshot {
				// let's create a random blockID to be stored in lazy loaded headers file
				invalidBlockID, _ := ulid.New(ulid.Now(), rand.Reader)
				// this snapshot will refer to invalid block, hence eager load wouldn't be executed

				return lazyLoadedHeadersSnapshot{
					IndexHeaderLastUsedTime: map[ulid.ULID]int64{invalidBlockID: time.Now().UnixMilli()},
					UserID:                  "anonymous",
				}
			},
		},
	}

	ctx := context.Background()
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			snapshotConfig := LazyLoadedHeadersSnapshotConfig{
				Path:                tmpDir,
				UserID:              "anonymous",
				EagerLoadingEnabled: testData.eagerLoadReaderEnabled,
			}
			if testData.persistLazyLoadedHeaderFn != nil {
				lazyLoadedSnapshot := testData.persistLazyLoadedHeaderFn(blockID)
				err := lazyLoadedSnapshot.persist(snapshotConfig.Path)
				require.NoError(t, err)
			}

			metrics := NewReaderPoolMetrics(nil)
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout, metrics, snapshotConfig)
			defer pool.Close()

			binaryReaderForInitialSync := testData.eagerLoadReaderEnabled
			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{IndexHeaderEagerLoadingStartupEnabled: testData.eagerLoadReaderEnabled}, binaryReaderForInitialSync)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			require.Equal(t, float64(testData.expectedLoadCountMetricBefore), promtestutil.ToFloat64(metrics.lazyReader.loadCount))

			// Ensure it can read data.
			labelNames, err := r.LabelNames()
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, labelNames)

			require.Equal(t, float64(testData.expectedLoadCountMetricAfter), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
		})
	}
}

func TestReaderPool_ShouldCloseIdleLazyReaders(t *testing.T) {
	const idleTimeout = time.Second
	ctx, tmpDir, bkt, blockID, metrics := prepareReaderPool(t)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()
	defer func() { require.NoError(t, bkt.Close()) }()

	// Note that we are creating a ReaderPool that doesn't run a background cleanup task for idle
	// Reader instances. We'll manually invoke the cleanup task when we need it as part of this test.
	pool := newReaderPool(log.NewNopLogger(), true, idleTimeout, false, metrics, nil)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{}, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// Wait enough time before checking it.
	time.Sleep(idleTimeout * 2)
	pool.closeIdleReaders()

	// We expect the reader has been closed, but not released from the pool.
	require.True(t, pool.isTracking(r.(*LazyBinaryReader)))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// Ensure it can still read data (will be re-opened).
	labelNames, err = r.LabelNames()
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.True(t, pool.isTracking(r.(*LazyBinaryReader)))
	require.Equal(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// We expect an explicit call to Close() to close the reader and release it from the pool too.
	require.NoError(t, r.Close())
	require.True(t, !pool.isTracking(r.(*LazyBinaryReader)))
	require.Equal(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(2), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))
}

func TestReaderPool_LoadedBlocks(t *testing.T) {
	usedAt := time.Now()
	id, err := ulid.New(ulid.Now(), rand.Reader)
	require.NoError(t, err)

	lb := LazyBinaryReader{
		blockID: id,
		usedAt:  atomic.NewInt64(usedAt.UnixNano()),
		// we just set to make reader != nil
		reader: &StreamBinaryReader{},
	}
	rp := ReaderPool{
		lazyReaderEnabled: true,
		lazyReaders:       map[*LazyBinaryReader]struct{}{&lb: {}},
	}
	require.Equal(t, map[ulid.ULID]int64{id: usedAt.UnixMilli()}, rp.LoadedBlocks())
}

func TestReaderPool_PersistLazyLoadedBlock(t *testing.T) {
	const idleTimeout = time.Second
	ctx, tmpDir, bkt, blockID, metrics := prepareReaderPool(t)

	// Note that we are creating a ReaderPool that doesn't run a background cleanup task for idle
	// Reader instances. We'll manually invoke the cleanup task when we need it as part of this test.
	pool := newReaderPool(log.NewNopLogger(), true, idleTimeout, true, metrics, nil)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{}, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	snapshot := lazyLoadedHeadersSnapshot{
		IndexHeaderLastUsedTime: pool.LoadedBlocks(),
		UserID:                  "anonymous",
	}

	err = snapshot.persist(tmpDir)
	require.NoError(t, err)

	persistedFile := filepath.Join(tmpDir, lazyLoadedHeadersListFile)
	persistedData, err := os.ReadFile(persistedFile)
	require.NoError(t, err)

	var expected string
	// we know that there is only one lazyReader, hence just use formatter to set the ULID and timestamp.
	require.Equal(t, 1, len(pool.lazyReaders), "expecting only one lazyReaders")
	for r := range pool.lazyReaders {
		expected = fmt.Sprintf(`{"index_header_last_used_time":{"%s":%d},"user_id":"anonymous"}`, r.blockID, r.usedAt.Load()/int64(time.Millisecond))
	}
	require.JSONEq(t, expected, string(persistedData))

	// Wait enough time before checking it.
	time.Sleep(idleTimeout * 2)
	pool.closeIdleReaders()

	// LoadedBlocks will update the IndexHeaderLastUsedTime map with the removal of
	// idle blocks.
	snapshot.IndexHeaderLastUsedTime = pool.LoadedBlocks()
	err = snapshot.persist(tmpDir)
	require.NoError(t, err)

	persistedData, err = os.ReadFile(persistedFile)
	require.NoError(t, err)

	require.JSONEq(t, `{"index_header_last_used_time":{},"user_id":"anonymous"}`, string(persistedData), "index_header_last_used_time should be cleared")
}

func prepareReaderPool(t *testing.T) (context.Context, string, *filesystem.Bucket, ulid.ULID, *ReaderPoolMetrics) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bkt.Close())
	})

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	metrics := NewReaderPoolMetrics(nil)
	return ctx, tmpDir, bkt, blockID, metrics
}
