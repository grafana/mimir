// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/gate"
	"github.com/oklog/ulid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled                           bool
		lazyReaderIdleTimeout                       time.Duration
		eagerLoadReaderEnabled                      bool
		initialSync                                 bool
		createLoadedBlocksSnapshotFn                func(blockId ulid.ULID) map[ulid.ULID]int64
		expectedLoadCountMetricBeforeLabelNamesCall int
		expectedLoadCountMetricAfterLabelNamesCall  int
	}{
		"lazy reader is disabled": {
			lazyReaderEnabled:                          false,
			expectedLoadCountMetricAfterLabelNamesCall: 0, // no lazy loading
		},
		"lazy reader is enabled but close on idle timeout is disabled": {
			lazyReaderEnabled:                          true,
			lazyReaderIdleTimeout:                      0,
			expectedLoadCountMetricAfterLabelNamesCall: 1,
		},
		"lazy reader and close on idle timeout are both enabled": {
			lazyReaderEnabled:                          true,
			lazyReaderIdleTimeout:                      time.Minute,
			expectedLoadCountMetricAfterLabelNamesCall: 1,
		},
		"block is present in pre-shutdown loaded blocks and eager-loading is enabled, loading index header during initial sync": {
			lazyReaderEnabled:      true,
			lazyReaderIdleTimeout:  time.Minute,
			eagerLoadReaderEnabled: true,
			initialSync:            true,
			expectedLoadCountMetricBeforeLabelNamesCall: 1, // the index header will be eagerly loaded before the operation
			expectedLoadCountMetricAfterLabelNamesCall:  1,
			createLoadedBlocksSnapshotFn: func(blockId ulid.ULID) map[ulid.ULID]int64 {
				return map[ulid.ULID]int64{blockId: time.Now().UnixMilli()}
			},
		},
		"block is present in pre-shutdown loaded blocks and eager-loading is enabled, loading index header after initial sync": {
			lazyReaderEnabled:      true,
			lazyReaderIdleTimeout:  time.Minute,
			eagerLoadReaderEnabled: true,
			initialSync:            false,
			expectedLoadCountMetricBeforeLabelNamesCall: 0, // the index header is not eager loaded if not during initial-sync
			expectedLoadCountMetricAfterLabelNamesCall:  1,
			createLoadedBlocksSnapshotFn: func(blockId ulid.ULID) map[ulid.ULID]int64 {
				return map[ulid.ULID]int64{blockId: time.Now().UnixMilli()}
			},
		},
		"block is not present in pre-shutdown loaded blocks snapshot and eager-loading is enabled": {
			lazyReaderEnabled:      true,
			lazyReaderIdleTimeout:  time.Minute,
			eagerLoadReaderEnabled: true,
			initialSync:            true,
			expectedLoadCountMetricBeforeLabelNamesCall: 0, // although eager loading is enabled, this test will not do eager loading because the block ID is not in the lazy loaded file.
			expectedLoadCountMetricAfterLabelNamesCall:  1,
			createLoadedBlocksSnapshotFn: func(_ ulid.ULID) map[ulid.ULID]int64 {
				// let's create a random fake blockID to be stored in lazy loaded headers file
				fakeBlockID := ulid.MustNew(ulid.Now(), rand.Reader)
				// this snapshot will refer to fake block, hence eager load wouldn't be executed for the real block that we test
				return map[ulid.ULID]int64{fakeBlockID: time.Now().UnixMilli()}
			},
		},
		"pre-shutdown loaded blocks snapshot doesn't exist and eager-loading is enabled": {
			lazyReaderEnabled:      true,
			lazyReaderIdleTimeout:  time.Minute,
			eagerLoadReaderEnabled: true,
			initialSync:            true,
			expectedLoadCountMetricBeforeLabelNamesCall: 0, // although eager loading is enabled, this test will not do eager loading because there is no snapshot file
			expectedLoadCountMetricAfterLabelNamesCall:  1,
		},
	}

	ctx := context.Background()
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			var lazyLoadedBlocks map[ulid.ULID]int64
			if testData.createLoadedBlocksSnapshotFn != nil {
				lazyLoadedBlocks = testData.createLoadedBlocksSnapshotFn(blockID)
				require.NotNil(t, lazyLoadedBlocks)
			}

			metrics := NewReaderPoolMetrics(nil)
			indexHeaderConfig := Config{
				LazyLoadingEnabled:         testData.lazyReaderEnabled,
				LazyLoadingIdleTimeout:     testData.lazyReaderIdleTimeout,
				EagerLoadingStartupEnabled: testData.eagerLoadReaderEnabled,
			}
			pool := NewReaderPool(log.NewNopLogger(), indexHeaderConfig, gate.NewNoop(), metrics, lazyLoadedBlocks)
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, indexHeaderConfig, testData.initialSync)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			require.Equal(t, float64(testData.expectedLoadCountMetricBeforeLabelNamesCall), promtestutil.ToFloat64(metrics.lazyReader.loadCount))

			// Ensure it can read data.
			labelNames, err := r.LabelNames()
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, labelNames)

			require.Equal(t, float64(testData.expectedLoadCountMetricAfterLabelNamesCall), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
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
	pool := newReaderPool(log.NewNopLogger(), Config{
		LazyLoadingEnabled:         true,
		LazyLoadingIdleTimeout:     idleTimeout,
		EagerLoadingStartupEnabled: false,
	}, gate.NewNoop(), metrics, nil)
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
