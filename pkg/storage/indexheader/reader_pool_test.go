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
	"testing/synctest"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/gate"
	"github.com/oklog/ulid/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled                           bool
		lazyReaderIdleTimeout                       time.Duration
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
	}

	ctx := context.Background()
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {

			metrics := NewReaderPoolMetrics(nil)
			indexHeaderConfig := Config{
				LazyLoadingEnabled:     testData.lazyReaderEnabled,
				LazyLoadingIdleTimeout: testData.lazyReaderIdleTimeout,
			}
			pool := NewReaderPool(log.NewNopLogger(), indexHeaderConfig, gate.NewNoop(), metrics)

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, indexHeaderConfig)
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			require.Equal(t, float64(testData.expectedLoadCountMetricBeforeLabelNamesCall), promtestutil.ToFloat64(metrics.lazyReader.loadCount))

			// Ensure it can read data.
			labelNames, err := r.LabelNames(ctx)
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

	synctest.Test(t, func(t *testing.T) {
		// Note that we are creating a ReaderPool that doesn't run a background cleanup task for idle
		// Reader instances. We'll manually invoke the cleanup task when we need it as part of this test.
		pool := newReaderPool(log.NewNopLogger(), Config{
			LazyLoadingEnabled:     true,
			LazyLoadingIdleTimeout: idleTimeout,
		}, gate.NewNoop(), metrics)

		r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
		require.NoError(t, err)

		// Ensure it can read data.
		labelNames, err := r.LabelNames(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

		// Wait enough time before checking it.
		time.Sleep(idleTimeout * 2)
		require.NoError(t, pool.unloadIdleReaders(context.Background()), "closing idle readers shouldn't ever fail because it will abort periodically checking for idle readers")

		// We expect the reader has been closed, but not released from the pool.
		require.True(t, pool.isTracking(r.(*LazyBinaryReader)))
		require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
		require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

		// Ensure it can still read data (will be re-opened).
		labelNames, err = r.LabelNames(ctx)
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
	})
}

// TestReaderPool_OnDiskVersionMetric asserts that indexheader_on_disk reflects the on-disk index-header version for Reader implementations even before any read,
// and that closing the reader does not decrement it.
func TestReaderPool_OnDiskVersionMetric(t *testing.T) {
	ctx, tmpDir, bkt, blockID, metrics := prepareReaderPool(t)
	blockDir := filepath.Join(tmpDir, blockID.String())

	for _, lazyReaderEnabled := range []bool{false, true} {
		name := "non-lazy reader"
		if lazyReaderEnabled {
			name = "lazy reader"
		}
		t.Run(name, func(t *testing.T) {
			pool := newReaderPool(log.NewNopLogger(), Config{LazyLoadingEnabled: lazyReaderEnabled}, gate.NewNoop(), metrics)

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
			require.NoError(t, err)
			require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

			require.NoError(t, r.Close())
			require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")),
				"Closing the reader must not change the gauge if the file is still on disk")

			pool.RecordOnDiskIndexHeaderRemoval(blockDir)
			require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))
		})
	}
}

// TestReaderPool_OnDiskVersionMetricIdleUnload asserts that indexheader_on_disk reflects a
// block's index-header's on-disk state for the reader's whole tenure in the pool, regardless of
// if an index-header is unloaded due to the idle timeout, and that neither an idle unload/reload
// cycle nor a final Close() ever changes it - only RecordOnDiskIndexHeaderRemoval, called right before the block's
// directory is actually removed, does.
func TestReaderPool_OnDiskVersionMetricIdleUnload(t *testing.T) {
	const idleTimeout = time.Second
	ctx, tmpDir, bkt, blockID, metrics := prepareReaderPool(t)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()
	blockDir := filepath.Join(tmpDir, blockID.String())

	pool := newReaderPool(log.NewNopLogger(), Config{
		LazyLoadingEnabled:     true,
		LazyLoadingIdleTimeout: idleTimeout,
	}, gate.NewNoop(), metrics)

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
	require.NoError(t, err)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

	// Load index-header into memory
	labelNames, err := r.LabelNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

	// Set last used at time beyond the idle timeout window, then ensure no double-counting on reload
	r.(*LazyBinaryReader).usedAt.Store(time.Now().Add(-2 * idleTimeout).UnixNano())
	require.NoError(t, pool.unloadIdleReaders(context.Background()))
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

	labelNames, err = r.LabelNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

	// Closing the reader must not change the gauge - the file is still on disk.
	require.NoError(t, r.Close())
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))

	// Only accounting for the actual removal decrements it.
	pool.RecordOnDiskIndexHeaderRemoval(blockDir)
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.onDiskVersion.WithLabelValues("1")))
}

func TestReaderPool_LoadedBlocks(t *testing.T) {
	usedAt := time.Now()
	id, err := ulid.New(ulid.Now(), rand.Reader)
	require.NoError(t, err)

	lb := LazyBinaryReader{
		blockID: id,
		usedAt:  atomic.NewInt64(usedAt.UnixNano()),
	}
	rp := ReaderPool{
		lazyReaderEnabled: true,
		lazyReaders:       map[*LazyBinaryReader]struct{}{&lb: {}},
	}
	loadedBlocks := rp.LoadedBlocks()
	require.Equal(t, []ulid.ULID{id}, loadedBlocks)
}

func prepareReaderPool(t *testing.T) (context.Context, string, objstore.InstrumentedBucketReader, ulid.ULID, *ReaderPoolMetrics) {
	ctx := context.Background()

	tmpDir := t.TempDir()

	ubkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	bkt := objstore.WithNoopInstr(ubkt)

	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ubkt.Close())
		require.NoError(t, bkt.Close())
	})

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	_, err = block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil)
	require.NoError(t, err)

	metrics := NewReaderPoolMetrics(nil)
	return ctx, tmpDir, bkt, blockID, metrics
}
