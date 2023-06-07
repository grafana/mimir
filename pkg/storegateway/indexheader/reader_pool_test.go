// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestReaderPool_NewBinaryReader(t *testing.T) {
	tests := map[string]struct {
		lazyReaderEnabled     bool
		lazyReaderIdleTimeout time.Duration
	}{
		"lazy reader is disabled": {
			lazyReaderEnabled: false,
		},
		"lazy reader is enabled but close on idle timeout is disabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: 0,
		},
		"lazy reader and close on idle timeout are both enabled": {
			lazyReaderEnabled:     true,
			lazyReaderIdleTimeout: 2 * time.Minute,
		},
	}

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "test-indexheader")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	defer func() { require.NoError(t, bkt.Close()) }()

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout, NewReaderPoolMetrics(nil), HeadersLazyLoadedTracker{})
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
			require.NoError(t, err)
			defer func() { require.NoError(t, r.Close()) }()

			// Ensure it can read data.
			labelNames, err := r.LabelNames()
			require.NoError(t, err)
			require.Equal(t, []string{"a"}, labelNames)
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
	pool := newReaderPool(log.NewNopLogger(), true, idleTimeout, metrics, HeadersLazyLoadedTracker{})
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
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

func TestReaderPool_PersistLazyLoadedBlock(t *testing.T) {
	const idleTimeout = time.Second
	ctx, tmpDir, bkt, blockID, metrics := prepareReaderPool(t)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()
	defer func() { require.NoError(t, bkt.Close()) }()

	lazyLoadedTracker := HeadersLazyLoadedTracker{
		// Path stores where lazy loaded blocks will be tracked in per-tenant-files
		Path: filepath.Join(tmpDir, "lazy-loaded.proto"),
		State: storepb.HeadersLazyLoadedTrackerState{
			UserId: "anonymous",
		},
	}
	// Note that we are creating a ReaderPool that doesn't run a background cleanup task for idle
	// Reader instances. We'll manually invoke the cleanup task when we need it as part of this test.
	pool := newReaderPool(log.NewNopLogger(), true, idleTimeout, metrics, lazyLoadedTracker)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, Config{})
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()

	// Ensure it can read data.
	labelNames, err := r.LabelNames()
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, labelNames)
	require.Equal(t, float64(1), promtestutil.ToFloat64(metrics.lazyReader.loadCount))
	require.Equal(t, float64(0), promtestutil.ToFloat64(metrics.lazyReader.unloadCount))

	// lazyLoaded tracker will track the lazyLoadedBlocks into persistent file
	pool.lazyLoadedTracker.copyLazyLoadedState(pool.lazyReaders)
	err = pool.lazyLoadedTracker.persist()
	require.NoError(t, err)
	require.Greater(t, pool.lazyLoadedTracker.State.LazyLoadedBlocks[blockID.String()], 0, "lazyLoadedBlocks state must be set")

	// Wait enough time before checking it.
	time.Sleep(idleTimeout * 2)
	pool.closeIdleReaders()

	// lazyLoaded tracker will remove the track from persistent file
	pool.lazyLoadedTracker.copyLazyLoadedState(pool.lazyReaders)
	err = pool.lazyLoadedTracker.persist()
	require.NoError(t, err)
	require.NotContains(t, pool.lazyLoadedTracker.State.LazyLoadedBlocks, blockID.String(), "lazyLoadedBlocks state must be unset")
}

func prepareReaderPool(t *testing.T) (context.Context, string, *filesystem.Bucket, ulid.ULID, *ReaderPoolMetrics) {
	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "test-indexheader")
	require.NoError(t, err)

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)

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
