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
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storegateway/testhelper"
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
			lazyReaderIdleTimeout: time.Minute,
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
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			pool := NewReaderPool(log.NewNopLogger(), testData.lazyReaderEnabled, testData.lazyReaderIdleTimeout, NewReaderPoolMetrics(nil))
			defer pool.Close()

			r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, BinaryReaderConfig{})
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

	ctx := context.Background()

	tmpDir, err := os.MkdirTemp("", "test-indexheader")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmpDir)) }()

	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	defer func() { require.NoError(t, bkt.Close()) }()

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		{{Name: "a", Value: "1"}},
		{{Name: "a", Value: "2"}},
	}, 100, 0, 1000, labels.Labels{{Name: "ext1", Value: "1"}}, 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	metrics := NewReaderPoolMetrics(nil)
	pool := NewReaderPool(log.NewNopLogger(), true, idleTimeout, metrics)
	defer pool.Close()

	r, err := pool.NewBinaryReader(ctx, log.NewNopLogger(), bkt, tmpDir, blockID, 3, BinaryReaderConfig{})
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
