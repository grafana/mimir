// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
)

func TestNewLazyBinaryReader_ShouldFailIfUnableToBuildIndexHeader(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	testLazyBinaryReader(t, bkt, tmpDir, ulid.ULID{}, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.Error(t, err)
	})
}

func TestNewLazyBinaryReader_ShouldBuildIndexHeaderFromBucket(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		// Should lazy load the index upon first usage.
		v, err := r.IndexVersion()
		require.NoError(t, err)
		require.Equal(t, 2, v)
		require.True(t, r.reader != nil)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		labelNames, err := r.LabelNames()
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))
	})
}

func TestNewLazyBinaryReader_ShouldRebuildCorruptedIndexHeader(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	// Write a corrupted index-header for the block.
	headerFilename := filepath.Join(tmpDir, blockID.String(), block.IndexHeaderFilename)
	require.NoError(t, os.WriteFile(headerFilename, []byte("xxx"), os.ModePerm))

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		// Ensure it can read data.
		labelNames, err := r.LabelNames()
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))
	})
}

func TestLazyBinaryReader_ShouldReopenOnUsageAfterClose(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)

		// Should lazy load the index upon first usage.
		labelNames, err := r.LabelNames()
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))

		// Close it.
		require.NoError(t, r.Close())
		require.True(t, r.reader == nil)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.unloadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadFailedCount))

		// Should lazy load again upon next usage.
		labelNames, err = r.LabelNames()
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(2), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))

		// Closing an already closed lazy reader should be a no-op.
		for i := 0; i < 2; i++ {
			require.NoError(t, r.Close())
			require.Equal(t, float64(2), promtestutil.ToFloat64(r.metrics.unloadCount))
			require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadFailedCount))
		}
	})
}

func TestLazyBinaryReader_unload_ShouldReturnErrorIfNotIdle(t *testing.T) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		// Should lazy load the index upon first usage.
		labelNames, err := r.LabelNames()
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadFailedCount))

		// Try to unload but not idle since enough time.
		require.Equal(t, errNotIdle, r.unloadIfIdleSince(time.Now().Add(-time.Minute).UnixNano()))
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadFailedCount))

		// Try to unload and idle since enough time.
		require.NoError(t, r.unloadIfIdleSince(time.Now().UnixNano()))
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.unloadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadFailedCount))
	})
}

func TestLazyBinaryReader_LoadUnloadRaceCondition(t *testing.T) {
	// Run the test for a fixed amount of time.
	const runDuration = 5 * time.Second

	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := testhelper.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"), 124, metadata.NoneFunc)
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), metadata.NoneFunc))

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		done := make(chan struct{})
		time.AfterFunc(runDuration, func() { close(done) })
		wg := sync.WaitGroup{}
		wg.Add(2)

		// Start a goroutine which continuously try to unload the reader.
		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					require.NoError(t, r.unloadIfIdleSince(0))
				}
			}
		}()

		// Try to read multiple times, while the other goroutine continuously try to unload it.
		go func() {
			defer wg.Done()

			for {
				select {
				case <-done:
					return
				default:
					_, err := r.PostingsOffset("a", "1")
					require.True(t, err == nil || errors.Is(err, errUnloadedWhileLoading))
				}
			}
		}()

		// Wait until both goroutines have done.
		wg.Wait()
	})
}

func testLazyBinaryReader(t *testing.T, bkt objstore.BucketReader, dir string, id ulid.ULID, test func(t *testing.T, r *LazyBinaryReader, err error)) {
	t.Run("BinaryReader", func(t *testing.T) {
		ctx := context.Background()
		logger := log.NewNopLogger()
		factory := func() (Reader, error) {
			return NewBinaryReader(ctx, logger, bkt, dir, id, 3, Config{})
		}

		reader, err := NewLazyBinaryReader(ctx, factory, logger, bkt, dir, id, NewLazyBinaryReaderMetrics(nil), nil)
		test(t, reader, err)
	})

	t.Run("StreamBinaryReader", func(t *testing.T) {
		ctx := context.Background()
		logger := log.NewNopLogger()
		factory := func() (Reader, error) {
			return NewStreamBinaryReader(ctx, logger, bkt, dir, id, 3, Config{})
		}

		reader, err := NewLazyBinaryReader(ctx, factory, logger, bkt, dir, id, NewLazyBinaryReaderMetrics(nil), nil)
		test(t, reader, err)
	})
}
