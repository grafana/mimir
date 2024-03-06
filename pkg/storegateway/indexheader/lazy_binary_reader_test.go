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
	"github.com/grafana/dskit/gate"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
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
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

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
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

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
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

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
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

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

	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

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
					require.True(t, err == nil || errors.Is(err, errUnloadedWhileLoading), "unexpected error: %s", err)
				}
			}
		}()

		// Wait until both goroutines have done.
		wg.Wait()
	})
}

func TestNewLazyBinaryReader_EagerLoadLazyLoadedIndexHeaders(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		r.EagerLoad()

		require.NoError(t, err)
		require.NotNil(t, r.reader, "t.reader must already eagerly loaded")
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		// The index should already be loaded, the following call will return reader already loaded above
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

func initBucketAndBlocksForTest(t *testing.T) (string, *filesystem.Bucket, ulid.ULID) {
	ctx := context.Background()

	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	// Create block.
	blockID, err := block.CreateBlock(ctx, tmpDir, []labels.Labels{
		labels.FromStrings("a", "1"),
		labels.FromStrings("a", "2"),
		labels.FromStrings("a", "3"),
	}, 100, 0, 1000, labels.FromStrings("ext1", "1"))
	require.NoError(t, err)
	require.NoError(t, block.Upload(ctx, log.NewNopLogger(), bkt, filepath.Join(tmpDir, blockID.String()), nil))
	return tmpDir, bkt, blockID
}

func testLazyBinaryReader(t *testing.T, bkt objstore.BucketReader, dir string, id ulid.ULID, test func(t *testing.T, r *LazyBinaryReader, err error)) {
	ctx := context.Background()
	logger := log.NewNopLogger()
	factory := func() (Reader, error) {
		return NewStreamBinaryReader(ctx, logger, bkt, dir, id, 3, NewStreamBinaryReaderMetrics(nil), Config{})
	}

	reader, err := NewLazyBinaryReader(ctx, factory, logger, bkt, dir, id, NewLazyBinaryReaderMetrics(nil), nil, gate.NewNoop())
	test(t, reader, err)
}

// TestLazyBinaryReader_ShouldBlockMaxConcurrency tests if LazyBinaryReader blocks
// concurrent loads such that it doesn't pass the configured maximum.
func TestLazyBinaryReader_ShouldBlockMaxConcurrency(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	logger := log.NewNopLogger()

	const (
		numLazyReader          = 20
		maxLazyLoadConcurrency = 3
	)

	var (
		totalLoaded = atomic.NewUint32(0)
		inflight    = atomic.NewUint32(0)
	)

	errOhNo := errors.New("oh no")

	factory := func() (Reader, error) {
		testInflight := inflight.Inc()
		require.LessOrEqual(t, testInflight, uint32(maxLazyLoadConcurrency))
		totalLoaded.Inc()

		time.Sleep(3 * time.Second)

		inflight.Dec()

		return nil, errOhNo
	}

	var lazyReaders [numLazyReader]*LazyBinaryReader
	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))

	for i := 0; i < numLazyReader; i++ {
		var err error
		lazyReaders[i], err = NewLazyBinaryReader(context.Background(), factory, logger, bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(numLazyReader)

	// Attempt to concurrently load 20 index-headers.
	for i := 0; i < numLazyReader; i++ {
		index := i
		go func() {
			_, err := lazyReaders[index].IndexVersion()
			require.ErrorIs(t, err, errOhNo)
			wg.Done()
		}()
	}

	wg.Wait()
	require.Equal(t, totalLoaded.Load(), uint32(numLazyReader))
}

func TestLazyBinaryReader_ConcurrentLoadingOfSameIndexReader(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	const (
		maxLazyLoadConcurrency = 1
		numClients             = 25
	)

	factory := func() (Reader, error) { return nil, errors.New("error") }

	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))
	lazyReader, err := NewLazyBinaryReader(context.Background(), factory, log.NewNopLogger(), bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
	require.NoError(t, err)

	var clientWG sync.WaitGroup
	clientWG.Add(numClients)

	start := make(chan struct{})

	// Start many clients for the same lazyReader
	for i := 0; i < numClients; i++ {
		go func() {
			<-start
			_, _ = lazyReader.IndexVersion()
			clientWG.Done()
		}()
	}

	// Give goroutines chance to start and wait for start channel.
	time.Sleep(1 * time.Second)
	close(start)

	done := make(chan struct{})
	go func() {
		// Wait until all of them finish.
		clientWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(10 * time.Second):
		require.Fail(t, "goroutines did not finish in time")
	}
}

func TestLazyBinaryReader_SymbolReaderAndUnload(t *testing.T) {
	t.Parallel()
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)
		require.Nil(t, r.reader)
		t.Cleanup(func() {
			require.NoError(t, r.Close())
		})

		closed := atomic.NewBool(false)

		sr, err := r.SymbolsReader()
		require.NoError(t, err)

		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-time.After(1 * time.Second)

			closed.Store(true)
			require.NoError(t, sr.Close()) // sr.Close() unblocks unloadIfIdleSince

			// Multiple close calls should not panic (wg.Done could panic if called too many times).
			// (It can return error, or not. We don't care).
			_ = sr.Close()
			_ = sr.Close()
		}()

		require.NoError(t, r.unloadIfIdleSince(0))

		// We should only get here if symbols reader was already closed. If it wasn't, unload unloaded unclosed reader :(
		require.True(t, closed.Load(), "symbols reader is not closed yet")

		wg.Wait()
	})
}
