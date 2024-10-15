// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
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
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

func TestNewLazyBinaryReader_ShouldFailIfUnableToBuildIndexHeader(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-indexheader")
	bkt, err := filesystem.NewBucket(filepath.Join(tmpDir, "bkt"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })

	testLazyBinaryReader(t, bkt, tmpDir, ulid.ULID{}, func(t *testing.T, _ *LazyBinaryReader, err error) {
		require.Error(t, err)
	})
}

func TestNewLazyBinaryReader_ShouldBuildIndexHeaderFromBucket(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)

		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		// Should lazy load the index upon first usage.
		v, err := r.IndexVersion(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, v)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		labelNames, err := r.LabelNames(context.Background())
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

		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))

		// Ensure it can read data.
		labelNames, err := r.LabelNames(context.Background())
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, labelNames)
		require.Equal(t, float64(1), promtestutil.ToFloat64(r.metrics.loadCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.loadFailedCount))
		require.Equal(t, float64(0), promtestutil.ToFloat64(r.metrics.unloadCount))
	})
}

func TestLazyBinaryReader_unload_ShouldReturnErrorIfNotIdle(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)

		// Should lazy load the index upon first usage.
		labelNames, err := r.LabelNames(context.Background())
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
	t.Parallel()
	// Run the test for a fixed amount of time.
	const runDuration = 5 * time.Second

	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)

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
					_, err := r.PostingsOffset(context.Background(), "a", "1")
					require.True(t, err == nil || errors.Is(err, errUnloadedWhileLoading), "unexpected error: %s", err)
				}
			}
		}()

		// Wait until both goroutines have done.
		wg.Wait()
	})
}

func initBucketAndBlocksForTest(t testing.TB) (string, *filesystem.Bucket, ulid.ULID) {
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
	if err == nil {
		t.Cleanup(func() { require.NoError(t, reader.Close()) })
	}
	test(t, reader, err)
}

// TestLazyBinaryReader_ShouldBlockMaxConcurrency tests if LazyBinaryReader blocks
// concurrent loads such that it doesn't pass the configured maximum.
func TestLazyBinaryReader_ShouldBlockMaxConcurrency(t *testing.T) {
	t.Parallel()
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

		time.Sleep(time.Second)

		inflight.Dec()

		return nil, errOhNo
	}

	var lazyReaders [numLazyReader]*LazyBinaryReader
	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))

	for i := 0; i < numLazyReader; i++ {
		var err error
		lazyReaders[i], err = NewLazyBinaryReader(context.Background(), factory, logger, bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
		require.NoError(t, err)
		readerToClose := lazyReaders[i]
		t.Cleanup(func() { require.NoError(t, readerToClose.Close()) })
	}

	var wg sync.WaitGroup
	wg.Add(numLazyReader)

	// Attempt to concurrently load 20 index-headers.
	for i := 0; i < numLazyReader; i++ {
		index := i
		go func() {
			_, err := lazyReaders[index].IndexVersion(context.Background())
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
	t.Cleanup(func() { require.NoError(t, lazyReader.Close()) })

	var clientWG sync.WaitGroup
	clientWG.Add(numClients)

	start := make(chan struct{})

	// Start many clients for the same lazyReader
	for i := 0; i < numClients; i++ {
		go func() {
			<-start
			_, _ = lazyReader.IndexVersion(context.Background())
			clientWG.Done()
		}()
	}

	// Give goroutines chance to start and wait for start channel.
	time.Sleep(1 * time.Second)
	close(start)

	assert.NoError(t, wgWaitTimeout(&clientWG, 10*time.Second))
}

func wgWaitTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout waiting for WaitGroup")
	}
}

type mockReader struct {
	IndexVersionFunc func(ctx context.Context) (int, error)
}

func (m mockReader) Close() error {
	return nil
}

func (m mockReader) IndexVersion(ctx context.Context) (int, error) {
	return m.IndexVersionFunc(ctx)
}

func (m mockReader) PostingsOffset(context.Context, string, string) (index.Range, error) {
	panic("not implemented")
}

func (m mockReader) LookupSymbol(context.Context, uint32) (string, error) {
	panic("not implemented")
}

func (m mockReader) SymbolsReader(context.Context) (streamindex.SymbolsReader, error) {
	panic("not implemented")
}

func (m mockReader) LabelValuesOffsets(context.Context, string, string, func(string) bool) ([]streamindex.PostingListOffset, error) {
	panic("not implemented")
}

func (m mockReader) LabelNames(context.Context) ([]string, error) {
	panic("not implemented")
}

func TestLazyBinaryReader_CancellingContextReturnsCallButDoesntStopLazyLoading(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	const (
		maxLazyLoadConcurrency = 1
		numClients             = 25
		mockIndexVersion       = -42
	)

	waitLoad := make(chan struct{})
	loadStarted := make(chan struct{})

	factory := func() (Reader, error) {
		close(loadStarted) // will panic if closed twice; no panic means that the factory was invoked only once
		<-waitLoad
		reader := mockReader{
			IndexVersionFunc: func(context.Context) (int, error) { return mockIndexVersion, nil },
		}
		return reader, nil
	}

	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))
	lazyReader, err := NewLazyBinaryReader(context.Background(), factory, log.NewNopLogger(), bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, lazyReader.Close()) })

	var clientWG sync.WaitGroup
	clientWG.Add(numClients)

	ctx, cancel := context.WithCancel(context.Background())

	// Start many clients for the same lazyReader
	for i := 0; i < numClients; i++ {
		go func() {
			_, _ = lazyReader.IndexVersion(ctx)
			clientWG.Done()
		}()
	}
	<-loadStarted                                            // wait until the first load is started
	cancel()                                                 // abort waiting for lazy load
	assert.NoError(t, wgWaitTimeout(&clientWG, time.Second)) // all clients should return

	close(waitLoad) // unblock the lazy load

	version, err := lazyReader.IndexVersion(context.Background()) // try to use the reader implementation now that it has loaded
	assert.NoError(t, err)
	assert.Equal(t, mockIndexVersion, version)
}

func TestLazyBinaryReader_CancellingContextReturnsCallButDoesntStopLazyLoading_LoadingReturnsAnError(t *testing.T) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	const (
		maxLazyLoadConcurrency = 1
		numClients             = 25
	)

	waitLoad := make(chan struct{})
	loadStarted := make(chan struct{})

	reader, loadErr := Reader(nil), assert.AnError

	factory := func() (Reader, error) {
		close(loadStarted)
		<-waitLoad
		return reader, loadErr
	}

	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))
	lazyReader, err := NewLazyBinaryReader(context.Background(), factory, log.NewNopLogger(), bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, lazyReader.Close()) })

	var clientWG sync.WaitGroup
	clientWG.Add(numClients)

	ctx, cancel := context.WithCancel(context.Background())

	// Start many clients for the same lazyReader and cancel them before lazy loading completes.
	for i := 0; i < numClients; i++ {
		go func() {
			_, _ = lazyReader.IndexVersion(ctx)
			clientWG.Done()
		}()
	}
	<-loadStarted                                            // wait until the first load is started
	cancel()                                                 // abort waiting for lazy load
	assert.NoError(t, wgWaitTimeout(&clientWG, time.Second)) // all clients should return

	close(waitLoad) // unblock the lazy load

	// Start another client to make sure the factory is invoked again if the first invocation returned an error.
	loadStarted = make(chan struct{})
	_, err = lazyReader.IndexVersion(context.Background()) // try to use the reader implementation now that it has loaded
	assert.ErrorIs(t, err, assert.AnError)

	// Since we got an error the previous time we try to load the reader again.
	loadErr = fmt.Errorf("a different error")
	loadStarted = make(chan struct{})
	_, err = lazyReader.IndexVersion(context.Background()) // try to use the reader implementation now that it has loaded
	assert.ErrorIs(t, err, loadErr)
}

func TestLazyBinaryReader_CancellingContextReturnsCallButDoesntStopLazyLoading_NoZombieReaders(t *testing.T) {
	// This test makes sure that if we requested a reader, but then gave up, then the reader is properly closed and
	// isn't open forever.
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	const (
		maxLazyLoadConcurrency = 1
		numClients             = 25
		testRuns               = 100
	)

	factory := func() (Reader, error) {
		return mockReader{
			IndexVersionFunc: func(context.Context) (int, error) { return 0, nil },
		}, nil
	}

	lazyLoadingGate := gate.NewInstrumented(prometheus.NewRegistry(), maxLazyLoadConcurrency, gate.NewBlocking(maxLazyLoadConcurrency))
	lazyReader, err := NewLazyBinaryReader(context.Background(), factory, log.NewNopLogger(), bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, lazyLoadingGate)
	t.Cleanup(func() { require.NoError(t, lazyReader.Close()) })

	require.NoError(t, err)

	for i := 0; i < testRuns; i++ {
		var clientWG sync.WaitGroup
		clientWG.Add(numClients)
		ctx, cancel := context.WithCancel(context.Background())

		// Start many clients for the same lazyReader and cancel them before lazy loading completes.
		for i := 0; i < numClients; i++ {
			go func() {
				_, _ = lazyReader.IndexVersion(ctx)
				clientWG.Done()
			}()
		}
		cancel()                                                 // abort waiting for lazy load
		assert.NoError(t, wgWaitTimeout(&clientWG, time.Second)) // all clients should return
		assert.NoError(t, lazyReader.unloadIfIdleSince(0))       // unload the index header
	}
}

func TestLazyBinaryReader_SymbolReaderAndUnload(t *testing.T) {
	t.Parallel()
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(t)

	testLazyBinaryReader(t, bkt, tmpDir, blockID, func(t *testing.T, r *LazyBinaryReader, err error) {
		require.NoError(t, err)

		closed := atomic.NewBool(false)

		sr, err := r.SymbolsReader(context.Background())
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

func BenchmarkNewLazyBinaryReader(b *testing.B) {
	tmpDir, bkt, blockID := initBucketAndBlocksForTest(b)

	factory := func() (Reader, error) {
		reader := mockReader{
			IndexVersionFunc: func(context.Context) (int, error) { return 1, nil },
		}
		return reader, nil
	}

	lazyReader, err := NewLazyBinaryReader(context.Background(), factory, log.NewNopLogger(), bkt, tmpDir, blockID, NewLazyBinaryReaderMetrics(nil), nil, gate.NewNoop())
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.Cleanup(func() { require.NoError(b, lazyReader.Close()) })

	wg := &sync.WaitGroup{}

	for _, readConcurrency := range []int{1, 2, 10, 20, 50, 100} {
		b.Run(fmt.Sprintf("concurrency=%d", readConcurrency), func(b *testing.B) {
			wg.Add(readConcurrency)
			for readerIdx := 0; readerIdx < readConcurrency; readerIdx++ {
				go func() {
					defer wg.Done()
					for i := 0; i < b.N; i++ {
						_, _ = lazyReader.IndexVersion(ctx) // ignore the mocked values above
					}
				}()
			}
			wg.Wait()
		})
	}
}
