// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_e2e_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcutil"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/indexheader"
	"github.com/grafana/mimir/pkg/util/test"
)

var (
	minTime    = time.Unix(0, 0)
	maxTime, _ = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")
)

type swappableCache struct {
	indexcache.IndexCache
}

func (c *swappableCache) SwapIndexCacheWith(cache indexcache.IndexCache) {
	c.IndexCache = cache
}

type storeSuite struct {
	store            *BucketStore
	minTime, maxTime int64
	cache            *swappableCache
	metricsRegistry  *prometheus.Registry

	logger log.Logger
}

// When nonOverlappingBlocks is false, prepareTestBlocks creates 2 blocks per block range.
// When nonOverlappingBlocks is true, it shifts the 2nd block ahead by 2hrs for every block range.
// This way the first and the last blocks created have no overlapping blocks.
func prepareTestBlocks(t testing.TB, now time.Time, count int, dir string, bkt objstore.Bucket,
	series []labels.Labels, extLset labels.Labels, nonOverlappingBlocks bool) (minTime, maxTime int64) {
	ctx := context.Background()
	logger := log.NewNopLogger()

	for i := 0; i < count; i++ {
		mint := timestamp.FromTime(now)
		now = now.Add(2 * time.Hour)
		maxt := timestamp.FromTime(now)

		if minTime == 0 {
			minTime = mint
		}
		maxTime = maxt

		// Create two blocks per time slot. Only add 10 samples each so only one chunk
		// gets created each. This way we can easily verify we got 10 chunks per series below.
		id1, err := block.CreateBlock(ctx, dir, series[:4], 10, mint, maxt, extLset)
		assert.NoError(t, err)
		if nonOverlappingBlocks {
			mint = maxt
			maxt = timestamp.FromTime(now.Add(2 * time.Hour))
			maxTime = maxt
		}
		id2, err := block.CreateBlock(ctx, dir, series[4:], 10, mint, maxt, extLset)
		assert.NoError(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Replace labels to the meta of the second block.
		meta, err := block.ReadMetaFromDir(dir2)
		require.NoError(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		assert.NoError(t, meta.WriteToDir(logger, dir2))

		assert.NoError(t, block.Upload(ctx, logger, bkt, dir1, nil))
		assert.NoError(t, block.Upload(ctx, logger, bkt, dir2, nil))

		assert.NoError(t, os.RemoveAll(dir1))
		assert.NoError(t, os.RemoveAll(dir2))
	}

	return
}

type prepareStoreConfig struct {
	tempDir              string
	manyParts            bool
	chunksLimiterFactory ChunksLimiterFactory
	seriesLimiterFactory SeriesLimiterFactory
	series               []labels.Labels
	indexCache           indexcache.IndexCache
	metricsRegistry      *prometheus.Registry
	logger               log.Logger
	postingsStrategy     postingsSelectionStrategy
	// When nonOverlappingBlocks is false, prepare store creates 2 blocks per block range.
	// When nonOverlappingBlocks is true, it shifts the 2nd block ahead by 2hrs for every block range.
	// This way the first and the last blocks created have no overlapping blocks.
	nonOverlappingBlocks bool
	numBlocks            int
	bucketStoreConfig    mimir_tsdb.BucketStoreConfig
}

func (c *prepareStoreConfig) apply(opts ...prepareStoreConfigOption) *prepareStoreConfig {
	for _, o := range opts {
		o(c)
	}
	return c
}

func defaultPrepareStoreConfig(t testing.TB) *prepareStoreConfig {
	return &prepareStoreConfig{
		metricsRegistry: prometheus.NewRegistry(),
		numBlocks:       6,
		logger:          log.NewNopLogger(),
		tempDir:         t.TempDir(),
		manyParts:       false,
		bucketStoreConfig: mimir_tsdb.BucketStoreConfig{
			// We want to force each Series() call to use more than one batch to catch some edge cases.
			// This should make the implementation slightly slower, although most tests time
			// is dominated by the setup.
			StreamingBatchSize:          10,
			BlockSyncConcurrency:        20,
			PostingOffsetsInMemSampling: mimir_tsdb.DefaultPostingOffsetInMemorySampling,
			IndexHeader: indexheader.Config{
				EagerLoadingStartupEnabled:  true,
				EagerLoadingPersistInterval: time.Minute,
				LazyLoadingEnabled:          true,
				LazyLoadingIdleTimeout:      time.Minute,
			},
		},
		seriesLimiterFactory: newStaticSeriesLimiterFactory(0),
		chunksLimiterFactory: newStaticChunksLimiterFactory(0),
		indexCache:           noopCache{},
		postingsStrategy:     selectAllStrategy{},
		series: []labels.Labels{
			labels.FromStrings("a", "1", "b", "1"),
			labels.FromStrings("a", "1", "b", "2"),
			labels.FromStrings("a", "2", "b", "1"),
			labels.FromStrings("a", "2", "b", "2"),
			labels.FromStrings("a", "1", "c", "1"),
			labels.FromStrings("a", "1", "c", "2"),
			labels.FromStrings("a", "2", "c", "1"),
			labels.FromStrings("a", "2", "c", "2"),
		},
	}
}

type prepareStoreConfigOption func(config *prepareStoreConfig)

func withManyParts() prepareStoreConfigOption {
	return func(config *prepareStoreConfig) {
		config.manyParts = true
	}
}

func prepareStoreWithTestBlocks(t testing.TB, bkt objstore.Bucket, cfg *prepareStoreConfig) *storeSuite {
	extLset := labels.FromStrings("ext1", "value1")
	minTime, maxTime := prepareTestBlocks(t, time.Now(), cfg.numBlocks/2, cfg.tempDir, bkt, cfg.series, extLset, cfg.nonOverlappingBlocks)

	s := &storeSuite{
		logger:          cfg.logger,
		metricsRegistry: cfg.metricsRegistry,
		cache:           &swappableCache{IndexCache: cfg.indexCache},
		minTime:         minTime,
		maxTime:         maxTime,
	}

	metaFetcher, err := block.NewMetaFetcher(s.logger, 20, objstore.WithNoopInstr(bkt), cfg.tempDir, nil, []block.MetadataFilter{}, nil, 0)
	assert.NoError(t, err)

	// Have our options in the beginning so tests can override logger and index cache if they need to
	storeOpts := []BucketStoreOption{WithLogger(s.logger), WithIndexCache(s.cache)}

	store, err := NewBucketStore(
		"tenant",
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		cfg.tempDir,
		cfg.bucketStoreConfig,
		cfg.postingsStrategy,
		cfg.chunksLimiterFactory,
		cfg.seriesLimiterFactory,
		newGapBasedPartitioners(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		hashcache.NewSeriesHashCache(1024*1024),
		NewBucketStoreMetrics(s.metricsRegistry),
		storeOpts...,
	)
	assert.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, s.store.RemoveBlocksAndClose())
	})

	s.store = store

	if cfg.manyParts {
		s.store.partitioners = blockPartitioners{naivePartitioner{}, naivePartitioner{}, naivePartitioner{}}
	}
	ctx := context.Background()
	require.NoError(t, services.StartAndAwaitRunning(ctx, store))
	require.NoError(t, store.InitialSync(ctx))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, store))
	})
	return s
}

type testBucketStoreCase struct {
	req              *storepb.SeriesRequest
	expected         [][]mimirpb.LabelAdapter
	expectedChunkLen int
}

// TODO(bwplotka): Benchmark Series.
//
//nolint:revive
func testBucketStore_e2e(t *testing.T, ctx context.Context, s *storeSuite, additionalCases ...testBucketStoreCase) {
	t.Helper()

	mint, maxt := s.store.TimeRange()
	assert.Equal(t, s.minTime, mint)
	assert.Equal(t, s.maxTime, maxt)

	vals, err := s.store.LabelValues(ctx, &storepb.LabelValuesRequest{
		Label: "a",
		Start: timestamp.FromTime(minTime),
		End:   timestamp.FromTime(maxTime),
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2"}, vals.Values)

	srv := newStoreGatewayTestServer(t, s.store)

	// TODO(bwplotka): Add those test cases to TSDB querier_test.go as well, there are no tests for matching.
	testCases := []testBucketStoreCase{
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "a", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "a", Value: "not_existing"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NRE, Name: "not_existing", Value: "1"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "b", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "2"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
			},
		},
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_NEQ, Name: "a", Value: "not_existing"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
			expectedChunkLen: 3,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
			},
		},
		// Regression https://github.com/thanos-io/thanos/issues/833.
		// Problem: Matcher that was selecting NO series, was ignored instead of passed as emptyPosting to Intersect.
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
					{Type: storepb.LabelMatcher_RE, Name: "non_existing", Value: "something"},
				},
				MinTime: mint,
				MaxTime: maxt,
			},
		},
		// Test skip-chunk option.
		{
			req: &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				MinTime:    mint,
				MaxTime:    maxt,
				SkipChunks: true,
			},
			expectedChunkLen: 0,
			expected: [][]mimirpb.LabelAdapter{
				{{Name: "a", Value: "1"}, {Name: "b", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "b", Value: "2"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
				{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
			},
		},
	}
	for i, tcase := range append(testCases, additionalCases...) {
		for _, streamingBatchSize := range []int{0, 1, 5, 256} {
			if ok := t.Run(fmt.Sprintf("%d,streamingBatchSize=%d", i, streamingBatchSize), func(t *testing.T) {
				tcase.req.StreamingChunksBatchSize = uint64(streamingBatchSize)
				seriesSet, _, _, _, err := srv.Series(context.Background(), tcase.req)
				require.NoError(t, err)

				assert.Equal(t, len(tcase.expected), len(seriesSet))

				for i, s := range seriesSet {
					assert.Equal(t, tcase.expected[i], s.Labels)
					assert.Equal(t, tcase.expectedChunkLen, len(s.Chunks))
				}
				assertQueryStatsMetricsRecorded(t, len(tcase.expected), tcase.expectedChunkLen, s.metricsRegistry)
			}); !ok {
				return
			}
		}
	}
}

func assertQueryStatsMetricsRecorded(t *testing.T, numSeries int, numChunksPerSeries int, registry *prometheus.Registry) {
	t.Helper()

	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err, "couldn't gather metrics from BucketStore")

	if numSeries > 0 {
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_result_series", metrics))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "series"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "series"))

		assert.NotZero(t, numObservationsForHistogram(t, "cortex_bucket_store_series_request_stage_duration_seconds", metrics))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_blocks_queried", metrics, "source", "test", "level", "1"))
	}
	if numChunksPerSeries > 0 {
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "chunks"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "chunks"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_blocks_queried", metrics, "source", "test", "level", "1"))
	}
}

func TestBucketStore_e2e(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		if ok := t.Run("no caches", func(t *testing.T) {
			s.cache.SwapIndexCacheWith(noopCache{})
			testBucketStore_e2e(t, ctx, s)
		}); !ok {
			return
		}

		if ok := t.Run("with large, sufficient index cache", func(t *testing.T) {
			indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
				MaxItemSize: 1e5,
				MaxSize:     2e5,
			})
			assert.NoError(t, err)
			s.cache.SwapIndexCacheWith(indexCache)
			testBucketStore_e2e(t, ctx, s)
		}); !ok {
			return
		}

		t.Run("with small index cache", func(t *testing.T) {
			indexCache2, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
				MaxItemSize: 50,
				MaxSize:     100,
			})
			assert.NoError(t, err)
			s.cache.SwapIndexCacheWith(indexCache2)
			testBucketStore_e2e(t, ctx, s)
		})
	})
}

func TestBucketStore_e2e_StreamingEdgeCases(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite(func(config *prepareStoreConfig) {
			config.nonOverlappingBlocks = true
		})

		_, maxt := s.store.TimeRange()
		additionalCases := []testBucketStoreCase{
			{ // This tests if the first phase of streaming that sends only the series is filtering the series by chunk time range.
				// The request time range overlaps with 2 blocks with 4 timeseries each, but only the 2nd block
				// has some overlapping data that should be returned.
				req: &storepb.SeriesRequest{
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
					},
					// A block spans 120 mins. So 121 grabs the second to last block.
					MinTime: maxt - 121*int64(time.Minute/time.Millisecond),
					MaxTime: maxt,
				},
				expectedChunkLen: 1,
				expected: [][]mimirpb.LabelAdapter{
					{{Name: "a", Value: "1"}, {Name: "c", Value: "1"}},
					{{Name: "a", Value: "1"}, {Name: "c", Value: "2"}},
					{{Name: "a", Value: "2"}, {Name: "c", Value: "1"}},
					{{Name: "a", Value: "2"}, {Name: "c", Value: "2"}},
				},
			},
		}

		if ok := t.Run("no caches", func(t *testing.T) {
			s.cache.SwapIndexCacheWith(noopCache{})
			testBucketStore_e2e(t, ctx, s, additionalCases...)
		}); !ok {
			return
		}

		if ok := t.Run("with large, sufficient index cache", func(t *testing.T) {
			indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
				MaxItemSize: 1e5,
				MaxSize:     2e5,
			})
			assert.NoError(t, err)
			s.cache.SwapIndexCacheWith(indexCache)
			testBucketStore_e2e(t, ctx, s, additionalCases...)
		}); !ok {
			return
		}

		t.Run("with small index cache", func(t *testing.T) {
			indexCache2, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
				MaxItemSize: 50,
				MaxSize:     100,
			})
			assert.NoError(t, err)
			s.cache.SwapIndexCacheWith(indexCache2)
			testBucketStore_e2e(t, ctx, s)
		})
	})
}

type naivePartitioner struct{}

func (g naivePartitioner) Partition(length int, rng func(int) (uint64, uint64)) (parts []Part) {
	for i := 0; i < length; i++ {
		s, e := rng(i)
		parts = append(parts, Part{Start: s, End: e, ElemRng: [2]int{i, i + 1}})
	}
	return parts
}

// Naive partitioner splits the array equally (it does not combine anything).
// This tests if our, sometimes concurrent, fetches for different parts works.
// Regression test against: https://github.com/thanos-io/thanos/issues/829.
func TestBucketStore_ManyParts_e2e(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		prepareCfg := defaultPrepareStoreConfig(t)
		prepareCfg.manyParts = true

		s := newSuite(withManyParts())

		indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(s.logger, nil, indexcache.InMemoryIndexCacheConfig{
			MaxItemSize: 1e5,
			MaxSize:     2e5,
		})
		assert.NoError(t, err)
		s.cache.SwapIndexCacheWith(indexCache)

		testBucketStore_e2e(t, ctx, s)
	})
}

func TestBucketStore_Series_ChunksLimiter_e2e(t *testing.T) {
	// The query will fetch 4 series from 3 blocks each, so we do expect to hit a total of 12 chunks.
	expectedChunks := uint64(4 * 3)

	cases := map[string]struct {
		maxChunksLimit uint64
		maxSeriesLimit uint64
		expectedErr    string
		expectedCode   codes.Code
	}{
		"should succeed if the max chunks limit is not exceeded": {
			maxChunksLimit: expectedChunks,
		},
		"should succeed if the max series limit is not exceeded": {
			// The streaming case should not count the series twice.
			maxSeriesLimit: 4,
		},
		"should fail if the max chunks limit is exceeded - 422": {
			maxChunksLimit: expectedChunks - 1,
			expectedErr:    "the query exceeded the maximum number of chunks (limit: 11 chunks) (err-mimir-max-chunks-per-query)",
			expectedCode:   http.StatusUnprocessableEntity,
		},
		"should fail if the max series limit is exceeded - 422": {
			maxChunksLimit: expectedChunks,
			expectedErr:    "the query exceeded the maximum number of series (limit: 1 series) (err-mimir-max-series-per-query)",
			maxSeriesLimit: 1,
			expectedCode:   http.StatusUnprocessableEntity,
		},
	}

	for testName, testData := range cases {
		t.Run(testName, func(t *testing.T) {
			for _, streamingBatchSize := range []int{0, 1, 5} {
				t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
					bkt := objstore.NewInMemBucket()

					prepConfig := defaultPrepareStoreConfig(t)
					prepConfig.chunksLimiterFactory = newStaticChunksLimiterFactory(testData.maxChunksLimit)
					prepConfig.seriesLimiterFactory = newStaticSeriesLimiterFactory(testData.maxSeriesLimit)

					s := prepareStoreWithTestBlocks(t, bkt, prepConfig)

					req := &storepb.SeriesRequest{
						Matchers: []storepb.LabelMatcher{
							{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
						},
						MinTime:                  timestamp.FromTime(minTime),
						MaxTime:                  timestamp.FromTime(maxTime),
						StreamingChunksBatchSize: uint64(streamingBatchSize),
					}

					srv := newStoreGatewayTestServer(t, s.store)
					_, _, _, _, err := srv.Series(context.Background(), req)

					if testData.expectedErr == "" {
						assert.NoError(t, err)
					} else {
						assert.Error(t, err)
						assert.Contains(t, err.Error(), testData.expectedErr)
						status, ok := grpcutil.ErrorToStatus(err)
						assert.Equal(t, true, ok)
						assert.Equal(t, testData.expectedCode, status.Code())
					}
				})
			}
		})
	}
}

func TestBucketStore_EagerLoading(t *testing.T) {
	testCases := map[string]struct {
		eagerLoadReaderEnabled       bool
		expectedEagerLoadedBlocks    int
		createLoadedBlocksSnapshotFn func([]ulid.ULID) map[ulid.ULID]int64
	}{
		"block is present in pre-shutdown loaded blocks and eager-loading is disabled": {
			eagerLoadReaderEnabled:    false,
			expectedEagerLoadedBlocks: 0,
			createLoadedBlocksSnapshotFn: func(blockIDs []ulid.ULID) map[ulid.ULID]int64 {
				snapshot := make(map[ulid.ULID]int64)
				for _, blockID := range blockIDs {
					snapshot[blockID] = time.Now().UnixMilli()
				}
				return snapshot
			},
		},
		"block is present in pre-shutdown loaded blocks and eager-loading is enabled, loading index header during initial sync": {
			eagerLoadReaderEnabled:    true,
			expectedEagerLoadedBlocks: 6,
			createLoadedBlocksSnapshotFn: func(blockIDs []ulid.ULID) map[ulid.ULID]int64 {
				snapshot := make(map[ulid.ULID]int64)
				for _, blockID := range blockIDs {
					snapshot[blockID] = time.Now().UnixMilli()
				}
				return snapshot
			},
		},
		"block is present in pre-shutdown loaded blocks and eager-loading is enabled, loading index header after initial sync": {
			eagerLoadReaderEnabled:    true,
			expectedEagerLoadedBlocks: 6,
			createLoadedBlocksSnapshotFn: func(blockIDs []ulid.ULID) map[ulid.ULID]int64 {
				snapshot := make(map[ulid.ULID]int64)
				for _, blockID := range blockIDs {
					snapshot[blockID] = time.Now().UnixMilli()
				}
				return snapshot
			},
		},
		"block is not present in pre-shutdown loaded blocks snapshot and eager-loading is enabled": {
			eagerLoadReaderEnabled:    true,
			expectedEagerLoadedBlocks: 0, // although eager loading is enabled, this test will not do eager loading because the block ID is not in the lazy loaded file.
			createLoadedBlocksSnapshotFn: func(_ []ulid.ULID) map[ulid.ULID]int64 {
				// let's create a random fake blockID to be stored in lazy loaded headers file
				fakeBlockID := ulid.MustNew(ulid.Now(), nil)
				// this snapshot will refer to fake block, hence eager load wouldn't be executed for the real block that we test
				return map[ulid.ULID]int64{fakeBlockID: time.Now().UnixMilli()}
			},
		},
		"pre-shutdown loaded blocks snapshot doesn't exist and eager-loading is enabled": {
			eagerLoadReaderEnabled:    true,
			expectedEagerLoadedBlocks: 0,
		},
	}

	assertLoadedBlocks := func(t *testing.T, cfg *prepareStoreConfig, expectedLoadedBlocks int) {
		assert.NoError(t, testutil.GatherAndCompare(cfg.metricsRegistry, strings.NewReader(fmt.Sprintf(`
 				# HELP cortex_bucket_store_indexheader_lazy_load_total Total number of index-header lazy load operations.
				# TYPE cortex_bucket_store_indexheader_lazy_load_total counter
				cortex_bucket_store_indexheader_lazy_load_total %d
				`, expectedLoadedBlocks)),
			"cortex_bucket_store_indexheader_lazy_load_total",
		))
	}

	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			bkt := objstore.NewInMemBucket()
			cfg := defaultPrepareStoreConfig(t)
			cfg.logger = test.NewTestingLogger(t)
			cfg.bucketStoreConfig.IndexHeader.EagerLoadingStartupEnabled = testData.eagerLoadReaderEnabled
			ctx := context.Background()

			// Start the store so we generate some blocks and can use them in the mock snapshot.
			store := prepareStoreWithTestBlocks(t, bkt, cfg)
			assertLoadedBlocks(t, cfg, 0)

			if testData.createLoadedBlocksSnapshotFn != nil {
				// Create the snapshot manually so that we don't rely on the periodic snapshotting.
				loadedBlocks := store.store.blockSet.openBlocksULIDs()
				staticLoader := staticLoadedBlocks(testData.createLoadedBlocksSnapshotFn(loadedBlocks))
				snapshotter := indexheader.NewSnapshotter(cfg.logger, indexheader.SnapshotterConfig{
					PersistInterval: time.Hour,
					Path:            cfg.tempDir,
				}, staticLoader)

				require.NoError(t, snapshotter.PersistLoadedBlocks())
			}
			// Stop store and start a new one using the same directory. It should pick up the stored blocks.
			require.NoError(t, services.StopAndAwaitTerminated(ctx, store.store))
			cfg.metricsRegistry = prometheus.NewRegistry() // The store-gateway will reregister its metrics; replace the registry to prevent a panic
			cfg.numBlocks = 0                              // we don't want to generate blocks again to speed the test up

			_ = prepareStoreWithTestBlocks(t, bkt, cfg) // we create and start the store only to trigger eager loading.
			assertLoadedBlocks(t, cfg, testData.expectedEagerLoadedBlocks)
		})
	}
}

func TestBucketStore_PersistsLazyLoadedBlocks(t *testing.T) {
	t.Parallel()

	const persistInterval = 100 * time.Millisecond
	bkt := objstore.NewInMemBucket()
	cfg := defaultPrepareStoreConfig(t)
	cfg.logger = test.NewTestingLogger(t)
	cfg.bucketStoreConfig.IndexHeader.EagerLoadingPersistInterval = persistInterval
	cfg.bucketStoreConfig.IndexHeader.EagerLoadingStartupEnabled = true
	cfg.bucketStoreConfig.IndexHeader.LazyLoadingIdleTimeout = persistInterval * 3
	ctx := context.Background()
	readBlocksInSnapshot := func() map[ulid.ULID]int64 {
		blocks, err := indexheader.RestoreLoadedBlocks(cfg.tempDir)
		assert.NoError(t, err)
		return blocks
	}

	// Start the store so we generate some blocks and can use them in the mock snapshot.
	store := prepareStoreWithTestBlocks(t, bkt, cfg)
	// Wait for the snapshot to be persisted.
	time.Sleep(persistInterval * 2)

	// The snapshot should be empty.
	assert.Empty(t, readBlocksInSnapshot())

	// Run a simple request to trigger loading the blocks
	resp, err := store.store.LabelNames(ctx, &storepb.LabelNamesRequest{End: math.MaxInt64})
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Names)

	// The snapshot should now contain the blocks we queried.
	assert.Eventually(t, func() bool {
		return len(readBlocksInSnapshot()) == cfg.numBlocks
	}, persistInterval*5, persistInterval/2)

	// Wait for the blocks to be unloaded due to the lazy loading idle timeout.
	// The snapshot should be empty.
	assert.Eventually(t, func() bool {
		return len(readBlocksInSnapshot()) == 0
	}, persistInterval*5, persistInterval/2)
}

type staticLoadedBlocks map[ulid.ULID]int64

func (b staticLoadedBlocks) LoadedBlocks() map[ulid.ULID]int64 {
	return b
}

func assertQueryStatsLabelNamesMetricsRecorded(t *testing.T, numLabelNames int, registry *prometheus.Registry) {
	t.Helper()

	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err, "couldn't gather metrics from BucketStore")

	if numLabelNames > 0 {
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "series"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "series"))

		assert.NotZero(t, numObservationsForHistogram(t, "cortex_bucket_store_series_request_stage_duration_seconds", metrics))
	}
}

func assertQueryStatsLabelValuesMetricsRecorded(t *testing.T, registry *prometheus.Registry) {
	t.Helper()

	metrics, err := dskit_metrics.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err, "couldn't gather metrics from BucketStore")

	assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "postings"))
	assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_touched", metrics, "data_type", "series"))
	assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "postings"))
	assert.NotZero(t, numObservationsForSummaries(t, "cortex_bucket_store_series_data_fetched", metrics, "data_type", "series"))
}

func TestBucketStore_LabelNames_e2e(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		mint, maxt := s.store.TimeRange()
		assert.Equal(t, s.minTime, mint)
		assert.Equal(t, s.maxTime, maxt)

		for name, tc := range map[string]struct {
			req      *storepb.LabelNamesRequest
			expected []string
		}{
			"basic labelNames": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"a", "b", "c"},
			},
			"basic labelNames, limit = 1": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Limit: 1,
				},
				expected: []string{"a"},
			},
			"outside the time range": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"matcher matching everything": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: []string{"a", "b", "c"},
			},
			"b=1 matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "b",
							Value: "1",
						},
					},
				},
				expected: []string{"a", "b"},
			},

			"b='' matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "b",
							Value: "",
						},
					},
				},
				expected: []string{"a", "c"},
			},
			"outside the time range, with matcher": {
				req: &storepb.LabelNamesRequest{
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: nil,
			},
		} {
			t.Run(name, func(t *testing.T) {
				vals, err := s.store.LabelNames(ctx, tc.req)
				assert.NoError(t, err)

				assert.Equal(t, tc.expected, vals.Names)

				assertQueryStatsLabelNamesMetricsRecorded(t, len(tc.expected), s.metricsRegistry)
			})
		}
	})
}

func TestBucketStore_LabelValues_e2e(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		mint, maxt := s.store.TimeRange()
		assert.Equal(t, s.minTime, mint)
		assert.Equal(t, s.maxTime, maxt)

		for name, tc := range map[string]struct {
			req      *storepb.LabelValuesRequest
			expected []string
		}{
			"label a": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: []string{"1", "2"},
			},
			"label a, outside time range": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(time.Now().Add(-24 * time.Hour)),
					End:   timestamp.FromTime(time.Now().Add(-23 * time.Hour)),
				},
				expected: nil,
			},
			"label a, a=1": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "1",
						},
					},
				},
				expected: []string{"1"},
			},
			"label a, a=2, c=2": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Matchers: []storepb.LabelMatcher{
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "a",
							Value: "2",
						},
						{
							Type:  storepb.LabelMatcher_EQ,
							Name:  "c",
							Value: "2",
						},
					},
				},
				expected: []string{"2"},
			},
			"label a, limit=1": {
				req: &storepb.LabelValuesRequest{
					Label: "a",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
					Limit: 1,
				},
				expected: []string{"1"},
			},
			"label ext1": {
				req: &storepb.LabelValuesRequest{
					Label: "ext1",
					Start: timestamp.FromTime(minTime),
					End:   timestamp.FromTime(maxTime),
				},
				expected: nil, // External labels are not returned.
			},
		} {
			t.Run(name, func(t *testing.T) {
				vals, err := s.store.LabelValues(ctx, tc.req)
				assert.NoError(t, err)

				assert.Equal(t, tc.expected, emptyToNil(vals.Values))

				assertQueryStatsLabelValuesMetricsRecorded(t, s.metricsRegistry)
			})
		}
	})
}

func TestBucketStore_ValueTypes_e2e(t *testing.T) {
	for _, streamingBatchSize := range []int{0, 1, 5} {
		t.Run(fmt.Sprintf("streamingBatchSize=%d", streamingBatchSize), func(t *testing.T) {
			foreachStore(t, func(t *testing.T, newSuite suiteFactory) {

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				s := newSuite()

				mint, maxt := s.store.TimeRange()
				assert.Equal(t, s.minTime, mint)
				assert.Equal(t, s.maxTime, maxt)

				req := &storepb.SeriesRequest{
					MinTime: mint,
					MaxTime: maxt,
					Matchers: []storepb.LabelMatcher{
						{Type: storepb.LabelMatcher_RE, Name: "a", Value: "1|2"},
					},
					StreamingChunksBatchSize: uint64(streamingBatchSize),
				}

				srv := newStoreGatewayTestServer(t, s.store)
				seriesSet, _, _, _, err := srv.Series(ctx, req)
				require.NoError(t, err)

				counts := map[storepb.Chunk_Encoding]int{}
				for _, series := range seriesSet {
					for _, chunk := range series.Chunks {
						counts[chunk.Raw.Type]++
					}
				}
				for _, chunkType := range []storepb.Chunk_Encoding{storepb.Chunk_XOR, storepb.Chunk_Histogram, storepb.Chunk_FloatHistogram} {
					count, ok := counts[chunkType]
					assert.True(t, ok, fmt.Sprintf("value type %s is not present", storepb.Chunk_Encoding_name[int32(chunkType)]))
					assert.NotEmpty(t, count)
				}
			})
		})
	}
}

func emptyToNil(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return values
}

type suiteFactory func(...prepareStoreConfigOption) *storeSuite

func foreachStore(t *testing.T, runTest func(t *testing.T, newSuite suiteFactory)) {
	t.Parallel()

	// Mandatory Inmem. Not parallel, to detect problem early.
	if ok := t.Run("inmem", func(t *testing.T) {
		factory := func(opts ...prepareStoreConfigOption) *storeSuite {
			return prepareStoreWithTestBlocks(t, objstore.NewInMemBucket(), defaultPrepareStoreConfig(t).apply(opts...))
		}
		runTest(t, factory)
	}); !ok {
		return
	}

	// Mandatory Filesystem.
	t.Run("filesystem", func(t *testing.T) {
		t.Parallel()

		b, err := filesystem.NewBucket(t.TempDir())
		assert.NoError(t, err)
		factory := func(opts ...prepareStoreConfigOption) *storeSuite {
			return prepareStoreWithTestBlocks(t, b, defaultPrepareStoreConfig(t).apply(opts...))
		}
		runTest(t, factory)
	})
}

func numObservationsForSummaries(t *testing.T, summaryName string, metrics dskit_metrics.MetricFamilyMap, labelValuePairs ...string) uint64 {
	t.Helper()

	summaryData := &dskit_metrics.SummaryData{}
	for _, metric := range dskit_metrics.FindMetricsInFamilyMatchingLabels(metrics[summaryName], labelValuePairs...) {
		summaryData.AddSummary(metric.GetSummary())
	}
	m := &dto.Metric{}
	require.NoError(t, summaryData.Metric(prometheus.NewDesc("test", "", nil, nil)).Write(m))
	return m.GetSummary().GetSampleCount()
}

func numObservationsForHistogram(t *testing.T, histogramName string, metrics dskit_metrics.MetricFamilyMap, labelValuePairs ...string) uint64 {
	t.Helper()

	histogramData := &dskit_metrics.HistogramData{}
	for _, metric := range dskit_metrics.FindMetricsInFamilyMatchingLabels(metrics[histogramName], labelValuePairs...) {
		histogramData.AddHistogram(metric.GetHistogram())
	}
	m := &dto.Metric{}
	require.NoError(t, histogramData.Metric(prometheus.NewDesc("test", "", nil, nil)).Write(m))
	return m.GetHistogram().GetSampleCount()
}
