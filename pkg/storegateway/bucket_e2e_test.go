// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket_e2e_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"github.com/weaveworks/common/httpgrpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/storegateway/testhelper"
	"github.com/grafana/mimir/pkg/util"
)

var (
	minTime    = time.Unix(0, 0)
	maxTime, _ = time.Parse(time.RFC3339, "9999-12-31T23:59:59Z")
)

type swappableCache struct {
	indexcache.IndexCache
}

type customLimiter struct {
	limiter *Limiter
	code    codes.Code
}

func (c *customLimiter) Reserve(num uint64) error {
	err := c.limiter.Reserve(num)
	if err != nil {
		return httpgrpc.Errorf(int(c.code), err.Error())
	}

	return nil
}

func (c *swappableCache) SwapWith(cache indexcache.IndexCache) {
	c.IndexCache = cache
}

type storeSuite struct {
	store            *BucketStore
	minTime, maxTime int64
	cache            *swappableCache
	metricsRegistry  *prometheus.Registry

	logger log.Logger
}

func prepareTestBlocks(t testing.TB, now time.Time, count int, dir string, bkt objstore.Bucket,
	series []labels.Labels, extLset labels.Labels) (minTime, maxTime int64) {
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
		id1, err := testhelper.CreateBlock(ctx, dir, series[:4], 10, mint, maxt, extLset)
		assert.NoError(t, err)
		id2, err := testhelper.CreateBlock(ctx, dir, series[4:], 10, mint, maxt, extLset)
		assert.NoError(t, err)

		dir1, dir2 := filepath.Join(dir, id1.String()), filepath.Join(dir, id2.String())

		// Replace labels to the meta of the second block.
		meta, err := metadata.ReadFromDir(dir2)
		assert.NoError(t, err)
		meta.Thanos.Labels = map[string]string{"ext2": "value2"}
		assert.NoError(t, meta.WriteToDir(logger, dir2))

		assert.NoError(t, block.Upload(ctx, logger, bkt, dir1, nil))
		assert.NoError(t, block.Upload(ctx, logger, bkt, dir2, nil))

		assert.NoError(t, os.RemoveAll(dir1))
		assert.NoError(t, os.RemoveAll(dir2))
	}

	return
}

func newCustomChunksLimiterFactory(limit uint64, code codes.Code) ChunksLimiterFactory {
	return func(failedCounter prometheus.Counter) ChunksLimiter {
		return &customLimiter{
			limiter: NewLimiter(limit, failedCounter),
			code:    code,
		}
	}
}

func newCustomSeriesLimiterFactory(limit uint64, code codes.Code) SeriesLimiterFactory {
	return func(failedCounter prometheus.Counter) SeriesLimiter {
		return &customLimiter{
			limiter: NewLimiter(limit, failedCounter),
			code:    code,
		}
	}
}

type prepareStoreConfig struct {
	tempDir              string
	manyParts            bool
	chunksLimiterFactory ChunksLimiterFactory
	seriesLimiterFactory SeriesLimiterFactory
	series               []labels.Labels
	indexCache           indexcache.IndexCache
	bucketStoreOpts      []BucketStoreOption
	metricsRegistry      *prometheus.Registry
}

func (c *prepareStoreConfig) apply(opts ...prepareStoreConfigOption) *prepareStoreConfig {
	for _, o := range opts {
		o(c)
	}
	return c
}

func defaultPrepareStoreConfig(t testing.TB) *prepareStoreConfig {
	return &prepareStoreConfig{
		metricsRegistry:      prometheus.NewRegistry(),
		tempDir:              t.TempDir(),
		manyParts:            false,
		seriesLimiterFactory: NewSeriesLimiterFactory(0),
		chunksLimiterFactory: NewChunksLimiterFactory(0),
		indexCache:           noopCache{},
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

func withBucketStoreOptions(opts ...BucketStoreOption) prepareStoreConfigOption {
	return func(config *prepareStoreConfig) {
		config.bucketStoreOpts = opts
	}
}

func prepareStoreWithTestBlocks(t testing.TB, bkt objstore.Bucket, cfg *prepareStoreConfig) *storeSuite {
	extLset := labels.FromStrings("ext1", "value1")

	minTime, maxTime := prepareTestBlocks(t, time.Now(), 3, cfg.tempDir, bkt, cfg.series, extLset)

	s := &storeSuite{
		logger:          log.NewNopLogger(),
		metricsRegistry: cfg.metricsRegistry,
		cache:           &swappableCache{IndexCache: cfg.indexCache},
		minTime:         minTime,
		maxTime:         maxTime,
	}

	metaFetcher, err := block.NewMetaFetcher(s.logger, 20, objstore.WithNoopInstr(bkt), cfg.tempDir, nil, []block.MetadataFilter{})
	assert.NoError(t, err)

	// Have our options in the beginning so tests can override logger and index cache if they need to
	storeOpts := append([]BucketStoreOption{WithLogger(s.logger), WithIndexCache(s.cache)}, cfg.bucketStoreOpts...)

	store, err := NewBucketStore(
		"tenant",
		objstore.WithNoopInstr(bkt),
		metaFetcher,
		cfg.tempDir,
		cfg.chunksLimiterFactory,
		cfg.seriesLimiterFactory,
		newGapBasedPartitioner(mimir_tsdb.DefaultPartitionerMaxGapSize, nil),
		20,
		mimir_tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.Config{},
		true,
		time.Minute,
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
		s.store.partitioner = naivePartitioner{}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	assert.NoError(t, store.SyncBlocks(ctx))
	return s
}

// TODO(bwplotka): Benchmark Series.
//
//nolint:revive
func testBucketStore_e2e(t *testing.T, ctx context.Context, s *storeSuite) {
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

	// TODO(bwplotka): Add those test cases to TSDB querier_test.go as well, there are no tests for matching.
	for i, tcase := range []struct {
		req              *storepb.SeriesRequest
		expected         [][]mimirpb.LabelAdapter
		expectedChunkLen int
	}{
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
	} {
		if ok := t.Run(fmt.Sprint(i), func(t *testing.T) {
			srv := newBucketStoreSeriesServer(ctx)

			assert.NoError(t, s.store.Series(tcase.req, srv))
			assert.Equal(t, len(tcase.expected), len(srv.SeriesSet))

			for i, s := range srv.SeriesSet {
				assert.Equal(t, tcase.expected[i], s.Labels)
				assert.Equal(t, tcase.expectedChunkLen, len(s.Chunks))
			}
			assertQueryStatsMetricsRecorded(t, len(tcase.expected), tcase.expectedChunkLen, s.store.maxSeriesPerBatch > 0, s.metricsRegistry)
		}); !ok {
			return
		}
	}
}

func assertQueryStatsMetricsRecorded(t *testing.T, numSeries int, numChunksPerSeries int, streamingEnabled bool, registry *prometheus.Registry) {
	t.Helper()

	metrics, err := util.NewMetricFamilyMapFromGatherer(registry)
	require.NoError(t, err, "couldn't gather metrics from BucketStore")

	toLabels := func(labelValuePairs []string) (result labels.Labels) {
		if len(labelValuePairs)%2 != 0 {
			t.Fatalf("invalid label name-value pairs %s", strings.Join(labelValuePairs, ""))
		}
		for i := 0; i < len(labelValuePairs); i += 2 {
			result = append(result, labels.Label{Name: labelValuePairs[i], Value: labelValuePairs[i+1]})
		}
		return
	}

	numObservationsForSummaries := func(summaryName string, labelValuePairs ...string) uint64 {
		summaryData := &util.SummaryData{}
		for _, metric := range getMetricsMatchingLabels(metrics[summaryName], toLabels(labelValuePairs)) {
			summaryData.AddSummary(metric.GetSummary())
		}
		m := &dto.Metric{}
		require.NoError(t, summaryData.Metric(&prometheus.Desc{}).Write(m))
		return m.GetSummary().GetSampleCount()
	}

	numObservationsForHistogram := func(histogramName string, labelValuePairs ...string) uint64 {
		histogramData := &util.HistogramData{}
		for _, metric := range getMetricsMatchingLabels(metrics[histogramName], toLabels(labelValuePairs)) {
			histogramData.AddHistogram(metric.GetHistogram())
		}
		m := &dto.Metric{}
		require.NoError(t, histogramData.Metric(&prometheus.Desc{}).Write(m))
		return m.GetHistogram().GetSampleCount()
	}

	if numSeries > 0 {
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_result_series"))
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_touched", "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_touched", "data_type", "series"))
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_fetched", "data_type", "postings"))
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_fetched", "data_type", "series"))

		if streamingEnabled {
			assert.NotZero(t, numObservationsForHistogram("cortex_bucket_store_series_request_stage_duration_seconds"))
			assert.NotZero(t, numObservationsForHistogram("cortex_bucket_store_series_refs_fetch_duration_seconds"))
		} else {
			assert.NotZero(t, numObservationsForHistogram("cortex_bucket_store_series_get_all_duration_seconds"))
			assert.NotZero(t, numObservationsForHistogram("cortex_bucket_store_series_merge_duration_seconds"))
		}
	}
	if numChunksPerSeries > 0 {
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_touched", "data_type", "chunks"))
		assert.NotZero(t, numObservationsForSummaries("cortex_bucket_store_series_data_fetched", "data_type", "chunks"))
	}
}

func getMetricsMatchingLabels(mf *dto.MetricFamily, selectors labels.Labels) []*dto.Metric {
	var result []*dto.Metric
	for _, m := range mf.GetMetric() {
		if !matchesSelectors(m, selectors) {
			continue
		}
		result = append(result, m)
	}
	return result
}

func matchesSelectors(m *dto.Metric, selectors labels.Labels) bool {
	for _, l := range selectors {
		found := false
		for _, lp := range m.GetLabel() {
			if l.Name != lp.GetName() || l.Value != lp.GetValue() {
				continue
			}

			found = true
			break
		}

		if !found {
			return false
		}
	}

	return true
}

func TestBucketStore_e2e(t *testing.T) {
	foreachStore(t, func(t *testing.T, newSuite suiteFactory) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s := newSuite()

		if ok := t.Run("no index cache", func(t *testing.T) {
			s.cache.SwapWith(noopCache{})
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
			s.cache.SwapWith(indexCache)
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
			s.cache.SwapWith(indexCache2)
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
		s.cache.SwapWith(indexCache)

		testBucketStore_e2e(t, ctx, s)
	})
}

func TestBucketStore_Series_ChunksLimiter_e2e(t *testing.T) {
	// The query will fetch 2 series from 6 blocks, so we do expect to hit a total of 12 chunks.
	expectedChunks := uint64(2 * 6)

	cases := map[string]struct {
		maxChunksLimit uint64
		maxSeriesLimit uint64
		expectedErr    string
		code           codes.Code
	}{
		"should succeed if the max chunks limit is not exceeded": {
			maxChunksLimit: expectedChunks,
		},
		"should fail if the max chunks limit is exceeded - ResourceExhausted": {
			maxChunksLimit: expectedChunks - 1,
			expectedErr:    "exceeded chunks limit",
			code:           codes.ResourceExhausted,
		},
		"should fail if the max chunks limit is exceeded - 422": {
			maxChunksLimit: expectedChunks - 1,
			expectedErr:    "exceeded chunks limit",
			code:           422,
		},
		"should fail if the max series limit is exceeded - 422": {
			maxChunksLimit: expectedChunks,
			expectedErr:    "exceeded series limit",
			maxSeriesLimit: 1,
			code:           422,
		},
	}

	for testName, testData := range cases {
		t.Run(testName, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			bkt := objstore.NewInMemBucket()

			prepConfig := defaultPrepareStoreConfig(t)
			prepConfig.chunksLimiterFactory = newCustomChunksLimiterFactory(testData.maxChunksLimit, testData.code)
			prepConfig.seriesLimiterFactory = newCustomSeriesLimiterFactory(testData.maxSeriesLimit, testData.code)

			s := prepareStoreWithTestBlocks(t, bkt, prepConfig)
			assert.NoError(t, s.store.SyncBlocks(ctx))

			req := &storepb.SeriesRequest{
				Matchers: []storepb.LabelMatcher{
					{Type: storepb.LabelMatcher_EQ, Name: "a", Value: "1"},
				},
				MinTime: timestamp.FromTime(minTime),
				MaxTime: timestamp.FromTime(maxTime),
			}

			srv := newBucketStoreSeriesServer(ctx)
			err := s.store.Series(req, srv)

			if testData.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.True(t, strings.Contains(err.Error(), testData.expectedErr))
				status, ok := status.FromError(err)
				assert.Equal(t, true, ok)
				assert.Equal(t, testData.code, status.Code())
			}
		})
	}
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
			})
		}
	})
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

	t.Run("streaming", func(t *testing.T) {
		t.Parallel()

		b, err := filesystem.NewBucket(t.TempDir())
		assert.NoError(t, err)
		factory := func(opts ...prepareStoreConfigOption) *storeSuite {
			// We want to force each Series() call to use more than one batch to catch some edge cases.
			// This should make the implementation slightly slower, although test time
			// should be dominated by the setup.
			opts = append(opts, withBucketStoreOptions(WithStreamingSeriesPerBatch(10)))
			return prepareStoreWithTestBlocks(t, b, defaultPrepareStoreConfig(t).apply(opts...))
		}
		runTest(t, factory)
	})
}
