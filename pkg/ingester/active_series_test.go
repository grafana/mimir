// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/sharding"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_ActiveSeries(t *testing.T) {
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}

	seriesWithLabelsOfSize := func(size, index int) mimirpb.PreallocTimeseries {
		// 24 bytes of static strings and slice overhead, the remaining bytes are used to
		// pad the value of the "lbl" label.
		require.Greater(t, size, 24, "minimum message size is 24 bytes")
		tpl := fmt.Sprintf("%%0%dd", size-24)
		return mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(
					labels.FromStrings(model.MetricNameLabel, "test", "lbl", fmt.Sprintf(tpl, index))),
				Samples: samples,
			},
		}
	}

	totalMessageCount := 4
	totalSeriesSize := totalMessageCount * activeSeriesMaxSizeBytes

	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	currentSize := 0
	for i := 0; currentSize < totalSeriesSize; i++ {
		s := seriesWithLabelsOfSize(1024, i)
		writeReq.Timeseries = append(writeReq.Timeseries, s)
		currentSize += s.Size()
	}

	// Write the series.
	ingesterClient := prepareHealthyIngester(t, nil)
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ingesterClient.Push(ctx, writeReq)
	require.NoError(t, err)

	testCases := []struct {
		matchers             []*labels.Matcher
		expectedMessageCount int
		expectedSeriesCount  int
	}{
		{ // Match all series by name.
			matchers:             []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test")},
			expectedMessageCount: totalMessageCount,
			expectedSeriesCount:  len(writeReq.Timeseries),
		},
		{ // Match all series by blanket regex, but sharded.
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, "1_of_4"),
				labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"),
			},
			expectedMessageCount: totalMessageCount / 4,
			expectedSeriesCount:  1012, // Note this number should change if the sample data changes.
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.matchers), func(t *testing.T) {
			// Get active series
			req, err := client.ToActiveSeriesRequest(tc.matchers)
			require.NoError(t, err)

			server := &mockActiveSeriesServer{ctx: ctx}
			err = ingesterClient.ActiveSeries(req, server)
			require.NoError(t, err)

			// Check that all series were returned.
			returnedSeriesCount := 0
			for _, res := range server.responses {
				returnedSeriesCount += len(res.Metric)
				// Check that all series have the expected number of labels.
				for _, m := range res.Metric {
					assert.Equal(t, 2, len(m.Labels))
				}
			}
			assert.Equal(t, tc.expectedSeriesCount, returnedSeriesCount)
			assert.Equal(t, tc.expectedMessageCount, len(server.responses))
		})
	}

	t.Run("canceled sharded match-all context", func(t *testing.T) {
		db := ingesterClient.getTSDB(userID)
		require.NotNil(t, db)
		idx, err := db.Head().Index()
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, idx.Close())
		})

		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()
		postings, err := getPostings(canceledCtx, db, idx, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, "1_of_4"),
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"),
		}, false)
		require.NoError(t, err)
		require.False(t, postings.Next())
		require.ErrorIs(t, postings.Err(), context.Canceled)
	})

	t.Run("sharded match-all compatibility fallback", func(t *testing.T) {
		db := ingesterClient.getTSDB(userID)
		require.NotNil(t, db)
		idx, err := db.Head().Index()
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, idx.Close())
		})

		postings, err := getPostings(ctx, db, indexReaderWithoutShardedAll{idx}, []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, sharding.ShardLabel, "1_of_4"),
			labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".*"),
		}, false)
		require.NoError(t, err)
		count := 0
		for postings.Next() {
			count++
		}
		require.NoError(t, postings.Err())
		require.Equal(t, 1012, count)
	})
}

type indexReaderWithoutShardedAll struct {
	tsdb.IndexReader
}

func TestIngester_ShardedPostingsBufferRecycler(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.ShardedPostingsBufferRecyclerMaxRetainedBytes = 16 * 1024 * 1024
	registry := prometheus.NewRegistry()
	in, ring, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, in, ring)
	require.NotNil(t, in.shardedPostingsBufferRecycler)

	metricNames := []string{
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_hits_total",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_misses_total",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_evictions_total",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_rejections_total",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_retained_buffers",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_retained_capacity_bytes",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_pending_retirement_buffers",
		"cortex_ingester_tsdb_sharded_postings_buffer_recycler_pending_retirement_capacity_bytes",
	}
	count, err := testutil.GatherAndCount(registry, metricNames...)
	require.NoError(t, err)
	require.Equal(t, len(metricNames), count)
}

func TestIngester_ActiveNativeHistogramSeries(t *testing.T) {
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	histograms := []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(1_000, util_test.GenerateTestHistogram(1))}

	seriesWithLabelsOfSize := func(size, index int, isHistogram bool) mimirpb.PreallocTimeseries {
		// 24 bytes of static strings and slice overhead, the remaining bytes are used to
		// pad the value of the "lbl" label.
		require.Greater(t, size, 24, "minimum message size is 24 bytes")
		tpl := fmt.Sprintf("%%0%dd", size-24)
		ts := &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, "test", "lbl", fmt.Sprintf(tpl, index))),
		}
		if isHistogram {
			ts.Histograms = histograms
		} else {
			ts.Samples = samples
		}
		return mimirpb.PreallocTimeseries{TimeSeries: ts}
	}

	expectedMessageCount := 4
	totalSeriesSize := expectedMessageCount * activeSeriesMaxSizeBytes

	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	currentSize := 0
	for i := 0; currentSize < totalSeriesSize; i++ {
		isHistogram := i%2 != 0 // Half of the series will be float and the other half will be native histograms.
		s := seriesWithLabelsOfSize(1024, i, isHistogram)
		writeReq.Timeseries = append(writeReq.Timeseries, s)
		if isHistogram {
			currentSize += s.Size()
		}
	}

	// Write the series.
	ingesterClient := prepareHealthyIngester(t, func(limits *validation.Limits) { limits.NativeHistogramsIngestionEnabled = true })
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err := ingesterClient.Push(ctx, writeReq)
	require.NoError(t, err)

	// Get active series
	req, err := client.ToActiveSeriesRequest([]*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "test"),
	})
	req.Type = client.NATIVE_HISTOGRAM_SERIES
	require.NoError(t, err)

	server := &mockActiveSeriesServer{ctx: ctx}
	err = ingesterClient.ActiveSeries(req, server)
	require.NoError(t, err)

	// Check that all series were returned.
	returnedSeriesCount := 0
	for _, res := range server.responses {
		returnedSeriesCount += len(res.Metric)
		// Check that all series have a corresponding bucket count.
		assert.Equal(t, len(res.Metric), len(res.BucketCount), "All series should have a bucket count.")
		for _, bc := range res.BucketCount {
			assert.Equal(t, uint64(8), bc)
		}
	}
	assert.Equal(t, len(writeReq.Timeseries)/2, returnedSeriesCount)

	// Check that we got the correct number of messages.
	assert.Equal(t, expectedMessageCount, len(server.responses))
}

func BenchmarkIngester_ActiveSeries(b *testing.B) {
	const (
		userID     = "test"
		numSeries  = 2e6
		metricName = "metric_name"
	)

	in := prepareActiveSeriesBenchmarkIngester(b)
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	for s := 0; s < numSeries; s++ {
		writeReq.Timeseries = append(writeReq.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					model.MetricNameLabel, metricName,
					// Use mod prime to make label values repeat every n series
					"mod_10", strconv.Itoa(s%(2*5)),
					"mod_4199", strconv.Itoa(s%(13*17*19)))),
				Samples: samples,
			},
		})
	}
	_, err := in.Push(ctx, writeReq)
	require.NoError(b, err)

	for _, bc := range []struct {
		name     string
		matchers []*client.LabelMatcher
	}{
		{
			name:     "few series",
			matchers: []*client.LabelMatcher{{Name: "mod_4199", Value: "0", Type: client.EQUAL}},
		},
		{
			name:     "~10% of series",
			matchers: []*client.LabelMatcher{{Name: "mod_10", Value: "0", Type: client.EQUAL}},
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				req := &client.ActiveSeriesRequest{Matchers: bc.matchers}
				server := &mockActiveSeriesServer{ctx: ctx}
				require.NoError(b, in.ActiveSeries(req, server))
			}
		})
	}

	db := in.getTSDB(userID)
	require.NotNil(b, db)
	benchmarkPopulateInactiveHeadSeries(b, ctx, in, db)

	// Match-all requests use ShardedAllPostings; selective requests use
	// ShardedPostings. Each method and creation mode gets disjoint buckets.
	for _, method := range benchmarkActiveSeriesMethods() {
		b.Run("method="+method.name, func(b *testing.B) {
			for _, state := range []struct {
				name         string
				bucketOffset uint64
				concurrent   bool
			}{
				{name: "clean"},
				{
					name:         "dirty-after-concurrent-series-creation",
					bucketOffset: benchmarkBucketsPerCase,
					concurrent:   true,
				},
			} {
				run := 0
				b.Run("state="+state.name, func(b *testing.B) {
					if state.concurrent && runtime.GOMAXPROCS(0) < 2 {
						b.Skip("concurrent series creation requires GOMAXPROCS >= 2")
					}
					if b.N > benchmarkMaxChurnIterations {
						b.Skip("run with -benchtime=10x or lower to bound Head growth")
					}
					if run >= benchmarkBucketsPerCase {
						b.Skipf("benchmark supports at most %d isolated repetitions", benchmarkBucketsPerCase)
					}

					bucket := method.bucketBase + state.bucketOffset + uint64(run)
					run++
					buckets := []uint64{bucket}
					requests := benchmarkActiveSeriesShardRequests(b, method.matchers, bucket)
					require.Zero(b, benchmarkShardBucketStatsFor(b, db.Head(), buckets).dirtyBuckets)
					baseline, err := benchmarkActiveSeriesQueryWave(b, ctx, in, requests)
					require.NoError(b, err)
					require.Positive(b, baseline)

					dirtyBuckets := 0
					targetRefs := 0
					b.ReportAllocs()
					b.ResetTimer()
					b.StopTimer()
					for iteration := range b.N {
						generation := fmt.Sprintf("%s-%s-%d-%d", method.name, state.name, bucket, iteration)
						writeRequests := benchmarkSingleBucketWriteRequests(b, bucket, generation)
						benchmarkPushRequests(b, ctx, in, writeRequests, state.concurrent)
						deactivateBenchmarkSeries(b, ctx, db, generation, benchmarkSingleBucketSeriesCount)

						stats := benchmarkShardBucketStatsFor(b, db.Head(), buckets)
						if state.concurrent && stats.dirtyBuckets != 1 {
							b.Skipf("concurrent creation left %d of 1 target buckets dirty with GOMAXPROCS=%d", stats.dirtyBuckets, runtime.GOMAXPROCS(0))
						}
						if !state.concurrent {
							require.Zero(b, stats.dirtyBuckets)
						}
						dirtyBuckets += stats.dirtyBuckets
						targetRefs += stats.refs

						b.StartTimer()
						got, err := benchmarkActiveSeriesQueryWave(b, ctx, in, requests)
						b.StopTimer()
						require.NoError(b, err)
						require.Equal(b, baseline, got)
						require.Zero(b, benchmarkShardBucketStatsFor(b, db.Head(), buckets).dirtyBuckets)
					}
					b.ReportMetric(float64(dirtyBuckets)/float64(b.N), "dirty-buckets/op")
					b.ReportMetric(float64(targetRefs)/float64(b.N), "target-refs/op")
				})
			}
		})
	}
}

func BenchmarkIngester_ActiveSeriesFullFanout(b *testing.B) {
	const userID = "test"

	for _, recycler := range []struct {
		name             string
		maxRetainedBytes uint64
	}{
		{name: "off"},
		{name: "on", maxRetainedBytes: benchmarkShardedPostingsRecyclerBytes},
	} {
		b.Run("recycler="+recycler.name, func(b *testing.B) {
			for _, method := range benchmarkActiveSeriesMethods() {
				b.Run("method="+method.name, func(b *testing.B) {
					for _, state := range []struct {
						name       string
						concurrent bool
						warm       bool
					}{
						{name: "clean"},
						{name: "dirty-cold", concurrent: true},
						{name: "dirty-warm", concurrent: true, warm: true},
					} {
						b.Run("state="+state.name, func(b *testing.B) {
							if b.N != 1 {
								b.Skip("run with -benchtime=1x so each sample measures one aggregate replacement")
							}
							if state.concurrent && runtime.GOMAXPROCS(0) < 2 {
								b.Skip("concurrent series creation requires GOMAXPROCS >= 2")
							}

							in, registry := prepareActiveSeriesBenchmarkIngesterWithRecycler(b, recycler.maxRetainedBytes)
							ctx := user.InjectOrgID(context.Background(), userID)
							benchmarkPopulateActiveHeadSeries(b, ctx, in)
							db := in.getTSDB(userID)
							require.NotNil(b, db)
							benchmarkPopulateInactiveHeadSeries(b, ctx, in, db)

							buckets := benchmarkAllShardBuckets()
							require.Zero(b, benchmarkShardBucketStatsFor(b, db.Head(), buckets).dirtyBuckets)
							requests := benchmarkActiveSeriesFullFanoutRequests(b, method.matchers)
							baseline, err := benchmarkActiveSeriesQueryWave(b, ctx, in, requests)
							require.NoError(b, err)
							require.Positive(b, baseline)

							prepare := func(generation string) benchmarkShardBucketStats {
								waves := 1
								if state.concurrent {
									waves = benchmarkFullFanoutChurnWaves
								}
								for wave := range waves {
									waveGeneration := fmt.Sprintf("%s-%d", generation, wave)
									writeRequestGroups := benchmarkFullFanoutWriteRequestGroups(b, waveGeneration)
									benchmarkPushRequestGroups(b, ctx, in, writeRequestGroups, state.concurrent)
									deactivateBenchmarkSeries(b, ctx, db, waveGeneration, benchmarkFullFanoutSeriesCount)
								}
								stats := benchmarkShardBucketStatsFor(b, db.Head(), buckets)
								if state.concurrent && stats.dirtyBuckets != len(buckets) {
									b.Fatalf("concurrent creation left %d of %d target buckets dirty with GOMAXPROCS=%d", stats.dirtyBuckets, len(buckets), runtime.GOMAXPROCS(0))
								}
								if !state.concurrent {
									require.Zero(b, stats.dirtyBuckets)
								}
								return stats
							}

							if state.warm {
								benchmarkPrimeActiveSeriesRecycler(b, in)
							}

							stats := prepare(fmt.Sprintf("full-fanout-%s-%s-measured", method.name, state.name))
							beforeHits := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_hits_total")
							beforeMisses := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_misses_total")
							beforeAllocations := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_head_shard_bucket_repair_buffer_allocations_total")
							retainedBuffers := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_retained_buffers")
							retainedBytes := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_retained_capacity_bytes")
							pendingBuffers := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_pending_retirement_buffers")
							pendingBytes := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_pending_retirement_capacity_bytes")

							b.ReportAllocs()
							b.ResetTimer()
							got, err := benchmarkActiveSeriesQueryWave(b, ctx, in, requests)
							b.StopTimer()
							require.NoError(b, err)
							require.Equal(b, baseline, got)
							require.Zero(b, benchmarkShardBucketStatsFor(b, db.Head(), buckets).dirtyBuckets)
							hits := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_hits_total") - beforeHits
							misses := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_sharded_postings_buffer_recycler_misses_total") - beforeMisses
							allocations := benchmarkMetricValue(b, registry, "cortex_ingester_tsdb_head_shard_bucket_repair_buffer_allocations_total") - beforeAllocations
							if state.warm && recycler.maxRetainedBytes > 0 {
								require.Equal(b, float64(len(buckets)), retainedBuffers)
								require.Equal(b, float64(stats.dirtyBuckets), hits)
								require.Zero(b, misses)
								require.Zero(b, allocations)
							}
							b.ReportMetric(float64(stats.dirtyBuckets), "dirty-buckets/op")
							b.ReportMetric(hits, "recycler-hits/op")
							b.ReportMetric(misses, "recycler-misses/op")
							b.ReportMetric(pendingBuffers, "recycler-pending-buffers")
							b.ReportMetric(pendingBytes, "recycler-pending-bytes")
							b.ReportMetric(retainedBuffers, "recycler-retained-buffers")
							b.ReportMetric(retainedBytes, "recycler-retained-bytes")
							b.ReportMetric(allocations, "repair-buffer-allocations/op")
							b.ReportMetric(float64(stats.refs), "target-refs/op")
						})
					}
				})
			}
		})
	}
}

const (
	benchmarkActiveSeriesShardCount       = 4 * tsdb.DefaultShardedPostingsBuckets
	benchmarkBucketsPerCase               = tsdb.DefaultShardedPostingsBuckets / 4
	benchmarkInactiveSeriesCount          = 1_000_000
	benchmarkInactiveSeriesBatch          = 10_000
	benchmarkSingleBucketSeriesCount      = 128
	benchmarkFullFanoutChurnWaves         = 4
	benchmarkFullFanoutRequestsPerBucket  = tsdb.DefaultShardedPostingsBuckets
	benchmarkFullFanoutSeriesPerRequest   = 4
	benchmarkFullFanoutSeriesCount        = benchmarkFullFanoutRequestsPerBucket * tsdb.DefaultShardedPostingsBuckets * benchmarkFullFanoutSeriesPerRequest
	benchmarkMaxChurnIterations           = 10
	benchmarkGenerationLabel              = "benchmark_generation"
	benchmarkCandidateLabel               = "benchmark_candidate"
	benchmarkChurnMetric                  = "benchmark_shard_bucket_churn"
	benchmarkHeadCompactionInterval       = 15 * time.Minute
	benchmarkActiveHeadSeriesCount        = 2 * 5 * 13 * 17 * 19
	benchmarkRecyclerDonorSeriesCount     = benchmarkInactiveSeriesCount + 15*benchmarkInactiveSeriesCount/100
	benchmarkShardedPostingsRecyclerBytes = 16 * 1024 * 1024
)

type benchmarkActiveSeriesMethod struct {
	name       string
	bucketBase uint64
	matchers   []*client.LabelMatcher
}

func benchmarkActiveSeriesMethods() []benchmarkActiveSeriesMethod {
	return []benchmarkActiveSeriesMethod{
		{
			name: "ShardedAllPostings",
			matchers: []*client.LabelMatcher{{
				Name: model.MetricNameLabel, Value: ".*", Type: client.REGEX_MATCH,
			}},
		},
		{
			name:       "ShardedPostings",
			bucketBase: 2 * benchmarkBucketsPerCase,
			matchers: []*client.LabelMatcher{{
				Name: "mod_10", Value: "0", Type: client.EQUAL,
			}},
		},
	}
}

func prepareActiveSeriesBenchmarkIngester(b testing.TB) *Ingester {
	b.Helper()

	in, _ := prepareActiveSeriesBenchmarkIngesterWithRecycler(b, 0)
	return in
}

func prepareActiveSeriesBenchmarkIngesterWithRecycler(b testing.TB, maxRetainedBytes uint64) (*Ingester, *prometheus.Registry) {
	b.Helper()

	cfg := defaultIngesterTestConfig(b)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = benchmarkHeadCompactionInterval
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalWhileStarting = benchmarkHeadCompactionInterval
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIntervalJitterEnabled = false
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 0
	cfg.BlocksStorageConfig.TSDB.ShardedPostingsBufferRecyclerMaxRetainedBytes = maxRetainedBytes
	limits := defaultLimitsTestConfig()
	limits.MaxGlobalSeriesPerMetric = 0
	limits.MaxGlobalSeriesPerUser = 0
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 0

	registry := prometheus.NewRegistry()
	in, ring, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", registry)
	require.NoError(b, err)
	startAndWaitHealthy(b, in, ring)
	return in, registry
}

func benchmarkPrimeActiveSeriesRecycler(b testing.TB, in *Ingester) {
	b.Helper()

	const donorUserID = "recycler-donor"
	ctx := user.InjectOrgID(context.Background(), donorUserID)
	benchmarkPopulateActiveHeadSeries(b, ctx, in)
	db := in.getTSDB(donorUserID)
	require.NotNil(b, db)
	benchmarkPopulateInactiveHeadSeriesCount(b, ctx, in, db, benchmarkRecyclerDonorSeriesCount)

	buckets := benchmarkAllShardBuckets()
	for wave := range benchmarkFullFanoutChurnWaves {
		generation := fmt.Sprintf("recycler-donor-%d", wave)
		benchmarkPushRequestGroups(b, ctx, in, benchmarkFullFanoutWriteRequestGroups(b, generation), true)
		deactivateBenchmarkSeries(b, ctx, db, generation, benchmarkFullFanoutSeriesCount)
	}
	stats := benchmarkShardBucketStatsFor(b, db.Head(), buckets)
	require.Equal(b, len(buckets), stats.dirtyBuckets)

	readers := make([]tsdb.IndexReader, len(buckets))
	defer func() {
		for _, reader := range readers {
			if reader != nil {
				require.NoError(b, reader.Close())
			}
		}
	}()
	for shardIndex := range buckets {
		reader, err := db.Head().Index()
		require.NoError(b, err)
		readers[shardIndex] = reader
		shardedReader, ok := reader.(tsdb.ShardedAllPostingsReader)
		require.True(b, ok)
		postings := shardedReader.ShardedAllPostings(ctx, uint64(shardIndex), uint64(len(buckets)))
		for postings.Next() {
		}
		require.NoError(b, postings.Err())
	}
	for i, reader := range readers {
		require.NoError(b, reader.Close())
		readers[i] = nil
	}
	require.Zero(b, benchmarkShardBucketStatsFor(b, db.Head(), buckets).dirtyBuckets)
	runtime.GC()
}

func benchmarkActiveSeriesShardRequests(b testing.TB, matchers []*client.LabelMatcher, bucket uint64) []*client.ActiveSeriesRequest {
	b.Helper()

	bucketCount := uint64(tsdb.DefaultShardedPostingsBuckets)
	requests := make([]*client.ActiveSeriesRequest, 0, benchmarkActiveSeriesShardCount/bucketCount)
	for shardIndex := bucket; shardIndex < benchmarkActiveSeriesShardCount; shardIndex += bucketCount {
		requestMatchers := append([]*client.LabelMatcher(nil), matchers...)
		requestMatchers = append(requestMatchers, &client.LabelMatcher{
			Name:  sharding.ShardLabel,
			Value: sharding.ShardSelector{ShardIndex: shardIndex, ShardCount: benchmarkActiveSeriesShardCount}.LabelValue(),
			Type:  client.EQUAL,
		})
		requests = append(requests, &client.ActiveSeriesRequest{Matchers: requestMatchers})
	}
	return requests
}

func benchmarkActiveSeriesFullFanoutRequests(b testing.TB, matchers []*client.LabelMatcher) []*client.ActiveSeriesRequest {
	b.Helper()

	requests := make([]*client.ActiveSeriesRequest, 0, tsdb.DefaultShardedPostingsBuckets)
	for shardIndex := range uint64(tsdb.DefaultShardedPostingsBuckets) {
		requestMatchers := append([]*client.LabelMatcher(nil), matchers...)
		requestMatchers = append(requestMatchers, &client.LabelMatcher{
			Name:  sharding.ShardLabel,
			Value: sharding.ShardSelector{ShardIndex: shardIndex, ShardCount: tsdb.DefaultShardedPostingsBuckets}.LabelValue(),
			Type:  client.EQUAL,
		})
		requests = append(requests, &client.ActiveSeriesRequest{Matchers: requestMatchers})
	}
	return requests
}

func benchmarkActiveSeriesQueryWave(b testing.TB, ctx context.Context, in *Ingester, requests []*client.ActiveSeriesRequest) (int, error) {
	b.Helper()

	servers := make([]*benchmarkActiveSeriesServer, len(requests))
	start := make(chan struct{})
	var group errgroup.Group
	for i, req := range requests {
		servers[i] = &benchmarkActiveSeriesServer{ctx: ctx}
		group.Go(func() error {
			<-start
			return in.ActiveSeries(req, servers[i])
		})
	}
	close(start)
	if err := group.Wait(); err != nil {
		return 0, err
	}

	total := 0
	for _, server := range servers {
		total += server.series
	}
	return total, nil
}

func benchmarkPopulateActiveHeadSeries(b testing.TB, ctx context.Context, in *Ingester) {
	b.Helper()

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	req := &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: make([]mimirpb.PreallocTimeseries, 0, benchmarkActiveHeadSeriesCount)}
	for series := range benchmarkActiveHeadSeriesCount {
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					model.MetricNameLabel, "metric_name",
					"mod_10", strconv.Itoa(series%(2*5)),
					"mod_4199", strconv.Itoa(series%(13*17*19)),
				)),
				Samples: samples,
			},
		})
	}
	_, err := in.Push(ctx, req)
	require.NoError(b, err)
}

// benchmarkPopulateInactiveHeadSeries models churn retained in Head without
// increasing the active-series response size.
func benchmarkPopulateInactiveHeadSeries(b testing.TB, ctx context.Context, in *Ingester, db *userTSDB) {
	b.Helper()
	benchmarkPopulateInactiveHeadSeriesCount(b, ctx, in, db, benchmarkInactiveSeriesCount)
}

func benchmarkPopulateInactiveHeadSeriesCount(b testing.TB, ctx context.Context, in *Ingester, db *userTSDB, seriesCount int) {
	b.Helper()

	before := db.Head().NumSeries()
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	for start := 0; start < seriesCount; start += benchmarkInactiveSeriesBatch {
		count := min(benchmarkInactiveSeriesBatch, seriesCount-start)
		generation := fmt.Sprintf("inactive-%d", start)
		req := &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: make([]mimirpb.PreallocTimeseries, 0, count)}
		for i := range count {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
						model.MetricNameLabel, benchmarkChurnMetric,
						benchmarkCandidateLabel, strconv.Itoa(start+i),
						benchmarkGenerationLabel, generation,
					)),
					Samples: samples,
				},
			})
		}
		_, err := in.Push(ctx, req)
		require.NoError(b, err)
		deactivateBenchmarkSeries(b, ctx, db, generation, count)
	}
	require.Equal(b, before+uint64(seriesCount), db.Head().NumSeries())
}

func benchmarkMetricValue(b testing.TB, gatherer prometheus.Gatherer, name string) float64 {
	b.Helper()

	families, err := gatherer.Gather()
	require.NoError(b, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		value := 0.0
		for _, metric := range family.Metric {
			if metric.Counter != nil {
				value += metric.GetCounter().GetValue()
			}
			if metric.Gauge != nil {
				value += metric.GetGauge().GetValue()
			}
		}
		return value
	}
	return 0
}

func benchmarkSingleBucketWriteRequests(b testing.TB, bucket uint64, generation string) []*mimirpb.WriteRequest {
	b.Helper()

	bucketMask := uint64(tsdb.DefaultShardedPostingsBuckets - 1)
	requests := make([]*mimirpb.WriteRequest, 0, benchmarkSingleBucketSeriesCount)
	for candidate := 0; len(requests) < benchmarkSingleBucketSeriesCount; candidate++ {
		seriesLabels := labels.FromStrings(
			model.MetricNameLabel, benchmarkChurnMetric,
			benchmarkCandidateLabel, strconv.Itoa(candidate),
			benchmarkGenerationLabel, generation,
		)
		if labels.StableHash(seriesLabels)&bucketMask != bucket {
			continue
		}
		requests = append(requests, writeRequestSingleSeries(seriesLabels, []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}))
	}
	return requests
}

// benchmarkFullFanoutWriteRequestGroups builds equal per-bucket setup waves.
func benchmarkFullFanoutWriteRequestGroups(b testing.TB, generation string) [][]*mimirpb.WriteRequest {
	b.Helper()

	bucketMask := uint64(tsdb.DefaultShardedPostingsBuckets - 1)
	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	requestGroups := make([][]*mimirpb.WriteRequest, tsdb.DefaultShardedPostingsBuckets)
	candidate := 0
	for targetBucket := range requestGroups {
		requests := make([]*mimirpb.WriteRequest, benchmarkFullFanoutRequestsPerBucket)
		for requestIndex := range requests {
			req := &mimirpb.WriteRequest{Source: mimirpb.API, Timeseries: make([]mimirpb.PreallocTimeseries, 0, benchmarkFullFanoutSeriesPerRequest)}
			for len(req.Timeseries) < benchmarkFullFanoutSeriesPerRequest {
				seriesLabels := labels.FromStrings(
					model.MetricNameLabel, benchmarkChurnMetric,
					benchmarkCandidateLabel, strconv.Itoa(candidate),
					benchmarkGenerationLabel, generation,
				)
				candidate++
				if labels.StableHash(seriesLabels)&bucketMask != uint64(targetBucket) {
					continue
				}
				req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
					TimeSeries: &mimirpb.TimeSeries{
						Labels:  mimirpb.FromLabelsToLabelAdapters(seriesLabels),
						Samples: samples,
					},
				})
			}
			requests[requestIndex] = req
		}
		requestGroups[targetBucket] = requests
	}
	return requestGroups
}

func benchmarkPushRequests(b testing.TB, ctx context.Context, in *Ingester, requests []*mimirpb.WriteRequest, concurrent bool) {
	b.Helper()

	if !concurrent {
		for _, req := range requests {
			_, err := in.Push(ctx, req)
			require.NoError(b, err)
		}
		return
	}

	start := make(chan struct{})
	var group errgroup.Group
	for _, req := range requests {
		group.Go(func() error {
			<-start
			_, err := in.Push(ctx, req)
			return err
		})
	}
	close(start)
	require.NoError(b, group.Wait())
}

func benchmarkPushRequestGroups(b testing.TB, ctx context.Context, in *Ingester, requestGroups [][]*mimirpb.WriteRequest, concurrent bool) {
	b.Helper()

	for _, requests := range requestGroups {
		benchmarkPushRequests(b, ctx, in, requests, concurrent)
	}
}

type benchmarkShardBucketState struct {
	refs  int
	dirty bool
}

type benchmarkShardBucketStats struct {
	refs         int
	dirtyBuckets int
}

// benchmarkShardBucketStates observes the pinned Prometheus implementation
// after benchmark readers and writers have stopped.
func benchmarkShardBucketStates(b testing.TB, head *tsdb.Head) []benchmarkShardBucketState {
	b.Helper()

	headValue := reflect.ValueOf(head)
	if headValue.Kind() != reflect.Pointer || headValue.IsNil() {
		b.Fatalf("expected non-nil *tsdb.Head, got %v", headValue.Kind())
	}
	shardBuckets := headValue.Elem().FieldByName("shardBuckets")
	if !shardBuckets.IsValid() || shardBuckets.Kind() != reflect.Pointer || shardBuckets.IsNil() {
		b.Fatal("Prometheus Head.shardBuckets is unavailable")
	}
	buckets := shardBuckets.Elem().FieldByName("buckets")
	if !buckets.IsValid() || buckets.Kind() != reflect.Slice {
		b.Fatal("Prometheus shardBucketPostings.buckets is unavailable")
	}

	states := make([]benchmarkShardBucketState, buckets.Len())
	for i := range buckets.Len() {
		bucket := buckets.Index(i)
		refs := bucket.FieldByName("refs")
		dirty := bucket.FieldByName("dirty")
		if !refs.IsValid() || refs.Kind() != reflect.Slice || !dirty.IsValid() || dirty.Kind() != reflect.Int {
			b.Fatalf("Prometheus shard bucket %d has an incompatible layout", i)
		}
		dirtyIndex := int(dirty.Int())
		if dirtyIndex < -1 || dirtyIndex > refs.Len() {
			b.Fatalf("Prometheus shard bucket %d has invalid dirty index %d for %d refs", i, dirtyIndex, refs.Len())
		}
		states[i] = benchmarkShardBucketState{refs: refs.Len(), dirty: dirtyIndex >= 0}
	}
	return states
}

func benchmarkShardBucketStatsFor(b testing.TB, head *tsdb.Head, buckets []uint64) benchmarkShardBucketStats {
	b.Helper()

	states := benchmarkShardBucketStates(b, head)
	stats := benchmarkShardBucketStats{}
	for _, bucket := range buckets {
		if bucket >= uint64(len(states)) {
			b.Fatalf("target shard bucket %d is outside %d buckets", bucket, len(states))
		}
		state := states[bucket]
		stats.refs += state.refs
		if state.dirty {
			stats.dirtyBuckets++
		}
	}
	return stats
}

func benchmarkAllShardBuckets() []uint64 {
	buckets := make([]uint64, tsdb.DefaultShardedPostingsBuckets)
	for i := range buckets {
		buckets[i] = uint64(i)
	}
	return buckets
}

func deactivateBenchmarkSeries(b testing.TB, ctx context.Context, db *userTSDB, generation string, expected int) {
	b.Helper()

	idx, err := db.Head().Index()
	require.NoError(b, err)
	defer func() {
		require.NoError(b, idx.Close())
	}()

	postings, err := idx.Postings(ctx, benchmarkGenerationLabel, generation)
	require.NoError(b, err)
	refs := make([]chunks.HeadSeriesRef, 0, expected)
	for postings.Next() {
		refs = append(refs, chunks.HeadSeriesRef(postings.At()))
	}
	require.NoError(b, postings.Err())
	require.Len(b, refs, expected)

	for _, ref := range refs {
		db.activeSeries.Delete(ref, idx)
	}
}

type benchmarkActiveSeriesServer struct {
	client.Ingester_ActiveSeriesServer
	ctx    context.Context
	series int
}

func (s *benchmarkActiveSeriesServer) Send(resp *client.ActiveSeriesResponse) error {
	s.series += len(resp.Metric)
	return nil
}

func (s *benchmarkActiveSeriesServer) Context() context.Context {
	return s.ctx
}

type mockActiveSeriesServer struct {
	client.Ingester_ActiveSeriesServer
	responses []*client.ActiveSeriesResponse
	ctx       context.Context
}

func (s *mockActiveSeriesServer) Send(resp *client.ActiveSeriesResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *mockActiveSeriesServer) Context() context.Context {
	return s.ctx
}

func TestMatchAllSeries(t *testing.T) {
	tests := []struct {
		name     string
		matchers []*labels.Matcher
		expected bool
	}{
		{
			name:     "empty matchers",
			matchers: []*labels.Matcher{},
			expected: false,
		},
		{
			name: "single matcher with regexp .*",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
			},
			expected: true,
		},
		{
			name: "single matcher on __name__ with regexp .+",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
			},
			expected: true,
		},
		{
			name: "single matcher on __name__ with not-equal to empty string",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchNotEqual, model.MetricNameLabel, ""),
			},
			expected: true,
		},
		{
			name: "single matcher with different regexp pattern",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar"),
			},
			expected: false,
		},
		{
			name: "multiple matchers with regexp .*",
			matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
				labels.MustNewMatcher(labels.MatchEqual, "bar", "baz"),
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchAllSeries(tc.matchers)
			assert.Equal(t, tc.expected, result)
		})
	}
}
