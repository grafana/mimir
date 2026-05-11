// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngester_filterUsersToCompactToReduceInMemorySeries(t *testing.T) {
	tests := map[string]struct {
		numMemorySeries              int64
		earlyCompactionMinSeries     int64
		earlyCompactionMinPercentage int
		estimations                  []seriesReductionEstimation
		expected                     []string
	}{
		"should return no tenant if compacting all tenants the ingester wouldn't reduce the number of in-memory at least by the configured percentage": {
			numMemorySeries:              100,
			earlyCompactionMinSeries:     80,
			earlyCompactionMinPercentage: 20,
			estimations: []seriesReductionEstimation{
				{userID: "1", estimatedCount: 1, estimatedPercentage: 9},
				{userID: "2", estimatedCount: 2, estimatedPercentage: 10},
				{userID: "3", estimatedCount: 3, estimatedPercentage: 11},
				{userID: "4", estimatedCount: 4, estimatedPercentage: 50},
			},
			expected: nil,
		},
		"should return tenants with estimated series reduction >= configured percentage": {
			numMemorySeries:              100,
			earlyCompactionMinSeries:     80,
			earlyCompactionMinPercentage: 20,
			estimations: []seriesReductionEstimation{
				{userID: "1", estimatedCount: 1, estimatedPercentage: 19},
				{userID: "2", estimatedCount: 2, estimatedPercentage: 20},
				{userID: "3", estimatedCount: 3, estimatedPercentage: 21},
				{userID: "4", estimatedCount: 50, estimatedPercentage: 50},
			},
			expected: []string{"2", "3", "4"},
		},
		"should return tenants with estimated series reduction < configured percentage if required to reach the minimum overall reduction": {
			numMemorySeries:              100,
			earlyCompactionMinSeries:     80,
			earlyCompactionMinPercentage: 20,
			estimations: []seriesReductionEstimation{
				{userID: "1", estimatedCount: 6, estimatedPercentage: 1},
				{userID: "2", estimatedCount: 7, estimatedPercentage: 2},
				{userID: "3", estimatedCount: 8, estimatedPercentage: 3},
				{userID: "4", estimatedCount: 15, estimatedPercentage: 4},
			},
			expected: []string{"3", "4"},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actual := filterUsersToCompactToReduceInMemorySeries(testData.numMemorySeries, testData.earlyCompactionMinSeries, testData.earlyCompactionMinPercentage, testData.estimations)
			assert.ElementsMatch(t, testData.expected, actual)
		})
	}
}

func TestIngester_compactBlocksToReduceInMemorySeries_ShouldTriggerCompactionOnlyIfEstimatedSeriesReductionIsGreaterThanConfiguredPercentage(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
		now         = time.Now()

		// Use a constant sample for the timestamp so that TSDB head is guaranteed to not span across 2h boundaries.
		// The sample timestamp is irrelevant towards active series tracking.
		sampleTimestamp = time.Now().UnixMilli()
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 50

	ingester, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push 10 series.
	for seriesID := 0; seriesID < 10; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: sampleTimestamp, Val: 0}},
		}}))
	}

	// TSDB head early compaction should not trigger because there are no inactive series yet.
	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)

	// Use a trick to track all series we've written so far as "inactive".
	ingester.getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// Pre-condition check.
	require.Equal(t, uint64(10), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _, _ := ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 0, totalActiveSeries)

	// Push 20 more series.
	for seriesID := 10; seriesID < 30; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: sampleTimestamp, Val: 0}},
		}}))
	}

	// The last 20 series are active so since only 33% of series are inactive we expect the early compaction to not trigger yet.
	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)

	require.Equal(t, uint64(30), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _, _ = ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 20, totalActiveSeries)

	// Advance time until the last series are inactive too. Now we expect the early compaction to trigger.
	now = now.Add(30 * time.Minute)

	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)

	require.Equal(t, uint64(0), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _, _ = ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 0, totalActiveSeries)
}

func TestIngester_compactBlocksToReduceInMemorySeries_ShouldCompactHeadUpUntilNowMinusActiveSeriesMetricsIdleTimeout(t *testing.T) {
	var (
		ctx          = context.Background()
		ctxWithUser  = user.InjectOrgID(ctx, userID)
		metricName   = "metric_1"
		metricLabels = labels.FromStrings(model.MetricNameLabel, metricName)
		metricModel  = map[model.LabelName]model.LabelValue{model.MetricNameLabel: model.LabelValue(metricName)}
		now          = time.Now()
		sampleTimes  []time.Time
	)

	ingesterCfg := defaultIngesterTestConfig(t)
	ingesterCfg.ActiveSeriesMetrics.Enabled = true
	ingesterCfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	ingesterCfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
	ingesterCfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
	ingesterCfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.CreationGracePeriod = model.Duration(24 * time.Hour) // This test writes samples in the future.

	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limitsCfg, nil, "", nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push a series and trigger early TSDB head compaction
	{
		// Push a series with a sample.
		sampleTime := now
		sampleTimes = append(sampleTimes, sampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  metricLabels,
			Samples: []util_test.Sample{{TS: sampleTime.UnixMilli(), Val: 1.0}},
		}}))

		// Advance time and then check if TSDB head early compaction is triggered.
		// We expect no block to be created because there's no sample before "now - active series idle timeout".
		now = now.Add(10 * time.Minute)
		ingester.compactBlocksToReduceInMemorySeries(ctx, now)
		require.Len(t, listBlocksInDir(t, userBlocksDir), 0)

		// Further advance time and then check if TSDB head early compaction is triggered.
		// The previously written sample is expected to be compacted.
		now = now.Add(11 * time.Minute)
		ingester.compactBlocksToReduceInMemorySeries(ctx, now)

		require.Len(t, listBlocksInDir(t, userBlocksDir), 1)
		newBlockID := listBlocksInDir(t, userBlocksDir)[0]

		assert.Equal(t, model.Matrix{
			{
				Metric: metricModel,
				Values: []model.SamplePair{{Timestamp: model.Time(sampleTime.UnixMilli()), Value: 1.0}},
			},
		}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, newBlockID.String()), metricName))

		// We expect the series to be dropped from TSDB Head because there was no sample more recent than
		// "now - active series idle timeout".
		db := ingester.getTSDB(userID)
		require.NotNil(t, db)
		assert.Equal(t, uint64(0), db.Head().NumSeries())
	}

	// Push again the same series and trigger TSDB head early compaction.
	{
		// Advance time and push another sample to the same series.
		sampleTime := now
		sampleTimes = append(sampleTimes, sampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  metricLabels,
			Samples: []util_test.Sample{{TS: sampleTime.UnixMilli(), Val: 2.0}},
		}}))

		// Advance time, trigger the TSDB head early compaction, and then check the compacted block.
		now = now.Add(20 * time.Minute)
		ingester.compactBlocksToReduceInMemorySeries(ctx, now)

		require.Len(t, listBlocksInDir(t, userBlocksDir), 2)
		newBlockID := listBlocksInDir(t, userBlocksDir)[1]

		assert.Equal(t, model.Matrix{
			{
				Metric: metricModel,
				Values: []model.SamplePair{{Timestamp: model.Time(sampleTime.UnixMilli()), Value: 2.0}},
			},
		}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, newBlockID.String()), metricName))

		// We expect the series to be dropped from TSDB Head because there was no sample more recent than
		// "now - active series idle timeout".
		db := ingester.getTSDB(userID)
		require.NotNil(t, db)
		assert.Equal(t, uint64(0), db.Head().NumSeries())
	}

	// Push again the same series and trigger the normal TSDB head compaction
	{
		// Push a sample with the timestamp BEFORE the next TSDB block range boundary.
		firstSampleTime := now
		sampleTimes = append(sampleTimes, firstSampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  metricLabels,
			Samples: []util_test.Sample{{TS: firstSampleTime.UnixMilli(), Val: 3.0}},
		}}))

		// Push a sample with the timestamp AFTER the next TSDB block range boundary.
		now = now.Add(4 * time.Hour)
		secondSampleTime := now
		sampleTimes = append(sampleTimes, secondSampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  metricLabels,
			Samples: []util_test.Sample{{TS: secondSampleTime.UnixMilli(), Val: 4.0}},
		}}))

		// Trigger a normal TSDB head compaction.
		ingester.compactBlocks(ctx, false, 0, nil)

		require.Len(t, listBlocksInDir(t, userBlocksDir), 3)
		newBlockID := listBlocksInDir(t, userBlocksDir)[2]

		assert.Equal(t, model.Matrix{
			{
				Metric: metricModel,
				Values: []model.SamplePair{{Timestamp: model.Time(firstSampleTime.UnixMilli()), Value: 3.0}},
			},
		}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, newBlockID.String()), metricName))

		// We expect the series to NOT be dropped from TSDB Head because there's a sample which is still in the Head.
		db := ingester.getTSDB(userID)
		require.NotNil(t, db)
		assert.Equal(t, uint64(1), db.Head().NumSeries())
	}

	// Querying ingester should return all samples
	{
		req := &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: metricName},
			},
		}

		s := stream{ctx: ctxWithUser}
		err = ingester.QueryStream(req, &s)
		require.NoError(t, err)

		res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		assert.ElementsMatch(t, model.Matrix{{
			Metric: metricModel,
			Values: []model.SamplePair{
				{Timestamp: model.Time(sampleTimes[0].UnixMilli()), Value: 1.0},
				{Timestamp: model.Time(sampleTimes[1].UnixMilli()), Value: 2.0},
				{Timestamp: model.Time(sampleTimes[2].UnixMilli()), Value: 3.0},
				{Timestamp: model.Time(sampleTimes[3].UnixMilli()), Value: 4.0},
			},
		}}, res)
	}
}

func TestIngester_compactBlocksToReduceInMemorySeries_ShouldCompactBlocksHonoringBlockRangePeriod(t *testing.T) {
	var (
		ctx          = context.Background()
		ctxWithUser  = user.InjectOrgID(ctx, userID)
		metricName   = "metric_1"
		metricLabels = labels.FromStrings(model.MetricNameLabel, metricName)
		metricModel  = map[model.LabelName]model.LabelValue{model.MetricNameLabel: model.LabelValue(metricName)}
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 0                         // Consider all series as inactive, so that the early compaction will always run.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

	ingester, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	// Push samples spanning across multiple block ranges.
	startTime, err := time.Parse(time.RFC3339, "2023-06-24T00:00:00Z")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  metricLabels,
			Samples: []util_test.Sample{{TS: startTime.Add(time.Duration(i) * time.Hour).UnixMilli(), Val: float64(i)}},
		}}))
	}

	// TSDB Head early compaction.
	ingester.compactBlocksToReduceInMemorySeries(ctx, time.Now())

	// Check compacted blocks.
	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	blockIDs := listBlocksInDir(t, userBlocksDir)
	require.Len(t, blockIDs, 3)

	assert.Equal(t, model.Matrix{
		{
			Metric: metricModel,
			Values: []model.SamplePair{
				{Timestamp: model.Time(startTime.UnixMilli()), Value: 0},
				{Timestamp: model.Time(startTime.Add(time.Hour).UnixMilli()), Value: 1},
			},
		},
	}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, blockIDs[0].String()), metricName))

	assert.Equal(t, model.Matrix{
		{
			Metric: metricModel,
			Values: []model.SamplePair{
				{Timestamp: model.Time(startTime.Add(2 * time.Hour).UnixMilli()), Value: 2},
				{Timestamp: model.Time(startTime.Add(3 * time.Hour).UnixMilli()), Value: 3},
			},
		},
	}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, blockIDs[1].String()), metricName))

	assert.Equal(t, model.Matrix{
		{
			Metric: metricModel,
			Values: []model.SamplePair{
				{Timestamp: model.Time(startTime.Add(4 * time.Hour).UnixMilli()), Value: 4},
			},
		},
	}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, blockIDs[2].String()), metricName))
}

func TestIngester_compactBlocksToReduceInMemorySeries_ShouldFailIngestingSamplesOlderThanActiveSeriesIdleTimeoutAfterEarlyCompaction(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

	ingester, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	// Push some samples.
	startTime, err := time.Parse(time.RFC3339, "2023-06-24T00:00:00Z")
	require.NoError(t, err)

	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_1"),
		Samples: []util_test.Sample{{TS: startTime.UnixMilli(), Val: 1.0}},
	}}))
	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_1"),
		Samples: []util_test.Sample{{TS: startTime.Add(20 * time.Minute).UnixMilli(), Val: 2.0}},
	}}))

	// TSDB Head early compaction.
	ingester.compactBlocksToReduceInMemorySeries(ctx, startTime.Add(30*time.Minute))

	// Check compacted blocks.
	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)
	assert.Equal(t, model.Matrix{
		{
			Metric: map[model.LabelName]model.LabelValue{model.MetricNameLabel: "metric_1"},
			Values: []model.SamplePair{
				{Timestamp: model.Time(startTime.UnixMilli()), Value: 1.0},
			},
		},
	}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, listBlocksInDir(t, userBlocksDir)[0].String()), "metric_1"))

	// Should allow to push samples after "now - active series idle timeout", but not before that.
	assert.ErrorContains(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_2"),
		Samples: []util_test.Sample{{TS: startTime.UnixMilli(), Val: 1.0}},
	}}), "the sample has been rejected because its timestamp is too old")
	assert.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_2"),
		Samples: []util_test.Sample{{TS: startTime.Add(20 * time.Minute).UnixMilli(), Val: 2.0}},
	}}))
	assert.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_1"),
		Samples: []util_test.Sample{{TS: startTime.Add(30 * time.Minute).UnixMilli(), Val: 3.0}},
	}}))
}

func TestIngester_compactBlocksToReduceInMemorySeries_Concurrency(t *testing.T) {
	util_test.VerifyNoLeak(t)

	const (
		numRuns             = 3
		numSeries           = 50
		numSamplesPerSeries = 120 * 10 // Ensure we create multiple chunks.
		numWriters          = 5
		numReaders          = 5
	)

	// Requests may be modified by the ingester so we need to create a new one for
	// every call to QueryStream()
	newReaderReq := func() *client.QueryRequest {
		return &client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "series_.*"},
			},
			StreamingChunksBatchSize: 64,
		}
	}

	for r := 0; r < numRuns; r++ {
		t.Run(fmt.Sprintf("Run %d", r), func(t *testing.T) {
			var (
				ctx         = context.Background()
				ctxWithUser = user.InjectOrgID(ctx, userID)
				startTime   = time.Now()

				startEarlyCompaction          = make(chan struct{})
				stopReadersAndEarlyCompaction = make(chan struct{})
			)

			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.Enabled = true
			cfg.ActiveSeriesMetrics.IdleTimeout = 0                         // Consider all series as inactive so that the early compaction is always triggered.
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
			cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
			cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

			ingester, ring, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
			require.NoError(t, err)
			startAndWaitHealthy(t, ingester, ring)

			// Keep track of the last timestamp written by each writer.
			writerTimesMx := sync.Mutex{}
			writerTimes := make([]int64, numWriters)

			// Start writers.
			writers := sync.WaitGroup{}
			writers.Add(numWriters)

			for i := 0; i < numWriters; i++ {
				go func(writerID int) {
					defer writers.Done()

					// Decide the series written by this writer.
					fromSeriesID := writerID * (numSeries / numWriters)
					toSeriesID := fromSeriesID + (numSeries / numWriters) - 1
					t.Logf("Write worker %d writing series with IDs between %d and %d (both inclusive)", writerID, fromSeriesID, toSeriesID)

					for sampleIdx := 0; sampleIdx < numSamplesPerSeries; sampleIdx++ {
						timestamp := startTime.Add(time.Duration(sampleIdx) * time.Millisecond).UnixMilli()

						// Prepare the series to write.
						seriesToWrite := make([]util_test.Series, 0, toSeriesID-fromSeriesID+1)
						for seriesIdx := fromSeriesID; seriesIdx <= toSeriesID; seriesIdx++ {
							seriesToWrite = append(seriesToWrite, util_test.Series{
								Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("series_%05d", seriesIdx)),
								Samples: []util_test.Sample{{TS: timestamp, Val: float64(sampleIdx)}},
							})
						}

						require.NoErrorf(t, pushSeriesToIngester(ctxWithUser, t, ingester, seriesToWrite), "worker: %d, sample idx: %d, sample timestamp: %d (%s)", writerID, sampleIdx, timestamp, time.UnixMilli(timestamp).String())

						// Keep track of the last timestamp written.
						writerTimesMx.Lock()
						writerTimes[writerID] = timestamp
						writerTimesMx.Unlock()

						// Start the early compaction once we've written some samples.
						if writerID == 0 && sampleIdx == 200 {
							close(startEarlyCompaction)
						}

						// Throttle.
						time.Sleep(time.Millisecond)
					}
				}(i)
			}

			// Start readers (each reader reads all series).
			readersAndEarlyCompaction := sync.WaitGroup{}
			readersAndEarlyCompaction.Add(numReaders)

			for i := 0; i < numReaders; i++ {
				go func() {
					defer readersAndEarlyCompaction.Done()

					for {
						select {
						case <-stopReadersAndEarlyCompaction:
							return
						case <-time.After(100 * time.Millisecond):
							s := stream{ctx: ctxWithUser}
							err := ingester.QueryStream(newReaderReq(), &s)
							require.NoError(t, err)

							res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
							require.NoError(t, err)

							// We expect a response consistent with the samples written.
							for entryIdx, entry := range res {
								for sampleIdx, sample := range entry.Values {
									expectedTime := model.Time(startTime.Add(time.Duration(sampleIdx) * time.Millisecond).UnixMilli())
									expectedValue := model.SampleValue(sampleIdx)

									require.Equalf(t, expectedTime, sample.Timestamp, "response entry: %d series: %s sample idx: %d", entryIdx, entry.Metric.String(), sampleIdx)
									require.Equalf(t, expectedValue, sample.Value, "response entry: %d series: %s sample idx: %d", entryIdx, entry.Metric.String(), sampleIdx)
								}
							}
						}
					}
				}()
			}

			// Start a goroutine continuously triggering the TSDB head early compaction.
			readersAndEarlyCompaction.Add(1)

			go func() {
				defer readersAndEarlyCompaction.Done()

				// Wait until the start has been signaled.
				select {
				case <-stopReadersAndEarlyCompaction:
					return
				case <-startEarlyCompaction:
				}

				for {
					select {
					case <-stopReadersAndEarlyCompaction:
						return
					case <-time.After(100 * time.Millisecond):
						lowestWriterTimeMilli := int64(math.MaxInt64)

						// Find the lowest sample written. We compact up until that timestamp.
						writerTimesMx.Lock()
						for _, ts := range writerTimes {
							lowestWriterTimeMilli = min(lowestWriterTimeMilli, ts)
						}
						writerTimesMx.Unlock()

						// Ensure all writers have written at least 1 batch of samples.
						if lowestWriterTimeMilli == 0 {
							continue
						}

						lowestWriterTime := time.UnixMilli(lowestWriterTimeMilli)
						t.Logf("Triggering early compaction with 'now' timestamp set to %d (%s)", lowestWriterTimeMilli, lowestWriterTime.String())
						ingester.compactBlocksToReduceInMemorySeries(ctx, lowestWriterTime)
					}
				}
			}()

			// Wait until all writers have done.
			writers.Wait()

			// We can now stop reader and early compaction.
			close(stopReadersAndEarlyCompaction)
			readersAndEarlyCompaction.Wait()

			// Ensure at least 2 early compactions have been done.
			blocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
			assert.Greater(t, len(listBlocksInDir(t, blocksDir)), 1)

			// Query again all series. We expect to read back all written series and samples.
			s := stream{ctx: ctxWithUser}
			err = ingester.QueryStream(newReaderReq(), &s)
			require.NoError(t, err)

			res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
			require.NoError(t, err)

			slices.SortFunc(res, func(a, b *model.SampleStream) int {
				if a.Metric.Before(b.Metric) {
					return -1
				}
				if a.Metric.Equal(b.Metric) {
					return 0
				}
				return 1
			})

			require.Len(t, res, numSeries)

			for entryIdx, entry := range res {
				expectedMetric := model.Metric{model.MetricNameLabel: model.LabelValue(fmt.Sprintf("series_%05d", entryIdx))}
				require.Equalf(t, expectedMetric, entry.Metric, "response entry: %d", entryIdx)
				require.Lenf(t, entry.Values, numSamplesPerSeries, "response entry: %d", entryIdx)

				for sampleIdx, sample := range entry.Values {
					expectedTime := model.Time(startTime.Add(time.Duration(sampleIdx) * time.Millisecond).UnixMilli())
					expectedValue := model.SampleValue(sampleIdx)

					require.Equalf(t, expectedTime, sample.Timestamp, "response entry: %d series: %s sample idx: %d", entryIdx, entry.Metric.String(), sampleIdx)
					require.Equalf(t, expectedValue, sample.Value, "response entry: %d series: %s sample idx: %d", entryIdx, entry.Metric.String(), sampleIdx)
				}
			}
		})
	}
}

func listBlocksInDir(t *testing.T, dir string) (ids []ulid.ULID) {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		blockID, err := ulid.Parse(entry.Name())
		if err != nil {
			continue
		}

		ids = append(ids, blockID)
	}

	// Ensure the block IDs are sorted.
	slices.SortFunc(ids, func(a, b ulid.ULID) int {
		return a.Compare(b)
	})

	return ids
}

func readMetricSamplesFromBlockDir(t *testing.T, blockDir string, metricName string) (results model.Matrix) {
	block, err := tsdb.OpenBlock(promslog.NewNopLogger(), blockDir, nil, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, block.Close())
	}()

	return readMetricSamplesFromBlock(t, block, metricName)
}

func readMetricSamplesFromBlock(t *testing.T, block *tsdb.Block, metricName string) (matrix model.Matrix) {
	indexReader, err := block.Index()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, indexReader.Close())
	}()

	chunksReader, err := block.Chunks()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, chunksReader.Close())
	}()

	ctx := context.Background()

	postings, err := indexReader.Postings(ctx, model.MetricNameLabel, metricName)
	require.NoError(t, err)

	for postings.Next() {
		builder := labels.NewScratchBuilder(0)
		var chks []chunks.Meta
		require.NoError(t, indexReader.Series(postings.At(), &builder, &chks))

		// Build the series labels.
		result := &model.SampleStream{Metric: map[model.LabelName]model.LabelValue{}}
		builder.Labels().Range(func(l labels.Label) {
			result.Metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
		})

		// Read samples from chunks.
		for idx, chk := range chks {
			chunk, iter, err := chunksReader.ChunkOrIterable(chk)
			require.NoError(t, err)
			require.Nil(t, iter)
			chks[idx].Chunk = chunk

			it := chks[idx].Chunk.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				switch typ {
				case chunkenc.ValFloat:
					ts, v := it.At()
					result.Values = append(result.Values, model.SamplePair{
						Timestamp: model.Time(ts),
						Value:     model.SampleValue(v),
					})
				}
			}
			require.NoError(t, it.Err())
		}

		matrix = append(matrix, result)
	}
	require.NoError(t, postings.Err())

	return matrix
}

func TestIngester_compactBlocksToReduceOwnedSeries_DisabledByDefault(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
		now         = time.Now()
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UseIngesterOwnedSeriesForLimits = true // Enable owned series for limits

	limitsCfg := defaultLimitsTestConfig()
	// EarlyHeadCompactionOwnedSeriesThreshold defaults to 0, which means disabled

	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limitsCfg, nil, "", nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push some series
	for seriesID := 0; seriesID < 10; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: now.UnixMilli(), Val: 0}},
		}}))
	}

	// Mark all series as inactive
	ingester.getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// Per-tenant early compaction should not trigger because threshold is 0 (disabled)
	ingester.compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(30*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)
}

func TestIngester_compactBlocksToReduceOwnedSeries_TriggersWhenThresholdExceeded(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
		now         = time.Now()
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UseIngesterOwnedSeriesForLimits = true // Enable owned series for limits

	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.EarlyHeadCompactionOwnedSeriesThreshold = 20 // Trigger when global per-tenant owned series >= 20
	limitsCfg.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 10

	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 2

	// We create 6 ingesters, 2 per zone in 3 zones.
	ingesters := setupTestIngesterRing(t, zones, ingestersPerZone, cfg, limitsCfg)
	userBlocksDir := filepath.Join(ingesters[0].cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Global threshold is 20. With 2 ingesters per zone the local per-ingester threshold is 10.
	// We will push 12 series, which is below the global threshold but above the local one.
	for seriesID := 0; seriesID < 12; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingesters[0], []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: now.UnixMilli(), Val: 0}},
		}}))
	}

	// Mark all series as inactive
	ingesters[0].getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// Pre-condition: owned series (12) is below the global threshold (20),
	// but at or above the local per-ingester threshold (20 / 2 = 10).
	ownedState := ingesters[0].getTSDB(userID).ownedSeriesState()
	require.Equal(t, 12, ownedState.ownedSeriesCount)
	localThreshold := ingesters[0].limiter.ringStrategy.convertGlobalToLocalLimit(userID, limitsCfg.EarlyHeadCompactionOwnedSeriesThreshold)
	require.Equal(t, 10, localThreshold)
	require.Less(t, ownedState.ownedSeriesCount, limitsCfg.EarlyHeadCompactionOwnedSeriesThreshold) // 12 < 20: below global
	require.GreaterOrEqual(t, ownedState.ownedSeriesCount, localThreshold)                          // 12 >= 10: at or above local

	// Per-tenant early compaction should trigger
	ingesters[0].compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(30*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)
}

func TestIngester_compactBlocksToReduceOwnedSeries_RespectsCooldown(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
		now         = time.Now()
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UseIngesterOwnedSeriesForLimits = true // Enable owned series for limits

	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.EarlyHeadCompactionOwnedSeriesThreshold = 20 // Trigger when global per-tenant owned series >= 20
	limitsCfg.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 10
	limitsCfg.CreationGracePeriod = model.Duration(24 * time.Hour) // This test writes samples in the future.

	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 2

	// We create 6 ingesters, 2 per zone in 3 zones.
	ingesters := setupTestIngesterRing(t, zones, ingestersPerZone, cfg, limitsCfg)
	userBlocksDir := filepath.Join(ingesters[0].cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push 12 series
	for seriesID := 0; seriesID < 12; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingesters[0], []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: now.UnixMilli(), Val: 0}},
		}}))
	}

	// Mark all series as inactive
	ingesters[0].getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// First compaction should trigger
	ingesters[0].compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(30*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)

	// Push more series
	for seriesID := 12; seriesID < 24; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingesters[0], []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: now.Add(40 * time.Minute).UnixMilli(), Val: 0}},
		}}))
	}

	// Mark new series as inactive
	ingesters[0].getTSDB(userID).activeSeries.Purge(now.Add(70*time.Minute), nil)

	// Try compaction again immediately (within cooldown period)
	// Should not create a new block because cooldown hasn't passed
	ingesters[0].compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(40*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1) // Still just 1 block

	// Advance time past the cooldown period (IdleTimeout = 20 min)
	// Now compaction should trigger again
	ingesters[0].compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(60*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 2) // Now 2 blocks
}

func TestIngester_compactBlocksToReduceOwnedSeries_RequiresOwnedSeriesForLimits(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
		now         = time.Now()
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 20 * time.Minute
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UseIngesterOwnedSeriesForLimits = false // Owned series for limits is disabled

	limitsCfg := defaultLimitsTestConfig()
	limitsCfg.EarlyHeadCompactionOwnedSeriesThreshold = 5 // Threshold is set

	zones := []string{"zone-a", "zone-b", "zone-c"}
	ingestersPerZone := 2

	// We create 6 ingesters, 2 per zone in 3 zones.
	ingesters := setupTestIngesterRing(t, zones, ingestersPerZone, cfg, limitsCfg)
	userBlocksDir := filepath.Join(ingesters[0].cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push 12 series
	for seriesID := 0; seriesID < 12; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingesters[0], []util_test.Series{{
			Labels:  labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", seriesID)),
			Samples: []util_test.Sample{{TS: now.UnixMilli(), Val: 0}},
		}}))
	}

	// Mark all series as inactive
	ingesters[0].getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// Per-tenant early compaction should NOT trigger because UseIngesterOwnedSeriesForLimits is false
	ingesters[0].compactBlocksToReducePerTenantOwnedSeries(ctx, now.Add(30*time.Minute))
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)
}

func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldNotCompactWhenDisabled(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	// EarlyCompactionNonOwnedSeriesEnabled defaults to false.

	ingester, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)

	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_1"),
		Samples: []util_test.Sample{{TS: sampleTime.UnixMilli(), Val: 1.0}},
	}}))

	// Simulate non-owned series detection by queueing a ref directly.
	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	db.addPendingNonOwnedRefs([]storage.SeriesRef{1})

	// Should not compact because the feature is disabled.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	require.Empty(t, listBlocksInDir(t, userBlocksDir))
}

func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldNotCompactWhenNoPendingCompaction(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = 0 // run eviction immediately for tests

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1

	ingester, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)

	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
		Labels:  labels.FromStrings(model.MetricNameLabel, "metric_1"),
		Samples: []util_test.Sample{{TS: sampleTime.UnixMilli(), Val: 1.0}},
	}}))

	// No pending refs queued: compaction should not run.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	require.Empty(t, listBlocksInDir(t, userBlocksDir))
}

func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldFlushDataToBlock(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = 0 // run eviction immediately for tests

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1

	// 3 zones x 1 ingester each makes localThreshold == globalThreshold, so threshold=1
	// gates on at least 1 owned series.
	ingesters := setupTestIngesterRing(t, []string{"zone-a", "zone-b", "zone-c"}, 1, cfg, limits)
	ingester := ingesters[0]

	// Push two samples at different timestamps so the head's MinTime < MaxTime. Push two
	// series so that one can remain owned to satisfy the per-tenant gate, while the other
	// is marked non-owned and exercises the eviction path.
	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()
	t2 := t1 + 1

	labelsA := labels.FromStrings(model.MetricNameLabel, "metric_a")
	labelsB := labels.FromStrings(model.MetricNameLabel, "metric_b")

	hashA := mimirpb.ShardByAllLabels(userID, labelsA)
	hashB := mimirpb.ShardByAllLabels(userID, labelsB)
	require.NotEqual(t, hashA, hashB)
	var ownedLabels, nonOwnedLabels labels.Labels
	var minHash uint32
	if hashA < hashB {
		ownedLabels, nonOwnedLabels, minHash = labelsA, labelsB, hashA
	} else {
		ownedLabels, nonOwnedLabels, minHash = labelsB, labelsA, hashB
	}
	nonOwnedName := nonOwnedLabels.Get(model.MetricNameLabel)
	nonOwnedMetricModel := model.Metric{model.MetricNameLabel: model.LabelValue(nonOwnedName)}

	for _, lbls := range []labels.Labels{ownedLabels, nonOwnedLabels} {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t1, Val: 1.0}},
		}}))
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t2, Val: 2.0}},
		}}))
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(2), db.Head().NumSeries())

	// Configure ownership: the lower-hash series is owned, the other is non-owned and gets
	// queued for eviction.
	db.ownedTokenRanges = ring.TokenRanges{0, minHash}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	require.Equal(t, 1, db.ownedSeriesState().ownedSeriesCount, "exactly one series should be owned")

	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	// A block should have been created containing the non-owned series.
	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	blockIDs := listBlocksInDir(t, userBlocksDir)
	require.Len(t, blockIDs, 1)

	// The block contains the non-owned series's two samples, unmodified (no synthetic samples
	// are appended on this path).
	assert.Equal(t, model.Matrix{{
		Metric: nonOwnedMetricModel,
		Values: []model.SamplePair{
			{Timestamp: model.Time(t1), Value: 1.0},
			{Timestamp: model.Time(t2), Value: 2.0},
		},
	}}, readMetricSamplesFromBlockDir(t, filepath.Join(userBlocksDir, blockIDs[0].String()), nonOwnedName))

	// The non-owned series should have been evicted; the owned series should remain.
	assert.Equal(t, uint64(1), db.Head().NumSeries())

	// The pending list should have been consumed, so a second call creates no new block.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)

	// lastEarlyCompaction is intentionally NOT updated by this path (see comment in
	// compactBlocksDueToNonOwnedSeries): non-owned eviction must not gate the unrelated
	// per-tenant early compaction.
	assert.True(t, db.getLastEarlyCompaction().IsZero())

	// Data for the non-owned series should still be queryable from the ingester (served from
	// the local block).
	s := stream{ctx: ctxWithUser}
	require.NoError(t, ingester.QueryStream(&client.QueryRequest{
		StartTimestampMs: math.MinInt64,
		EndTimestampMs:   math.MaxInt64,
		Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: nonOwnedName}},
	}, &s))

	res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
	require.NoError(t, err)
	assert.Equal(t, model.Matrix{{
		Metric: nonOwnedMetricModel,
		Values: []model.SamplePair{
			{Timestamp: model.Time(t1), Value: 1.0},
			{Timestamp: model.Time(t2), Value: 2.0},
		},
	}}, res)
}

// TestIngester_compactBlocksDueToNonOwnedSeries_ShouldFlushOnlyNonOwnedSeries verifies that
// when the head holds a mix of owned and non-owned series, compactBlocksDueToNonOwnedSeries
// flushes only the non-owned ones into a block and evicts them, leaving the owned series in
// the head untouched. Both series remain queryable through the ingester (owned from the head,
// non-owned from the local block).
func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldFlushOnlyNonOwnedSeries(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = 0 // run eviction immediately for tests

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1

	// 3 zones x 1 ingester each makes the per-tenant local threshold equal
	// to the global threshold, so threshold=1 gives localThreshold=1 and the gate passes when
	// one series is owned. We interact with the first ingester for the rest of the test.
	ingesters := setupTestIngesterRing(t, []string{"zone-a", "zone-b", "zone-c"}, 1, cfg, limits)
	ingester := ingesters[0]

	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()
	t2 := t1 + 1

	labelsA := labels.FromStrings(model.MetricNameLabel, "metric_a")
	labelsB := labels.FromStrings(model.MetricNameLabel, "metric_b")

	// Designate the series with the lower secondary hash as owned. The ingester's secondary
	// hash function is mimirpb.ShardByAllLabels(userID, ls).
	hashA := mimirpb.ShardByAllLabels(userID, labelsA)
	hashB := mimirpb.ShardByAllLabels(userID, labelsB)
	require.NotEqual(t, hashA, hashB)
	var ownedLabels, nonOwnedLabels labels.Labels
	var minHash uint32
	if hashA < hashB {
		ownedLabels, nonOwnedLabels, minHash = labelsA, labelsB, hashA
	} else {
		ownedLabels, nonOwnedLabels, minHash = labelsB, labelsA, hashB
	}
	ownedName := ownedLabels.Get(model.MetricNameLabel)
	nonOwnedName := nonOwnedLabels.Get(model.MetricNameLabel)

	// Push two samples per series at different timestamps so the head's MinTime < MaxTime
	// (required for both the chunk-range loop and the eviction-step early-return guard to
	// do work). pushSeriesToIngester only pushes the first sample of each series, so we use
	// two calls per series.
	for _, lbls := range []labels.Labels{ownedLabels, nonOwnedLabels} {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t1, Val: 1.0}},
		}}))
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t2, Val: 2.0}},
		}}))
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(2), db.Head().NumSeries())

	// Configure ownership and queue the non-owned ref.
	db.ownedTokenRanges = ring.TokenRanges{0, minHash}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	require.Equal(t, 1, db.ownedSeriesState().ownedSeriesCount, "exactly one series should be owned")

	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	// The non-owned series should have been evicted; the owned series should remain.
	assert.Equal(t, uint64(1), db.Head().NumSeries())

	// A single block should have been created, containing only the non-owned series and both
	// of its samples. The owned series and its samples must not appear in the block.
	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	blockIDs := listBlocksInDir(t, userBlocksDir)
	require.Len(t, blockIDs, 1)

	blockDir := filepath.Join(userBlocksDir, blockIDs[0].String())
	assert.Equal(t, model.Matrix{{
		Metric: model.Metric{model.MetricNameLabel: model.LabelValue(nonOwnedName)},
		Values: []model.SamplePair{
			{Timestamp: model.Time(t1), Value: 1.0},
			{Timestamp: model.Time(t2), Value: 2.0},
		},
	}}, readMetricSamplesFromBlockDir(t, blockDir, nonOwnedName))
	assert.Empty(t, readMetricSamplesFromBlockDir(t, blockDir, ownedName))

	// Both series remain queryable from the ingester: the owned one from the head, the
	// non-owned one from the local block produced by the targeted compaction.
	queryStream := func(metricName string) model.Matrix {
		s := stream{ctx: ctxWithUser}
		require.NoError(t, ingester.QueryStream(&client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: metricName}},
		}, &s))
		res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		return res
	}
	expected := func(metricName string) model.Matrix {
		return model.Matrix{{
			Metric: model.Metric{model.MetricNameLabel: model.LabelValue(metricName)},
			Values: []model.SamplePair{
				{Timestamp: model.Time(t1), Value: 1.0},
				{Timestamp: model.Time(t2), Value: 2.0},
			},
		}}
	}

	assert.Equal(t, expected(ownedName), queryStream(ownedName))
	assert.Equal(t, expected(nonOwnedName), queryStream(nonOwnedName))
}

// TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleOOOSamples verifies the two-step
// flow when both series carry out-of-order data. CompactOOOHead (step 1) flushes the OOO data
// of both series into an OOO block and clears their s.ooo state; CompactSelectedSeries (step 2)
// then evicts the non-owned series from the head's index — the OOO filter inside the primitive
// no longer skips it, because step 1 made s.ooo nil. The owned series stays in the head with
// its in-order chunks intact and its OOO chunks now persisted on disk.
func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleOOOSamples(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = 0 // run eviction immediately for tests

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1
	// Enable OOO ingestion at the tenant level so the ingester accepts samples whose
	// timestamps are below the head's MaxTime.
	limits.OutOfOrderTimeWindow = model.Duration(time.Hour)

	// 3 zones x 1 ingester makes localThreshold==globalThreshold so threshold=1 gates on 1
	// owned series.
	ingesters := setupTestIngesterRing(t, []string{"zone-a", "zone-b", "zone-c"}, 1, cfg, limits)
	ingester := ingesters[0]

	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()
	t2 := t1 + 1
	tOOO := t1 - 100 // out-of-order, well within the configured OOO window

	labelsA := labels.FromStrings(model.MetricNameLabel, "metric_a")
	labelsB := labels.FromStrings(model.MetricNameLabel, "metric_b")

	hashA := mimirpb.ShardByAllLabels(userID, labelsA)
	hashB := mimirpb.ShardByAllLabels(userID, labelsB)
	require.NotEqual(t, hashA, hashB)
	var ownedLabels, nonOwnedLabels labels.Labels
	var minHash uint32
	if hashA < hashB {
		ownedLabels, nonOwnedLabels, minHash = labelsA, labelsB, hashA
	} else {
		ownedLabels, nonOwnedLabels, minHash = labelsB, labelsA, hashB
	}
	ownedName := ownedLabels.Get(model.MetricNameLabel)
	nonOwnedName := nonOwnedLabels.Get(model.MetricNameLabel)

	// Push for each series: two in-order samples at t1 and t2, then an OOO sample at tOOO so
	// the series enters the OOO ingestion path (s.ooo becomes non-nil).
	pushOne := func(lbls labels.Labels, ts int64, val float64) {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: ts, Val: val}},
		}}))
	}
	for _, lbls := range []labels.Labels{ownedLabels, nonOwnedLabels} {
		pushOne(lbls, t1, 1.0)
		pushOne(lbls, t2, 2.0)
		pushOne(lbls, tOOO, 0.5)
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(2), db.Head().NumSeries())

	// Configure ownership and queue the non-owned ref.
	db.ownedTokenRanges = ring.TokenRanges{0, minHash}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	require.Equal(t, 1, db.ownedSeriesState().ownedSeriesCount, "exactly one series should be owned")

	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	// The non-owned series should have been evicted; the owned series should remain.
	assert.Equal(t, uint64(1), db.Head().NumSeries())

	// Two blocks should be on disk: one OOO block (from step 1, containing the OOO data of
	// both series) and one selected-series block (from step 2, containing the in-order data of
	// the non-owned series only).
	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	blockIDs := listBlocksInDir(t, userBlocksDir)
	require.Len(t, blockIDs, 2)

	var oooBlockDir, selectedBlockDir string
	for _, blockID := range blockIDs {
		blockDir := filepath.Join(userBlocksDir, blockID.String())
		block, err := tsdb.OpenBlock(promslog.NewNopLogger(), blockDir, nil, nil)
		require.NoError(t, err)
		bm := block.Meta()
		require.NoError(t, block.Close())
		switch {
		case bm.Compaction.FromOutOfOrder():
			oooBlockDir = blockDir
		case bm.Compaction.FromSelectedSeries():
			selectedBlockDir = blockDir
		default:
			t.Fatalf("unexpected block hints: %+v", bm.Compaction.Hints)
		}
	}
	require.NotEmpty(t, oooBlockDir, "expected one block tagged FromOutOfOrder")
	require.NotEmpty(t, selectedBlockDir, "expected one block tagged FromSelectedSeries")

	// The OOO block contains the OOO sample of both series.
	oooSample := []model.SamplePair{{Timestamp: model.Time(tOOO), Value: 0.5}}
	assert.Equal(t, model.Matrix{{
		Metric: model.Metric{model.MetricNameLabel: model.LabelValue(ownedName)},
		Values: oooSample,
	}}, readMetricSamplesFromBlockDir(t, oooBlockDir, ownedName))
	assert.Equal(t, model.Matrix{{
		Metric: model.Metric{model.MetricNameLabel: model.LabelValue(nonOwnedName)},
		Values: oooSample,
	}}, readMetricSamplesFromBlockDir(t, oooBlockDir, nonOwnedName))

	// The selected-series block contains only the non-owned series's in-order samples.
	assert.Empty(t, readMetricSamplesFromBlockDir(t, selectedBlockDir, ownedName))
	assert.Equal(t, model.Matrix{{
		Metric: model.Metric{model.MetricNameLabel: model.LabelValue(nonOwnedName)},
		Values: []model.SamplePair{
			{Timestamp: model.Time(t1), Value: 1.0},
			{Timestamp: model.Time(t2), Value: 2.0},
		},
	}}, readMetricSamplesFromBlockDir(t, selectedBlockDir, nonOwnedName))

	// Both series remain queryable end-to-end. The result merges head data (owned series's
	// in-order samples), the local OOO block, and the local selected-series block.
	queryStream := func(metricName string) model.Matrix {
		s := stream{ctx: ctxWithUser}
		require.NoError(t, ingester.QueryStream(&client.QueryRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			Matchers:         []*client.LabelMatcher{{Type: client.EQUAL, Name: model.MetricNameLabel, Value: metricName}},
		}, &s))
		res, err := client.StreamsToMatrixForTests(model.Earliest, model.Latest, s.responses)
		require.NoError(t, err)
		return res
	}
	expected := func(metricName string) model.Matrix {
		return model.Matrix{{
			Metric: model.Metric{model.MetricNameLabel: model.LabelValue(metricName)},
			Values: []model.SamplePair{
				{Timestamp: model.Time(tOOO), Value: 0.5},
				{Timestamp: model.Time(t1), Value: 1.0},
				{Timestamp: model.Time(t2), Value: 2.0},
			},
		}}
	}

	assert.Equal(t, expected(ownedName), queryStream(ownedName))
	assert.Equal(t, expected(nonOwnedName), queryStream(nonOwnedName))
}

// TestIngester_compactBlocksDueToNonOwnedSeries_ShouldRespectGracePeriod verifies that the
// configured grace period gates the eviction: while the period has not elapsed since the last
// pending-refs update, compactBlocksDueToNonOwnedSeries does not compact or evict anything;
// once the per-tenant last-update timestamp is older than the threshold, eviction proceeds.
func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldRespectGracePeriod(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	// A long grace period that no real-time elapse can cross during the test.
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = time.Hour

	limits := defaultLimitsTestConfig()
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 1

	// 3 zones x 1 ingester each makes localThreshold == globalThreshold so threshold=1 gates
	// on at least 1 owned series.
	ingesters := setupTestIngesterRing(t, []string{"zone-a", "zone-b", "zone-c"}, 1, cfg, limits)
	ingester := ingesters[0]

	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()
	t2 := t1 + 1

	// Push two series so one can remain owned (satisfying the per-tenant gate) while the
	// other is non-owned and exercises the grace-period gating.
	labelsA := labels.FromStrings(model.MetricNameLabel, "metric_a")
	labelsB := labels.FromStrings(model.MetricNameLabel, "metric_b")

	hashA := mimirpb.ShardByAllLabels(userID, labelsA)
	hashB := mimirpb.ShardByAllLabels(userID, labelsB)
	require.NotEqual(t, hashA, hashB)
	var ownedLabels, nonOwnedLabels labels.Labels
	var minHash uint32
	if hashA < hashB {
		ownedLabels, nonOwnedLabels, minHash = labelsA, labelsB, hashA
	} else {
		ownedLabels, nonOwnedLabels, minHash = labelsB, labelsA, hashB
	}

	for _, lbls := range []labels.Labels{ownedLabels, nonOwnedLabels} {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t1, Val: 1.0}},
		}}))
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t2, Val: 2.0}},
		}}))
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(2), db.Head().NumSeries())

	// Configure ownership and queue the non-owned ref. This stamps pendingNonOwnedRefsLastUpdate
	// to time.Now().
	db.ownedTokenRanges = ring.TokenRanges{0, minHash}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	require.Equal(t, 1, db.ownedSeriesState().ownedSeriesCount, "exactly one series should be owned")

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// First call: the grace period is fully in effect, so eviction must be skipped.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)
	require.Empty(t, listBlocksInDir(t, userBlocksDir), "no block should be produced while the grace period is in effect")
	require.Equal(t, uint64(2), db.Head().NumSeries(), "both series should still be in head while the grace period is in effect")

	// Backdate the last-update timestamp so the grace period appears to have elapsed.
	db.pendingNonOwnedRefsMtx.Lock()
	db.pendingNonOwnedRefsLastUpdate = time.Now().Add(-2 * time.Hour)
	db.pendingNonOwnedRefsMtx.Unlock()

	// Second call: eviction should now proceed for the non-owned series.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1, "block should be produced after the grace period elapses")
	require.Equal(t, uint64(1), db.Head().NumSeries(), "non-owned series should be evicted from the head after the grace period elapses")
}

// TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleScaleUp simulates the HPA scale-up
// scenario: an ingester accumulates series under one ring topology, more ingesters then join
// the ring (an HPA scale-up plus a limits-operator shard-size bump), and the existing replica
// is left holding series that are now non-owned. The test verifies that
// compactBlocksDueToNonOwnedSeries correctly evicts those series under the smaller post-scale-up
// per-ingester local threshold, even when the per-ingester owned-series count has dropped along
// with it.
//
// The actual ring transition is simulated by setting up the post-scale-up topology (4 ingesters
// per zone, 12 total) directly and manipulating ingester[0]'s token ranges to mimic the "I now
// own only a fraction of what I used to" state. This is deterministic and avoids the timing /
// token-distribution sensitivity of running the ownership-recomputation service on a live ring
// transition.
func TestIngester_compactBlocksDueToNonOwnedSeries_ShouldHandleScaleUp(t *testing.T) {
	var (
		ctx         = context.Background()
		ctxWithUser = user.InjectOrgID(ctx, userID)
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour
	cfg.UpdateIngesterOwnedSeries = true
	cfg.EarlyCompactionNonOwnedSeriesEnabled = true
	cfg.EarlyCompactionNonOwnedSeriesGracePeriod = 0

	limits := defaultLimitsTestConfig()
	// Threshold = 4. With the post-scale-up topology (4 ingesters/zone, 3 zones, RF=3), the
	// locally adjusted threshold is (4 * 3) / (3 * 4) = 1. With the pre-scale-up topology
	// (2 ingesters/zone) it would have been 2. The new gate sees Head().NumSeries() = numSeries
	// (well above either threshold), so eviction proceeds regardless of how the per-ingester
	// owned-series count has shifted.
	limits.EarlyHeadCompactionOwnedSeriesThreshold = 4

	zones := []string{"zone-a", "zone-b", "zone-c"}

	// Post-scale-up ring: 4 ingesters per zone (12 total). This sets up the smaller local
	// threshold the gate will operate against.
	ingesters := setupTestIngesterRing(t, zones, 4, cfg, limits)
	ingester := ingesters[0]

	// Push numSeries series, each with two samples at different timestamps so the head's
	// MinTime < MaxTime.
	sampleTime, err := time.Parse(time.RFC3339, "2026-05-05T00:00:00Z")
	require.NoError(t, err)
	t1 := sampleTime.UnixMilli()
	t2 := t1 + 1

	const numSeries = 20
	for i := 0; i < numSeries; i++ {
		lbls := labels.FromStrings(model.MetricNameLabel, fmt.Sprintf("metric_%d", i))
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t1, Val: 1.0}},
		}}))
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []util_test.Series{{
			Labels:  lbls,
			Samples: []util_test.Sample{{TS: t2, Val: 2.0}},
		}}))
	}

	db := ingester.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(numSeries), db.Head().NumSeries(), "all series should be in the head")

	// Simulate the post-scale-up ownership state by handing ingester[0] a small slice of the
	// keyspace (the rebalance gave most of what it used to own to the new ingesters). With
	// ownedTokenRanges = {0, 1} ingester[0] owns only series whose secondary hash is in [0, 1],
	// which for our randomly-distributed labels is essentially none of them.
	db.ownedTokenRanges = ring.TokenRanges{0, 1}
	require.True(t, db.recomputeOwnedSeries(0, "test", log.NewNopLogger()), "recomputeOwnedSeries should succeed")
	postScaleOwned := db.ownedSeriesState().ownedSeriesCount
	require.Less(t, postScaleOwned, numSeries, "most series should be non-owned after the simulated scale-up")

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)
	require.Empty(t, listBlocksInDir(t, userBlocksDir), "no blocks before eviction runs")

	// Run the eviction. The new gate uses Head().NumSeries() (= numSeries, well above the
	// post-scale-up localThreshold of 1), so it fires despite the owned-series count having
	// dropped, and the queued non-owned refs get evicted.
	ingester.compactBlocksDueToNonOwnedSeries(ctx)

	// A block should have been produced; only the (very few) owned series remain in the head.
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1, "block should be produced after scale-up")
	require.Equal(t, uint64(postScaleOwned), db.Head().NumSeries(), "only owned series should remain in the head")
}

func addIngestersToRing(t *testing.T, consulClient *consul.Client, zones []string, ingestersPerZone, startIdx int, cfg Config, limits validation.Limits) []*Ingester {
	ingesters := make([]*Ingester, 0, len(zones)*ingestersPerZone)
	var ring ring.ReadRing
	for _, zone := range zones {
		for i := startIdx; i < startIdx+ingestersPerZone; i++ {
			var (
				ingester *Ingester
				err      error
			)
			ingesterId := fmt.Sprintf("ingester-%s-%d", zone, i)
			cfg.IngesterRing.KVStore.Mock = consulClient
			cfg.IngesterRing.InstanceID = ingesterId
			cfg.IngesterRing.InstanceAddr = ingesterId
			cfg.IngesterRing.ZoneAwarenessEnabled = true
			cfg.IngesterRing.InstanceZone = zone

			ingester, ring, err = prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, ring, "", nil)
			require.NoError(t, err)
			startAndWaitHealthy(t, ingester, ring)

			ingesters = append(ingesters, ingester)
		}
	}
	return ingesters
}

func setupTestIngesterRing(t *testing.T, zones []string, ingestersPerZone int, cfg Config, limitsCfg validation.Limits) []*Ingester {
	// Create a shared consul KV store so all ingesters join the same ring.
	client, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })
	return addIngestersToRing(t, client, zones, ingestersPerZone, 0, cfg, limitsCfg)
}
