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

	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	util_test "github.com/grafana/mimir/pkg/util/test"
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

	ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push 10 series.
	for seriesID := 0; seriesID < 10; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{
			{labels.FromStrings(labels.MetricName, fmt.Sprintf("metric_%d", seriesID)), 0, sampleTimestamp},
		}))
	}

	// TSDB head early compaction should not trigger because there are no inactive series yet.
	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)

	// Use a trick to track all series we've written so far as "inactive".
	ingester.getTSDB(userID).activeSeries.Purge(now.Add(30*time.Minute), nil)

	// Pre-condition check.
	require.Equal(t, uint64(10), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _ := ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 0, totalActiveSeries)

	// Push 20 more series.
	for seriesID := 10; seriesID < 30; seriesID++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{
			{labels.FromStrings(labels.MetricName, fmt.Sprintf("metric_%d", seriesID)), 0, sampleTimestamp},
		}))
	}

	// The last 20 series are active so since only 33% of series are inactive we expect the early compaction to not trigger yet.
	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 0)

	require.Equal(t, uint64(30), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _ = ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 20, totalActiveSeries)

	// Advance time until the last series are inactive too. Now we expect the early compaction to trigger.
	now = now.Add(30 * time.Minute)

	ingester.compactBlocksToReduceInMemorySeries(ctx, now)
	require.Len(t, listBlocksInDir(t, userBlocksDir), 1)

	require.Equal(t, uint64(0), ingester.getTSDB(userID).Head().NumSeries())
	totalActiveSeries, _, _ = ingester.getTSDB(userID).activeSeries.Active()
	require.Equal(t, 0, totalActiveSeries)
}

func TestIngester_compactBlocksToReduceInMemorySeries_ShouldCompactHeadUpUntilNowMinusActiveSeriesMetricsIdleTimeout(t *testing.T) {
	var (
		ctx          = context.Background()
		ctxWithUser  = user.InjectOrgID(ctx, userID)
		metricName   = "metric_1"
		metricLabels = labels.FromStrings(labels.MetricName, metricName)
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

	ingester, err := prepareIngesterWithBlocksStorageAndLimits(t, ingesterCfg, limitsCfg, nil, "", nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	userBlocksDir := filepath.Join(ingester.cfg.BlocksStorageConfig.TSDB.Dir, userID)

	// Push a series and trigger early TSDB head compaction
	{
		// Push a series with a sample.
		sampleTime := now
		sampleTimes = append(sampleTimes, sampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{metricLabels, 1.0, sampleTime.UnixMilli()}}))

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
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{metricLabels, 2.0, sampleTime.UnixMilli()}}))

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
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{metricLabels, 3.0, firstSampleTime.UnixMilli()}}))

		// Push a sample with the timestamp AFTER the next TSDB block range boundary.
		now = now.Add(4 * time.Hour)
		secondSampleTime := now
		sampleTimes = append(sampleTimes, secondSampleTime)
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{metricLabels, 4.0, secondSampleTime.UnixMilli()}}))

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

		res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
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
		metricLabels = labels.FromStrings(labels.MetricName, metricName)
		metricModel  = map[model.LabelName]model.LabelValue{model.MetricNameLabel: model.LabelValue(metricName)}
	)

	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true
	cfg.ActiveSeriesMetrics.IdleTimeout = 0                         // Consider all series as inactive, so that the early compaction will always run.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
	cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

	ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	// Push samples spanning across multiple block ranges.
	startTime, err := time.Parse(time.RFC3339, "2023-06-24T00:00:00Z")
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{metricLabels, float64(i), startTime.Add(time.Duration(i) * time.Hour).UnixMilli()}}))
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

	ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	// Push some samples.
	startTime, err := time.Parse(time.RFC3339, "2023-06-24T00:00:00Z")
	require.NoError(t, err)

	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{labels.FromStrings(labels.MetricName, "metric_1"), 1.0, startTime.UnixMilli()}}))
	require.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{labels.FromStrings(labels.MetricName, "metric_1"), 2.0, startTime.Add(20 * time.Minute).UnixMilli()}}))

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
	assert.ErrorContains(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{labels.FromStrings(labels.MetricName, "metric_2"), 1.0, startTime.UnixMilli()}}), "the sample has been rejected because its timestamp is too old")
	assert.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{labels.FromStrings(labels.MetricName, "metric_2"), 2.0, startTime.Add(20 * time.Minute).UnixMilli()}}))
	assert.NoError(t, pushSeriesToIngester(ctxWithUser, t, ingester, []series{{labels.FromStrings(labels.MetricName, "metric_1"), 3.0, startTime.Add(30 * time.Minute).UnixMilli()}}))
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

	for r := 0; r < numRuns; r++ {
		t.Run(fmt.Sprintf("Run %d", r), func(t *testing.T) {
			var (
				ctx         = context.Background()
				ctxWithUser = user.InjectOrgID(ctx, userID)
				startTime   = time.Now()
				readerReq   = &client.QueryRequest{
					StartTimestampMs: math.MinInt64,
					EndTimestampMs:   math.MaxInt64,
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "series_.*"},
					},
				}

				startEarlyCompaction          = make(chan struct{})
				stopReadersAndEarlyCompaction = make(chan struct{})
			)

			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.Enabled = true
			cfg.ActiveSeriesMetrics.IdleTimeout = 0                         // Consider all series as inactive so that the early compaction is always triggered.
			cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = time.Hour // Do not trigger it during the test, so that we trigger it manually.
			cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinInMemorySeries = 1
			cfg.BlocksStorageConfig.TSDB.EarlyHeadCompactionMinEstimatedSeriesReductionPercentage = 0

			ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, ingester))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, ingester))
			})

			// Wait until it's ACTIVE.
			test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
				return ingester.lifecycler.GetState()
			})

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
						seriesToWrite := make([]series, 0, toSeriesID-fromSeriesID+1)
						for seriesIdx := fromSeriesID; seriesIdx <= toSeriesID; seriesIdx++ {
							seriesToWrite = append(seriesToWrite, series{
								lbls:      labels.FromStrings(labels.MetricName, fmt.Sprintf("series_%05d", seriesIdx)),
								value:     float64(sampleIdx),
								timestamp: timestamp,
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
							err := ingester.QueryStream(readerReq, &s)
							require.NoError(t, err)

							res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
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
			err = ingester.QueryStream(readerReq, &s)
			require.NoError(t, err)

			res, err := client.StreamsToMatrix(model.Earliest, model.Latest, s.responses)
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

	postings, err := indexReader.Postings(ctx, labels.MetricName, metricName)
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
