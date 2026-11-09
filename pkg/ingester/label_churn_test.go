// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
)

func TestSeriesRefHistory(t *testing.T) {
	history := newSeriesRefHistory(3)

	_, found := history.firstRecordNotBefore(0)
	require.False(t, found)

	for timestampMs := int64(1); timestampMs <= 5; timestampMs++ {
		history.record(timestampMs*1000, storage.SeriesRef(timestampMs*10))
	}

	// The buffer holds 3 records, thus the records at 1s and 2s are gone.
	record, found := history.firstRecordNotBefore(0)
	require.True(t, found)
	require.Equal(t, seriesRefRecord{timestampMs: 3000, seriesRef: 30}, record)

	record, found = history.firstRecordNotBefore(3500)
	require.True(t, found)
	require.Equal(t, seriesRefRecord{timestampMs: 4000, seriesRef: 40}, record)

	record, found = history.firstRecordNotBefore(5000)
	require.True(t, found)
	require.Equal(t, seriesRefRecord{timestampMs: 5000, seriesRef: 50}, record)

	_, found = history.firstRecordNotBefore(5001)
	require.False(t, found)
}

func TestIngester_LabelChurn(t *testing.T) {
	const userID = "test"
	ctx := user.InjectOrgID(context.Background(), userID)

	cfg := defaultIngesterTestConfig(t)
	cfg.LabelChurnEnabled = true
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	push := func(series ...labels.Labels) {
		for _, lbls := range series {
			_, err := i.Push(ctx, mockWriteRequest(t, lbls, 1, time.Now().UnixMilli()))
			require.NoError(t, err)
		}
	}

	push(
		labels.FromStrings(model.MetricNameLabel, "up", "pod", "a", "job", "x"),
		labels.FromStrings(model.MetricNameLabel, "up", "pod", "b", "job", "x"),
	)

	now := time.Now()
	recordTimestamp := now.Add(-10 * time.Minute)
	i.recordLabelChurnSeriesRefs(recordTimestamp)

	push(
		labels.FromStrings(model.MetricNameLabel, "up", "pod", "c", "job", "x"),
		labels.FromStrings(model.MetricNameLabel, "up", "pod", "d", "job", "x"),
		labels.FromStrings(model.MetricNameLabel, "other", "pod", "e", "job", "x"),
	)

	podScore := labelChurnScore{
		LabelName: "pod",
		Score:     3,
		MetricNames: []labelChurnMetricName{
			{MetricName: "up", SampledSeries: 2 * labelChurnSampledSeriesPerValue},
			{MetricName: "other", SampledSeries: labelChurnSampledSeriesPerValue},
		},
	}
	metricNameScore := labelChurnScore{
		LabelName:   model.MetricNameLabel,
		Score:       1,
		MetricNames: []labelChurnMetricName{{MetricName: "other", SampledSeries: labelChurnSampledSeriesPerValue}},
	}
	since := now.Add(-30 * time.Minute).UnixMilli()
	db := i.getTSDB(userID)

	router := mux.NewRouter()
	router.HandleFunc("/ingester/label_churn/{tenant}", i.LabelChurnHandler)
	serveLabelChurn := func(path string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	response, err := db.labelChurn(ctx, since, 0)
	require.NoError(t, err)
	require.Equal(t, &labelChurnResponse{
		Since:  time.UnixMilli(recordTimestamp.UnixMilli()).UTC(),
		Scores: []labelChurnScore{podScore, metricNameScore},
	}, response)

	t.Run("the ingester returns only the limit highest scores", func(t *testing.T) {
		response, err := db.labelChurn(ctx, since, 1)
		require.NoError(t, err)
		require.Equal(t, []labelChurnScore{podScore}, response.Scores)
	})

	t.Run("the handler returns the scores as JSON", func(t *testing.T) {
		recorder := serveLabelChurn("/ingester/label_churn/test?lookback=30m&limit=1")
		require.Equal(t, http.StatusOK, recorder.Code)

		var handlerResponse labelChurnResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &handlerResponse))
		require.Equal(t, []labelChurnScore{podScore}, handlerResponse.Scores)
	})

	t.Run("the handler rejects invalid requests", func(t *testing.T) {
		for path, expectedStatus := range map[string]int{
			"/ingester/label_churn/test?lookback=abc": http.StatusBadRequest,
			"/ingester/label_churn/test?limit=-1":     http.StatusBadRequest,
			"/ingester/label_churn/other-tenant":      http.StatusNotFound,
			"/ingester/label_churn/test?lookback=1s":  http.StatusServiceUnavailable,
		} {
			require.Equal(t, expectedStatus, serveLabelChurn(path).Code, path)
		}
	})

	t.Run("the time range starts after the last series removal", func(t *testing.T) {
		db.lastSeriesRemovalMs.Store(now.Add(-4 * time.Minute).UnixMilli())

		// No record exists after the last series removal.
		_, err := db.labelChurn(ctx, since, 0)
		require.ErrorIs(t, err, errLabelChurnCompactedTooRecently)

		// The requested time range starts after the last series removal and after the last record.
		_, err = db.labelChurn(ctx, now.Add(-time.Minute).UnixMilli(), 0)
		require.ErrorIs(t, err, errLabelChurnNoRecord)

		lastRecordTimestamp := now.Add(-3 * time.Minute)
		i.recordLabelChurnSeriesRefs(lastRecordTimestamp)

		response, err := db.labelChurn(ctx, since, 0)
		require.NoError(t, err)
		require.Equal(t, &labelChurnResponse{Since: time.UnixMilli(lastRecordTimestamp.UnixMilli()).UTC(), Scores: []labelChurnScore{}}, response)
	})

	t.Run("the handler rejects requests when label churn detection is disabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		(&Ingester{}).LabelChurnHandler(recorder, httptest.NewRequest(http.MethodGet, "/ingester/label_churn/test", nil))
		require.Equal(t, http.StatusNotFound, recorder.Code)
		require.Contains(t, recorder.Body.String(), errLabelChurnDisabled.Error())
	})
}

func TestIngester_LabelChurnAfterCompaction(t *testing.T) {
	const userID = "test"
	ctx := user.InjectOrgID(context.Background(), userID)

	cfg := defaultIngesterTestConfig(t)
	cfg.LabelChurnEnabled = true
	i, r, err := prepareIngesterWithBlocksStorage(t, cfg, nil, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	sampleTimestamp := time.Now()
	push := func(lbls labels.Labels) {
		sampleTimestamp = sampleTimestamp.Add(time.Millisecond)
		_, err := i.Push(ctx, mockWriteRequest(t, lbls, 1, sampleTimestamp.UnixMilli()))
		require.NoError(t, err)
	}

	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "a"))
	i.recordLabelChurnSeriesRefs(time.Now().Add(-10 * time.Minute))

	i.compactBlocks(ctx, true, math.MaxInt64, nil)
	db := i.getTSDB(userID)
	require.Zero(t, db.Head().NumSeries())

	// The records before the compaction are not used.
	since := time.Now().Add(-30 * time.Minute).UnixMilli()
	_, err = db.labelChurn(ctx, since, 0)
	require.ErrorIs(t, err, errLabelChurnCompactedTooRecently)

	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "b"))
	recordTimestamp := time.UnixMilli(db.lastSeriesRemovalMs.Load())
	i.recordLabelChurnSeriesRefs(recordTimestamp)
	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "c"))

	response, err := db.labelChurn(ctx, since, 0)
	require.NoError(t, err)
	require.Equal(t, &labelChurnResponse{
		Since: recordTimestamp.UTC(),
		Scores: []labelChurnScore{{
			LabelName:   "pod",
			Score:       1,
			MetricNames: []labelChurnMetricName{{MetricName: "up", SampledSeries: labelChurnSampledSeriesPerValue}},
		}},
	}, response)

	// No record is valid while a compaction is removing series.
	db.pendingSeriesRemovals.Inc()
	_, err = db.labelChurn(ctx, since, 0)
	require.ErrorIs(t, err, errLabelChurnCompactedTooRecently)
}

func TestIngester_LabelChurnAfterRestart(t *testing.T) {
	const userID = "test"
	ctx := user.InjectOrgID(context.Background(), userID)

	cfg := defaultIngesterTestConfig(t)
	cfg.LabelChurnEnabled = true
	limits := defaultLimitsTestConfig()
	dataDir := t.TempDir()

	i, r, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dataDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	push := func(lbls labels.Labels, sampleTimestamp time.Time) {
		_, err := i.Push(ctx, mockWriteRequest(t, lbls, 1, sampleTimestamp.UnixMilli()))
		require.NoError(t, err)
	}

	// The head spans more than 1.5 block ranges. Thus the compaction after the WAL replay
	// writes the old sample to a block and removes its series from the head.
	now := time.Now()
	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "a"), now.Add(-4*time.Hour))
	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "b"), now)
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	i, r, err = prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, dataDir, nil)
	require.NoError(t, err)
	startAndWaitHealthy(t, i, r)

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Zero(t, db.pendingSeriesRemovals.Load())
	require.Zero(t, db.lastSeriesRemovalMs.Load())

	since := now.Add(-30 * time.Minute).UnixMilli()
	_, err = db.labelChurn(ctx, since, 0)
	require.ErrorIs(t, err, errLabelChurnNoRecord)

	recordTimestamp := time.Now()
	i.recordLabelChurnSeriesRefs(recordTimestamp)
	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "c"), time.Now())

	response, err := db.labelChurn(ctx, since, 0)
	require.NoError(t, err)
	require.Equal(t, &labelChurnResponse{
		Since: time.UnixMilli(recordTimestamp.UnixMilli()).UTC(),
		Scores: []labelChurnScore{{
			LabelName:   "pod",
			Score:       1,
			MetricNames: []labelChurnMetricName{{MetricName: "up", SampledSeries: labelChurnSampledSeriesPerValue}},
		}},
	}, response)
}

func TestUserTSDB_UpdateLastSeriesRemoval(t *testing.T) {
	var db userTSDB

	db.updateLastSeriesRemoval()
	require.Zero(t, db.lastSeriesRemovalMs.Load())

	db.pendingSeriesRemovals.Inc()
	beforeUpdate := time.Now()
	db.updateLastSeriesRemoval()
	require.GreaterOrEqual(t, db.lastSeriesRemovalMs.Load(), beforeUpdate.UnixMilli())
	require.Zero(t, db.pendingSeriesRemovals.Load())
}
