// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestSeriesRefHistory(t *testing.T) {
	var history seriesRefHistory

	_, found := history.firstRecordNotBefore(0)
	require.False(t, found)

	for timestampMs := int64(1); timestampMs <= 5; timestampMs++ {
		history.record(timestampMs*1000, storage.SeriesRef(timestampMs*10), 3)
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

	i, r, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
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

	response, err := i.LabelChurn(ctx, &client.LabelChurnRequest{SinceTimestampMs: now.Add(-30 * time.Minute).UnixMilli()})
	require.NoError(t, err)
	require.Equal(t, &client.LabelChurnResponse{
		SinceTimestampMs: recordTimestamp.UnixMilli(),
		Scores: []client.LabelChurnScore{
			{
				LabelName:   model.MetricNameLabel,
				Score:       1,
				MetricNames: []client.LabelChurnMetricName{{MetricName: "other", SampledSeries: labelChurnSampledSeriesPerValue}},
			},
			{
				LabelName: "pod",
				Score:     3,
				MetricNames: []client.LabelChurnMetricName{
					{MetricName: "up", SampledSeries: 2 * labelChurnSampledSeriesPerValue},
					{MetricName: "other", SampledSeries: labelChurnSampledSeriesPerValue},
				},
			},
		},
	}, response)

	t.Run("the time range starts after the last series removal", func(t *testing.T) {
		i.getTSDB(userID).lastSeriesRemovalMs.Store(now.Add(-4 * time.Minute).UnixMilli())

		// No record exists after the last series removal.
		response, err := i.LabelChurn(ctx, &client.LabelChurnRequest{SinceTimestampMs: now.Add(-30 * time.Minute).UnixMilli()})
		require.NoError(t, err)
		require.Equal(t, &client.LabelChurnResponse{}, response)

		lastRecordTimestamp := now.Add(-3 * time.Minute)
		i.recordLabelChurnSeriesRefs(lastRecordTimestamp)

		response, err = i.LabelChurn(ctx, &client.LabelChurnRequest{SinceTimestampMs: now.Add(-30 * time.Minute).UnixMilli()})
		require.NoError(t, err)
		require.Equal(t, &client.LabelChurnResponse{SinceTimestampMs: lastRecordTimestamp.UnixMilli()}, response)
	})

	t.Run("a tenant without data has no scores", func(t *testing.T) {
		response, err := i.LabelChurn(user.InjectOrgID(context.Background(), "other-tenant"), &client.LabelChurnRequest{})
		require.NoError(t, err)
		require.Equal(t, &client.LabelChurnResponse{}, response)
	})
}

func TestIngester_LabelChurnAfterCompaction(t *testing.T) {
	const userID = "test"
	ctx := user.InjectOrgID(context.Background(), userID)

	i, r, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), nil, nil)
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
	response, err := i.LabelChurn(ctx, &client.LabelChurnRequest{SinceTimestampMs: since})
	require.NoError(t, err)
	require.Equal(t, &client.LabelChurnResponse{}, response)

	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "b"))
	recordTimestamp := time.UnixMilli(db.lastSeriesRemovalMs.Load())
	i.recordLabelChurnSeriesRefs(recordTimestamp)
	push(labels.FromStrings(model.MetricNameLabel, "up", "pod", "c"))

	response, err = i.LabelChurn(ctx, &client.LabelChurnRequest{SinceTimestampMs: since})
	require.NoError(t, err)
	require.Equal(t, &client.LabelChurnResponse{
		SinceTimestampMs: recordTimestamp.UnixMilli(),
		Scores: []client.LabelChurnScore{{
			LabelName:   "pod",
			Score:       1,
			MetricNames: []client.LabelChurnMetricName{{MetricName: "up", SampledSeries: labelChurnSampledSeriesPerValue}},
		}},
	}, response)
}

func TestUserTSDB_TrackSeriesRemoval(t *testing.T) {
	var db userTSDB

	require.NoError(t, db.trackSeriesRemoval(func() error { return nil }))
	require.Zero(t, db.lastSeriesRemovalMs.Load())

	compactionErr := errors.New("compaction failed")
	beforeCompaction := time.Now()
	err := db.trackSeriesRemoval(func() error {
		// PostDeletion stores the time that the removal started.
		db.lastSeriesRemovalMs.Store(1)
		return compactionErr
	})
	require.ErrorIs(t, err, compactionErr)
	require.GreaterOrEqual(t, db.lastSeriesRemovalMs.Load(), beforeCompaction.UnixMilli())
}
