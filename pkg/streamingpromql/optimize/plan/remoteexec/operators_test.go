// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestFinalize_NoPerStepStats(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())
	annos := annotations.New()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	queryStats, err := types.NewQueryStats(timeRange, false, memoryConsumptionTracker)
	require.NoError(t, err)

	annos.Add(annotations.NewBadBucketLabelWarning("the_metric", "x", posrange.PositionRange{Start: 1, End: 2}))
	annos.Add(annotations.NewPossibleNonCounterInfo("not_a_counter", posrange.PositionRange{Start: 3, End: 4}))
	queryStats.TotalSamples = 100

	resp := &mockResponse{
		stats: stats.Stats{SamplesProcessed: 456, FetchedChunkBytes: 9000},
	}

	err = finalize(ctx, resp, annos, queryStats)
	require.NoError(t, err)
	require.Equal(t, int64(100+456), queryStats.TotalSamples)
	require.Zero(t, querierStats.SamplesProcessed, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Empty(t, querierStats.SamplesProcessedPerStep, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Equal(t, uint64(9000), querierStats.FetchedChunkBytes)

	warnings, infos := annos.AsStrings("", 0, 0)
	require.ElementsMatch(t, warnings, []string{
		`PromQL warning: bucket label "le" is missing or has a malformed value of "x" for metric name "the_metric"`,
		`PromQL warning: encountered a mix of histograms and floats for metric name "mixed_metric"`,
	})

	require.ElementsMatch(t, infos, []string{
		`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "not_a_counter"`,
		`PromQL info: ignored histograms in a range containing both floats and histograms for metric name "another_mixed_metric"`,
	})
}

func TestFinalize_PerStepStats(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())
	annos := annotations.New()
	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(1000), timestamp.Time(3000), time.Second)
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	queryStats, err := types.NewQueryStats(timeRange, true, memoryConsumptionTracker)
	require.NoError(t, err)

	queryStats.IncrementSamplesAtTimestamp(1000, 1020)
	queryStats.IncrementSamplesAtTimestamp(2000, 1030)
	queryStats.IncrementSamplesAtTimestamp(3000, 1040)

	resp := &mockResponse{
		stats: stats.Stats{
			SamplesProcessed: 123,
			SamplesProcessedPerStep: []stats.StepStat{
				{Timestamp: 1000, Value: 200},
				{Timestamp: 2000, Value: 400},
				{Timestamp: 3000, Value: 600},
			},
			FetchedChunkBytes: 9000,
		},
	}

	err = finalize(ctx, resp, annos, queryStats)
	require.NoError(t, err)
	require.Equal(t, []int64{1220, 1430, 1640}, queryStats.TotalSamplesPerStep)
	require.Equal(t, int64(1220+1430+1640), queryStats.TotalSamples)
	require.Zero(t, querierStats.SamplesProcessed, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Empty(t, querierStats.SamplesProcessedPerStep, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Equal(t, uint64(9000), querierStats.FetchedChunkBytes)
}

type mockResponse struct {
	stats stats.Stats
}

func (m *mockResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	annos := annotations.New()
	annos.Add(annotations.NewMixedFloatsHistogramsWarning("mixed_metric", posrange.PositionRange{Start: 5, End: 6}))
	annos.Add(annotations.NewHistogramIgnoredInMixedRangeInfo("another_mixed_metric", posrange.PositionRange{Start: 5, End: 6}))

	return annos, m.stats, nil
}

func (m *mockResponse) Close() {
	panic("should not be called")
}
