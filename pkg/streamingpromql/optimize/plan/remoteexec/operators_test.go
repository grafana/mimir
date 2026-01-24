// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestFinalize(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())
	annos := annotations.New()
	queryStats := types.NewQueryStats()

	annos.Add(annotations.NewBadBucketLabelWarning("the_metric", "x", posrange.PositionRange{Start: 1, End: 2}))
	annos.Add(annotations.NewPossibleNonCounterInfo("not_a_counter", posrange.PositionRange{Start: 3, End: 4}))
	queryStats.TotalSamples = 100

	resp := &mockResponse{
		stats: stats.Stats{SamplesProcessed: 456, FetchedChunkBytes: 9000},
		annos: annotations.New(),
	}

	resp.annos.Add(annotations.NewMixedFloatsHistogramsWarning("mixed_metric", posrange.PositionRange{Start: 5, End: 6}))
	resp.annos.Add(annotations.NewHistogramIgnoredInMixedRangeInfo("another_mixed_metric", posrange.PositionRange{Start: 5, End: 6}))

	err := finalize(ctx, resp, annos, queryStats)
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

func TestFinalize_EmptyAnnotationsAndStats(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())
	annos := annotations.New()
	queryStats := types.NewQueryStats()

	annos.Add(annotations.NewBadBucketLabelWarning("the_metric", "x", posrange.PositionRange{Start: 1, End: 2}))
	annos.Add(annotations.NewPossibleNonCounterInfo("not_a_counter", posrange.PositionRange{Start: 3, End: 4}))
	queryStats.TotalSamples = 100

	resp := &mockResponse{
		stats: stats.Stats{},
		annos: nil,
	}

	err := finalize(ctx, resp, annos, queryStats)
	require.NoError(t, err)
	require.Equal(t, int64(100), queryStats.TotalSamples)
	require.Zero(t, querierStats.SamplesProcessed, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Empty(t, querierStats.SamplesProcessedPerStep, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")

	warnings, infos := annos.AsStrings("", 0, 0)
	require.ElementsMatch(t, warnings, []string{
		`PromQL warning: bucket label "le" is missing or has a malformed value of "x" for metric name "the_metric"`,
	})

	require.ElementsMatch(t, infos, []string{
		`PromQL info: metric might not be a counter, name does not end in _total/_sum/_count/_bucket: "not_a_counter"`,
	})
}

type mockResponse struct {
	stats stats.Stats
	annos *annotations.Annotations
}

func (m *mockResponse) Start(ctx context.Context) error {
	return nil
}

func (m *mockResponse) Finalize(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	return m.annos, m.stats, nil
}

func (m *mockResponse) Close() {
	panic("should not be called")
}

type finalizationTestMockResponse struct {
	Closed    bool
	Finalized bool
}

func (m *finalizationTestMockResponse) Start(ctx context.Context) error {
	return nil
}

func (m *finalizationTestMockResponse) Finalize(ctx context.Context) (*annotations.Annotations, stats.Stats, error) {
	m.Finalized = true
	return annotations.New(), stats.Stats{}, nil
}

func (m *finalizationTestMockResponse) Close() {
	m.Closed = true
}

func (m *finalizationTestMockResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	panic("not supported")
}

func (m *finalizationTestMockResponse) GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	panic("not supported")
}

func (m *finalizationTestMockResponse) AdvanceToNextSeries(ctx context.Context) error {
	panic("not supported")
}

func (m *finalizationTestMockResponse) GetNextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	panic("not supported")
}

func (m *finalizationTestMockResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	panic("not supported")
}
