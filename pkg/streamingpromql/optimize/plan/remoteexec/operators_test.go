// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestFinishedReading(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())

	resp := &mockResponse{
		stats: stats.Stats{SamplesProcessed: 456, FetchedChunkBytes: 9000},
	}

	err := finishedReading(ctx, resp)
	require.NoError(t, err)
	require.Zero(t, querierStats.SamplesProcessed, "should not directly update number of samples processed on querier stats as this will be captured by the frontend when the query is complete")
	require.Equal(t, uint64(9000), querierStats.FetchedChunkBytes)
}

type mockResponse struct {
	stats stats.Stats
}

func (m *mockResponse) Start(ctx context.Context) error {
	return nil
}

func (m *mockResponse) FinishedReading(ctx context.Context) (stats.Stats, error) {
	return m.stats, nil
}

func (m *mockResponse) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	panic("not supported")
}

func (m *mockResponse) Close() {
	panic("should not be called")
}

type finishedReadingTestMockResponse struct {
	Closed                bool
	FinishedReadingCalled bool
}

func (m *finishedReadingTestMockResponse) Start(ctx context.Context) error {
	return nil
}

func (m *finishedReadingTestMockResponse) FinishedReading(ctx context.Context) (stats.Stats, error) {
	m.FinishedReadingCalled = true
	return stats.Stats{}, nil
}

func (m *finishedReadingTestMockResponse) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	panic("not supported")
}

func (m *finishedReadingTestMockResponse) Close() {
	m.Closed = true
}

func (m *finishedReadingTestMockResponse) GetSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	panic("not supported")
}

func (m *finishedReadingTestMockResponse) GetNextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	panic("not supported")
}

func (m *finishedReadingTestMockResponse) AdvanceToNextSeries(ctx context.Context) error {
	panic("not supported")
}

func (m *finishedReadingTestMockResponse) GetNextStepSamples(ctx context.Context) (*types.RangeVectorStepData, error) {
	panic("not supported")
}

func (m *finishedReadingTestMockResponse) GetValues(ctx context.Context) (types.ScalarData, error) {
	panic("not supported")
}
