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

func TestFinalize(t *testing.T) {
	querierStats, ctx := stats.ContextWithEmptyStats(context.Background())

	resp := &mockResponse{
		stats: stats.Stats{SamplesProcessed: 456, FetchedChunkBytes: 9000},
	}

	err := finalize(ctx, resp)
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

func (m *mockResponse) Finalize(ctx context.Context) (stats.Stats, error) {
	return m.stats, nil
}

func (m *mockResponse) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	panic("not supported")
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

func (m *finalizationTestMockResponse) Finalize(ctx context.Context) (stats.Stats, error) {
	m.Finalized = true
	return stats.Stats{}, nil
}

func (m *finalizationTestMockResponse) Stats(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	panic("not supported")
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
