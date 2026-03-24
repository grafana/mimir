// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock implementations for testing
type mockChunkSeries struct {
	labels labels.Labels
	chunks []chunks.Meta
	err    error
}

func (m *mockChunkSeries) Labels() labels.Labels {
	return m.labels
}

func (m *mockChunkSeries) Iterator(chunks.Iterator) chunks.Iterator {
	return storage.NewListChunkSeriesIterator(m.chunks...)
}

func (m *mockChunkSeries) IteratorFactory() storage.ChunkIterable {
	return m
}

func (m *mockChunkSeries) ChunkCount() (int, error) {
	if m.err != nil {
		return 0, m.err
	}
	return len(m.chunks), nil
}

type mockChunkSeriesSet struct {
	series []storage.ChunkSeries
	pos    int
	err    error
}

func (m *mockChunkSeriesSet) Next() bool {
	if m.err != nil {
		return false
	}
	if m.pos >= len(m.series) {
		return false
	}
	m.pos++
	return true
}

func (m *mockChunkSeriesSet) At() storage.ChunkSeries {
	if m.pos == 0 || m.pos > len(m.series) {
		return nil
	}
	return m.series[m.pos-1]
}

func (m *mockChunkSeriesSet) Err() error {
	return m.err
}

func (m *mockChunkSeriesSet) Warnings() annotations.Annotations {
	return nil
}

type mockChunkQuerier struct {
	series []storage.ChunkSeries
	err    error
}

func (m *mockChunkQuerier) Select(context.Context, bool, *storage.SelectHints, ...*labels.Matcher) storage.ChunkSeriesSet {
	return &mockChunkSeriesSet{series: m.series, err: m.err}
}

func (m *mockChunkQuerier) LabelValues(context.Context, string, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (m *mockChunkQuerier) LabelNames(context.Context, *storage.LabelHints, ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func (m *mockChunkQuerier) Close() error {
	return nil
}

func TestRetainingChunkSeriesSet(t *testing.T) {
	series1 := &mockChunkSeries{
		labels: labels.FromStrings("__name__", "metric1"),
		chunks: []chunks.Meta{{MinTime: 1000, MaxTime: 2000}},
	}
	series2 := &mockChunkSeries{
		labels: labels.FromStrings("__name__", "metric2"),
		chunks: []chunks.Meta{{MinTime: 2000, MaxTime: 3000}},
	}

	testCases := []struct {
		name           string
		series         []storage.ChunkSeries
		err            error
		expectedLabels []labels.Labels
		expectError    bool
	}{
		{
			name:           "basic functionality with two series",
			series:         []storage.ChunkSeries{series1, series2},
			expectedLabels: []labels.Labels{series1.Labels(), series2.Labels()},
			expectError:    false,
		},
		{
			name:           "empty set",
			series:         nil,
			expectedLabels: []labels.Labels{},
			expectError:    false,
		},
		{
			name:           "error propagation",
			series:         nil,
			err:            fmt.Errorf("test error"),
			expectedLabels: []labels.Labels{},
			expectError:    true,
		},
		{
			name:           "single series",
			series:         []storage.ChunkSeries{series1},
			expectedLabels: []labels.Labels{series1.Labels()},
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			delegate := &mockChunkSeriesSet{
				series: tc.series,
				err:    tc.err,
			}
			r := &retainingChunkSeriesSet{delegate: delegate}

			// Iterate through all series
			var collectedSeries []storage.ChunkSeries
			for r.Next() {
				series := r.At()
				collectedSeries = append(collectedSeries, series)

				// Test that calling At() again doesn't change anything
				assert.Equal(t, series, r.At())
			}

			// Check error
			if tc.expectError {
				assert.Error(t, r.Err())
			} else {
				assert.NoError(t, r.Err())
			}

			// Check collected series count
			assert.Len(t, collectedSeries, len(tc.expectedLabels))

			// Check retained labels
			assert.Len(t, r.labels, len(tc.expectedLabels))
			for i, expectedLabel := range tc.expectedLabels {
				assert.Equal(t, expectedLabel, r.labels[i])
			}
		})
	}
}

func TestMirroredChunkQuerier_CompareResults(t *testing.T) {
	series1 := &mockChunkSeries{
		labels: labels.FromStrings("__name__", "metric1"),
		chunks: []chunks.Meta{{MinTime: time.Now().Add(-time.Hour).UnixMilli(), MaxTime: time.Now().Add(-time.Minute).UnixMilli()}},
	}
	series2 := &mockChunkSeries{
		labels: labels.FromStrings("__name__", "metric2"),
		chunks: []chunks.Meta{{MinTime: time.Now().Add(-2 * time.Hour).UnixMilli(), MaxTime: time.Now().UnixMilli()}},
	}

	testCases := []struct {
		name                string
		primarySeries       []labels.Labels
		secondarySeries     []storage.ChunkSeries
		secondaryErr        error
		expectedMetricLabel string
	}{
		{
			name:                "matching series",
			primarySeries:       []labels.Labels{series1.Labels(), series2.Labels()},
			secondarySeries:     []storage.ChunkSeries{series1, series2},
			expectedMetricLabel: "success",
		},
		{
			name:                "extra series in primary",
			primarySeries:       []labels.Labels{series1.Labels(), series2.Labels()},
			secondarySeries:     []storage.ChunkSeries{series1},
			expectedMetricLabel: "extra_series",
		},
		{
			name:                "missing series from primary",
			primarySeries:       []labels.Labels{series1.Labels()},
			secondarySeries:     []storage.ChunkSeries{series1, series2},
			expectedMetricLabel: "missing_series",
		},
		{
			name:                "both extra and missing series",
			primarySeries:       []labels.Labels{series1.Labels()},
			secondarySeries:     []storage.ChunkSeries{series2},
			expectedMetricLabel: "extra_and_missing_series",
		},
		{
			name:                "no series in either",
			primarySeries:       []labels.Labels{},
			secondarySeries:     []storage.ChunkSeries{},
			expectedMetricLabel: "success",
		},
		{
			name:                "secondary error",
			primarySeries:       []labels.Labels{},
			secondarySeries:     []storage.ChunkSeries{},
			secondaryErr:        fmt.Errorf("secondary error"),
			expectedMetricLabel: "secondary_error",
		},
		{
			name:          "chunk trimming - series beyond maxT ignored",
			primarySeries: []labels.Labels{series1.Labels()}, // Only series1 in primary
			secondarySeries: []storage.ChunkSeries{
				series1, // This should be included (chunk at 1000-2000, maxT=3000)
				&mockChunkSeries{ // This should be ignored (chunk at 4000-5000, beyond the time the query finished)
					labels: labels.FromStrings("__name__", "metric3"),
					chunks: []chunks.Meta{{MinTime: time.Now().Add(time.Second).UnixMilli(), MaxTime: time.Now().Add(2 * time.Second).UnixMilli()}},
				},
			},
			expectedMetricLabel: "success", // Should match because the beyond-maxT series is ignored
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.NewNopLogger()
			comparisonOutcomes := promauto.With(prometheus.NewPedanticRegistry()).NewCounterVec(
				prometheus.CounterOpts{Name: "test_comparison_outcomes_" + tc.name},
				[]string{"outcome"},
			)

			querier := &mirroredChunkQuerier{
				logger:             logger,
				comparisonOutcomes: comparisonOutcomes,
				delegate:           &mockChunkQuerier{},
				returnedSeries: &retainingChunkSeriesSet{
					labels: tc.primarySeries,
				},
			}
			querier.recordedRequest.ctx = context.Background()
			querier.recordedRequest.finishedAt = time.Now()

			secondary := &mockChunkSeriesSet{
				series: tc.secondarySeries,
				err:    tc.secondaryErr,
			}

			querier.compareResults(secondary)

			// Should record expected metric
			metric, err := comparisonOutcomes.GetMetricWithLabelValues(tc.expectedMetricLabel)
			require.NoError(t, err)
			assert.Equal(t, 1.0, testutil.ToFloat64(metric))
		})
	}
}

func TestMirroredChunkQuerier(t *testing.T) {
	series1 := &mockChunkSeries{
		labels: labels.FromStrings("__name__", "metric1"),
		chunks: []chunks.Meta{{MinTime: 1000, MaxTime: 2000}},
	}

	ctx := context.Background()

	logger := log.NewNopLogger()
	comparisonOutcomes := promauto.With(prometheus.NewPedanticRegistry()).NewCounterVec(
		prometheus.CounterOpts{Name: "test_comparison_outcomes_cancelled_after_select"},
		[]string{"outcome", "user"},
	)

	t.Run("context_cancelled_before_select", func(t *testing.T) {
		t.Cleanup(comparisonOutcomes.Reset)
		delegate := &mockChunkQuerier{
			series: []storage.ChunkSeries{series1},
			err:    fmt.Errorf("test: %w", context.Canceled),
		}
		querier := newMirroredChunkQuerierWithMeta("test-user", comparisonOutcomes, 1000, 2000, tsdb.BlockMeta{}, logger, delegate)

		// Call Select to set up the querier
		ss := querier.Select(ctx, true, nil, &labels.Matcher{Type: labels.MatchEqual, Name: "__name__", Value: "metric1"})

		// Iterate through series to populate the retaining series set
		for ss.Next() {
			ss.At()
		}

		// Series set should have the cancelled error
		assert.ErrorIs(t, ss.Err(), context.Canceled)

		// Close should detect the cancellation and record the metric
		assert.NoError(t, querier.Close())

		// Should record context cancelled
		metric, err := comparisonOutcomes.GetMetricWithLabelValues("context_cancelled", "test-user")
		require.NoError(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(metric))
	})

	t.Run("context_cancelled_before_close", func(t *testing.T) {
		t.Cleanup(comparisonOutcomes.Reset)
		delegate := &mockChunkQuerier{
			series: []storage.ChunkSeries{series1},
		}
		querier := newMirroredChunkQuerierWithMeta("test-user", comparisonOutcomes, 1000, 2000, tsdb.BlockMeta{}, logger, delegate)

		// Call Select to set up the querier
		ss := querier.Select(ctx, true, nil, &labels.Matcher{Type: labels.MatchEqual, Name: "__name__", Value: "metric1"})

		// Iterate through series to populate the retaining series set
		for ss.Next() {
			ss.At()
		}

		// No error initially
		assert.NoError(t, ss.Err())

		// Set the error right before calling Close
		delegate.err = fmt.Errorf("test: %w", context.Canceled)

		// Close should detect the cancellation and record the metric
		assert.NoError(t, querier.Close())

		// Should record context cancelled
		metric, err := comparisonOutcomes.GetMetricWithLabelValues("context_cancelled", "test-user")
		require.NoError(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(metric))
	})

	t.Run("select_not_called", func(t *testing.T) {
		t.Cleanup(comparisonOutcomes.Reset)
		delegate := &mockChunkQuerier{
			series: []storage.ChunkSeries{series1},
		}
		querier := newMirroredChunkQuerierWithMeta("test-user", comparisonOutcomes, 1000, 2000, tsdb.BlockMeta{}, logger, delegate)
		// Directly call Close without calling Select
		assert.NoError(t, querier.Close())
		// Should record no_select
		metric, err := comparisonOutcomes.GetMetricWithLabelValues("no_select", "test-user")
		require.NoError(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(metric))
	})
}
