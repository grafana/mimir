// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestFinalize(t *testing.T) {
	ctx := context.Background()
	annos := annotations.New()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
	queryStats, err := types.NewQueryStats(timeRange, false, memoryConsumptionTracker)
	require.NoError(t, err)

	annos.Add(annotations.NewBadBucketLabelWarning("the_metric", "x", posrange.PositionRange{Start: 1, End: 2}))
	annos.Add(annotations.NewPossibleNonCounterInfo("not_a_counter", posrange.PositionRange{Start: 3, End: 4}))
	queryStats.TotalSamples = 100

	err = finalise(ctx, &mockResponse{}, annos, queryStats)
	require.NoError(t, err)
	require.Equal(t, int64(100+456), queryStats.TotalSamples)

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

type mockResponse struct{}

func (m *mockResponse) GetEvaluationInfo(ctx context.Context) (*annotations.Annotations, int64, error) {
	annos := annotations.New()
	annos.Add(annotations.NewMixedFloatsHistogramsWarning("mixed_metric", posrange.PositionRange{Start: 5, End: 6}))
	annos.Add(annotations.NewHistogramIgnoredInMixedRangeInfo("another_mixed_metric", posrange.PositionRange{Start: 5, End: 6}))

	return annos, 456, nil
}

func (m *mockResponse) Close() {
	panic("should not be called")
}
