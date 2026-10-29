// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
)

func TestIngesterSampleQueryCost(t *testing.T) {
	i := requireActiveIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	ctx := user.InjectOrgID(t.Context(), "sampling-tenant")
	for n := 0; n < 100; n++ {
		ls := labels.FromStrings("__name__", "up", "instance", fmt.Sprint(n))
		for ts := int64(0); ts <= 60000; ts += 15000 {
			_, err := i.Push(ctx, mockWriteRequest(t, ls, 1, ts))
			require.NoError(t, err)
		}
	}
	req := cost.NewRequest(0, 60000, false, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")})
	res, err := i.SampleQueryCost(ctx, req)
	require.NoError(t, err)
	require.NoError(t, res.Validate())
	require.False(t, res.Unsupported)
	require.Equal(t, uint64(cost.MaxChunks), res.ChunksExamined)
	require.LessOrEqual(t, res.SeriesExamined, uint64(cost.MaxSeries))
	stats, _, err := res.Statistics()
	require.NoError(t, err)
	require.Equal(t, float64(15000), stats.IntervalMilliseconds)
	require.Equal(t, float64(1), stats.AveragePointCost)

	partial := *req
	partial.Start = 30001
	partial.End = 45000
	measured, err := i.SampleQueryCost(ctx, &partial)
	require.NoError(t, err)
	measuredStats, _, err := measured.Statistics()
	require.NoError(t, err)
	require.Equal(t, float64(15000), measuredStats.IntervalMilliseconds)

	empty, err := i.SampleQueryCost(user.InjectOrgID(t.Context(), "other-tenant"), req)
	require.NoError(t, err)
	require.Zero(t, empty.Points)
	_, err = i.SampleQueryCost(ctx, &cost.SamplingRequest{Start: 10, End: 0})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestIngesterSampleQueryCostUnsupportedLayouts(t *testing.T) {
	for _, layout := range []string{"out of order", "head and block"} {
		t.Run(layout, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			limits := defaultLimitsTestConfig()
			limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
			i, ring, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", prometheus.NewRegistry())
			require.NoError(t, err)
			startAndWaitHealthy(t, i, ring)
			ctx := user.InjectOrgID(t.Context(), "tenant")
			ls := labels.FromStrings("__name__", "up")
			_, err = i.Push(ctx, mockWriteRequest(t, ls, 1, 60000))
			require.NoError(t, err)
			if layout == "head and block" {
				i.Flush()
			}
			ts := int64(30000)
			if layout == "head and block" {
				ts = 90000
			}
			_, err = i.Push(ctx, mockWriteRequest(t, ls, 1, ts))
			require.NoError(t, err)
			req := cost.NewRequest(0, 120000, false, []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "up")})
			res, err := i.SampleQueryCost(ctx, req)
			require.NoError(t, err)
			require.True(t, res.Unsupported)
			require.Zero(t, res.ChunkBytes)
			require.Zero(t, res.ChunksExamined)
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			_, err = i.SampleQueryCost(canceled, req)
			// A cancelled request must not become a successful fallback.
			require.Error(t, err)
		})
	}
}
