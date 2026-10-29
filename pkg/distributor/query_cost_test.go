// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"testing"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func (i *mockIngester) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest, _ ...grpc.CallOption) (*cost.SamplingResponse, error) {
	return i.sampleQueryCostHook(ctx, req)
}

func TestDistributorSampleQueryCost(t *testing.T) {
	for _, mode := range []string{"measurements", "unsupported", "failed", "invalid", "chunk limit"} {
		t.Run(mode, func(t *testing.T) {
			ds, ingesters, _, _ := prepare(t, prepConfig{numDistributors: 1, numIngesters: 3, happyIngesters: 3, replicationFactor: 1})
			req := &cost.SamplingRequest{Start: 0, End: 60000}
			for idx, ing := range ingesters {
				ing.sampleQueryCostHook = func(_ context.Context, got *cost.SamplingRequest) (*cost.SamplingResponse, error) {
					require.Equal(t, req, got)
					if mode == "unsupported" {
						return nil, status.Error(codes.Unimplemented, "older ingester")
					}
					if mode == "failed" {
						return nil, status.Error(codes.Internal, "sampling failed")
					}
					if mode == "invalid" {
						return &cost.SamplingResponse{ChunkBytes: cost.MaxChunkBytes + 1}, nil
					}
					return &cost.SamplingResponse{Points: 1, ChunksExamined: 2, PointCost: float64(idx + 1), Intervals: 2, IntervalMilliseconds: 30000}, nil
				}
			}
			ctx := user.InjectOrgID(t.Context(), "tenant")
			if mode == "chunk limit" {
				ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 0, 1, 0, stats.NewQueryMetrics(prometheus.NewRegistry())))
			}
			res, err := ds[0].SampleQueryCost(ctx, req)
			switch mode {
			case "failed":
				require.ErrorContains(t, err, "sampling failed")
			case "chunk limit":
				require.ErrorContains(t, err, "err-mimir-max-chunks-per-query")
			case "invalid":
				require.ErrorContains(t, err, "exceeds backend budgets")
			case "unsupported":
				require.NoError(t, err)
				require.True(t, res.Unsupported)
			default:
				require.NoError(t, err)
				stats, _, err := res.Statistics()
				require.NoError(t, err)
				require.Equal(t, uint64(3), res.Points)
				require.Equal(t, float64(2), stats.AveragePointCost)
				require.Equal(t, float64(15000), stats.IntervalMilliseconds)
			}
		})
	}
}
