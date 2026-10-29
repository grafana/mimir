// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func (*storeGatewayClientMock) SampleQueryCost(context.Context, *cost.SamplingRequest, ...grpc.CallOption) (*cost.SamplingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SampleQueryCost not implemented")
}

func TestQueryCostSamplingUnsupportedQueryable(t *testing.T) {
	q := NewErrorTranslateSampleAndChunkQueryable(NewSampleAndChunkQueryable(storage.QueryableFunc(func(int64, int64) (storage.Querier, error) {
		t.Fatal("unsupported sampling must not open ordinary storage")
		return nil, nil
	})))
	_, _, err := q.(storage.CostStatsQueryable).SampleQueryCost(t.Context(), 0, 60000, false)
	require.ErrorIs(t, err, storage.ErrCostSamplingUnsupported)
}

func (*mockStoreGatewayServer) SampleQueryCost(context.Context, *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method SampleQueryCost not implemented")
}

func (m *cancelerStoreGatewayClientMock) SampleQueryCost(ctx context.Context, _ *cost.SamplingRequest, _ ...grpc.CallOption) (*cost.SamplingResponse, error) {
	m.cancel()
	return nil, ctx.Err()
}

type samplingStoreGatewayClient struct {
	storeGatewayClientMock
	sample func(*cost.SamplingRequest) (*cost.SamplingResponse, error)
}

func (c *samplingStoreGatewayClient) SampleQueryCost(_ context.Context, req *cost.SamplingRequest, _ ...grpc.CallOption) (*cost.SamplingResponse, error) {
	return c.sample(req)
}

func TestBlocksStoreQuerierSampleQueryCost(t *testing.T) {
	for _, mode := range []string{"measurements", "unsupported", "failed", "missing block", "byte limit"} {
		t.Run(mode, func(t *testing.T) {
			id := ulid.MustNew(1, nil)
			client := &samplingStoreGatewayClient{sample: func(req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
				require.Equal(t, []string{id.String()}, req.BlockIds)
				require.Equal(t, int64(0), req.Start)
				require.Equal(t, int64(60000), req.End)
				require.True(t, req.SkipHistogramBuckets)
				switch mode {
				case "unsupported":
					return nil, status.Error(codes.Unimplemented, "older store-gateway")
				case "failed":
					return nil, status.Error(codes.Internal, "sampling failed")
				case "missing block":
					return &cost.SamplingResponse{}, nil
				}
				return &cost.SamplingResponse{Points: 1, PointCost: 2, ChunkBytes: 100, AvailableBlocks: []string{id.String()}}, nil
			}}
			client.remoteAddr = "store-gateway"
			stores := &blocksStoreSetMock{mockedResponses: []interface{}{map[BlocksStoreClient][][]ulid.ULID{client: {{id}}}}}
			if mode == "missing block" {
				stores.mockedResponses = append(stores.mockedResponses, errors.New("no other replica"))
			}
			finder := &blocksFinderMock{}
			finder.On("GetBlocks", mock.Anything, "tenant", int64(0), int64(60000)).Return(bucketindex.Blocks{{ID: id}}, &bucketindex.Metadata{}, nil)
			reg := prometheus.NewRegistry()
			q := &blocksStoreQuerier{
				minT: 0, maxT: 60000,
				compartments:       []blocksStoreCompartment{{finder: finder, stores: stores}},
				dynamicReplication: newDynamicReplication(),
				consistency:        NewBlocksConsistency(0, reg), logger: log.NewNopLogger(),
				metrics: newBlocksStoreQueryableMetrics(compartments.Config{}, reg), limits: &blocksStoreLimitsMock{},
			}
			ctx := user.InjectOrgID(t.Context(), "tenant")
			if mode == "byte limit" {
				ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, 1, 0, 0, stats.NewQueryMetrics(prometheus.NewRegistry())))
			}
			res, err := q.SampleQueryCost(ctx, &cost.SamplingRequest{End: 60000, SkipHistogramBuckets: true})
			switch mode {
			case "unsupported":
				require.ErrorIs(t, err, storage.ErrCostSamplingUnsupported)
			case "failed":
				require.ErrorContains(t, err, "sampling failed")
			case "byte limit":
				require.ErrorContains(t, err, "err-mimir-max-chunks-bytes-per-query")
			case "missing block":
				require.ErrorContains(t, err, "err-mimir-store-consistency-check-failed")
				require.Nil(t, res)
			default:
				require.NoError(t, err)
				require.Equal(t, float64(2), res.PointCost)
			}
			require.NoError(t, q.Close())
		})
	}
}
