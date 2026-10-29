// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// SampleQueryCost samples each ingester with a server-enforced budget.
func (d *Distributor) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	matchers, err := req.Validate()
	if err != nil {
		return nil, err
	}
	sets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
	if err != nil {
		return nil, err
	}
	queryLimiter := limiter.QueryLimiterFromContextWithFallback(ctx)
	responses, err := forReplicationSets(ctx, d, sets, func(ctx context.Context, c client.IngesterClient) (*cost.SamplingResponse, error) {
		res, err := c.SampleQueryCost(ctx, req)
		if status.Code(err) == codes.Unimplemented {
			return &cost.SamplingResponse{Unsupported: true}, nil
		}
		if err != nil {
			return nil, err
		}
		if err := res.Validate(); err != nil {
			return nil, err
		}
		if err := queryLimiter.AddChunkBytes(int(res.ChunkBytes)); err != nil {
			return nil, err
		}
		if err := queryLimiter.AddChunks(int(res.ChunksExamined)); err != nil {
			return nil, err
		}
		return res, nil
	})
	if err != nil {
		return nil, err
	}
	result := &cost.SamplingResponse{}
	for _, res := range responses {
		result.Merge(res)
	}
	return result, nil
}
