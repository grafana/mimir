// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// costSamplingQueryable keeps bounded sampling separate from ordinary sample queries.
type costSamplingQueryable struct {
	storage.SampleAndChunkQueryable
	sampling storage.Queryable
}

func (q *costSamplingQueryable) SampleQueryCost(ctx context.Context, mint, maxt int64, skipHistogramBuckets bool, matchers ...*labels.Matcher) (result storage.CostSamplingStats, warnings annotations.Annotations, err error) {
	querier, err := q.sampling.Querier(mint, maxt)
	if err != nil {
		return result, nil, err
	}
	defer func() { err = errors.Join(err, querier.Close()) }()
	sampler, ok := querier.(cost.Sampler)
	if !ok {
		return result, nil, storage.ErrCostSamplingUnsupported
	}
	res, err := sampler.SampleQueryCost(ctx, cost.NewRequest(mint, maxt, skipHistogramBuckets, matchers))
	if err != nil {
		return result, nil, err
	}
	return res.Statistics()
}

func (mq *multiQuerier) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	spanLog := spanlogger.FromContext(ctx, mq.logger)
	start, end, err := validateQueryTimeRange(userID, req.Start, req.End, time.Now().UnixMilli(), mq.limits, spanLog)
	if errors.Is(err, errEmptyTimeRange) {
		return &cost.SamplingResponse{}, nil
	}
	if err != nil {
		return nil, err
	}
	start = clampToMaxLabelQueryLength(spanLog, start, end, time.Now().UnixMilli(), mq.limits.MaxLabelsQueryLength(userID).Milliseconds())
	if limit := mq.limits.MaxPartialQueryLength(userID); limit > 0 && time.UnixMilli(end).Sub(time.UnixMilli(start)) > limit {
		return nil, NewMaxQueryLengthError(time.UnixMilli(end).Sub(time.UnixMilli(start)), limit)
	}
	ctx, queriers, _, _, err := mq.getQueriers(ctx, start, end)
	if err != nil {
		return nil, err
	}
	request := *req
	request.Start, request.End = start, end
	result := &cost.SamplingResponse{}
	for _, q := range queriers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sampler, ok := q.(cost.Sampler)
		if !ok {
			return nil, storage.ErrCostSamplingUnsupported
		}
		res, err := sampler.SampleQueryCost(ctx, &request)
		if err != nil {
			return nil, err
		}
		result.Merge(res)
	}
	return result, nil
}

func (q *distributorQuerier) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	sampler, ok := q.distributor.(cost.Sampler)
	if !ok {
		return nil, storage.ErrCostSamplingUnsupported
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	request := *req
	request.Start = clampMinTime(spanlogger.FromContext(ctx, q.logger), request.Start, time.Now().UnixMilli(), -q.cfgProvider.QueryIngestersWithin(userID), "query ingesters within")
	if request.End < request.Start {
		return &cost.SamplingResponse{}, nil
	}
	return sampler.SampleQueryCost(ctx, &request)
}

func (q *blocksStoreQuerier) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (*cost.SamplingResponse, error) {
	matchers, err := req.Validate()
	if err != nil {
		return nil, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SampleQueryCost")
	defer spanLog.Finish()
	result := &cost.SamplingResponse{}
	var mtx sync.Mutex
	queryLimiter := limiter.QueryLimiterFromContextWithFallback(ctx)
	query := func(ctx context.Context, clients map[BlocksStoreClient][][]ulid.ULID, mint, maxt int64, meta *bucketindex.Metadata) ([]ulid.ULID, error) {
		ctx = grpcContextWithBucketStoreRequestMeta(ctx, userID, meta)
		var available []ulid.ULID
		for client, partitions := range clients {
			for _, ids := range partitions {
				request := *req
				request.Start, request.End = mint, maxt
				request.BlockIds = convertULIDsToString(ids)
				res, err := client.SampleQueryCost(ctx, &request)
				if status.Code(err) == codes.Unimplemented {
					return nil, storage.ErrCostSamplingUnsupported
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
				for _, id := range res.AvailableBlocks {
					parsed, err := ulid.Parse(id)
					if err != nil {
						return nil, err
					}
					available = append(available, parsed)
				}
				mtx.Lock()
				result.Merge(res)
				mtx.Unlock()
			}
		}
		return available, nil
	}
	if err := q.queryCompartmentsWithConsistencyCheck(ctx, q.targetCompartments(userID, matchers), spanLog, req.Start, req.End, userID, nil, query); err != nil {
		return nil, err
	}
	return result, nil
}
