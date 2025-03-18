// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type frontendToSchedulerAdapter struct {
	log    log.Logger
	cfg    Config
	limits Limits
	codec  querymiddleware.Codec
}

func (a *frontendToSchedulerAdapter) frontendToSchedulerEnqueueRequest(
	req *frontendRequest, frontendAddr string,
) (*schedulerpb.FrontendToScheduler, error) {
	var addlQueueDims []string
	var err error
	addlQueueDims, err = a.extractAdditionalQueueDimensions(req.ctx, req.request, time.Now())
	if err != nil {
		return nil, err
	}

	return &schedulerpb.FrontendToScheduler{
		Type:                      schedulerpb.ENQUEUE,
		QueryID:                   req.queryID,
		UserID:                    req.userID,
		HttpRequest:               req.request,
		FrontendAddress:           frontendAddr,
		StatsEnabled:              req.statsEnabled,
		AdditionalQueueDimensions: addlQueueDims,
	}, nil
}

const ShouldQueryIngestersQueueDimension = "ingester"
const ShouldQueryStoreGatewayQueueDimension = "store-gateway"
const ShouldQueryIngestersAndStoreGatewayQueueDimension = "ingester-and-store-gateway"

func (a *frontendToSchedulerAdapter) extractAdditionalQueueDimensions(
	ctx context.Context, request *httpgrpc.HTTPRequest, now time.Time,
) ([]string, error) {
	var err error

	httpRequest, err := httpgrpc.ToHTTPRequest(ctx, request)
	if err != nil {
		return nil, err
	}

	tenantIDs, err := tenant.TenantIDs(httpRequest.Context())
	if err != nil {
		return nil, err
	}

	switch {
	case querymiddleware.IsRangeQuery(httpRequest.URL.Path), querymiddleware.IsInstantQuery(httpRequest.URL.Path):
		decodedRequest, err := a.codec.DecodeMetricsQueryRequest(httpRequest.Context(), httpRequest)
		if err != nil {
			return nil, err
		}
		minT := decodedRequest.GetMinT()
		maxT := decodedRequest.GetMaxT()

		return a.queryComponentQueueDimensionFromTimeParams(tenantIDs, minT, maxT, now), nil
	case querymiddleware.IsLabelsQuery(httpRequest.URL.Path):
		decodedRequest, err := a.codec.DecodeLabelsSeriesQueryRequest(httpRequest.Context(), httpRequest)
		if err != nil {
			return nil, err
		}

		return a.queryComponentQueueDimensionFromTimeParams(
			tenantIDs, decodedRequest.GetStart(), decodedRequest.GetEnd(), now,
		), nil
	case querymiddleware.IsCardinalityQuery(httpRequest.URL.Path), querymiddleware.IsActiveSeriesQuery(httpRequest.URL.Path), querymiddleware.IsActiveNativeHistogramMetricsQuery(httpRequest.URL.Path):
		// cardinality only hits ingesters
		return []string{ShouldQueryIngestersQueueDimension}, nil
	default:
		// no query time params to parse; cannot infer query component
		level.Debug(a.log).Log("msg", "unsupported request type for additional queue dimensions", "query", httpRequest.URL.String())
		return nil, nil
	}
}

func (a *frontendToSchedulerAdapter) queryComponentQueueDimensionFromTimeParams(
	tenantIDs []string, queryStartUnixMs, queryEndUnixMs int64, now time.Time,
) []string {
	longestQueryIngestersWithinWindow := validation.MaxDurationPerTenant(tenantIDs, a.limits.QueryIngestersWithin)
	shouldQueryIngesters := querier.ShouldQueryIngesters(
		longestQueryIngestersWithinWindow, now, queryEndUnixMs,
	)
	shouldQueryBlockStore := querier.ShouldQueryBlockStore(
		a.cfg.QueryStoreAfter, now, queryStartUnixMs,
	)

	if shouldQueryIngesters && !shouldQueryBlockStore {
		return []string{ShouldQueryIngestersQueueDimension}
	} else if !shouldQueryIngesters && shouldQueryBlockStore {
		return []string{ShouldQueryStoreGatewayQueueDimension}
	}
	return []string{ShouldQueryIngestersAndStoreGatewayQueueDimension}
}
