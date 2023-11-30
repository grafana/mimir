// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"time"

	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	v1 "github.com/grafana/mimir/pkg/frontend/v1"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type frontendToSchedulerAdapter struct {
	cfg             Config
	limits          v1.Limits
	prometheusCodec querymiddleware.Codec
}

func (a *frontendToSchedulerAdapter) frontendToSchedulerEnqueueRequest(
	req *frontendRequest, frontendAddr string,
) *schedulerpb.FrontendToScheduler {
	return &schedulerpb.FrontendToScheduler{
		Type:                      schedulerpb.ENQUEUE,
		QueryID:                   req.queryID,
		UserID:                    req.userID,
		HttpRequest:               req.request,
		FrontendAddress:           frontendAddr,
		StatsEnabled:              req.statsEnabled,
		AdditionalQueueDimensions: a.extractAdditionalQueueDimensions(req.ctx, req.request),
	}
}

const ShouldQueryIngestersQueueDimension = "ingester"
const ShouldQueryStoreGatewayQueueDimension = "store-gateway"
const ShouldQueryIngestersAndStoreGatewayQueueDimension = "ingester|store-gateway"

func (a *frontendToSchedulerAdapter) extractAdditionalQueueDimensions(
	ctx context.Context, request *httpgrpc.HTTPRequest,
) []string {
	httpRequest, err := httpgrpc.ToHTTPRequest(ctx, request)
	if err != nil {
		return nil
	}
	promRequest, err := a.prometheusCodec.DecodeRequest(ctx, httpRequest)
	if err != nil {
		return nil
	}

	now := time.Now()
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil
	}

	latestQueryIngestersWithinWindow := validation.MinDurationPerTenant(tenantIDs, a.limits.QueryIngestersWithin)
	shouldQueryIngesters := querier.ShouldQueryIngesters(latestQueryIngestersWithinWindow, now, promRequest.GetEnd())
	shouldQueryBlockStore := querier.ShouldQueryBlockStore(a.cfg.QueryStoreAfter, now, promRequest.GetStart())

	if shouldQueryIngesters && !shouldQueryBlockStore {
		return []string{ShouldQueryIngestersQueueDimension}
	} else if !shouldQueryIngesters && shouldQueryBlockStore {
		return []string{ShouldQueryStoreGatewayQueueDimension}
	}
	return []string{ShouldQueryIngestersAndStoreGatewayQueueDimension}
}
