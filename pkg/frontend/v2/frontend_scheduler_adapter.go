// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/types"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/tenant"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
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
	msg := &schedulerpb.FrontendToScheduler{
		Type:            schedulerpb.ENQUEUE,
		QueryID:         req.queryID,
		UserID:          req.userID,
		FrontendAddress: frontendAddr,
		StatsEnabled:    req.statsEnabled,
	}

	if req.httpRequest == nil && req.protobufRequest == nil {
		return nil, errors.New("got neither a HTTP nor a Protobuf request payload")
	}

	if req.httpRequest != nil && req.protobufRequest != nil {
		return nil, errors.New("got both a HTTP and a Protobuf request payload")
	}

	if req.httpRequest != nil {
		var err error
		msg.AdditionalQueueDimensions, err = a.extractAdditionalQueueDimensionsForHTTPRequest(req.ctx, req.httpRequest, time.Now())
		if err != nil {
			return nil, err
		}

		// Propagate trace context for this query in the HTTP payload.
		// We can't use the trace headers from the gRPC request, as it is a long-running stream from the frontend to the scheduler
		// that handles many queries.
		otel.GetTextMapPropagator().Inject(req.ctx, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.httpRequest))

		msg.Payload = &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: req.httpRequest}
	} else {
		// TODO: set additional queue dimensions based on queried time range of request (taking into account lookback etc.)

		encodedRequest, err := types.MarshalAny(req.protobufRequest)
		if err != nil {
			return nil, err
		}

		// Propagate trace context for this query in the request payload.
		// We can't use the trace headers from the gRPC request, as it is a long-running stream from the frontend to the scheduler
		// that handles many queries.
		traceHeaders := map[string]string{}
		otel.GetTextMapPropagator().Inject(req.ctx, propagation.MapCarrier(traceHeaders))

		msg.Payload = &schedulerpb.FrontendToScheduler_ProtobufRequest{
			ProtobufRequest: &schedulerpb.ProtobufRequest{
				Payload:      encodedRequest,
				TraceHeaders: traceHeaders,
			},
		}
	}

	return msg, nil
}

const ShouldQueryIngestersQueueDimension = "ingester"
const ShouldQueryStoreGatewayQueueDimension = "store-gateway"
const ShouldQueryIngestersAndStoreGatewayQueueDimension = "ingester-and-store-gateway"

func (a *frontendToSchedulerAdapter) extractAdditionalQueueDimensionsForHTTPRequest(
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
