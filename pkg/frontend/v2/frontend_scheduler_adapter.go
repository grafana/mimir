// SPDX-License-Identifier: AGPL-3.0-only

package v2

import (
	"errors"

	"github.com/gogo/protobuf/types"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
)

type frontendToSchedulerAdapter struct{}

func (a *frontendToSchedulerAdapter) frontendToSchedulerEnqueueRequest(
	req *frontendRequest, frontendAddr string,
) (*schedulerpb.FrontendToScheduler, error) {
	msg := &schedulerpb.FrontendToScheduler{
		Type:                      schedulerpb.ENQUEUE,
		QueryID:                   req.queryID,
		UserID:                    req.userID,
		FrontendAddress:           frontendAddr,
		StatsEnabled:              req.statsEnabled,
		AdditionalQueueDimensions: req.touchedQueryComponents,
	}

	if req.httpRequest == nil && req.protobufRequest == nil {
		return nil, errors.New("got neither a HTTP nor a Protobuf request payload")
	}

	if req.httpRequest != nil && req.protobufRequest != nil {
		return nil, errors.New("got both a HTTP and a Protobuf request payload")
	}

	if req.httpRequest != nil {
		// Propagate trace context for this query in the HTTP payload.
		// We can't use the trace headers from the gRPC request, as it is a long-running stream from the frontend to the scheduler
		// that handles many queries.
		otel.GetTextMapPropagator().Inject(req.ctx, (*httpgrpcutil.HttpgrpcHeadersCarrier)(req.httpRequest))

		msg.Payload = &schedulerpb.FrontendToScheduler_HttpRequest{HttpRequest: req.httpRequest}
	} else {
		encodedRequest, err := types.MarshalAny(req.protobufRequest)
		if err != nil {
			return nil, err
		}

		// Propagate trace context for this query in the request payload.
		// We can't use the trace headers from the gRPC request, as it is a long-running stream from the frontend to the scheduler
		// that handles many queries.
		metadata := req.protobufRequestHeaders
		otel.GetTextMapPropagator().Inject(req.ctx, schedulerpb.MetadataMapTracingCarrier(metadata))

		msg.Payload = &schedulerpb.FrontendToScheduler_ProtobufRequest{
			ProtobufRequest: &schedulerpb.ProtobufRequest{
				Payload:  encodedRequest,
				Metadata: schedulerpb.MapToMetadataSlice(metadata),
			},
		}
	}

	return msg, nil
}
