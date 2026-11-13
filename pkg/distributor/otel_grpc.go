// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	otlpMetricsServiceName = "opentelemetry.proto.collector.metrics.v1.MetricsService"

	// OTLPGRPCExportMethod is the full gRPC method name of the OTLP metrics export RPC.
	OTLPGRPCExportMethod = "/" + otlpMetricsServiceName + "/Export"
)

// otlpMetricsServer is the server interface of the OTLP metrics service, as registered with gRPC.
type otlpMetricsServer interface {
	Export(context.Context, *otlpExportRequest) (pmetricotlp.ExportResponse, error)
}

// otlpExportRequest is a decoded OTLP metrics export request. It records the size of the message it was decoded from.
type otlpExportRequest struct {
	pmetricotlp.ExportRequest
	// size is recorded by UnmarshalProto, which the gRPC codec calls while decoding the request.
	size int
}

// UnmarshalProto decodes the request. The gRPC codec calls it instead of the generated protobuf-go unmarshaling.
func (r *otlpExportRequest) UnmarshalProto(data []byte) error {
	r.ExportRequest = pmetricotlp.NewExportRequest()
	r.size = len(data)
	return r.ExportRequest.UnmarshalProto(data)
}

// otlpGRPCReceiver serves the OTLP metrics service on Mimir's main gRPC server. It converts and pushes requests like
// the OTLP HTTP receiver, and reports failures with the gRPC codes derived from the HTTP status the receiver would use.
type otlpGRPCReceiver struct {
	maxRequestSize                          int
	limits                                  OTLPHandlerLimits
	resourceAttributePromotionConfig        OTelResourceAttributePromotionConfig
	keepIdentifyingResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig
	otlpPushMiddlewares                     []OTLPPushMiddleware
	push                                    PushFunc
	pushMetrics                             *PushMetrics
	retryCfg                                RetryConfig
	logger                                  log.Logger
}

// RegisterOTLPGRPCService registers the OTLP metrics service on the given gRPC server.
func RegisterOTLPGRPCService(server grpc.ServiceRegistrar, cfg Config, limits OTLPHandlerLimits, push PushFunc, pushMetrics *PushMetrics, logger log.Logger) {
	receiver := &otlpGRPCReceiver{
		maxRequestSize:                          cfg.MaxOTLPRequestSize,
		limits:                                  limits,
		resourceAttributePromotionConfig:        cfg.OTelResourceAttributePromotionConfig,
		keepIdentifyingResourceAttributesConfig: cfg.KeepIdentifyingOTelResourceAttributesConfig,
		otlpPushMiddlewares:                     cfg.OTLPPushMiddlewares,
		push:                                    push,
		pushMetrics:                             pushMetrics,
		retryCfg:                                cfg.RetryConfig,
		logger:                                  logger,
	}
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: otlpMetricsServiceName,
		HandlerType: (*otlpMetricsServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Export",
			Handler:    exportHandler,
		}},
	}, receiver)
}

// exportHandler decodes an export request and calls the server's Export method, through the interceptor if there is one.
// It mirrors the generated handler of gRPC services, but decodes into the pdata message instead of a protobuf-go one.
func exportHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(otlpExportRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(otlpMetricsServer).Export(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: OTLPGRPCExportMethod}
	handler := func(ctx context.Context, req any) (any, error) {
		return srv.(otlpMetricsServer).Export(ctx, req.(*otlpExportRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// Export implements otlpMetricsServer.
func (r *otlpGRPCReceiver) Export(ctx context.Context, in *otlpExportRequest) (resp pmetricotlp.ExportResponse, retErr error) {
	// gRPC doesn't recover from panics, unlike net/http, and this method takes requests from external clients.
	defer func() {
		if p := recover(); p != nil {
			_ = level.Error(utillog.WithContext(ctx, r.logger)).Log("msg", "recovered from panic while handling OTLP gRPC request", "panic", p, "stack", string(debug.Stack()))
			resp, retErr = pmetricotlp.ExportResponse{}, grpcStatusError(codes.Internal, fmt.Sprintf("panic while handling request: %v", p))
		}
	}()

	// The server bounds what it reads with -server.grpc-max-recv-msg-size-bytes. This is the OTLP-specific limit,
	// checked the same way as for HTTP requests.
	if in.size > r.maxRequestSize {
		sizeErr := distributorMaxOTLPRequestSizeErr{actual: in.size, limit: r.maxRequestSize}
		return pmetricotlp.ExportResponse{}, grpcStatusError(codes.InvalidArgument, sizeErr.Error())
	}

	logger := utillog.WithContext(ctx, r.logger)
	otlpConverter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
	var schemeOverride *model.ValidationScheme
	convert := newOTLPConvertFunc(
		false, // Translation headers are only supported by the HTTP receiver.
		r.limits, r.resourceAttributePromotionConfig, r.keepIdentifyingResourceAttributesConfig,
		otlpConverter, r.pushMetrics, r.otlpPushMiddlewares, &schemeOverride,
	)

	supplier := func() (*mimirpb.WriteRequest, func(), int, error) {
		spanLogger, spanCtx := spanlogger.New(ctx, logger, tracer, "Distributor.OTLPGRPCReceiver.convert")
		defer spanLogger.Finish()

		var wr mimirpb.PreallocWriteRequest
		size, err := convert(spanCtx, in.ExportRequest, otlpConvertParams{
			contentType: pbContentType,
			// The compressed wire size isn't known once gRPC has decompressed the message. Leaving it at 0
			// skips the compression ratio observation, which would otherwise always be 1.
			contentLength:        0,
			uncompressedBodySize: in.size,
		}, &wr, logger, spanLogger)
		if err != nil {
			// Like the HTTP receiver's supplier, report conversion errors as client errors, which map to InvalidArgument.
			if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
				err = httpgrpc.Error(http.StatusBadRequest, err.Error())
			}
			return nil, nil, 0, err
		}
		return &wr.WriteRequest, func() { mimirpb.ReuseSlice(wr.Timeseries) }, size, nil
	}

	req := newRequest(supplier)
	// The decoded size is checked against the inflight bytes limit before the request is converted.
	req.contentLength = int64(in.size)
	req.contentLengthDecoded = true
	req.onInit = func() {
		req.nameValidationSchemeOverride = schemeOverride
	}

	pushErr := r.push(ctx, req)
	if pushErr == nil {
		if otlpErr := otlpConverter.Err(); otlpErr != nil {
			// The push succeeded, but the converter left out some samples: tell the client.
			return pmetricotlp.ExportResponse{}, grpcStatusError(codes.InvalidArgument, otlpErr.Error())
		}
		return pmetricotlp.NewExportResponse(), nil
	}

	if errors.Is(pushErr, context.Canceled) {
		return pmetricotlp.ExportResponse{}, grpcStatusError(codes.Canceled, "push request context canceled")
	}
	if labelValueTooLongErr := (labelValueTooLongError{}); errors.As(pushErr, &labelValueTooLongErr) {
		// Translate from Mimir to OTel domain terminology.
		pushErr = newValidationError(otelAttributeValueTooLongError{labelValueTooLongErr})
	}

	// The gRPC code is derived from the HTTP status, as the OTLP HTTP receiver classifies the error. Unlike the HTTP
	// receiver, the status isn't mapped by httpRetryableToOTLPRetryable, so 501 is reported as Unimplemented.
	pushError := classifyOTLPPushError(pushErr)
	if pushError.isSoft {
		// Only some data points were rejected: the request is a partial success.
		resp := pmetricotlp.NewExportResponse()
		resp.PartialSuccess().SetRejectedDataPoints(rejectedDataPointsFromPushError(pushErr))
		resp.PartialSuccess().SetErrorMessage(validUTF8Message(pushErr.Error()))
		return resp, nil
	}
	if pushError.httpCode == http.StatusAccepted {
		// Samples from non-elected HA replicas are expected to be dropped. The HTTP receiver answers 202 Accepted,
		// so report success rather than a failure that would make the client log errors and retry.
		return pmetricotlp.NewExportResponse(), nil
	}
	return pmetricotlp.ExportResponse{}, r.errorWithRetryInfo(httpStatusToOTLPGRPCCode(pushError.httpCode), pushError.msg)
}

// errorWithRetryInfo returns a gRPC error for the given code. For the retryable codes it also includes a RetryInfo
// detail when -distributor.retry-after-header.enabled is set, which is what OTLP clients use to back off. For
// ResourceExhausted, clients only retry the request when the RetryInfo is present.
func (r *otlpGRPCReceiver) errorWithRetryInfo(code codes.Code, msg string) error {
	st := status.New(code, validUTF8Message(msg))
	if !r.retryCfg.Enabled || (code != codes.ResourceExhausted && code != codes.Unavailable) {
		return st.Err()
	}

	// Clients don't send a retry attempt over gRPC, so the first attempt's backoff is used.
	seconds := retryAfterSeconds("", r.retryCfg.MinBackoff, r.retryCfg.MaxBackoff)
	withDetails, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Duration(seconds) * time.Second)})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func grpcStatusError(code codes.Code, msg string) error {
	return status.Error(code, validUTF8Message(msg))
}

// httpStatusToOTLPGRPCCode maps the HTTP status codes used by the shared write path to gRPC codes
// that clients interpret correctly according to the OTLP specification.
func httpStatusToOTLPGRPCCode(httpCode int) codes.Code {
	switch {
	case httpCode == http.StatusTooManyRequests:
		return codes.ResourceExhausted
	case httpCode == http.StatusNotImplemented:
		return codes.Unimplemented
	case httpCode/100 == 5:
		return codes.Unavailable
	case httpCode/100 == 4 || httpCode/100 == 3:
		return codes.InvalidArgument
	default:
		return codes.Internal
	}
}
