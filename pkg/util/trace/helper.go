package mimirtrace

import (
	"context"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

func ExtractSampledTraceID(ctx context.Context) (string, bool) {
	sp := trace.SpanFromContext(ctx)
	if sp.SpanContext().HasTraceID() {
		return sp.SpanContext().TraceID().String(), sp.SpanContext().IsSampled()
	}
	return "", false
}

func GRPCClientInstrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otelgrpc.UnaryClientInterceptor(),
			middleware.ClientUserHeaderInterceptor,
			middleware.UnaryClientInstrumentInterceptor(requestDuration),
		}, []grpc.StreamClientInterceptor{
			otelgrpc.StreamClientInterceptor(),
			middleware.StreamClientUserHeaderInterceptor,
			middleware.StreamClientInstrumentInterceptor(requestDuration),
		}
}
