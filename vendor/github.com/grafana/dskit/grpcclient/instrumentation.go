package grpcclient

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func Instrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			middleware.ClientUserHeaderInterceptor,
			middleware.UnaryClientInstrumentInterceptor(requestDuration),
		}, []grpc.StreamClientInterceptor{
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			middleware.StreamClientUserHeaderInterceptor,
			middleware.StreamClientInstrumentInterceptor(requestDuration),
		}
}

func OtelInstrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
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
