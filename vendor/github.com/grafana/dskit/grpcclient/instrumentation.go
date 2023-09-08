package grpcclient

import (
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"

	"github.com/grafana/dskit/middleware"
)

func Instrument(requestDuration *prometheus.HistogramVec) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	return []grpc.UnaryClientInterceptor{
			otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()),
			otelgrpc.UnaryClientInterceptor(),
			middleware.ClientUserHeaderInterceptor,
			middleware.UnaryClientInstrumentInterceptor(requestDuration),
		}, []grpc.StreamClientInterceptor{
			otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()),
			otelgrpc.StreamClientInterceptor(),
			middleware.StreamClientUserHeaderInterceptor,
			middleware.StreamClientInstrumentInterceptor(requestDuration),
		}
}
