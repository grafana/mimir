// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	otgrpc "github.com/opentracing-contrib/go-grpc"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerpb"
	"github.com/grafana/mimir/pkg/util"
)

func newUsageTrackerClientPool(discovery client.PoolServiceDiscovery, clientName string, clientConfig Config, logger log.Logger, registerer prometheus.Registerer) *client.Pool {
	poolCfg := client.PoolConfig{
		CheckInterval:          10 * time.Second,
		HealthCheckEnabled:     true,
		HealthCheckTimeout:     10 * time.Second,
		HealthCheckGracePeriod: clientConfig.GRPCClientConfig.HealthCheckGracePeriod,
	}

	clientsCount := promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
		Namespace:   "cortex",
		Name:        "usage_tracker_clients",
		Help:        "The current number of usage-tracker clients in the pool.",
		ConstLabels: map[string]string{"client": clientName},
	})

	var factory client.PoolFactory
	if clientConfig.ClientFactory != nil {
		factory = clientConfig.ClientFactory
	} else {
		factory = newUsageTrackerClientFactory(clientName, clientConfig.GRPCClientConfig.Config, registerer, logger)
	}

	return client.NewPool("usage-tracker", poolCfg, discovery, factory, clientsCount, logger)
}

func newUsageTrackerClientFactory(clientName string, clientCfg grpcclient.Config, reg prometheus.Registerer, logger log.Logger) client.PoolFactory {
	requestDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_usage_tracker_client_request_duration_seconds",
		Help:                            "Time spent executing  a single request to a usage-tracker instance.",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
		ConstLabels:                     prometheus.Labels{"client": clientName},
	}, []string{"operation", "status_code"})

	invalidClusterValidation := util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "usage-tracker", util.GRPCProtocol)

	return client.PoolInstFunc(func(inst ring.InstanceDesc) (client.PoolClient, error) {
		return dialUsageTracker(clientCfg, inst, requestDuration, invalidClusterValidation, logger)
	})
}

func dialUsageTracker(clientCfg grpcclient.Config, instance ring.InstanceDesc, requestDuration *prometheus.HistogramVec, invalidClusterValidation *prometheus.CounterVec, logger log.Logger) (*usageTrackerClient, error) {
	unary, stream := grpcclientInstrument(requestDuration)
	opts, err := clientCfg.DialOption(unary, stream, util.NewInvalidClusterValidationReporter(clientCfg.ClusterValidation.Label, invalidClusterValidation, logger))
	if err != nil {
		return nil, err
	}

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(instance.Addr, opts...)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to dial usage-tracker %s %s", instance.Id, instance.Addr)
	}

	return &usageTrackerClient{
		UsageTrackerClient: usagetrackerpb.NewUsageTrackerClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,
	}, nil
}

type usageTrackerClient struct {
	usagetrackerpb.UsageTrackerClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn
}

func (c *usageTrackerClient) Close() error {
	return c.conn.Close()
}

func (c *usageTrackerClient) String() string {
	return c.RemoteAddress()
}

func (c *usageTrackerClient) RemoteAddress() string {
	return c.conn.Target()
}

// grpcclientInstrument is a copy of grpcclient.Instrument, but it doesn't add the ClientUserHeaderInterceptor for the method that doesn't need auth.
func grpcclientInstrument(requestDuration *prometheus.HistogramVec, instrumentationLabelOptions ...middleware.InstrumentationOption) ([]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor) {
	noAuthMethods := map[string]bool{
		"/usagetrackerpb.UsageTracker/GetUsersCloseToLimit": true,
	}
	var (
		unary  []grpc.UnaryClientInterceptor
		stream []grpc.StreamClientInterceptor
	)
	if opentracing.IsGlobalTracerRegistered() {
		unary = append(unary, otgrpc.OpenTracingClientInterceptor(opentracing.GlobalTracer()))
		stream = append(stream, otgrpc.OpenTracingStreamClientInterceptor(opentracing.GlobalTracer()))
	}
	return append(unary,
			func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
				if noAuthMethods[method] {
					return invoker(ctx, method, req, reply, cc, opts...)
				}
				return middleware.ClientUserHeaderInterceptor(ctx, method, req, reply, cc, invoker, opts...)
			},
			middleware.UnaryClientInstrumentInterceptor(requestDuration, instrumentationLabelOptions...),
		),
		append(stream,
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				if noAuthMethods[method] {
					return streamer(ctx, desc, cc, method, opts...)
				}
				return middleware.StreamClientUserHeaderInterceptor(ctx, desc, cc, method, streamer, opts...)
			},
			middleware.StreamClientInstrumentInterceptor(requestDuration, instrumentationLabelOptions...),
		)
}
