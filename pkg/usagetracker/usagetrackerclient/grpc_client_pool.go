// SPDX-License-Identifier: AGPL-3.0-only

package usagetrackerclient

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/user"
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
		Name:                        "cortex_usage_tracker_client_request_duration_seconds",
		Help:                        "Time spent executing  a single request to a usage-tracker instance.",
		NativeHistogramBucketFactor: 1.1,
		ConstLabels:                 prometheus.Labels{"client": clientName},
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

	c := &usageTrackerClient{
		UsageTrackerClient: usagetrackerpb.NewUsageTrackerClient(conn),
		HealthClient:       grpc_health_v1.NewHealthClient(conn),
		conn:               conn,

		logger: logger,

		flushDelay:   100 * time.Millisecond,
		maxBatchSize: 1000, // Flush when batch reaches this size
	}

	go c.flusher()

	return c, nil
}

type usageTrackerClient struct {
	usagetrackerpb.UsageTrackerClient
	grpc_health_v1.HealthClient
	conn *grpc.ClientConn

	logger log.Logger

	mu           sync.Mutex
	pending      []*usagetrackerpb.TrackSeriesSubrequest
	flushTimer   *time.Timer
	flushDelay   time.Duration
	maxBatchSize int
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

// AsyncTrackSeries queues a TrackSeries request to be batched and flushed on a timer.
// This method is non-blocking and does not return rejected series.
func (c *usageTrackerClient) AsyncTrackSeries(ctx context.Context, req *usagetrackerpb.TrackSeriesRequest) {
	// Convert the request to a subrequest for batching.
	subreq := &usagetrackerpb.TrackSeriesSubrequest{
		UserID:       req.UserID,
		Partition:    req.Partition,
		SeriesHashes: req.SeriesHashes,
	}

	c.mu.Lock()
	c.pending = append(c.pending, subreq)

	// Flush if batch size is reached.
	if len(c.pending) >= c.maxBatchSize {
		c.mu.Unlock()
		c.flush()
	} else {
		c.mu.Unlock()
	}
}

func (c *usageTrackerClient) flusher() {
	// Dangles for now. Fix later.

	for {
		time.Sleep(util.DurationWithJitter(c.flushDelay, 0.15))
		c.flush()
	}
}

// flush sends all pending requests as a single batched request.
func (c *usageTrackerClient) flush() {
	c.mu.Lock()

	if len(c.pending) == 0 {
		c.mu.Unlock()
		return
	}

	// Create a batched request with all pending subrequests.
	batchReq := &usagetrackerpb.TrackSeriesRequest{
		Subrequests: slices.Clone(c.pending),
	}

	// Clear pending requests and reset timer.
	pending := c.pending
	c.pending = nil

	// Release the lock before making the gRPC call.
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Just trying to make things succeed.
	tctx := user.InjectOrgID(ctx, "100")

	_, err := c.TrackSeries(tctx, batchReq)
	if err != nil {
		level.Warn(c.logger).Log(
			"msg", "failed to send batched TrackSeries request",
			"batch_size", len(pending),
			"err", err,
		)
	} else {
		level.Debug(c.logger).Log(
			"msg", "successfully sent batched TrackSeries request",
			"batch_size", len(pending),
		)
	}
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
