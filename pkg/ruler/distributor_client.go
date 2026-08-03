// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
)

// DistributorConfig defines distributor transport configuration for ruler writes.
type DistributorConfig struct {
	// Address is the gRPC address of the distributor(s) to push rule-result series to.
	Address string `yaml:"address" category:"experimental"`

	// RemoteTimeout is the timeout for a push request to remote distributors.
	RemoteTimeout time.Duration `yaml:"remote_timeout" category:"experimental"`

	// GRPCClientConfig contains gRPC specific config options.
	GRPCClientConfig grpcclient.Config `yaml:"grpc_client_config" doc:"description=Advanced standard gRPC client configuration used by rulers to communicate with distributors."`
}

func (c *DistributorConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(
		&c.Address,
		"ruler.distributor.address",
		"",
		"gRPC listen address of the distributor(s) to push rule-result series to. If empty, the ruler writes using the internal distributor. "+
			"Use a DNS address (prefixed with dns:///) to enable gRPC client-side load balancing; in Kubernetes, use the distributor headless service on the gRPC port.",
	)
	f.DurationVar(
		&c.RemoteTimeout,
		"ruler.distributor.remote-timeout",
		10*time.Second,
		"Timeout for requests to remote distributors.",
	)
	c.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	c.GRPCClientConfig.RegisterFlagsWithPrefix("ruler.distributor.grpc-client-config", f)
}

func (c *DistributorConfig) Validate() error {
	if err := c.GRPCClientConfig.Validate(); err != nil {
		return fmt.Errorf("ruler's distributor client gRPC settings: %w", err)
	}

	if c.Address == "" {
		return nil
	}

	if c.RemoteTimeout <= 0 {
		return fmt.Errorf("remote timeout must be greater than 0")
	}

	if strings.HasPrefix(c.Address, "http://") || strings.HasPrefix(c.Address, "https://") {
		return fmt.Errorf("ruler's distributor client address must be a gRPC address, got HTTP(S) address: %q", c.Address)
	}

	// Make sure the DNS prefix is correct (three slashes) if it is being used.
	// This is a gRPC specific requirement/format when using service discovery.
	if strings.HasPrefix(c.Address, "dns://") && !strings.HasPrefix(c.Address, "dns:///") {
		return fmt.Errorf(`ruler's distributor client address must have "dns:///" prefix when using gRPC service discovery, got: %q`, c.Address)
	}

	return nil
}

var errDistributorClientNotRunning = errors.New("ruler distributor client is not running")

// DistributorGRPCClient is a gRPC client used by rulers to push rule evaluation
// results to distributors.
type DistributorGRPCClient struct {
	services.Service

	logger                   log.Logger
	cfg                      DistributorConfig
	invalidClusterValidation *prometheus.CounterVec

	mu     sync.RWMutex
	conn   *grpc.ClientConn
	client distributorpb.DistributorClient
}

func NewDistributorGRPCClient(cfg DistributorConfig, reg prometheus.Registerer, logger log.Logger) (*DistributorGRPCClient, error) {
	c := &DistributorGRPCClient{
		logger:                   logger,
		cfg:                      cfg,
		invalidClusterValidation: util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "ruler-distributor", util.GRPCProtocol),
	}
	c.Service = services.NewIdleService(c.start, c.stop).WithName("ruler distributor client")
	return c, nil
}

func (c *DistributorGRPCClient) start(context.Context) error {
	if err := c.cfg.Validate(); err != nil {
		return err
	}

	opts, err := c.cfg.GRPCClientConfig.DialOption(
		[]grpc.UnaryClientInterceptor{
			middleware.ClientUserHeaderInterceptor,
		},
		nil,
		util.NewInvalidClusterValidationReporter(c.cfg.GRPCClientConfig.ClusterValidation.Label, c.invalidClusterValidation, c.logger),
	)
	if err != nil {
		return err
	}
	opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(c.cfg.Address, opts...)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	c.client = distributorpb.NewDistributorClient(conn)
	c.mu.Unlock()

	return nil
}

func (c *DistributorGRPCClient) stop(error) error {
	return c.Close()
}

// Push consumes req and releases its pooled resources when done, matching the
// ownership contract implemented by the in-process distributor. PusherAppender
// builds requests from pools and relies on the configured Pusher to clean them up.
func (c *DistributorGRPCClient) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	defer func() {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
	}()

	if c == nil {
		return nil, errDistributorClientNotRunning
	}

	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return nil, errDistributorClientNotRunning
	}

	pushAttempt := func() (*mimirpb.WriteResponse, error) {
		attemptCtx, cancel := context.WithTimeout(ctx, c.cfg.RemoteTimeout)
		defer cancel()
		return client.Push(attemptCtx, req)
	}

	maxAttempts := c.cfg.GRPCClientConfig.BackoffConfig.MaxRetries
	retry := backoff.New(ctx, c.cfg.GRPCClientConfig.BackoffConfig)
	var err error
	for retry.Ongoing() {
		var resp *mimirpb.WriteResponse
		resp, err = pushAttempt()
		if err == nil {
			return resp, nil
		}

		retryable := isRetryableDistributorPushError(err)
		level.Warn(c.logger).Log("msg", "failed to write to remote distributor", "err", err, "retryable", retryable, "attempt", retry.NumRetries()+1, "max_attempts", maxAttempts)
		if !retryable {
			return nil, err
		}

		retry.Wait()
	}
	if err != nil {
		// A retryable push error occurred, but either we reached max attempts or the context was canceled.
		return nil, err
	}
	// No push attempt was made because backoff was canceled before the first try.
	return nil, retry.Err()
}

func isRetryableDistributorPushError(err error) bool {
	if mimirpb.IsClientError(err) {
		return false
	}

	status, ok := grpcutil.ErrorToStatus(err)
	if !ok {
		return false
	}

	switch status.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal:
		return true
	default:
		return false
	}
}

func (c *DistributorGRPCClient) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.client = nil
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close()
}
