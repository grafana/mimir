// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
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
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/grpcencoding/s2"
)

const (
	remoteDistributorPushTimeout = 10 * time.Second
)

// DistributorConfig defines distributor transport configuration for ruler writes.
type DistributorConfig struct {
	// Address is the gRPC address of the distributor(s) to push rule-result series to.
	Address string `yaml:"address" category:"experimental"`

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
	c.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	c.GRPCClientConfig.RegisterFlagsWithPrefix("ruler.distributor.grpc-client-config", f)
}

func (c *DistributorConfig) Validate() error {
	c.GRPCClientConfig.CustomCompressors = []string{s2.Name}
	if err := c.GRPCClientConfig.Validate(); err != nil {
		return err
	}

	if c.Address == "" {
		return nil
	}

	if strings.HasPrefix(c.Address, "http://") || strings.HasPrefix(c.Address, "https://") {
		return fmt.Errorf("address must be a gRPC address, got HTTP(S) address: %q", c.Address)
	}

	// Make sure the DNS prefix is correct (three slashes) if it is being used.
	// This is a gRPC specific requirement/format when using service discovery.
	if strings.HasPrefix(c.Address, "dns://") && !strings.HasPrefix(c.Address, "dns:///") {
		return fmt.Errorf(`address must have "dns:///" prefix when using gRPC service discovery, got: %q`, c.Address)
	}

	return nil
}

type distributorPushConfig struct {
	backoff backoff.Config
	timeout time.Duration
}

// DistributorGRPCClient is a gRPC client used by rulers to push rule evaluation
// results to distributors.
type DistributorGRPCClient struct {
	distributorpb.DistributorClient

	conn     *grpc.ClientConn
	logger   log.Logger
	pushCfg  distributorPushConfig
	close    sync.Once
	closeErr error
}

func NewDistributorGRPCClient(cfg DistributorConfig, reg prometheus.Registerer, logger log.Logger) (*DistributorGRPCClient, error) {
	return newDistributorGRPCClient(cfg, reg, logger, distributorPushConfig{
		backoff: cfg.GRPCClientConfig.BackoffConfig,
		timeout: remoteDistributorPushTimeout,
	})
}

func newDistributorGRPCClient(cfg DistributorConfig, reg prometheus.Registerer, logger log.Logger, pushCfg distributorPushConfig) (*DistributorGRPCClient, error) {
	invalidClusterValidation := util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "ruler-distributor", util.GRPCProtocol)
	opts, err := cfg.GRPCClientConfig.DialOption(
		[]grpc.UnaryClientInterceptor{
			middleware.ClientUserHeaderInterceptor,
		},
		nil,
		util.NewInvalidClusterValidationReporter(cfg.GRPCClientConfig.ClusterValidation.Label, invalidClusterValidation, logger),
	)
	if err != nil {
		return nil, err
	}
	opts = append(opts, grpc.WithDefaultServiceConfig(serviceConfig))
	opts = append(opts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	// nolint:staticcheck // grpc.Dial() has been deprecated; we'll address it before upgrading to gRPC 2.
	conn, err := grpc.Dial(cfg.Address, opts...)
	if err != nil {
		return nil, err
	}

	return &DistributorGRPCClient{
		conn:              conn,
		DistributorClient: distributorpb.NewDistributorClient(conn),
		logger:            logger,
		pushCfg:           pushCfg,
	}, nil
}

// Push consumes req and releases its pooled resources when done, matching the
// ownership contract implemented by the in-process distributor. PusherAppender
// builds requests from pools and relies on the configured Pusher to clean them up.
func (c *DistributorGRPCClient) Push(ctx context.Context, req *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	defer func() {
		req.FreeBuffer()
		mimirpb.ReuseSlice(req.Timeseries)
	}()

	pushAttempt := func() (*mimirpb.WriteResponse, error) {
		attemptCtx, cancel := context.WithTimeout(ctx, c.pushCfg.timeout)
		defer cancel()
		return c.DistributorClient.Push(attemptCtx, req)
	}

	attempt := 1
	resp, err := pushAttempt()
	if err == nil {
		return resp, nil
	}
	if !isRetryableDistributorPushError(err) {
		return nil, err
	}

	retry := backoff.New(ctx, c.pushCfg.backoff)
	for retry.Ongoing() {
		delay := retry.NextDelay()
		level.Warn(c.logger).Log("msg", "failed to remotely push rule evaluation results, will retry", "err", err, "attempt", attempt, "max_retries", c.pushCfg.backoff.MaxRetries, "retry_delay", delay)

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}

		attempt++
		resp, err = pushAttempt()
		if err == nil {
			return resp, nil
		}
		if !isRetryableDistributorPushError(err) {
			return nil, err
		}
	}

	if retryErr := retry.Err(); retryErr != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		level.Debug(c.logger).Log("msg", "stopped retrying remote distributor push", "err", retryErr)
	}

	return nil, err
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
	if c == nil || c.conn == nil {
		return nil
	}
	c.close.Do(func() {
		c.closeErr = c.conn.Close()
	})
	return c.closeErr
}
