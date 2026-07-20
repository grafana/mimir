// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/grafana/dskit/grpcclient"
	"github.com/grafana/dskit/grpcencoding/snappy"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcgzip "google.golang.org/grpc/encoding/gzip"

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
	if _, err := maxUncompressedPayloadSize(c.GRPCClientConfig.MaxSendMsgSize, c.GRPCClientConfig.GRPCCompression); err != nil {
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
	requestsPerWriteRequest  prometheus.Histogram

	mu     sync.RWMutex
	conn   *grpc.ClientConn
	client distributorpb.DistributorClient
	// maxWriteRequestSize is the largest uncompressed protobuf payload guaranteed
	// to fit within the configured gRPC send limit after compression.
	maxWriteRequestSize int
}

func NewDistributorGRPCClient(cfg DistributorConfig, reg prometheus.Registerer, logger log.Logger) (*DistributorGRPCClient, error) {
	c := &DistributorGRPCClient{
		logger:                   logger,
		cfg:                      cfg,
		invalidClusterValidation: util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "ruler-distributor", util.GRPCProtocol),
		requestsPerWriteRequest: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_ruler_remote_distributor_requests_per_write_request",
			Help:    "The number of remote distributor requests a single ruler write request has been split into.",
			Buckets: prometheus.ExponentialBuckets(1, 2, 8),
		}),
	}
	c.Service = services.NewIdleService(c.start, c.stop).WithName("ruler distributor client")
	return c, nil
}

func (c *DistributorGRPCClient) start(context.Context) error {
	if err := c.cfg.Validate(); err != nil {
		return err
	}
	maxWriteRequestSize, err := maxUncompressedPayloadSize(c.cfg.GRPCClientConfig.MaxSendMsgSize, c.cfg.GRPCClientConfig.GRPCCompression)
	if err != nil {
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
	c.maxWriteRequestSize = maxWriteRequestSize
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
	maxWriteRequestSize := c.maxWriteRequestSize
	c.mu.RUnlock()
	if client == nil {
		return nil, errDistributorClientNotRunning
	}

	requests := splitWriteRequest(req, maxWriteRequestSize)
	c.requestsPerWriteRequest.Observe(float64(len(requests)))

	var resp *mimirpb.WriteResponse
	for idx, request := range requests {
		var err error
		resp, err = c.pushWithRetries(ctx, client, request, idx+1, len(requests))
		if err != nil {
			return nil, err
		}
	}

	return resp, nil
}

func splitWriteRequest(req *mimirpb.WriteRequest, maxSize int) []*mimirpb.WriteRequest {
	// The ruler currently creates Remote Write 1.0 requests. Preserve an RW2
	// request instead of partially splitting an unsupported shape.
	if maxSize <= 0 || len(req.TimeseriesRW2) > 0 {
		return []*mimirpb.WriteRequest{req}
	}

	reqSize := req.Size()
	if reqSize <= maxSize {
		return []*mimirpb.WriteRequest{req}
	}

	requests := mimirpb.SplitWriteRequestByMaxMarshalSize(req, reqSize, maxSize)
	if len(requests) == 0 {
		return []*mimirpb.WriteRequest{req}
	}
	return requests
}

func (c *DistributorGRPCClient) pushWithRetries(ctx context.Context, client distributorpb.DistributorClient, req *mimirpb.WriteRequest, requestNumber, totalRequests int) (*mimirpb.WriteResponse, error) {
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
		level.Warn(c.logger).Log("msg", "failed to write to remote distributor", "err", err, "retryable", retryable, "attempt", retry.NumRetries()+1, "max_attempts", maxAttempts, "request", requestNumber, "requests", totalRequests)
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
	c.maxWriteRequestSize = 0
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close()
}

const (
	// The common bound covers the gzip header, trailer, and final empty DEFLATE
	// block, plus the per-block overhead of gzip, framed Snappy, and framed S2.
	// Go's default DEFLATE writer can fill a block after 1<<14 literal tokens,
	// which is the smallest block size among these compressors.
	compressedPayloadFixedOverhead    = 23
	compressedPayloadBlockSize        = 1 << 14
	compressedPayloadMaxBlockOverhead = 8
)

func maxUncompressedPayloadSize(maxSendMsgSize int, compression string) (int, error) {
	switch compression {
	case "":
	case grpcgzip.Name, snappy.Name, s2.Name:
	default:
		return 0, fmt.Errorf("compression type %q has no payload-size bound", compression)
	}
	if maxSendMsgSize <= 0 {
		return 0, nil
	}
	if compression == "" {
		return maxSendMsgSize, nil
	}

	low, high := 0, maxSendMsgSize
	for low < high {
		mid := low + (high-low)/2
		if mid == low {
			mid++
		}

		if compressedPayloadFits(mid, maxSendMsgSize) {
			low = mid
		} else {
			high = mid - 1
		}
	}

	return low, nil
}

func compressedPayloadFits(uncompressedSize, maxCompressedSize int) bool {
	upperBound, ok := compressedPayloadSizeUpperBound(uncompressedSize)
	return ok && upperBound <= maxCompressedSize
}

func compressedPayloadSizeUpperBound(uncompressedSize int) (int, bool) {
	if uncompressedSize < 0 || uncompressedSize > math.MaxInt-compressedPayloadFixedOverhead {
		return 0, false
	}

	blocks := uncompressedSize / compressedPayloadBlockSize
	if uncompressedSize%compressedPayloadBlockSize != 0 {
		blocks++
	}

	remaining := math.MaxInt - uncompressedSize - compressedPayloadFixedOverhead
	if blocks > remaining/compressedPayloadMaxBlockOverhead {
		return 0, false
	}

	return uncompressedSize + compressedPayloadFixedOverhead + blocks*compressedPayloadMaxBlockOverhead, true
}
