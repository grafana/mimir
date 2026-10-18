// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"golang.org/x/time/rate"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip" // Register the gzip compressor so clients can compress OTLP gRPC requests.
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// otlpGRPCLogsPerSecond and otlpGRPCLogsBurst bound the rate of error logs emitted by the receiver,
	// so that a misbehaving client can't flood the logs.
	otlpGRPCLogsPerSecond = 10
	otlpGRPCLogsBurst     = 10

	// otlpGRPCShutdownTimeout is how long in-flight requests are given to complete before connections are forcibly closed.
	otlpGRPCShutdownTimeout = 30 * time.Second

	// retryAttemptMetadataKey is the gRPC metadata equivalent of the Retry-Attempt HTTP header.
	retryAttemptMetadataKey = "retry-attempt"

	// otlpGRPCUnprocessedRequestTimeout is how long an accepted request whose context is done is given to start being
	// processed by gRPC before it's released from the inflight push request limits. It matches dskit's gRPC server.
	otlpGRPCUnprocessedRequestTimeout = 10 * time.Second

	// otlpGRPCInfinity is the default for the connection lifetime settings, matching the -server.grpc.keepalive.* defaults.
	otlpGRPCInfinity = time.Duration(math.MaxInt64)
)

// OTLPGRPCConfig configures the dedicated OTLP gRPC receiver.
type OTLPGRPCConfig struct {
	ListenAddress string `yaml:"listen_address" category:"experimental"`
	ListenPort    int    `yaml:"listen_port" category:"experimental"`

	TLSCertPath     string `yaml:"tls_cert_path" category:"experimental"`
	TLSKeyPath      string `yaml:"tls_key_path" category:"experimental"`
	TLSClientCAPath string `yaml:"tls_client_ca_path" category:"experimental"`

	MaxConnectionIdle        time.Duration `yaml:"max_connection_idle" category:"experimental"`
	MaxConnectionAge         time.Duration `yaml:"max_connection_age" category:"experimental"`
	MaxConnectionAgeGrace    time.Duration `yaml:"max_connection_age_grace" category:"experimental"`
	KeepaliveTime            time.Duration `yaml:"keepalive_time" category:"experimental"`
	KeepaliveTimeout         time.Duration `yaml:"keepalive_timeout" category:"experimental"`
	KeepaliveMinPingInterval time.Duration `yaml:"keepalive_min_time_between_pings" category:"experimental"`
	KeepalivePingWithoutRPC  bool          `yaml:"keepalive_ping_without_stream_allowed" category:"experimental"`
}

// RegisterFlagsWithPrefix registers the flags with the given prefix.
func (cfg *OTLPGRPCConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.ListenAddress, prefix+"listen-address", "", "Address the OTLP gRPC receiver listens on. If empty, it listens on all interfaces.")
	f.IntVar(&cfg.ListenPort, prefix+"listen-port", 0, "Port the OTLP gRPC receiver listens on. 0 disables the receiver. The standard OTLP gRPC port is 4317.")
	f.StringVar(&cfg.TLSCertPath, prefix+"tls-cert-path", "", "OTLP gRPC receiver TLS server cert path. If empty, TLS is disabled.")
	f.StringVar(&cfg.TLSKeyPath, prefix+"tls-key-path", "", "OTLP gRPC receiver TLS server key path. Required when the cert path is set.")
	f.StringVar(&cfg.TLSClientCAPath, prefix+"tls-client-ca-path", "", "OTLP gRPC receiver TLS Client CA path. If set, clients must present a valid certificate (mutual TLS).")
	f.DurationVar(&cfg.MaxConnectionIdle, prefix+"keepalive.max-connection-idle", otlpGRPCInfinity, "The duration after which an idle connection should be closed. Default: infinity")
	f.DurationVar(&cfg.MaxConnectionAge, prefix+"keepalive.max-connection-age", otlpGRPCInfinity, "The duration for the maximum amount of time a connection may exist before it will be closed. Default: infinity")
	f.DurationVar(&cfg.MaxConnectionAgeGrace, prefix+"keepalive.max-connection-age-grace", otlpGRPCInfinity, "An additive period after max-connection-age after which the connection will be forcibly closed. Default: infinity")
	f.DurationVar(&cfg.KeepaliveTime, prefix+"keepalive.time", 2*time.Hour, "Duration after which a keepalive probe is sent in case of no activity over the connection. Default: 2h")
	f.DurationVar(&cfg.KeepaliveTimeout, prefix+"keepalive.timeout", 20*time.Second, "After having pinged for keepalive check, the duration after which an idle connection should be closed. Default: 20s")
	f.DurationVar(&cfg.KeepaliveMinPingInterval, prefix+"keepalive.min-time-between-pings", 10*time.Second, "Minimum amount of time a client should wait before sending a keepalive ping. If client sends keepalive ping more often, server will send GOAWAY and close the connection.")
	f.BoolVar(&cfg.KeepalivePingWithoutRPC, prefix+"keepalive.ping-without-stream-allowed", true, "If true, server allows keepalive pings even when there are no active streams(RPCs). If false, and client sends ping when there are no active streams, server will send GOAWAY and close the connection.")
}

// Enabled returns whether the OTLP gRPC receiver is enabled.
func (cfg *OTLPGRPCConfig) Enabled() bool {
	return cfg.ListenPort > 0
}

// Validate validates the configuration.
func (cfg *OTLPGRPCConfig) Validate() error {
	if (cfg.TLSCertPath == "") != (cfg.TLSKeyPath == "") {
		return errors.New("the OTLP gRPC receiver TLS certificate and key must be set together")
	}
	if cfg.TLSClientCAPath != "" && cfg.TLSCertPath == "" {
		return errors.New("the OTLP gRPC receiver TLS client CA requires the TLS certificate and key to be set")
	}
	return nil
}

func (cfg *OTLPGRPCConfig) serverCredentials() (credentials.TransportCredentials, error) {
	if cfg.TLSCertPath == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.TLSCertPath, cfg.TLSKeyPath)
	if err != nil {
		return nil, errors.Wrap(err, "load OTLP gRPC receiver TLS certificate")
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if cfg.TLSClientCAPath != "" {
		caPEM, err := os.ReadFile(cfg.TLSClientCAPath)
		if err != nil {
			return nil, errors.Wrap(err, "read OTLP gRPC receiver TLS client CA")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("no valid certificate found in the OTLP gRPC receiver TLS client CA")
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), nil
}

// OTLPGRPCRetryInfoLimits provides the per-tenant setting controlling whether gRPC errors include retry information.
type OTLPGRPCRetryInfoLimits interface {
	OTLPGRPCRetryInfoEnabled(tenantID string) bool
}

// InflightPushRequestLimiter accounts push requests against the distributor instance limits. It is implemented by Distributor.
type InflightPushRequestLimiter interface {
	// StartPushRequest is called before the request is read into memory. The request size is 0 when it isn't known yet.
	StartPushRequest(ctx context.Context, requestSize int64) (context.Context, error)
	// FinishPushRequest releases a request accepted by StartPushRequest.
	FinishPushRequest(ctx context.Context)
}

// otlpGRPCInflightLimiter implements gRPC tap.ServerInHandle and stats.Handler. It rejects Export calls exceeding the
// distributor inflight push request limits before the request is read into memory, like the main gRPC server does for
// push requests (see dskit's grpcInflightLimitCheck), so that an overloaded distributor doesn't buffer and unmarshal
// requests it's going to reject anyway.
type otlpGRPCInflightLimiter struct {
	limiter InflightPushRequestLimiter
}

type otlpGRPCInflightStateKey struct{}

type otlpGRPCInflightState struct {
	processingStarted chan struct{}
	startedOnce       sync.Once
	finishedOnce      sync.Once
	stopWatchingCtx   func() bool
}

// TapHandle is called by gRPC once the request headers have been received, before the request message is read.
func (l *otlpGRPCInflightLimiter) TapHandle(ctx context.Context, info *tap.Info) (context.Context, error) {
	if info.FullMethodName != colmetricpb.MetricsService_Export_FullMethodName {
		return ctx, nil
	}

	// The request size isn't known before the message is read. The size of the resulting write request is checked
	// by the push path once the request has been converted.
	ctx, err := l.limiter.StartPushRequest(ctx, 0)
	if err != nil {
		return ctx, status.Error(codes.Unavailable, err.Error())
	}

	state := &otlpGRPCInflightState{processingStarted: make(chan struct{})}
	ctx = context.WithValue(ctx, otlpGRPCInflightStateKey{}, state)

	// gRPC drops a stream whose context is already done right after the tap handle ran, without ever calling the stats
	// handler, so the request would never be released. When the context is done before processing started (signalled
	// by stats.InHeader), give gRPC some time to start processing anyway and release the request otherwise. Requests
	// whose processing started are released by the stats.End event.
	state.stopWatchingCtx = context.AfterFunc(ctx, func() {
		select {
		case <-state.processingStarted:
		case <-time.After(otlpGRPCUnprocessedRequestTimeout):
			l.finish(ctx, state)
		}
	})
	return ctx, nil
}

func (l *otlpGRPCInflightLimiter) finish(ctx context.Context, state *otlpGRPCInflightState) {
	state.finishedOnce.Do(func() { l.limiter.FinishPushRequest(ctx) })
}

// TagRPC implements stats.Handler.
func (l *otlpGRPCInflightLimiter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC implements stats.Handler.
func (l *otlpGRPCInflightLimiter) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	state, ok := ctx.Value(otlpGRPCInflightStateKey{}).(*otlpGRPCInflightState)
	if !ok {
		return
	}
	switch rpcStats.(type) {
	case *stats.InHeader:
		state.startedOnce.Do(func() { close(state.processingStarted) })
	case *stats.End:
		state.stopWatchingCtx()
		l.finish(ctx, state)
	}
}

// TagConn implements stats.Handler.
func (l *otlpGRPCInflightLimiter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

// HandleConn implements stats.Handler.
func (l *otlpGRPCInflightLimiter) HandleConn(context.Context, stats.ConnStats) {}

// OTLPGRPCServer is a dedicated gRPC server exposing the OTLP metrics service.
type OTLPGRPCServer struct {
	services.Service

	cfg      OTLPGRPCConfig
	opts     []grpc.ServerOption
	receiver *otlpGRPCReceiver
	server   *grpc.Server
	listener net.Listener
	logger   log.Logger
}

// NewOTLPGRPCServer returns a service running a dedicated gRPC server that receives OTLP metrics.
// The authInterceptor is responsible for injecting the tenant ID in the request context. If inflightLimiter is set,
// Export calls are accounted against the distributor inflight push request limits before they're read into memory.
func NewOTLPGRPCServer(
	cfg OTLPGRPCConfig,
	maxRecvMsgSize int,
	authInterceptor grpc.UnaryServerInterceptor,
	inflightLimiter InflightPushRequestLimiter,
	limits OTLPHandlerLimits,
	retryInfoLimits OTLPGRPCRetryInfoLimits,
	resourceAttributePromotionConfig OTelResourceAttributePromotionConfig,
	keepIdentifyingOTelResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig,
	otlpPushMiddlewares []OTLPPushMiddleware,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	logger log.Logger,
) *OTLPGRPCServer {
	receiver := &otlpGRPCReceiver{
		maxRecvMsgSize:                          maxRecvMsgSize,
		limits:                                  limits,
		resourceAttributePromotionConfig:        resourceAttributePromotionConfig,
		keepIdentifyingResourceAttributesConfig: keepIdentifyingOTelResourceAttributesConfig,
		otlpPushMiddlewares:                     otlpPushMiddlewares,
		push:                                    push,
		pushMetrics:                             pushMetrics,
		retryCfg:                                retryCfg,
		retryInfoLimits:                         retryInfoLimits,
		logLimiter:                              rate.NewLimiter(otlpGRPCLogsPerSecond, otlpGRPCLogsBurst),
		logger:                                  logger,
	}

	s := &OTLPGRPCServer{
		cfg: cfg,
		opts: []grpc.ServerOption{
			grpc.MaxRecvMsgSize(maxRecvMsgSize),
			grpc.ChainUnaryInterceptor(receiver.recoveryInterceptor, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				authenticated := false
				resp, err := authInterceptor(ctx, req, info, func(ctx context.Context, req any) (any, error) {
					authenticated = true
					return handler(ctx, req)
				})
				if err != nil && !authenticated {
					// The tenant couldn't be determined: report it as an authentication failure.
					return nil, grpcStatusError(codes.Unauthenticated, err.Error())
				}
				return resp, err
			}),
		},
		logger: logger,
	}
	if inflightLimiter != nil {
		l := &otlpGRPCInflightLimiter{limiter: inflightLimiter}
		s.opts = append(s.opts, grpc.InTapHandle(l.TapHandle), grpc.StatsHandler(l))
	}
	s.receiver = receiver
	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s
}

func (s *OTLPGRPCServer) starting(context.Context) error {
	creds, err := s.cfg.serverCredentials()
	if err != nil {
		return err
	}
	opts := append(slices.Clone(s.opts),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     s.cfg.MaxConnectionIdle,
			MaxConnectionAge:      s.cfg.MaxConnectionAge,
			MaxConnectionAgeGrace: s.cfg.MaxConnectionAgeGrace,
			Time:                  s.cfg.KeepaliveTime,
			Timeout:               s.cfg.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             s.cfg.KeepaliveMinPingInterval,
			PermitWithoutStream: s.cfg.KeepalivePingWithoutRPC,
		}),
	)
	if creds != nil {
		opts = append(opts, grpc.Creds(creds))
	}
	s.server = grpc.NewServer(opts...)
	colmetricpb.RegisterMetricsServiceServer(s.server, s.receiver)

	addr := net.JoinHostPort(s.cfg.ListenAddress, strconv.Itoa(s.cfg.ListenPort))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrapf(err, "listen on OTLP gRPC address %s", addr)
	}
	s.listener = lis
	level.Info(s.logger).Log("msg", "OTLP gRPC receiver listening", "addr", lis.Addr().String())
	return nil
}

func (s *OTLPGRPCServer) running(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() { errCh <- s.server.Serve(s.listener) }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

// stopping gracefully stops the server, forcibly closing the remaining connections after otlpGRPCShutdownTimeout.
func (s *OTLPGRPCServer) stopping(error) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(otlpGRPCShutdownTimeout):
		level.Warn(s.logger).Log("msg", "timed out gracefully stopping the OTLP gRPC receiver, forcing shutdown")
		s.server.Stop()
		<-done
	}
	return nil
}

// Addr returns the address the server listens on. It is only valid once the service has started.
func (s *OTLPGRPCServer) Addr() net.Addr {
	return s.listener.Addr()
}

type otlpGRPCReceiver struct {
	colmetricpb.UnimplementedMetricsServiceServer

	maxRecvMsgSize                          int
	limits                                  OTLPHandlerLimits
	resourceAttributePromotionConfig        OTelResourceAttributePromotionConfig
	keepIdentifyingResourceAttributesConfig KeepIdentifyingOTelResourceAttributesConfig
	otlpPushMiddlewares                     []OTLPPushMiddleware
	push                                    PushFunc
	pushMetrics                             *PushMetrics
	retryCfg                                RetryConfig
	retryInfoLimits                         OTLPGRPCRetryInfoLimits
	logLimiter                              *rate.Limiter
	logger                                  log.Logger
}

// Export implements colmetricpb.MetricsServiceServer.
func (r *otlpGRPCReceiver) Export(ctx context.Context, in *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	start := time.Now()
	resp, err := r.export(ctx, in)

	// The validated tenant ID is used as label to keep the cardinality bounded.
	if tenantID, tenantErr := tenant.TenantID(ctx); tenantErr == nil {
		r.pushMetrics.ObserveOTLPGRPCRequestDuration(tenantID, status.Code(err).String(), time.Since(start))
	}
	return resp, err
}

// recoveryInterceptor turns a panic while handling a request into an Internal error, so that a single bad request
// (for example, one hitting an inconsistent tenant configuration) can't crash the process.
func (r *otlpGRPCReceiver) recoveryInterceptor(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if p := recover(); p != nil {
			r.log(utillog.WithContext(ctx, r.logger), level.Error, "msg", "recovered from panic while handling OTLP gRPC request", "panic", p, "stack", string(debug.Stack()))
			resp, err = nil, grpcStatusError(codes.Internal, fmt.Sprintf("panic while handling request: %v", p))
		}
	}()
	return handler(ctx, req)
}

// log emits a log line unless the log rate limit has been reached.
func (r *otlpGRPCReceiver) log(logger log.Logger, lvl func(log.Logger) log.Logger, keyvals ...any) {
	if r.logLimiter.Allow() {
		_ = lvl(logger).Log(keyvals...)
	}
}

// errorWithRetryInfo returns a gRPC error for the given code. For the retryable codes it also includes a RetryInfo
// detail, which is what OTLP clients use to back off (and, for ResourceExhausted, to decide whether to retry at all).
//
// Omitting the RetryInfo is deliberate when it is disabled globally or for the tenant: per the OTLP specification,
// clients treat a ResourceExhausted error without RetryInfo as non-retryable and drop the data, so disabling it is a
// way to shed an over-limit tenant's traffic instead of having clients retry it. This mirrors Tempo's behaviour.
func (r *otlpGRPCReceiver) errorWithRetryInfo(ctx context.Context, code codes.Code, msg string) error {
	st := status.New(code, validUTF8Message(msg))
	if !r.retryCfg.Enabled || (code != codes.ResourceExhausted && code != codes.Unavailable) {
		return st.Err()
	}
	if tenantID, err := tenant.TenantID(ctx); err != nil || !r.retryInfoLimits.OTLPGRPCRetryInfoEnabled(tenantID) {
		return st.Err()
	}

	var retryAttempt string
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get(retryAttemptMetadataKey); len(values) > 0 {
			retryAttempt = values[0]
		}
	}
	seconds, err := strconv.Atoi(calculateRetryAfter(retryAttempt, r.retryCfg.MinBackoff, r.retryCfg.MaxBackoff))
	if err != nil {
		return st.Err()
	}

	withDetails, err := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(time.Duration(seconds) * time.Second)})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func (r *otlpGRPCReceiver) export(ctx context.Context, in *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	logger := utillog.WithContext(ctx, r.logger)

	otlpConverter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
	var schemeOverride *model.ValidationScheme
	convert := newOTLPConvertFunc(
		false, // Translation headers are only supported by the HTTP receiver.
		r.limits, r.resourceAttributePromotionConfig, r.keepIdentifyingResourceAttributesConfig,
		otlpConverter, r.pushMetrics, r.otlpPushMiddlewares, &schemeOverride,
	)

	supplier := func() (*mimirpb.WriteRequest, func(), int, error) {
		data, err := proto.Marshal(in)
		if err != nil {
			return nil, nil, 0, grpcStatusError(codes.InvalidArgument, fmt.Sprintf("marshal OTLP request: %s", err))
		}

		otlpReq := pmetricotlp.NewExportRequest()
		if err := otlpReq.UnmarshalProto(data); err != nil {
			return nil, nil, 0, grpcStatusError(codes.InvalidArgument, fmt.Sprintf("unmarshal OTLP request: %s", err))
		}

		spanLogger, spanCtx := spanlogger.New(ctx, logger, tracer, "Distributor.OTLPGRPCReceiver.decodeAndConvert")
		defer spanLogger.Finish()

		var wr mimirpb.PreallocWriteRequest
		size, err := convert(spanCtx, otlpReq, otlpConvertParams{
			contentType: pbContentType,
			// The compressed wire size isn't known once gRPC has decompressed the message. Leaving it at 0
			// skips the compression ratio observation, which would otherwise always be 1.
			contentLength:        0,
			uncompressedBodySize: len(data),
		}, &wr, logger, spanLogger)
		if err != nil {
			if _, ok := grpcutil.ErrorToStatus(err); !ok {
				err = grpcStatusError(codes.InvalidArgument, err.Error())
			}
			return nil, nil, 0, err
		}

		return &wr.WriteRequest, func() { mimirpb.ReuseSlice(wr.Timeseries) }, size, nil
	}

	req := newRequest(supplier)
	// Only checked against the inflight bytes limit when the request wasn't already accounted for by the inflight
	// limiter before being read.
	req.contentLength = int64(proto.Size(in))
	req.onInit = func() {
		req.nameValidationSchemeOverride = schemeOverride
	}

	pushErr := r.push(ctx, req)
	if pushErr == nil {
		if otlpErr := otlpConverter.Err(); otlpErr != nil {
			// The push succeeded, but the converter left out some samples: tell the client.
			return nil, grpcStatusError(codes.InvalidArgument, otlpErr.Error())
		}
		return &colmetricpb.ExportMetricsServiceResponse{}, nil
	}

	if errors.Is(pushErr, context.Canceled) {
		r.log(logger, level.Warn, "msg", "push request canceled", "err", pushErr)
		return nil, grpcStatusError(codes.Canceled, "push request context canceled")
	}
	if labelValueTooLongErr := (labelValueTooLongError{}); errors.As(pushErr, &labelValueTooLongErr) {
		pushErr = newValidationError(otelAttributeValueTooLongError{labelValueTooLongErr})
	}

	var (
		grpcCode codes.Code
		msg      string
	)
	if st, ok := grpcutil.ErrorToStatus(pushErr); ok {
		grpcCode, msg = st.Code(), st.Message()
		if util.IsHTTPStatusCode(grpcCode) {
			grpcCode = httpStatusToOTLPGRPCCode(int(grpcCode))
		}
	} else {
		var distributorErr Error
		if errors.As(pushErr, &distributorErr) && distributorErr.Cause() == mimirpb.ERROR_CAUSE_REPLICAS_DID_NOT_MATCH {
			// Samples from non-elected HA replicas are expected to be dropped. The HTTP receiver answers 202 Accepted,
			// so report success rather than a failure that would make the client log errors and retry.
			return &colmetricpb.ExportMetricsServiceResponse{}, nil
		}

		var (
			isSoft   bool
			httpCode int
		)
		grpcCode, httpCode, isSoft = toOtlpGRPCHTTPStatus(pushErr)
		if httpCode/100 == 5 {
			// Transient failures can be classified as Internal (e.g. unknown errors or deadline exceeded), which OTLP
			// clients don't retry. Use the same retryable classification as the HTTP receiver.
			grpcCode = httpStatusToOTLPGRPCCode(httpCode)
		}
		if isSoft {
			return &colmetricpb.ExportMetricsServiceResponse{
				PartialSuccess: &colmetricpb.ExportMetricsPartialSuccess{
					RejectedDataPoints: rejectedDataPointsFromPushError(pushErr),
					ErrorMessage:       validUTF8Message(pushErr.Error()),
				},
			}, nil
		}
		msg = pushErr.Error()
	}

	logLevel := level.Error
	if grpcCode == codes.InvalidArgument || grpcCode == codes.FailedPrecondition || grpcCode == codes.ResourceExhausted {
		logLevel = level.Warn
	}
	r.log(logger, logLevel, "msg", "detected an error while ingesting OTLP metrics request (the request may have been partially ingested)", "grpcCode", grpcCode, "err", pushErr)
	return nil, r.errorWithRetryInfo(ctx, grpcCode, msg)
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

// rejectedDataPointsFromPushError extracts the number of rejected samples from a soft push error.
func rejectedDataPointsFromPushError(pushErr error) int64 {
	var ingPushErr ingesterPushError
	var asErr activeSeriesLimitedError
	var vErr validationError
	switch {
	case errors.As(pushErr, &ingPushErr):
		return ingPushErr.rejectedSamples
	case errors.As(pushErr, &asErr):
		return asErr.rejectedSamples
	case errors.As(pushErr, &vErr):
		return vErr.rejectedSamples
	}
	return 0
}
