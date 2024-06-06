// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// PushFunc defines the type of the push. It is similar to http.HandlerFunc.
type PushFunc func(ctx context.Context, req *Request) error

// parserFunc defines how to read the body the request from an HTTP request. It takes an optional RequestBuffers.
type parserFunc func(ctx context.Context, r *http.Request, maxSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error

var (
	errRetryBaseLessThanOneSecond    = errors.New("retry base duration should not be less than 1 second")
	errNonPositiveMaxBackoffExponent = errors.New("max backoff exponent should be a positive value")
)

const (
	SkipLabelNameValidationHeader = "X-Mimir-SkipLabelNameValidation"
	statusClientClosedRequest     = 499
)

type RetryConfig struct {
	Enabled            bool `yaml:"enabled" category:"experimental"`
	BaseSeconds        int  `yaml:"base_seconds" category:"experimental"`
	MaxBackoffExponent int  `yaml:"max_backoff_exponent" category:"experimental"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *RetryConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "distributor.retry-after-header.enabled", false, "Enabled controls inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it.")
	f.IntVar(&cfg.BaseSeconds, "distributor.retry-after-header.base-seconds", 3, "Base duration in seconds for calculating the Retry-After header in responses to 429/5xx errors.")
	f.IntVar(&cfg.MaxBackoffExponent, "distributor.retry-after-header.max-backoff-exponent", 5, "Sets the upper limit on the number of Retry-Attempt considered for calculation. It caps the Retry-Attempt header without rejecting additional attempts, controlling exponential backoff calculations. For example, when the base-seconds is set to 3 and max-backoff-exponent to 5, the maximum retry duration would be 3 * 2^5 = 96 seconds.")
}

func (cfg *RetryConfig) Validate() error {
	if cfg.BaseSeconds < 1 {
		return errRetryBaseLessThanOneSecond
	}
	if cfg.MaxBackoffExponent < 1 {
		return errNonPositiveMaxBackoffExponent
	}
	return nil
}

// Handler is a http.Handler which accepts WriteRequests.
func Handler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	logger log.Logger,
) http.Handler {
	return handler(maxRecvMsgSize, requestBufferPool, sourceIPs, allowSkipLabelNameValidation, limits, retryCfg, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, _ log.Logger) error {
		protoBodySize, err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, buffers, req, util.RawSnappy)
		if errors.Is(err, util.MsgSizeTooLargeErr{}) {
			err = distributorMaxWriteMessageSizeErr{actual: int(r.ContentLength), limit: maxRecvMsgSize}
		}
		if err != nil {
			return err
		}
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}
		pushMetrics.ObserveUncompressedBodySize(tenantID, float64(protoBodySize))

		return nil
	})
}

type distributorMaxWriteMessageSizeErr struct {
	actual, limit int
}

func (e distributorMaxWriteMessageSizeErr) Error() string {
	msgSizeDesc := fmt.Sprintf(" of %d bytes", e.actual)
	if e.actual < 0 {
		msgSizeDesc = ""
	}
	return globalerror.DistributorMaxWriteMessageSize.MessageWithPerInstanceLimitConfig(fmt.Sprintf("the incoming push request has been rejected because its message size%s is larger than the allowed limit of %d bytes", msgSizeDesc, e.limit), "distributor.max-recv-msg-size")
}

func handler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	push PushFunc,
	logger log.Logger,
	parser parserFunc,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				logger = utillog.WithSourceIPs(source, logger)
			}
		}
		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest
			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
				}

				rb.CleanUp()
				return nil, nil, err
			}

			if allowSkipLabelNameValidation {
				req.SkipLabelNameValidation = req.SkipLabelNameValidation && r.Header.Get(SkipLabelNameValidationHeader) == "true"
			} else {
				req.SkipLabelNameValidation = false
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				rb.CleanUp()
			}
			return &req.WriteRequest, cleanup, nil
		}
		req := newRequest(supplier)
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				http.Error(w, err.Error(), statusClientClosedRequest)
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				return
			}
			var (
				code int
				msg  string
			)
			if resp, ok := httpgrpc.HTTPResponseFromError(err); ok {
				code, msg = int(resp.Code), string(resp.Body)
			} else {
				_, code = toGRPCHTTPStatus(ctx, err, limits)
				msg = err.Error()
			}
			if code != 202 {
				// This error message is consistent with error message in OTLP handler, and ingester's ingest-storage pushToStorage method.
				msgs := []interface{}{"msg", "detected an error while ingesting Prometheus remote-write request (the request may have been partially ingested)", "httpCode", code, "err", err}
				if code/100 == 4 {
					msgs = append(msgs, "insight", true)
				}
				level.Error(logger).Log(msgs...)
			}
			addHeaders(w, err, r, code, retryCfg)
			http.Error(w, msg, code)
		}
	})
}

func otlpHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	push PushFunc,
	logger log.Logger,
	parser parserFunc,
) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				logger = utillog.WithSourceIPs(source, logger)
			}
		}
		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest
			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
				}

				rb.CleanUp()
				return nil, nil, err
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				rb.CleanUp()
			}
			return &req.WriteRequest, cleanup, nil
		}
		req := newRequest(supplier)
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				writeErrorToHTTPResponseBody(w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
				return
			}
			var (
				httpCode int
				grpcCode codes.Code
				errorMsg string
			)
			if resp, ok := httpgrpc.HTTPResponseFromError(err); ok {
				// here the error would always be nil, since it is already checked in httpgrpc.HTTPResponseFromError
				s, _ := grpcutil.ErrorToStatus(err)
				httpCode = int(resp.Code)
				grpcCode = s.Code() // this will be the same as httpCode.
				errorMsg = string(resp.Body)
			} else {
				grpcCode, httpCode = toGRPCHTTPStatus(ctx, err, limits)
				errorMsg = err.Error()
			}
			if httpCode != 202 {
				// This error message is consistent with error message in Prometheus remote-write handler, and ingester's ingest-storage pushToStorage method.
				msgs := []interface{}{"msg", "detected an error while ingesting OTLP metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", err}
				if httpCode/100 == 4 {
					msgs = append(msgs, "insight", true)
				}
				level.Error(logger).Log(msgs...)
			}
			addHeaders(w, err, r, httpCode, retryCfg)
			writeErrorToHTTPResponseBody(w, httpCode, grpcCode, errorMsg, logger)
		}
	})
}

// writeErrorToHTTPResponseBody converts the given error into a grpc status and marshals it into a byte slice, in order to be written to the response body.
// See doc https://opentelemetry.io/docs/specs/otlp/#failures-1
func writeErrorToHTTPResponseBody(w http.ResponseWriter, httpCode int, grpcCode codes.Code, msg string, logger log.Logger) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(httpCode)

	respBytes, err := proto.Marshal(grpcstatus.New(grpcCode, msg).Proto())
	if err != nil {
		level.Error(logger).Log("msg", "otlp response marshal failed", "err", err)
		writeResponseFailedBody, _ := proto.Marshal(grpcstatus.New(codes.Internal, "failed to marshal OTLP response").Proto())
		_, _ = w.Write(writeResponseFailedBody)
		return
	}

	_, err = w.Write(respBytes)
	if err != nil {
		level.Error(logger).Log("msg", "write error to otlp response failed", "err", err)
	}
}

func calculateRetryAfter(retryAttemptHeader string, baseSeconds int, maxBackoffExponent int) string {
	retryAttempt, err := strconv.Atoi(retryAttemptHeader)
	// If retry-attempt is not valid, set it to default 1
	if err != nil || retryAttempt < 1 {
		retryAttempt = 1
	}
	if retryAttempt > maxBackoffExponent {
		retryAttempt = maxBackoffExponent
	}
	var minSeconds, maxSeconds int64
	minSeconds = int64(baseSeconds) << (retryAttempt - 1)
	maxSeconds = int64(minSeconds) << 1

	delaySeconds := minSeconds + rand.Int63n(maxSeconds-minSeconds)

	return strconv.FormatInt(delaySeconds, 10)
}

// toGRPCHTTPStatus converts the given error into an appropriate GRPC and HTTP status corresponding
// to that error, if the error is one of the errors from this package. Otherwise, codes.Internal and
// http.StatusInternalServerError is returned.
func toGRPCHTTPStatus(ctx context.Context, pushErr error, limits *validation.Overrides) (codes.Code, int) {
	if errors.Is(pushErr, context.DeadlineExceeded) {
		return codes.Internal, http.StatusInternalServerError
	}

	var distributorErr Error
	if errors.As(pushErr, &distributorErr) {
		switch distributorErr.Cause() {
		case mimirpb.BAD_DATA:
			return codes.InvalidArgument, http.StatusBadRequest
		case mimirpb.INGESTION_RATE_LIMITED, mimirpb.REQUEST_RATE_LIMITED:
			serviceOverloadErrorEnabled := false
			userID, err := tenant.TenantID(ctx)
			if err == nil {
				serviceOverloadErrorEnabled = limits.ServiceOverloadStatusCodeOnRateLimitEnabled(userID)
			}
			// Return a 429 or a 529 here depending on configuration to tell the client it is going too fast.
			// Client may discard the data or slow down and re-send.
			// Prometheus v2.26 added a remote-write option 'retry_on_http_429'.
			if serviceOverloadErrorEnabled {
				return codes.ResourceExhausted, StatusServiceOverloaded
			}
			return codes.ResourceExhausted, http.StatusTooManyRequests
		case mimirpb.REPLICAS_DID_NOT_MATCH:
			return codes.OK, http.StatusAccepted
		case mimirpb.TOO_MANY_CLUSTERS:
			return codes.InvalidArgument, http.StatusBadRequest
		case mimirpb.TSDB_UNAVAILABLE:
			return codes.Unavailable, http.StatusServiceUnavailable
		case mimirpb.CIRCUIT_BREAKER_OPEN:
			return codes.Unavailable, http.StatusServiceUnavailable
		case mimirpb.METHOD_NOT_ALLOWED:
			// Return a 501 (and not 405) to explicitly signal a misconfiguration and to possibly track that amongst other 5xx errors.
			return codes.Unimplemented, http.StatusNotImplemented
		}
	}

	return codes.Internal, http.StatusInternalServerError
}

func addHeaders(w http.ResponseWriter, err error, r *http.Request, responseCode int, retryCfg RetryConfig) {
	var doNotLogError middleware.DoNotLogError
	if errors.As(err, &doNotLogError) {
		w.Header().Set(server.DoNotLogErrorHeaderKey, "true")
	}

	if responseCode == http.StatusTooManyRequests || responseCode/100 == 5 {
		if retryCfg.Enabled {
			retryAttemptHeader := r.Header.Get("Retry-Attempt")
			retrySeconds := calculateRetryAfter(retryAttemptHeader, retryCfg.BaseSeconds, retryCfg.MaxBackoffExponent)
			w.Header().Set("Retry-After", retrySeconds)
			if sp := opentracing.SpanFromContext(r.Context()); sp != nil {
				sp.SetTag("retry-after", retrySeconds)
				sp.SetTag("retry-attempt", retryAttemptHeader)
			}
		}
	}
}
