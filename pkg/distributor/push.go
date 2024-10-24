// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

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
	errNonPositiveMinBackoffDuration = errors.New("min-backoff should be greater than or equal to 1s")
	errNonPositiveMaxBackoffDuration = errors.New("max-backoff should be greater than or equal to 1s")
)

const (
	SkipLabelNameValidationHeader  = "X-Mimir-SkipLabelNameValidation"
	SkipLabelCountValidationHeader = "X-Mimir-SkipLabelCountValidation"

	statusClientClosedRequest = 499
)

type RetryConfig struct {
	Enabled    bool          `yaml:"enabled" category:"advanced"`
	MinBackoff time.Duration `yaml:"min_backoff" category:"advanced"`
	MaxBackoff time.Duration `yaml:"max_backoff" category:"advanced"`
}

// RegisterFlags adds the flags required to config this to the given FlagSet.
func (cfg *RetryConfig) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "distributor.retry-after-header.enabled", true, "Enables inclusion of the Retry-After header in the response: true includes it for client retry guidance, false omits it.")
	f.DurationVar(&cfg.MinBackoff, "distributor.retry-after-header.min-backoff", 6*time.Second, "Minimum duration of the Retry-After HTTP header in responses to 429/5xx errors. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header.")
	f.DurationVar(&cfg.MaxBackoff, "distributor.retry-after-header.max-backoff", 96*time.Second, "Minimum duration of the Retry-After HTTP header in responses to 429/5xx errors. Must be greater than or equal to 1s. Backoff is calculated as MinBackoff*2^(RetryAttempt-1) seconds with random jitter of 50% in either direction. RetryAttempt is the value of the Retry-Attempt HTTP header.")
}

func (cfg *RetryConfig) Validate() error {
	if cfg.MinBackoff < time.Second {
		return errNonPositiveMinBackoffDuration
	}
	if cfg.MaxBackoff < time.Second {
		return errNonPositiveMaxBackoffDuration
	}
	return nil
}

// Handler is a http.Handler which accepts WriteRequests.
func Handler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	allowSkipLabelCountValidation bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	logger log.Logger,
) http.Handler {
	return handler(maxRecvMsgSize, requestBufferPool, sourceIPs, allowSkipLabelNameValidation, allowSkipLabelCountValidation, limits, retryCfg, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, _ log.Logger) error {
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

type distributorMaxOTLPRequestSizeErr struct {
	actual, limit int
}

func (e distributorMaxOTLPRequestSizeErr) Error() string {
	msgSizeDesc := fmt.Sprintf(" of %d bytes", e.actual)
	if e.actual < 0 {
		msgSizeDesc = ""
	}
	return globalerror.DistributorMaxOTLPRequestSize.MessageWithPerInstanceLimitConfig(fmt.Sprintf("the incoming OTLP request has been rejected because its message size%s is larger than the allowed limit of %d bytes", msgSizeDesc, e.limit), maxOTLPRequestSizeFlag)
}

func handler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	allowSkipLabelCountValidation bool,
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

			userID, err := tenant.TenantID(ctx)
			if err != nil && !errors.Is(err, user.ErrNoOrgID) { // ignore user.ErrNoOrgID
				return nil, nil, errors.Wrap(err, "failed to get tenant ID")
			}

			// userID might be empty if none was in the ctx, in this case just use the default setting.
			if limits.MaxGlobalExemplarsPerUser(userID) == 0 {
				// The user is not allowed to send exemplars, so there is no need to unmarshal them.
				// Optimization to avoid the allocations required for unmarshaling exemplars.
				req.SkipUnmarshalingExemplars = true
			}

			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Error(http.StatusBadRequest, err.Error())
				}

				rb.CleanUp()
				return nil, nil, err
			}

			if allowSkipLabelNameValidation {
				req.SkipLabelValidation = req.SkipLabelValidation && r.Header.Get(SkipLabelNameValidationHeader) == "true"
			} else {
				req.SkipLabelValidation = false
			}

			if allowSkipLabelCountValidation {
				req.SkipLabelCountValidation = req.SkipLabelCountValidation && r.Header.Get(SkipLabelCountValidationHeader) == "true"
			} else {
				req.SkipLabelCountValidation = false
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
				// This code is needed for a correct handling of errors returned by the supplier function.
				// These errors are created by using the httpgrpc package.
				code, msg = int(resp.Code), string(resp.Body)
			} else {
				code = toHTTPStatus(ctx, err, limits)
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

func calculateRetryAfter(retryAttemptHeader string, minBackoff, maxBackoff time.Duration) string {
	const jitterFactor = 0.5

	retryAttempt, err := strconv.Atoi(retryAttemptHeader)
	// If retry-attempt is not valid, set it to default 1
	if err != nil || retryAttempt < 1 {
		retryAttempt = 1
	}

	delaySeconds := minBackoff.Seconds() * math.Pow(2.0, float64(retryAttempt-1))
	delaySeconds = min(maxBackoff.Seconds(), delaySeconds)
	if jitterAmount := int64(delaySeconds * jitterFactor); jitterAmount > 0 {
		// The random jitter can be negative too, so we generate a 2x greater the random number and subtract the jitter.
		randomJitter := float64(rand.Int63n(jitterAmount*2+1) - jitterAmount) // #nosec G404 -- request jitter does not require a CSPRNG
		delaySeconds += randomJitter
	}
	// Jitter might have pushed the delaySeconds over maxBackoff or minBackoff, so we need to clamp it again.
	delaySeconds = min(maxBackoff.Seconds(), delaySeconds)
	delaySeconds = max(minBackoff.Seconds(), delaySeconds)

	return strconv.FormatInt(int64(delaySeconds), 10)
}

// toHTTPStatus converts the given error into an appropriate HTTP status corresponding
// to that error, if the error is one of the errors from this package. Otherwise, an
// http.StatusInternalServerError is returned.
func toHTTPStatus(ctx context.Context, pushErr error, limits *validation.Overrides) int {
	if errors.Is(pushErr, context.DeadlineExceeded) {
		return http.StatusInternalServerError
	}

	var distributorErr Error
	if errors.As(pushErr, &distributorErr) {
		serviceOverloadErrorEnabled := false
		userID, err := tenant.TenantID(ctx)
		if err == nil {
			serviceOverloadErrorEnabled = limits.ServiceOverloadStatusCodeOnRateLimitEnabled(userID)
		}
		return errorCauseToHTTPStatusCode(distributorErr.Cause(), serviceOverloadErrorEnabled)
	}

	return http.StatusInternalServerError
}

func addHeaders(w http.ResponseWriter, err error, r *http.Request, responseCode int, retryCfg RetryConfig) {
	var doNotLogError middleware.DoNotLogError
	if errors.As(err, &doNotLogError) {
		w.Header().Set(server.DoNotLogErrorHeaderKey, "true")
	}

	if responseCode == http.StatusTooManyRequests || responseCode/100 == 5 {
		if retryCfg.Enabled {
			retryAttemptHeader := r.Header.Get("Retry-Attempt")
			retrySeconds := calculateRetryAfter(retryAttemptHeader, retryCfg.MinBackoff, retryCfg.MaxBackoff)
			w.Header().Set("Retry-After", retrySeconds)
			if sp := opentracing.SpanFromContext(r.Context()); sp != nil {
				sp.SetTag("retry-after", retrySeconds)
				sp.SetTag("retry-attempt", retryAttemptHeader)
			}
		}
	}
}
