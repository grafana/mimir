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
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	promRemote "github.com/prometheus/prometheus/storage/remote"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// PushFunc defines the type of the push. It is similar to http.HandlerFunc.
type PushFunc func(ctx context.Context, req *Request) error

// The PushFunc might store promRemote.WriteResponseStats in the context.
type pushResponseStatsContextMarker struct{}

var (
	pushResponseStatsContextKey = &pushResponseStatsContextMarker{}
)

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
	newRequestBuffers func() *util.RequestBuffers,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	allowSkipLabelCountValidation bool,
	limits *validation.Overrides,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	logger log.Logger,
) http.Handler {
	return handler(maxRecvMsgSize, newRequestBuffers, sourceIPs, allowSkipLabelNameValidation, allowSkipLabelCountValidation, limits, retryCfg, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, _ log.Logger) error {
		protoBodySize, err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, buffers, req, util.RawSnappy)
		if e := (util.MsgSizeTooLargeErr{}); errors.As(err, &e) {
			err = distributorMaxWriteMessageSizeErr{compressed: e.Compressed, actual: e.Actual, limit: e.Limit}
		}
		if err != nil {
			return err
		}
		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			return err
		}
		pushMetrics.ObserveRequestBodySize(tenantID, "push", int64(protoBodySize), r.ContentLength)

		return nil
	})
}

type distributorMaxWriteMessageSizeErr struct {
	compressed, actual, limit int
}

func (e distributorMaxWriteMessageSizeErr) Error() string {
	msgSizeDesc := ""
	if e.actual > 0 {
		msgSizeDesc = fmt.Sprintf(" of %d bytes (uncompressed)", e.actual)
	} else if e.compressed > 0 {
		msgSizeDesc = fmt.Sprintf(" of %d bytes (compressed)", e.compressed)
	}
	return globalerror.DistributorMaxWriteMessageSize.MessageWithPerInstanceLimitConfig(fmt.Sprintf("the incoming push request has been rejected because its message size%s is larger than the allowed limit of %d bytes", msgSizeDesc, e.limit), "distributor.max-recv-msg-size")
}

type distributorMaxOTLPRequestSizeErr struct {
	compressed, actual, limit int
}

func (e distributorMaxOTLPRequestSizeErr) Error() string {
	msgSizeDesc := ""
	if e.actual > 0 {
		msgSizeDesc = fmt.Sprintf(" of %d bytes (uncompressed)", e.actual)
	} else if e.compressed > 0 {
		msgSizeDesc = fmt.Sprintf(" of %d bytes (compressed)", e.compressed)
	}
	return globalerror.DistributorMaxOTLPRequestSize.MessageWithPerInstanceLimitConfig(fmt.Sprintf("the incoming OTLP request has been rejected because its message size%s is larger than the allowed limit of %d bytes", msgSizeDesc, e.limit), maxOTLPRequestSizeFlag)
}

func handler(
	maxRecvMsgSize int,
	newRequestBuffers func() *util.RequestBuffers,
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

		isRW2, err := isRemoteWrite2(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			var rb *util.RequestBuffers
			if newRequestBuffers != nil {
				rb = newRequestBuffers()
			} else {
				rb = util.NewRequestBuffers(nil)
			}
			var req mimirpb.PreallocWriteRequest

			req.UnmarshalFromRW2 = isRW2

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
		req.contentLength = r.ContentLength
		if isRW2 {
			ctx = contextWithWriteResponseStats(ctx)
		}
		err = push(ctx, req)
		if isRW2 {
			if err := addWriteResponseStats(ctx, w); err != nil {
				// Should not happen, but we should not panic anyway.
				level.Error(logger).Log("msg", "error write response stats not found in context", "err", err)
			}
		}
		if err == nil {
			addSuccessHeaders(w, req.artificialDelay)
		} else {
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
				code = toHTTPStatus(err)
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
			addErrorHeaders(w, err, r, code, retryCfg)
			http.Error(w, validUTF8Message(msg), code)
		}
	})
}

func isRemoteWrite2(r *http.Request) (bool, error) {
	const appProtoContentType = "application/x-protobuf"

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		// If the content type is not set, we assume it is remote write v1.
		return false, nil
	}
	parts := strings.Split(contentType, ";")
	if parts[0] != appProtoContentType {
		// We didn't use to check the content type so if someone wrote for
		// example text/plain here, we'll accept and assume it is v1.
		return false, nil
	}

	// Parse potential https://www.rfc-editor.org/rfc/rfc9110#parameter
	for _, p := range parts[1:] {
		pair := strings.Split(p, "=")
		if len(pair) != 2 {
			return false, fmt.Errorf("as per https://www.rfc-editor.org/rfc/rfc9110#parameter expected parameters to be key-values, got %v in %v content-type", p, contentType)
		}
		if pair[0] == "proto" {
			switch pair[1] {
			case "prometheus.WriteRequest":
				return false, nil
			case "io.prometheus.write.v2.Request":
				return true, nil
			default:
				return false, fmt.Errorf("got %v content type; expected prometheus.WriteRequest or io.prometheus.write.v2.Request", contentType)
			}
		}
	}
	// No "proto=" parameter, assuming v1.
	return false, nil
}

// Consts from https://github.com/prometheus/prometheus/blob/main/storage/remote/stats.go
const (
	rw20WrittenSamplesHeader    = "X-Prometheus-Remote-Write-Samples-Written"
	rw20WrittenHistogramsHeader = "X-Prometheus-Remote-Write-Histograms-Written"
	rw20WrittenExemplarsHeader  = "X-Prometheus-Remote-Write-Exemplars-Written"
)

func contextWithWriteResponseStats(ctx context.Context) context.Context {
	return context.WithValue(ctx, pushResponseStatsContextKey, &promRemote.WriteResponseStats{})
}

func addWriteResponseStats(ctx context.Context, w http.ResponseWriter) error {
	rsValue := ctx.Value(pushResponseStatsContextKey)
	if rsValue == nil {
		return errors.New("remote write response stats not found in context")
	}
	prs, ok := rsValue.(*promRemote.WriteResponseStats)
	if !ok {
		return errors.New("remote write response stats not of type *promRemote.WriteResponseStats")
	}
	headers := w.Header()
	headers.Set(rw20WrittenSamplesHeader, strconv.Itoa(prs.Samples))
	headers.Set(rw20WrittenHistogramsHeader, strconv.Itoa(prs.Histograms))
	headers.Set(rw20WrittenExemplarsHeader, strconv.Itoa(prs.Exemplars))
	return nil
}

func updateWriteResponseStatsCtx(ctx context.Context, samples, histograms, exemplars int) {
	prs := ctx.Value(pushResponseStatsContextKey)
	if prs == nil {
		// Only present for RW2.0.
		return
	}
	prs.(*promRemote.WriteResponseStats).Samples += samples
	prs.(*promRemote.WriteResponseStats).Histograms += histograms
	prs.(*promRemote.WriteResponseStats).Exemplars += exemplars
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
		randomJitter := float64(rand.Int63n(jitterAmount*2+1) - jitterAmount)
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
func toHTTPStatus(pushErr error) int {
	if errors.Is(pushErr, context.DeadlineExceeded) {
		return http.StatusInternalServerError
	}

	var distributorErr Error
	if errors.As(pushErr, &distributorErr) {
		return errorCauseToHTTPStatusCode(distributorErr.Cause())
	}

	return http.StatusInternalServerError
}

func addSuccessHeaders(w http.ResponseWriter, delay time.Duration) {
	if delay >= 0 {
		durationInMs := strconv.FormatFloat(float64(delay)/float64(time.Millisecond), 'f', -1, 64)
		w.Header().Add("Server-Timing", fmt.Sprintf("artificial_delay;dur=%s", durationInMs))
	}
}

func addErrorHeaders(w http.ResponseWriter, err error, r *http.Request, responseCode int, retryCfg RetryConfig) {
	var doNotLogError middleware.DoNotLogError
	if errors.As(err, &doNotLogError) {
		w.Header().Set(server.DoNotLogErrorHeaderKey, "true")
	}

	if responseCode == http.StatusTooManyRequests || responseCode/100 == 5 {
		if retryCfg.Enabled {
			retryAttemptHeader := r.Header.Get("Retry-Attempt")
			retrySeconds := calculateRetryAfter(retryAttemptHeader, retryCfg.MinBackoff, retryCfg.MaxBackoff)
			w.Header().Set("Retry-After", retrySeconds)
			sp := trace.SpanFromContext(r.Context())
			sp.SetAttributes(
				attribute.String("retry-after", retrySeconds),
				attribute.String("retry-attempt", retryAttemptHeader),
			)
		}
	}
}
