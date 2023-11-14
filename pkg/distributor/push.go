// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/push/push.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/httpgrpc/server"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

// PushFunc defines the type of the push. It is similar to http.HandlerFunc.
type PushFunc func(ctx context.Context, req *Request) error

// parserFunc defines how to read the body the request from an HTTP request
type parserFunc func(ctx context.Context, r *http.Request, maxSize int, buffer []byte, req *mimirpb.PreallocWriteRequest, logger log.Logger) ([]byte, error)

// Wrap a slice in a struct so we can store a pointer in sync.Pool
type bufHolder struct {
	buf []byte
}

var bufferPool = sync.Pool{
	New: func() interface{} { return &bufHolder{buf: make([]byte, 256*1024)} },
}

const (
	SkipLabelNameValidationHeader = "X-Mimir-SkipLabelNameValidation"
	statusClientClosedRequest     = 499
)

// Handler is a http.Handler which accepts WriteRequests.
func Handler(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	limits *validation.Overrides,
	push PushFunc,
	logger log.Logger,
) http.Handler {
	return handler(maxRecvMsgSize, sourceIPs, allowSkipLabelNameValidation, limits, push, logger, func(ctx context.Context, r *http.Request, maxRecvMsgSize int, dst []byte, req *mimirpb.PreallocWriteRequest, _ log.Logger) ([]byte, error) {
		res, err := util.ParseProtoReader(ctx, r.Body, int(r.ContentLength), maxRecvMsgSize, dst, req, util.RawSnappy)
		if errors.Is(err, util.MsgSizeTooLargeErr{}) {
			err = distributorMaxWriteMessageSizeErr{actual: int(r.ContentLength), limit: maxRecvMsgSize}
		}
		return res, err
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
	sourceIPs *middleware.SourceIPExtractor,
	allowSkipLabelNameValidation bool,
	limits *validation.Overrides,
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
				ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
				logger = utillog.WithSourceIPs(source, logger)
			}
		}
		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			bufHolder := bufferPool.Get().(*bufHolder)
			var req mimirpb.PreallocWriteRequest
			buf, err := parser(ctx, r, maxRecvMsgSize, bufHolder.buf, &req, logger)
			if err != nil {
				// Check for httpgrpc error, default to client error if parsing failed
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Errorf(http.StatusBadRequest, err.Error())
				}

				bufferPool.Put(bufHolder)
				return nil, nil, err
			}
			// If decoding allocated a bigger buffer, put that one back in the pool.
			if buf = buf[:cap(buf)]; len(buf) > len(bufHolder.buf) {
				bufHolder.buf = buf
			}

			if allowSkipLabelNameValidation {
				req.SkipLabelNameValidation = req.SkipLabelNameValidation && r.Header.Get(SkipLabelNameValidationHeader) == "true"
			} else {
				req.SkipLabelNameValidation = false
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				bufferPool.Put(bufHolder)
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
				code, msg = toHTTPStatus(ctx, err, limits), err.Error()
			}
			if code != 202 {
				level.Error(logger).Log("msg", "push error", "err", err)
			}
			addHeaders(w, err)
			http.Error(w, msg, code)
		}
	})
}

// toHTTPStatus converts the given error into an appropriate HTTP status corresponding
// to that error, if the error is one of the errors from this package. Otherwise, an
// http.StatusInternalServerError is returned.
func toHTTPStatus(ctx context.Context, pushErr error, limits *validation.Overrides) int {
	if errors.Is(pushErr, context.DeadlineExceeded) {
		return http.StatusInternalServerError
	}

	var distributorErr distributorError
	if errors.As(pushErr, &distributorErr) {
		switch distributorErr.errorCause() {
		case mimirpb.BAD_DATA:
			return http.StatusBadRequest
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
				return StatusServiceOverloaded
			}
			return http.StatusTooManyRequests
		case mimirpb.REPLICAS_DID_NOT_MATCH:
			return http.StatusAccepted
		case mimirpb.TOO_MANY_CLUSTERS:
			return http.StatusBadRequest
		case mimirpb.TSDB_UNAVAILABLE:
			return http.StatusServiceUnavailable
		}
	}

	return http.StatusInternalServerError
}

func addHeaders(w http.ResponseWriter, err error) {
	var doNotLogError middleware.DoNotLogError
	if errors.As(err, &doNotLogError) {
		w.Header().Set(server.DoNotLogErrorHeaderKey, "true")
	}
}
