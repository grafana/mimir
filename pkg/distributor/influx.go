// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	io2 "github.com/influxdata/influxdb/v2/kit/io"

	"github.com/grafana/mimir/pkg/distributor/influxpush"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func parser(ctx context.Context, r *http.Request, maxSize int, _ *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error {
	spanLogger, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.InfluxHandler.decodeAndConvert")
	defer spanLogger.Span.Finish()

	spanLogger.SetTag("content_type", r.Header.Get("Content-Type"))
	spanLogger.SetTag("content_encoding", r.Header.Get("Content-Encoding"))
	spanLogger.SetTag("content_length", r.ContentLength)

	// TODO(alexg): need to get bytesRead back to be able to add to metrics/histogram
	ts, bytesRead, err := influxpush.ParseInfluxLineReader(ctx, r, maxSize)
	level.Debug(spanLogger).Log(
		"msg", "decodeAndConvert complete",
		"bytesRead", bytesRead,
	)
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		return err
	}

	// Sigh, a write API optimisation needs me to jump through hoops.
	pts := make([]mimirpb.PreallocTimeseries, 0, len(ts))
	for i := range ts {
		pts = append(pts, mimirpb.PreallocTimeseries{
			TimeSeries: &ts[i],
		})
	}

	level.Debug(spanLogger).Log(
		"msg", "Influx to Prometheus conversion complete",
		"metric_count", len(ts),
	)

	req.Timeseries = pts
	return nil
}

// InfluxHandler is a http.Handler which accepts Influx Line protocol and converts it to WriteRequests.
func InfluxHandler(
	maxRecvMsgSize int,
	requestBufferPool util.Pool,
	sourceIPs *middleware.SourceIPExtractor,
	retryCfg RetryConfig,
	push PushFunc,
	pushMetrics *PushMetrics,
	logger log.Logger,
) http.Handler {
	// TODO(alexg): mirror otel.go implementation where we do decoding here rather than in parser() func?
	// We may need to do this to get bytesRead of HTTP request to add to a histogram like pushMetrics.ObserveUncompressedBodySize() for otel,
	// currently only available in Influx's parser()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		logger := utillog.WithContext(ctx, logger)
		if sourceIPs != nil {
			source := sourceIPs.Get(r)
			if source != "" {
				logger = utillog.WithSourceIPs(source, logger)
			}
		}

		tenantID, err := tenant.TenantID(ctx)
		if err != nil {
			level.Warn(logger).Log("msg", "unable to obtain tenantID", "err", tryUnwrap(err))
			return
		}

		pushMetrics.IncOTLPRequest(tenantID)

		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest

			if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				// Check for httpgrpc error, default to client error if parsing failed.
				if _, ok := httpgrpc.HTTPResponseFromError(err); !ok {
					err = httpgrpc.Error(http.StatusBadRequest, err.Error())
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
		// https://docs.influxdata.com/influxdb/cloud/api/v2/#tag/Response-codes
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				w.WriteHeader(statusClientClosedRequest)
				return
			}
			if errors.Is(err, io2.ErrReadLimitExceeded) {
				// TODO(alexg): One thing we have seen in the past is that telegraf clients send a batch of data
				// if it is too big they should respond to the 413 below, but if a client doesn't understand this
				// it just sends the next batch that is even bigger. In the past this has had to be dealt with by
				// adding rate limits to drop the payloads.
				level.Warn(logger).Log("msg", "request too large", "err", err)
				// TODO(alexg): max size and bytes received in error?
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			// From: https://github.com/grafana/influx2cortex/blob/main/pkg/influx/errors.go

			var (
				httpCode int
				errorMsg string
			)
			if st, ok := grpcutil.ErrorToStatus(err); ok {
				// This code is needed for a correct handling of errors returned by the supplier function.
				// These errors are created by using the httpgrpc package.
				httpCode = int(st.Code())
				errorMsg = st.Message()
			} else {
				var distributorErr Error
				errorMsg = err.Error()
				if errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &distributorErr) {
					httpCode = http.StatusServiceUnavailable
				} else {
					httpCode = errorCauseToHTTPStatusCode(distributorErr.Cause(), false)
				}
			}
			if httpCode != 202 {
				// This error message is consistent with error message in Prometheus remote-write handler, and ingester's ingest-storage pushToStorage method.
				level.Error(logger).Log("msg", "detected an error while ingesting Influx metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", err)
			}
			if httpCode < 500 {
				level.Info(logger).Log("msg", errorMsg, "response_code", httpCode, "err", tryUnwrap(err))
			} else if httpCode >= 500 {
				level.Warn(logger).Log("msg", errorMsg, "response_code", httpCode, "err", tryUnwrap(err))
			}
			addHeaders(w, err, r, httpCode, retryCfg)
			w.WriteHeader(httpCode)
		} else {
			w.WriteHeader(http.StatusNoContent) // Needed for Telegraf, otherwise it tries to marshal JSON and considers the write a failure.
		}
	})
}

// Imported from: https://github.com/grafana/influx2cortex/blob/main/pkg/influx/errors.go

func tryUnwrap(err error) error {
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return wrapped.Unwrap()
	}
	return err
}
