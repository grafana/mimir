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
	influxio "github.com/influxdata/influxdb/v2/kit/io"

	"github.com/grafana/mimir/pkg/distributor/influxpush"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func influxRequestParser(ctx context.Context, r *http.Request, maxSize int, _ *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) (int, error) {
	spanLogger, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.InfluxHandler.decodeAndConvert")
	defer spanLogger.Span.Finish()

	spanLogger.SetTag("content_type", r.Header.Get("Content-Type"))
	spanLogger.SetTag("content_encoding", r.Header.Get("Content-Encoding"))
	spanLogger.SetTag("content_length", r.ContentLength)

	pts, bytesRead, err := influxpush.ParseInfluxLineReader(ctx, r, maxSize)
	level.Debug(spanLogger).Log("msg", "decodeAndConvert complete", "bytesRead", bytesRead)
	if err != nil {
		level.Error(logger).Log("msg", "failed to parse Influx push request", "err", err)
		return bytesRead, err
	}

	level.Debug(spanLogger).Log(
		"msg", "Influx to Prometheus conversion complete",
		"metric_count", len(pts),
	)

	req.Timeseries = pts
	return bytesRead, nil
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
			level.Warn(logger).Log("msg", "unable to obtain tenantID", "err", err)
			return
		}

		pushMetrics.IncInfluxRequest(tenantID)

		var bytesRead int

		supplier := func() (*mimirpb.WriteRequest, func(), error) {
			rb := util.NewRequestBuffers(requestBufferPool)
			var req mimirpb.PreallocWriteRequest

			if bytesRead, err = influxRequestParser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
				err = httpgrpc.Error(http.StatusBadRequest, err.Error())
				rb.CleanUp()
				return nil, nil, err
			}

			cleanup := func() {
				mimirpb.ReuseSlice(req.Timeseries)
				rb.CleanUp()
			}
			return &req.WriteRequest, cleanup, nil
		}

		pushMetrics.ObserveInfluxUncompressedBodySize(tenantID, float64(bytesRead))

		req := newRequest(supplier)
		// https://docs.influxdata.com/influxdb/cloud/api/v2/#tag/Response-codes
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				w.WriteHeader(statusClientClosedRequest)
				return
			}
			if errors.Is(err, influxio.ErrReadLimitExceeded) {
				// TODO(alexg): One thing we have seen in the past is that telegraf clients send a batch of data
				// if it is too big they should respond to the 413 below, but if a client doesn't understand this
				// it just sends the next batch that is even bigger. In the past this has had to be dealt with by
				// adding rate limits to drop the payloads.
				level.Warn(logger).Log("msg", "request too large", "err", err, "bytesRead", bytesRead, "maxMsgSize", maxRecvMsgSize)
				w.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			// From: https://github.com/grafana/influx2cortex/blob/main/pkg/influx/errors.go

			var httpCode int
			var errorMsg string

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
				msgs := []interface{}{"msg", "detected an error while ingesting Influx metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", err}
				if httpCode/100 == 4 {
					// This tag makes the error message visible for our Grafana Cloud customers.
					msgs = append(msgs, "insight", true)
				}
				level.Error(logger).Log(msgs...)
			}
			if httpCode < 500 {
				level.Info(logger).Log("msg", errorMsg, "response_code", httpCode, "err", err)
			} else {
				level.Warn(logger).Log("msg", errorMsg, "response_code", httpCode, "err", err)
			}
			addHeaders(w, err, r, httpCode, retryCfg)
			w.WriteHeader(httpCode)
		} else {
			w.WriteHeader(http.StatusNoContent) // Needed for Telegraf, otherwise it tries to marshal JSON and considers the write a failure.
		}
	})
}
