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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"

	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/distributor/influxpush"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	utillog "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func parser(ctx context.Context, r *http.Request, maxSize int, buffers *util.RequestBuffers, req *mimirpb.PreallocWriteRequest, logger log.Logger) error {
	spanLogger, ctx := spanlogger.NewWithLogger(ctx, logger, "Distributor.InfluxHandler.decodeAndConvert")
	defer spanLogger.Span.Finish()

	spanLogger.SetTag("content_type", r.Header.Get("Content-Type"))
	spanLogger.SetTag("content_encoding", r.Header.Get("Content-Encoding"))
	spanLogger.SetTag("content_length", r.ContentLength)

	ts, err, _ := influxpush.ParseInfluxLineReader(ctx, r, maxSize)
	level.Debug(spanLogger).Log("msg", "decoding complete, starting conversion")
	if err != nil {
		level.Error(logger).Log("err", err.Error())
		// TODO(alexg): need to pass on the http.StatusBadRequest
		// http.Error(w, err.Error(), http.StatusBadRequest)
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
	reg prometheus.Registerer,
	logger log.Logger,
) http.Handler {
	//TODO(alexg): mirror otel.go implementation where we do decoding here rather than in parser() func?
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
				// TODO(alexg): Do we even need httpgrpc here?
				// Check for httpgrpc error, default to client error if parsing failed
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
		if err := push(ctx, req); err != nil {
			if errors.Is(err, context.Canceled) {
				level.Warn(logger).Log("msg", "push request canceled", "err", err)
				writeErrorToHTTPResponseBody(r.Context(), w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
				return
			}
			// TODO(alexg): If error is from parser() we need to return http.StatusBadRequest
			// TODO(alexg): Do we even need httpgrpc here?
			var (
				httpCode int
				grpcCode codes.Code
				errorMsg string
			)
			if st, ok := grpcutil.ErrorToStatus(err); ok {
				// TODO(alexg): needed? If so we need to map correct codes here not borrow OTLP funcs
				// This code is needed for a correct handling of errors returned by the supplier function.
				// These errors are created by using the httpgrpc package.
				httpCode = httpRetryableToOTLPRetryable(int(st.Code()))
				grpcCode = st.Code()
				errorMsg = st.Message()
			} else {
				// TODO(alexg): needed? If so we need to map correct codes here not borrow OTLP funcs
				grpcCode, httpCode = toOtlpGRPCHTTPStatus(err)
				errorMsg = err.Error()
			}
			if httpCode != 202 {
				// This error message is consistent with error message in Prometheus remote-write handler, and ingester's ingest-storage pushToStorage method.
				msgs := []interface{}{"msg", "detected an error while ingesting Influx metrics request (the request may have been partially ingested)", "httpCode", httpCode, "err", err}
				if httpCode/100 == 4 {
					// TODO(alexg): what is this?
					msgs = append(msgs, "insight", true)
				}
				level.Error(logger).Log(msgs...)
			}
			addHeaders(w, err, r, httpCode, retryCfg)
			writeErrorToHTTPResponseBody(r.Context(), w, httpCode, grpcCode, errorMsg, logger)
		} else {
			w.WriteHeader(http.StatusNoContent) // Needed for Telegraf, otherwise it tries to marshal JSON and considers the write a failure.
		}
	})
}
