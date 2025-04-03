// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/tenant"
	influxio "github.com/influxdata/influxdb/v2/kit/io"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"

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
	level.Debug(spanLogger).Log("msg", "decodeAndConvert complete", "bytesRead", bytesRead, "metric_count", len(pts), "err", err)
	if err != nil {
		level.Error(logger).Log("msg", "failed to parse Influx push request", "err", err)
		return bytesRead, err
	}

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

// TimeseriesToInfluxRequest is used in tests.
func TimeseriesToInfluxRequest(timeseries []prompb.TimeSeries) string {
	var retBuffer bytes.Buffer

	for _, ts := range timeseries {
		name := ""
		others := make([]string, 0, 10)

		for _, l := range ts.Labels {
			if l.Name == model.MetricNameLabel {
				name = l.Value
				continue
			}
			if l.Name != "__proxy_source__" {
				others = append(others, l.Name+"="+l.Value)
			}
		}

		// We are going to assume that the __name__ value is the measurement name
		// and does not have a field name suffix (e.g. measurement,t1=v1 f1=1.5" -> "measurement_f1")
		// as we can't work out whether it was "measurement_f1 value=3" or "measurement f1=3" from the
		// created series.
		line := name
		if len(others) > 0 {
			line += "," + strings.Join(others, ",")
		}

		if len(ts.Samples) > 0 {
			// We only take the first sample
			// data: "measurement,t1=v1 value=1.5 1465839830100400200",
			line += fmt.Sprintf(" value=%f %d", ts.Samples[0].Value, ts.Samples[0].Timestamp*time.Millisecond.Nanoseconds())
		}
		retBuffer.WriteString(line)
		retBuffer.WriteString("\n")
	}

	return retBuffer.String()
}
