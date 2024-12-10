// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/middleware"

        "google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/distributor/influxpush"
        "github.com/grafana/mimir/pkg/util"
        utillog "github.com/grafana/mimir/pkg/util/log"
)

// InfluxHandler is a http.Handler which accepts Influx Line protocol and converts it to WriteRequests.
func InfluxHandler(
	maxRecvMsgSize int,
	sourceIPs *middleware.SourceIPExtractor,
        retryCfg RetryConfig,
        push PushFunc,
        pushMetrics *PushMetrics,
        reg prometheus.Registerer,
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

		ts, err, _ := influxpush.ParseInfluxLineReader(r.Context(), r, maxRecvMsgSize)
		if err != nil {
			level.Error(logger).Log("err", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Sigh, a write API optimisation needs me to jump through hoops.
		pts := make([]mimirpb.PreallocTimeseries, 0, len(ts))
		for i := range ts {
			pts = append(pts, mimirpb.PreallocTimeseries{
				TimeSeries: &ts[i],
			})
		}
		rwReq := &mimirpb.WriteRequest{
			Timeseries: pts,
		}

		// TODO(alexg): supplier type code from otel.go?
                supplier := func() (*mimirpb.WriteRequest, func(), error) {
                        rb := util.NewRequestBuffers(requestBufferPool)
                        var req mimirpb.PreallocWriteRequest

                        if err := parser(ctx, r, maxRecvMsgSize, rb, &req, logger); err != nil {
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
		if err := push(ctx, rwReq); err != nil {
                        if errors.Is(err, context.Canceled) {
                                level.Warn(logger).Log("msg", "push request canceled", "err", err)
                                writeErrorToHTTPResponseBody(r.Context(), w, statusClientClosedRequest, codes.Canceled, "push request context canceled", logger)
                                return
                        }
		}

		w.WriteHeader(http.StatusNoContent) // Needed for Telegraf, otherwise it tries to marshal JSON and considers the write a failure.
	})
}
