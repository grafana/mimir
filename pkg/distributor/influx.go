// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"net/http"

	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/middleware"

	"github.com/grafana/mimir/pkg/mimirpb"
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
		ctx, logger := requestContextAndLogger(r, sourceIPs)

		ts, err := parseInfluxLineReader(r.Context(), r, maxRecvMsgSize)
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

		if _, err := push(ctx, rwReq, func() {}); err != nil {
			handlePushError(w, err, logger)
		}

		w.WriteHeader(http.StatusNoContent) // Needed for Telegraf, otherwise it tries to marshal JSON and considers the write a failure.
	})
}
