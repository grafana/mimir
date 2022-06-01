// SPDX-License-Identifier: AGPL-3.0-only

package push

import (
	"context"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"

	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func requestContextAndLogger(r *http.Request, sourceIPs *middleware.SourceIPExtractor) (context.Context, log.Logger) {
	ctx := r.Context()
	logger := util_log.WithContext(ctx, util_log.Logger)
	if sourceIPs != nil {
		source := sourceIPs.Get(r)
		if source != "" {
			ctx = util.AddSourceIPsToOutgoingContext(ctx, source)
			logger = util_log.WithSourceIPs(source, logger)
		}
	}
	return ctx, logger
}

func handlePushError(w http.ResponseWriter, err error, logger log.Logger) {
	resp, ok := httpgrpc.HTTPResponseFromError(err)
	if !ok {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if resp.GetCode() != 202 {
		level.Error(logger).Log("msg", "push error", "err", err)
	}
	http.Error(w, string(resp.Body), int(resp.Code))
}
