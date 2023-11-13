// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/retry.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type retryMiddlewareMetrics struct {
	retriesCount prometheus.Histogram
}

func newRetryMiddlewareMetrics(registerer prometheus.Registerer) prometheus.Observer {
	return &retryMiddlewareMetrics{
		retriesCount: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Namespace: "cortex",
			Name:      "query_frontend_retries",
			Help:      "Number of times a request is retried.",
			Buckets:   []float64{0, 1, 2, 3, 4, 5},
		}),
	}
}

func (m *retryMiddlewareMetrics) Observe(v float64) {
	m.retriesCount.Observe(v)
}

type retry struct {
	log               log.Logger
	next              Handler
	maxRetries        int
	notRunningBackoff time.Duration

	metrics prometheus.Observer
}

// newRetryMiddleware returns a middleware that retries requests if they
// fail with 500 or a non-HTTP error.
func newRetryMiddleware(log log.Logger, maxRetries int, notRunningBackoff time.Duration, metrics prometheus.Observer) Middleware {
	if metrics == nil {
		metrics = newRetryMiddlewareMetrics(nil)
	}

	return MiddlewareFunc(func(next Handler) Handler {
		return retry{
			log:               log,
			next:              next,
			maxRetries:        maxRetries,
			notRunningBackoff: notRunningBackoff,
			metrics:           metrics,
		}
	})
}

func (r retry) Do(ctx context.Context, req Request) (Response, error) {
	tries := 0
	defer func() { r.metrics.Observe(float64(tries)) }()

	var lastErr error
	for ; tries < r.maxRetries; tries++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		resp, err := r.next.Do(ctx, req)
		if err == nil {
			return resp, nil
		}

		if r.isTerminalError(err) {
			return nil, err
		}
		// Retry if we get a HTTP 500 or a non-HTTP error.
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		if !ok || httpResp.Code/100 == 5 {
			lastErr = err
			log := util_log.WithContext(ctx, spanlogger.FromContext(ctx, r.log))
			level.Error(log).Log("msg", "error processing request", "try", tries, "err", err)

			if errors.Is(err, util.NotRunningError{}) && tries < r.maxRetries-1 {
				// We're still starting up. Wait a little until retrying.
				time.Sleep(r.notRunningBackoff)
			}

			continue
		}

		return nil, err
	}
	return nil, lastErr
}

func (r retry) isTerminalError(err error) bool {
	if apierror.IsNonRetryableAPIError(err) {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	notRunningError := util.NotRunningError{}
	if errors.As(err, &notRunningError) {
		return notRunningError.State != services.New && notRunningError.State != services.Starting
	}

	return false
}
