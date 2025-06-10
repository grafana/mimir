// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/retry.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querymiddleware

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/httpgrpc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type retryOperation[T any] func() (T, error)

func doWithRetries[T any](ctx context.Context, log log.Logger, maxRetries int, metrics prometheus.Observer, operation retryOperation[T]) (T, error) {
	tries := 0
	defer func() { metrics.Observe(float64(tries)) }()

	var lastErr error
	var zero T
	for ; tries < maxRetries; tries++ {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		resp, err := operation()
		if err == nil {
			return resp, nil
		}

		if !apierror.IsRetryableAPIError(err) || errors.Is(err, context.Canceled) {
			return zero, err
		}

		// Retry if we get a HTTP 500 or a non-HTTP error.
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		if !ok || httpResp.Code/100 == 5 {
			lastErr = err
			log := util_log.WithContext(ctx, spanlogger.FromContext(ctx, log))
			level.Error(log).Log("msg", "error processing request", "try", tries, "err", err)
			continue
		}

		return zero, err
	}
	return zero, lastErr
}

type retryMetrics struct {
	retriesCount prometheus.Histogram
}

func newRetryMetrics(registerer prometheus.Registerer) prometheus.Observer {
	return &retryMetrics{
		retriesCount: promauto.With(registerer).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_query_frontend_retries",
			Help:    "Number of times a request is retried.",
			Buckets: []float64{0, 1, 2, 3, 4, 5},
		}),
	}
}

func (m *retryMetrics) Observe(v float64) {
	m.retriesCount.Observe(v)
}

type retry struct {
	log        log.Logger
	next       MetricsQueryHandler
	maxRetries int

	metrics prometheus.Observer
}

// newRetryMiddleware returns a middleware that retries requests if they
// fail with 500 or a non-HTTP error.
func newRetryMiddleware(log log.Logger, maxRetries int, metrics prometheus.Observer) MetricsQueryMiddleware {
	if metrics == nil {
		metrics = newRetryMetrics(nil)
	}

	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return retry{
			log:        log,
			next:       next,
			maxRetries: maxRetries,
			metrics:    metrics,
		}
	})
}

func (r retry) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	return doWithRetries(ctx, r.log, r.maxRetries, r.metrics, func() (Response, error) {
		return r.next.Do(ctx, req)
	})
}

type retryRoundTripper struct {
	next       http.RoundTripper
	log        log.Logger
	maxRetries int

	metrics prometheus.Observer
}

func newRetryRoundTripper(next http.RoundTripper, log log.Logger, maxRetries int, metrics prometheus.Observer) http.RoundTripper {
	if metrics == nil {
		metrics = newRetryMetrics(nil)
	}

	return retryRoundTripper{
		next:       next,
		log:        log,
		maxRetries: maxRetries,
		metrics:    metrics,
	}
}

func (r retryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return doWithRetries(req.Context(), r.log, r.maxRetries, r.metrics, func() (*http.Response, error) {
		return r.next.RoundTrip(req)
	})
}
